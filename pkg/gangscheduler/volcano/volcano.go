/*
Copyright 2023 Hailiang Zhao.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package volcano

import (
	"context"
	"fmt"
	"github.com/hliangzhao/torch-on-k8s/apis"
	trainv1alpha1 "github.com/hliangzhao/torch-on-k8s/apis/train/v1alpha1"
	"github.com/hliangzhao/torch-on-k8s/pkg/features"
	"github.com/hliangzhao/torch-on-k8s/pkg/gangscheduler"
	"github.com/hliangzhao/torch-on-k8s/pkg/utils"
	"github.com/hliangzhao/torch-on-k8s/pkg/utils/resources"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	quotav1 "k8s.io/apiserver/pkg/quota/v1"
	"k8s.io/klog"
	"k8s.io/utils/pointer"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"strings"
	volcanoapisv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
)

func init() {
	// Add volcano CRD (`volcanoapisv1beta1.PodGroup`) to runtime scheme such that the reflector
	// can identify `volcano.PodGroup`.
	apis.AddToSchemes = append(apis.AddToSchemes, volcanoapisv1beta1.AddToScheme)
}

func NewVolcano(mgr manager.Manager) gangscheduler.GangScheduler {
	return &volcano{client: mgr.GetClient()}
}

var _ gangscheduler.GangScheduler = &volcano{}

type volcano struct {
	client client.Client
}

// CreatePodGroup creates a list of podgroups for the given job and its tasks based on the given minMember and schedulingPolicy.
func (v *volcano) CreatePodGroup(job metav1.Object, tasks map[trainv1alpha1.TaskType]*trainv1alpha1.TaskSpec,
	minMembers map[trainv1alpha1.TaskType]*int32, schedulingPolicy *trainv1alpha1.SchedulingPolicy) (runtime.Object, error) {

	accessor, err := meta.TypeAccessor(job)
	if err != nil {
		return nil, err
	}

	var (
		apiVersion                   = accessor.GetAPIVersion()
		kind                         = accessor.GetKind()
		queueName, priorityClassName string
		podgroups                    *volcanoapisv1beta1.PodGroupList
	)

	if schedulingPolicy != nil {
		queueName = schedulingPolicy.Queue
		priorityClassName = schedulingPolicy.PriorityClassName
	}

	// create podgroup(s) with different granularity
	if features.FeatureGates.Enabled(features.DAGScheduling) {
		podgroups, err = v.generatePodGroupsByRole(apiVersion, kind, job.GetName(), job.GetNamespace(), job.GetUID(), tasks, minMembers)
		if err != nil {
			return nil, err
		}
	} else {
		podgroups = v.generatePodGroupsByJob(apiVersion, kind, job.GetName(), job.GetNamespace(), job.GetUID(), tasks, schedulingPolicy)
	}

	// create these podgroups resource in cluster if necessary
	for i := range podgroups.Items {
		pg := &podgroups.Items[i]
		pg.Spec.Queue = queueName
		pg.Spec.PriorityClassName = priorityClassName
		err = v.client.Get(context.Background(), types.NamespacedName{Name: pg.Name, Namespace: pg.Namespace}, &volcanoapisv1beta1.PodGroup{})
		if err != nil {
			if errors.IsNotFound(err) {
				err = v.client.Create(context.Background(), pg)
			}
			return nil, err
		}
	}

	return podgroups, nil
}

// generatePodGroupsByRole creates a list of podgroups, each for a task type.
func (v *volcano) generatePodGroupsByRole(apiVersion, kind, jobName, namespace string, uid types.UID,
	tasks map[trainv1alpha1.TaskType]*trainv1alpha1.TaskSpec,
	minMembers map[trainv1alpha1.TaskType]*int32) (*volcanoapisv1beta1.PodGroupList, error) {

	pgs := volcanoapisv1beta1.PodGroupList{Items: make([]volcanoapisv1beta1.PodGroup, 0, len(tasks))}

	for tt, ts := range tasks {
		// aimaster is scheduled by the default kube-scheduler, just skip it
		if tt == trainv1alpha1.TaskTypeAIMaster {
			continue
		}
		rt := strings.ToLower(string(tt))
		podgroupName := fmt.Sprintf("%s-%s", jobName, rt)

		var (
			minMember            int32
			taskResourceRequests corev1.ResourceList
		)
		// Actually, this check on minMember is not required because we have
		// already set the minMembers as default when creating the job.
		minMemberPtr, ok := minMembers[tt]
		if !ok {
			minMember = *ts.NumTasks
			taskResourceRequests = resources.TaskResourceRequests(ts)
		} else {
			if *minMemberPtr > *ts.NumTasks {
				return nil, fmt.Errorf("the mimMember provided for task type %v is larger than NumTasks, "+
					"minMember provided: %d, NumTasks: %d", tt, *minMemberPtr, *ts.NumTasks)
			}
			minMember = *minMemberPtr
			taskResourceRequests = resources.MinTaskResourceRequests(ts, minMember)
		}

		// create the podgroup for this task type
		pgs.Items = append(pgs.Items, volcanoapisv1beta1.PodGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      podgroupName,
				Namespace: namespace,
				// these labels are enough to detect the corresponding job of this podgroup
				Labels: map[string]string{
					trainv1alpha1.LabelGangSchedulingJobName: jobName,
					trainv1alpha1.LabelTaskType:              rt,
				},
				// the podgroup is controlled by the corresponding job
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion:         apiVersion,
						Kind:               kind,
						Name:               jobName,
						UID:                uid,
						Controller:         pointer.BoolPtr(true),
						BlockOwnerDeletion: pointer.BoolPtr(true),
					},
				},
			},
			Spec: volcanoapisv1beta1.PodGroupSpec{
				MinMember:    minMember,
				MinResources: &taskResourceRequests,
			},
		})
	}

	return &pgs, nil
}

// generatePodGroupsByJob creates one podgroup for the whole job.
func (v *volcano) generatePodGroupsByJob(apiVersion, kind, name, namespace string, uid types.UID,
	tasks map[trainv1alpha1.TaskType]*trainv1alpha1.TaskSpec,
	schedPolicy *trainv1alpha1.SchedulingPolicy) *volcanoapisv1beta1.PodGroupList {

	pg := volcanoapisv1beta1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			// since we create only one podgroup for the whole job, the label below is enough to detect the corresponding job
			Labels: map[string]string{
				trainv1alpha1.LabelGangSchedulingJobName: name,
			},
			// the podgroup is controlled by the corresponding job
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion:         apiVersion,
					Kind:               kind,
					Name:               name,
					UID:                uid,
					Controller:         pointer.BoolPtr(true),
					BlockOwnerDeletion: pointer.BoolPtr(true),
				},
			},
		},
		Spec: volcanoapisv1beta1.PodGroupSpec{
			// NOTE: When DAG scheduling is not enabled, all tasks (pods) should be successfully scheduled.
			// This is because the gang scheduler does not differentiate the task types. If we do not set
			// the MinMember as the number of all tasks, it's possible for the gang scheduler to set the job as
			// Running even without successful running of Master task.
			MinMember: utils.GetTotalTasks(tasks),
		},
	}

	jobResource, _ := resources.JobResourceRequests(tasks)
	// aimaster is scheduled by the default kube-scheduler, here we subtract NumTasks and resource requests of them
	if masterTask := tasks[trainv1alpha1.TaskTypeAIMaster]; masterTask != nil && masterTask.NumTasks != nil {
		if *masterTask.NumTasks > 0 {
			pg.Spec.MinMember -= *masterTask.NumTasks
			jobResource = quotav1.SubtractWithNonNegativeResult(jobResource,
				resources.Multiply(int64(*masterTask.NumTasks), resources.TaskResourceRequests(masterTask)))
		}
	}

	// If MinAvailable is set in scheduling policy, use this value as the MinMember for the podgroup
	if schedPolicy != nil && schedPolicy.MinAvailable != nil && *schedPolicy.MinAvailable > 0 {
		pg.Spec.MinMember = *schedPolicy.MinAvailable
	}

	// TODO: This is buggy when schedPolicy.MinAvailable is set but MinResources is not updated!
	//  Here jobResource is the total resource requests of the job (by subtracting aimaster).
	//  To fix this, we need to add a new field named `MinResources` for `trainv1alpha1.SchedulingPolicy`.
	//  Will try to fix this later.
	pg.Spec.MinResources = &jobResource

	return &volcanoapisv1beta1.PodGroupList{Items: []volcanoapisv1beta1.PodGroup{pg}}
}

// BindPodToPodGroup binds the correct podgroup for the given pod of some job task.
// Steps:
// (1) Create the selector for the given job task.
// (2) Select the correct podgroup from the list that matches the job task.
// (3) Set the podgroup as the controller of the pod by appending the metav1.OwnerReference.
// (4) Add a new annotation to the pod spec such that the podgroup can be quickly find.
func (v *volcano) BindPodToPodGroup(job metav1.Object, podSpec *corev1.PodTemplateSpec, podgroups runtime.Object, taskType string) error {
	// aimaster of the job is scheduled by the default kube-scheduler, do nothing
	if taskType == strings.ToLower(string(trainv1alpha1.TaskTypeAIMaster)) {
		podSpec.Spec.SchedulerName = "default-scheduler"
		return nil
	}

	podGroups, ok := podgroups.(*volcanoapisv1beta1.PodGroupList)
	if !ok {
		klog.Warningf("object cannot convert to podgroup list, Object: %+v", podgroups)
		return nil
	}
	if len(podGroups.Items) == 0 {
		return fmt.Errorf("unexpected empty podgrpoup list, job name: %s", job.GetName())
	}

	// set selector according to podgroup creation types
	podGroupName := job.GetName()
	matchLabels := map[string]string{trainv1alpha1.LabelGangSchedulingJobName: job.GetName()}
	if features.FeatureGates.Enabled(features.DAGScheduling) {
		matchLabels[trainv1alpha1.LabelTaskType] = taskType
		// note that podGroupName is different when the podgroups are generated with different granularity
		podGroupName = fmt.Sprintf("%s-%s", job.GetName(), taskType)
	}
	selector := labels.SelectorFromSet(matchLabels)

	for i := range podGroups.Items {
		pg := &podGroups.Items[i]
		if pg.Labels != nil && selector.Matches(labels.Set(pg.Labels)) {
			// append the pg as the controller of the pod
			appendOwnerReference(podSpec, metav1.OwnerReference{
				APIVersion:         pg.APIVersion,
				Kind:               pg.Kind,
				Name:               pg.Name,
				UID:                pg.UID,
				Controller:         pointer.BoolPtr(false),
				BlockOwnerDeletion: pointer.BoolPtr(true),
			})
			podGroupName = pg.Name
			break
		}
	}

	if podSpec.Annotations == nil {
		podSpec.Annotations = map[string]string{}
	}
	podSpec.Annotations[volcanoapisv1beta1.KubeGroupNameAnnotationKey] = podGroupName

	return nil
}

func appendOwnerReference(obj metav1.Object, newRef metav1.OwnerReference) {
	ownerRef := obj.GetOwnerReferences()
	for _, ref := range ownerRef {
		if ownerReferenceEquals(ref, newRef) {
			return
		}
	}
	ownerRef = append(ownerRef, newRef)
	obj.SetOwnerReferences(ownerRef)
}

func ownerReferenceEquals(ref1, ref2 metav1.OwnerReference) bool {
	if ref1.APIVersion != ref2.APIVersion || ref1.Kind != ref2.Kind ||
		ref1.UID != ref2.UID {
		return false
	}
	return true
}

// GetPodGroup returns the list of podgroups for the given job.
func (v *volcano) GetPodGroup(jobName types.NamespacedName) (client.ObjectList, error) {
	podgroups := &volcanoapisv1beta1.PodGroupList{}
	if err := v.client.List(context.Background(), podgroups, client.MatchingLabels{
		trainv1alpha1.LabelGangSchedulingJobName: jobName.Name,
	}, client.InNamespace(jobName.Namespace)); err != nil {
		return nil, err
	}
	return podgroups, nil
}

// DeletePodGroup deletes the podgroups of the job.
func (v *volcano) DeletePodGroup(jobName types.NamespacedName) error {
	pgs, err := v.GetPodGroup(jobName)
	if err != nil {
		return err
	}
	podGroups := pgs.(*volcanoapisv1beta1.PodGroupList)

	for i := range podGroups.Items {
		pg := &podGroups.Items[i]
		if err = v.client.Delete(context.Background(), pg); err != nil {
			return err
		}
	}
	return err
}

func (v *volcano) SchedulerName() string {
	return "volcano"
}
