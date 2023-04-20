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
	commonapis "github.com/hliangzhao/torch-on-k8s/pkg/common/apis/v1alpha1"
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
	// Add volcano CRD (`PodGroup`) to runtime scheme such that the reflector
	// can identify `volcano.PodGroup`.
	apis.AddToSchemes = append(apis.AddToSchemes, volcanoapisv1beta1.AddToScheme)
}

func NewVolcano(manager manager.Manager) gangscheduler.GangScheduler {
	return &volcano{client: manager.GetClient()}
}

var _ gangscheduler.GangScheduler = &volcano{}

type volcano struct {
	client client.Client
}

func (v *volcano) CreatePodGroup(job metav1.Object, tasks map[commonapis.TaskType]*commonapis.TaskSpec,
	schedulingPolicy *commonapis.SchedulingPolicy) (runtime.Object, error) {

	accessor, err := meta.TypeAccessor(job)
	if err != nil {
		return nil, err
	}

	var (
		apiVersion                   = accessor.GetAPIVersion()
		kind                         = accessor.GetKind()
		queueName, priorityClassName string
		podGroups                    *volcanoapisv1beta1.PodGroupList
	)

	if schedulingPolicy != nil {
		queueName = schedulingPolicy.Queue
		priorityClassName = schedulingPolicy.PriorityClassName
	}

	// Create podgroup(s) with different granularity
	if features.FeatureGates.Enabled(features.DAGScheduling) {
		podGroups = v.generateGangByRole(apiVersion, kind, job.GetName(), job.GetNamespace(), job.GetUID(), tasks)
	} else {
		podGroups = v.generateGangByJob(apiVersion, kind, job.GetName(), job.GetNamespace(), job.GetUID(), tasks, schedulingPolicy)
	}

	// create these podgroups resource in cluster
	for i := range podGroups.Items {
		pg := &podGroups.Items[i]
		pg.Spec.Queue = queueName
		pg.Spec.PriorityClassName = priorityClassName
		err = v.client.Get(context.Background(), types.NamespacedName{Name: pg.Name, Namespace: pg.Namespace}, &volcanoapisv1beta1.PodGroup{})
		if err != nil && errors.IsNotFound(err) {
			err = v.client.Create(context.Background(), pg)
		}
		if err != nil {
			return nil, err
		}
	}

	return podGroups, err
}

// generateGangByRole creates a list of podgroups, each for a task.
func (v *volcano) generateGangByRole(apiVersion, kind, name, namespace string, uid types.UID,
	tasks map[commonapis.TaskType]*commonapis.TaskSpec) *volcanoapisv1beta1.PodGroupList {

	pgs := volcanoapisv1beta1.PodGroupList{Items: make([]volcanoapisv1beta1.PodGroup, 0, len(tasks))}

	for tt, ts := range tasks {
		// aimaster is scheduled by the default-scheduler, just skip it
		if tt == commonapis.TaskTypeAIMaster {
			continue
		}
		rt := strings.ToLower(string(tt))
		gangName := fmt.Sprintf("%s-%s", name, rt)
		taskResourceRequests := resources.TaskResourceRequests(ts)
		pgs.Items = append(pgs.Items, volcanoapisv1beta1.PodGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      gangName,
				Namespace: namespace,
				Labels: map[string]string{
					commonapis.LabelGangSchedulingJobName: name,
					commonapis.LabelTaskType:              rt,
				},
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
				MinMember:    *ts.NumTasks,
				MinResources: &taskResourceRequests,
			},
		})
	}

	return &pgs
}

// generateGangByJob creates one podgroup for the whole job.
func (v *volcano) generateGangByJob(apiVersion, kind, name, namespace string, uid types.UID,
	tasks map[commonapis.TaskType]*commonapis.TaskSpec,
	schedPolicy *commonapis.SchedulingPolicy) *volcanoapisv1beta1.PodGroupList {

	pg := volcanoapisv1beta1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				commonapis.LabelGangSchedulingJobName: name,
			},
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
		Spec: volcanoapisv1beta1.PodGroupSpec{MinMember: utils.GetTotalTasks(tasks)},
	}
	jobResource, _ := resources.JobResourceRequests(tasks)

	// aimaster is scheduled by the default-scheduler, just skip it
	if masterTask := tasks[commonapis.TaskTypeAIMaster]; masterTask != nil && masterTask.NumTasks != nil {
		if *masterTask.NumTasks > 0 {
			pg.Spec.MinMember -= *masterTask.NumTasks
			jobResource = quotav1.SubtractWithNonNegativeResult(jobResource,
				resources.Multiply(int64(*masterTask.NumTasks), resources.TaskResourceRequests(masterTask)))
		}
	}

	if schedPolicy != nil && schedPolicy.MinAvailable != nil && *schedPolicy.MinAvailable > 0 {
		pg.Spec.MinMember = *schedPolicy.MinAvailable
	}
	pg.Spec.MinResources = &jobResource
	return &volcanoapisv1beta1.PodGroupList{Items: []volcanoapisv1beta1.PodGroup{pg}}
}

func (v *volcano) BindPodToPodGroup(job metav1.Object, podSpec *corev1.PodTemplateSpec, podgroups runtime.Object, taskType string) error {
	// aimaster of the job is scheduled by the default scheduler, but the tasks of the job are scheduled by the gang scheduler
	if taskType == strings.ToLower(string(commonapis.TaskTypeAIMaster)) {
		podSpec.Spec.SchedulerName = "default-scheduler"
		return nil
	}

	podGroups, ok := podgroups.(*volcanoapisv1beta1.PodGroupList)
	if !ok {
		klog.Warningf("object cannot convert to podgroup entity list, entity: %+v", podgroups)
		return nil
	}
	if len(podGroups.Items) == 0 {
		return fmt.Errorf("unexpected empty podgrpoup entity list, job name: %s", job.GetName())
	}

	// set selector according to podgroup creation types
	podGroupName := job.GetName()
	matchLabels := map[string]string{commonapis.LabelGangSchedulingJobName: job.GetName()}
	if features.FeatureGates.Enabled(features.DAGScheduling) {
		matchLabels[commonapis.LabelTaskType] = taskType
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

func (v *volcano) GetPodGroup(jobName types.NamespacedName) (client.ObjectList, error) {
	podGroups := &volcanoapisv1beta1.PodGroupList{}
	if err := v.client.List(context.Background(), podGroups, client.MatchingLabels{
		commonapis.LabelGangSchedulingJobName: jobName.Name,
	}, client.InNamespace(jobName.Namespace)); err != nil {
		return nil, err
	}
	return podGroups, nil
}

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

func (v *volcano) PluginName() string {
	return "volcano"
}

func (v *volcano) SchedulerName() string {
	return "volcano"
}
