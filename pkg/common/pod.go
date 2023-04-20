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

package common

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/golang/glog"
	commonapis "github.com/hliangzhao/torch-on-k8s/pkg/common/apis/v1alpha1"
	"github.com/hliangzhao/torch-on-k8s/pkg/utils"
	patchutils "github.com/hliangzhao/torch-on-k8s/pkg/utils/patch"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/rand"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog"
	k8scontroller "k8s.io/kubernetes/pkg/controller"
	"reflect"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"strconv"
	"strings"
)

/* The PodControl structs and functions. */

const (
	// FailedCreatePodReason is added in an event and in a replica set condition
	// when a pod for a replica set is failed to be created.
	FailedCreatePodReason = "FailedCreatePod"

	// SuccessfulCreatePodReason is added in an event when a pod for a replica set
	// is successfully created.
	SuccessfulCreatePodReason = "SuccessfulCreatePod"

	// FailedDeletePodReason is added in an event and in a replica set condition
	// when a pod for a replica set is failed to be deleted.
	FailedDeletePodReason = "FailedDeletePod"

	// SuccessfulDeletePodReason is added in an event when a pod for a replica set
	// is successfully deleted.
	SuccessfulDeletePodReason = "SuccessfulDeletePod"
)

func NewPodControl(client client.Client, recorder record.EventRecorder) k8scontroller.PodControlInterface {
	return &PodControl{
		client:   client,
		recorder: recorder,
	}
}

var _ k8scontroller.PodControlInterface = &PodControl{}

type PodControl struct {
	client   client.Client
	recorder record.EventRecorder
}

// CreatePods creates new pods according to the spec, and sets job as the pod's controller.
func (pc *PodControl) CreatePods(namespace string, spec *corev1.PodTemplateSpec, job runtime.Object, controllerRef *metav1.OwnerReference) error {
	return pc.createPods("", namespace, spec, job, controllerRef)
}

// CreatePodsWithGenerateName creates new pods according to the spec, sets job as the pod's controller and sets pod's generateName.
func (pc *PodControl) CreatePodsWithGenerateName(namespace string, template *corev1.PodTemplateSpec, job runtime.Object, controllerRef *metav1.OwnerReference, generateName string) error {
	if err := validateControllerRef(controllerRef); err != nil {
		return err
	}
	template.Name = ""
	template.GenerateName = generateName
	return pc.createPods("", namespace, template, job, controllerRef)
}

// DeletePod deletes the pod identified by podID.
func (pc *PodControl) DeletePod(namespace string, name string, job runtime.Object) error {
	// fetch the pod resource from cluster
	pod := &corev1.Pod{}
	err := pc.client.Get(context.Background(), types.NamespacedName{Name: name, Namespace: namespace}, pod)
	if err != nil {
		return err
	}

	// if pod has the job finalizer, remove the finalizer first, such that the pod can be successfully deleted
	if utils.HasFinalizer(pod.Finalizers, commonapis.FinalizerPreemptProtector) {
		patch := patchutils.NewStrategicPatch()
		patch.RemoveFinalizer(commonapis.FinalizerPreemptProtector)
		if err = pc.client.Patch(context.Background(), pod, patch); err != nil {
			return err
		}
	}

	// if the pod has been marked as deleted, return directly
	if pod.DeletionTimestamp != nil {
		glog.V(3).Infof("pod %s/%s is terminating, skip deleting", pod.Namespace, pod.Name)
		return nil
	}

	// delete the pod resource from cluster and record event
	accessor, err := meta.Accessor(job)
	if err != nil {
		return fmt.Errorf("job does not have ObjectMeta, %v", err)
	}
	glog.V(2).Infof("Controller %v deleting pod %v/%v", accessor.GetName(), namespace, name)
	if err = pc.client.Delete(context.Background(), pod); err != nil {
		pc.recorder.Eventf(job, corev1.EventTypeWarning, FailedDeletePodReason, "error deleting: %v", err)
		return fmt.Errorf("unable to delete pods: %v", err)
	} else {
		pc.recorder.Eventf(job, corev1.EventTypeNormal, SuccessfulDeletePodReason, "successfully deleted pod: %v", name)
	}

	return nil
}

// PatchPod patches the pod with data.
func (pc *PodControl) PatchPod(namespace, name string, data []byte) error {
	pod := &corev1.Pod{}
	if err := pc.client.Get(context.Background(), types.NamespacedName{Name: name, Namespace: namespace}, pod); err != nil {
		return err
	}
	return pc.client.Patch(context.Background(), pod, client.RawPatch(types.StrategicMergePatchType, data))
}

func (pc *PodControl) createPods(nodeName, namespace string, spec *corev1.PodTemplateSpec, job runtime.Object, controllerRef *metav1.OwnerReference) error {
	// 1. Create pod instance with the given spec.
	// (1) get labels
	podLabels := make(labels.Set)
	for k, v := range spec.Labels {
		podLabels[k] = v
	}

	// (2) get annotations
	podAnnotations := make(labels.Set)
	for k, v := range spec.Annotations {
		podAnnotations[k] = v
	}

	// (3) get finalizers
	podFinalizers := make([]string, len(spec.Finalizers))
	copy(podFinalizers, spec.Finalizers)

	// (4) get owner references
	podOwnerReferences := make([]metav1.OwnerReference, len(spec.OwnerReferences))
	copy(podOwnerReferences, spec.OwnerReferences)

	// (5) create pod instances according to the template spec
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels:          podLabels,
			Annotations:     podAnnotations,
			Name:            spec.Name,
			GenerateName:    spec.GenerateName,
			Finalizers:      podFinalizers,
			OwnerReferences: podOwnerReferences,
		},
	}

	// (6) add controllerRef if required
	if controllerRef != nil {
		pod.OwnerReferences = append(pod.OwnerReferences, *controllerRef)
	}

	// (7) set pod spec
	pod.Spec = *spec.Spec.DeepCopy()

	// (8) set node name for assignment if specified
	if len(nodeName) != 0 {
		pod.Spec.NodeName = nodeName
	}

	// (9) set namespace
	pod.Namespace = namespace

	// (10) check labels (if empty, which means this pod is not controlled by any upstream controllers, error exists)
	if labels.Set(pod.Labels).AsSelectorPreValidated().Empty() {
		return fmt.Errorf("unable to create pods, no labels")
	}

	// 2. Create pod resource in cluster and record event.
	if err := pc.client.Create(context.Background(), pod); err != nil {
		pc.recorder.Eventf(job, corev1.EventTypeWarning, FailedCreatePodReason, "error creating: %v", err)
		return err
	} else {
		accessor, err := meta.Accessor(job)
		if err != nil {
			glog.Errorf("job does not have ObjectMeta, %v", err)
			return nil
		}
		glog.V(4).Infof("job %v created pod %v", accessor.GetName(), pod.Name)
		pc.recorder.Eventf(job, corev1.EventTypeNormal, SuccessfulCreatePodReason, "created pod: %v", pod.Name)
	}

	return nil
}

/* The pod reconcile functions of the job controller. */

const (
	// podTemplateRestartPolicyReason is the warning reason when the restart
	// policy is set in pod template.
	podTemplateRestartPolicyReason = "SettedPodTemplateRestartPolicy"

	// exitedWithCodeReason is the normal reason when the pod is exited because of the exit code.
	exitedWithCodeReason = "ExitedWithCode"
)

// OnPodCreateFunc returns ture if the creation event should be processed.
func (jc *JobController) OnPodCreateFunc(e event.CreateEvent) bool {
	// first of all, parse the event object into a pod resource
	pod := e.Object.(*corev1.Pod)
	if pod.DeletionTimestamp != nil {
		// if the pod is going to be deleted, do not create it
		if utils.HasFinalizer(pod.Finalizers, commonapis.FinalizerPreemptProtector) {
			// of course, delete the finalizer such that the pod can be successfully deleted
			patch := patchutils.NewStrategicPatch()
			patch.RemoveFinalizer(commonapis.FinalizerPreemptProtector)
			if err := jc.Client.Patch(context.Background(), pod, patch); err != nil {
				klog.Errorf("failed to remove finalizer %s, err: %v", commonapis.FinalizerPreemptProtector, err)
			}
		}
		return false
	}

	// find the behind job by controller reference, and create the corresponding creation expectation
	if controllerRef := metav1.GetControllerOf(pod); controllerRef != nil {
		job := jc.resolveControllerRef(pod.Namespace, controllerRef)
		if job == nil {
			if pod.Labels[commonapis.LabelGroupName] == jc.Controller.GetGroupName() {
				klog.Infof("the pod's job does not exist, pod name: %s", pod.Name)
			}
			return false
		}

		jobKey, err := GetJobKey(job)
		if err != nil {
			klog.Infof("Failed to get the job key: %v", err)
			return false
		}

		taskType, ok := pod.Labels[commonapis.LabelTaskType]
		if !ok {
			klog.Infof("the pod does not have job task type label, it is not created by %v, pod name: %s",
				jc.Controller.ControllerName(), pod.Name)
			return false
		}

		expectPodsKey := genExpectationPodsKey(jobKey, taskType)

		// we only need to create the expectation, the evolution to the expectation status will be handled by k8s mechanism
		jc.Expectations.CreationObserved(expectPodsKey)

		return true
	}

	return false
}

// OnPodUpdateFunc returns true if the update event should be processed.
func (jc *JobController) OnPodUpdateFunc(e event.UpdateEvent) bool {
	// first of all, parse the event objects into pod resources
	newPod := e.ObjectNew.(*corev1.Pod)
	oldPod := e.ObjectOld.(*corev1.Pod)

	// no update needs to process, just return false
	if newPod.ResourceVersion == oldPod.ResourceVersion {
		return false
	}

	newControllerRef := metav1.GetControllerOf(newPod)
	oldControllerRef := metav1.GetControllerOf(oldPod)
	controllerRefChanged := !reflect.DeepEqual(newControllerRef, oldControllerRef)

	// if the controller changed, update event should be processed
	if controllerRefChanged && oldControllerRef != nil {
		if job := jc.resolveControllerRef(oldPod.Namespace, oldControllerRef); job != nil {
			klog.Infof("pod controller ref updated: %v, %v", newPod, oldPod)
			return true
		}
	}

	// if the pod is added a new controller, update event should be processed
	if newControllerRef != nil {
		job := jc.resolveControllerRef(newPod.Namespace, newControllerRef)
		if job == nil {
			return false
		}
		klog.Infof("pod has a new controller ref: %v, %v", newPod, oldPod)
		return true
	}

	return false
}

// OnPodDeleteFunc returns true if the deletion event should be processed.
func (jc *JobController) OnPodDeleteFunc(e event.DeleteEvent) bool {
	// first of all, parse the event object into a pod resource
	pod, ok := e.Object.(*corev1.Pod)

	// remove the job finalizer such that the pod can be successfully deleted
	if utils.HasFinalizer(pod.Finalizers, commonapis.FinalizerPreemptProtector) {
		patch := patchutils.NewStrategicPatch()
		patch.RemoveFinalizer(commonapis.FinalizerPreemptProtector)
		if err := jc.Client.Patch(context.Background(), pod, patch); err != nil {
			klog.Errorf("failed to remove finalizer %s, err: %v", commonapis.FinalizerPreemptProtector, err)
		}
	}

	// the pod turns into a tombstone object
	if e.DeleteStateUnknown {
		klog.Warningf("pod %s is in delete unknown state", pod.Name)
	}

	ctrRef := metav1.GetControllerOf(pod)
	if ctrRef == nil {
		return false
	}
	job := jc.resolveControllerRef(pod.Namespace, ctrRef)
	if job == nil {
		return false
	}
	jobKey, err := GetJobKey(job)
	if err != nil {
		return false
	}
	taskType, ok := pod.Labels[commonapis.LabelTaskType]
	if !ok {
		klog.Infof("the pod does not have job task type label, it is not created by %v, pod name: %s",
			jc.Controller.ControllerName(), pod.Name)
		return false
	}
	expectationPodsKey := genExpectationPodsKey(jobKey, taskType)

	// create the delete expectation such that the deletion will be automatically detected and handled by k8s mechanism
	jc.Expectations.DeletionObserved(expectationPodsKey)

	return true
}

// ReconcilePods reconciles the pods of the given job task (identified by task key).
func (jc *JobController) ReconcilePods(ctx context.Context, job client.Object, jobStatus *commonapis.JobStatus,
	pods []*corev1.Pod, taskType commonapis.TaskType, taskSpec *commonapis.TaskSpec,
	tasks map[commonapis.TaskType]*commonapis.TaskSpec, runPolicy *commonapis.RunPolicy,
	restart *bool) error {

	// get the to-be-reconciled pods of certain task type
	tt := strings.ToLower(string(taskType))
	pods, err := jc.filterPodsForTaskType(pods, tt)
	if err != nil {
		return err
	}

	var (
		// Aggregate errors occur in reconciling loop instead of interrupting and returning directly, which
		// may cause an incorrect job status, e.g. create pod failed due to webhook forbidden and interrupt
		// reconciling, lead to a stale replicaStatus.
		errs              = newAggregatedErrors()
		failedPodContents = make(failedReasonToPods)
		numTasks          = int(*taskSpec.NumTasks)
		podSlices         = jc.getPodSlices(pods, numTasks)
		podsToFailover    = make([]*corev1.Pod, 0)
	)

	// setup context
	ctx = context.WithValue(ctx, commonapis.ContextFailedPodContents, failedPodContents)

	// initialize task status
	if jobStatus.TaskStatuses == nil {
		jobStatus.TaskStatuses = make(map[commonapis.TaskType]*commonapis.TaskStatus)
	}
	jobStatus.TaskStatuses[taskType] = &commonapis.TaskStatus{}

	for podIdx, podSlice := range podSlices {
		if len(podSlice) > 1 {
			klog.Warningf("We have too many pods for %s %d", tt, podIdx)
		} else if len(podSlice) == 0 {
			if podIdx >= numTasks {
				// this pod is going to be deleted, do nothing
				continue
			}

			klog.Infof("Need to create new pod: %s-%d", tt, podIdx)
			err = jc.createNewPod(ctx, job, tt, strconv.Itoa(podIdx), taskSpec, jc.Controller.IsMaster(tasks, taskType), runPolicy)
			if err != nil {
				// When controller tries to create a new pod but api-server returns an AlreadyExists error,
				// there may come with two case:
				// 1. another controller watched this job instance and try to create pod concurrently.
				// 2. when this job was just created, there were some pods stuck at Terminating state,
				//    and they belong to some job with same namespace/name.
				//
				// In the latter case, reconciling is interrupted and return a reconcile error, the underlying
				// work queue will requeue this request and try another round of reconciliation, however the
				// newly-arrived reconciliation just cancelled because no expectation satisfied, then no more
				// expected pods created. To fix this we generate a new expectation event when catch AlreadyExists
				// error.
				if k8serrors.IsAlreadyExists(err) {
					jobKey, keyFuncErr := GetJobKey(job)
					if keyFuncErr != nil {
						return err
					}

					expPodKey := genExpectationPodsKey(jobKey, tt)
					jc.Expectations.CreationObserved(expPodKey)
					expServiceKey := genExpectationServicesKey(jobKey, tt)
					jc.Expectations.CreationObserved(expServiceKey)

					klog.Infof("try to create new pod %s but got an AlreadyExists error, generate a new expectation",
						utils.GenGeneralName(job.GetName(), tt, strconv.Itoa(podIdx)))
				}
				return err
			}
		} else {
			// podSlice is of length 1, this is what we expected. Just reconcile this pod to its expectation.
			curPod := podSlice[0]
			failover, exitCode, err := jc.reconcileOnePod(ctx, job, jobStatus, taskSpec, curPod, podIdx, numTasks, taskType)
			// If the pod failed, we check it can be failovered or not.
			// If yes, append it to `podsToFailover` such that it can be failovered in the next reconciliation.
			// Otherwise, append it to `failedPodContents` for further problem tracking.
			if failover {
				podsToFailover = append(podsToFailover, curPod)
			} else if curPod.Status.Phase == corev1.PodFailed {
				failedPodContents.Add(curPod, exitCode)
			}

			*restart = *restart || failover
			errs.Collect(err)
		}
	}

	// Emit failed pods contents and its exitcode through events for problem tracking.
	if len(failedPodContents) > 0 {
		msg := fmt.Sprintf("job %s %d %v pods failed with non-retryable exitcode: %+v",
			job.GetName(), len(failedPodContents), taskType, failedPodContents.String())
		klog.Info(msg)
		jc.Recorder.Eventf(job, corev1.EventTypeWarning, "PodFailed", msg)
	}

	// do failover if required
	if *restart && len(podsToFailover) > 0 {
		errs.Collect(jc.doFailover(job, podsToFailover))
	}

	return nil
}

// getPodSlices separates the given pods by task index and returns them.
func (jc *JobController) getPodSlices(pods []*corev1.Pod, numTasks int) [][]*corev1.Pod {
	podSlices := make([][]*corev1.Pod, numTasks)
	for _, pod := range pods {
		if _, ok := pod.Labels[commonapis.LabelTaskIndex]; !ok {
			klog.Warning("The pod do not have the index label")
			continue
		}

		// check the task index of the given pod
		idx, err := strconv.Atoi(pod.Labels[commonapis.LabelTaskIndex])
		if err != nil {
			klog.Warningf("Error when strconv.Atoi: %v", err)
			continue
		}
		if idx < 0 {
			klog.Warningf("The label index is not expected: %d", idx)
			continue
		} else if idx >= numTasks {
			// Pod index out of range, which indicates that it is a scale in
			// reconciliation and pod index>=replica will be deleted later, so
			// we'd increase capacity of pod slice to collect.
			newPodSlices := make([][]*corev1.Pod, idx+1)
			copy(newPodSlices, podSlices)
			podSlices = newPodSlices
		}

		podSlices[idx] = append(podSlices[idx], pod)
	}

	return podSlices
}

// createNewPod creates a job pod for the given job according to the taskSpec.
// Two steps:
// (1) Set pod template spec according to the task spec.
// (2) Create the pod by the settled pod template.
func (jc *JobController) createNewPod(ctx context.Context, job interface{}, taskType, taskIdx string,
	taskSpec *commonapis.TaskSpec, masterRole bool, runPolicy *commonapis.RunPolicy) error {

	// check job
	jobMetaObj, ok := job.(metav1.Object)
	if !ok {
		return fmt.Errorf("job is not a metav1.Object type")
	}
	jobRuntimeObj, ok := job.(runtime.Object)
	if !ok {
		return fmt.Errorf("job is not a runtime.Object type")
	}

	podTpl := taskSpec.Template.DeepCopy()

	// set labels for the pod
	addedLabels := jc.GenerateLabels(jobMetaObj.GetName())
	addedLabels[commonapis.LabelTaskType] = taskType
	addedLabels[commonapis.LabelTaskIndex] = taskIdx
	if masterRole {
		addedLabels[commonapis.LabelTaskRole] = "master"
	}
	if jc.Controller.EnableElasticScaling(jobMetaObj, runPolicy) {
		podTpl.Finalizers = append(podTpl.Finalizers, commonapis.FinalizerPreemptProtector)
		addedLabels[commonapis.LabelGeneration] = strconv.Itoa(int(jobMetaObj.GetGeneration()))
	}

	// set up hostnetwork if enabled
	if EnableHostNetwork(jobMetaObj) {
		klog.Infof("pod enable host network, name: %s, masterRole: %v", jobMetaObj.GetName(), masterRole)
		// randomly select a port
		port := int32(rand.IntnRange(jc.Config.HostNetworkPortRange.Base,
			jc.Config.HostNetworkPortRange.Base+jc.Config.HostNetworkPortRange.Size-1))
		// mark hostnetwork as true
		podTpl.Spec.HostNetwork = true
		// setup dns policy with host net instead of ClusterFirst by default.
		podTpl.Spec.DNSPolicy = corev1.DNSClusterFirstWithHostNet
		// set container pot and host port for the given pod
		setupContainerHostNetworkPort(podTpl, jc.Controller.GetDefaultContainerName(), jc.Controller.GetDefaultContainerPortName(), port)
		// save the port into context
		storeHostNetworkPortToContext(ctx, taskType, taskIdx, port)
	}

	podTpl.Labels = mergeMap(podTpl.Labels, addedLabels)

	// Submit a warning event if the user specifies restart policy for
	// the pod template. We recommend to set it from the task level.
	if podTpl.Spec.RestartPolicy != "" {
		errMsg := "Restart policy in pod template will be overwritten by restart policy in task spec"
		klog.Warning(errMsg)
		jc.Recorder.Event(jobRuntimeObj, corev1.EventTypeWarning, podTemplateRestartPolicyReason, errMsg)
	}
	// setup restart policy for pod template according to the task spec
	if taskSpec.RestartPolicy == commonapis.RestartPolicyOnExitCode {
		// RestartPolicyOnExitCode is not supported by `corev1.PodTemplateSpec` anymore
		podTpl.Spec.RestartPolicy = corev1.RestartPolicyNever
	} else {
		podTpl.Spec.RestartPolicy = corev1.RestartPolicy(taskSpec.RestartPolicy)
	}

	// set the pod containers details according to the distributed training setup info indicated by job
	if err := jc.Controller.SetClusterSpec(ctx, job, podTpl, taskType, taskIdx); err != nil {
		return err
	}

	// bind Gang scheduler if enabled and non-bind
	if jc.Config.EnableGangScheduling {
		// create Gang
		gangEntity, err := jc.GangScheduler.GetPodGroup(types.NamespacedName{
			Name:      jobMetaObj.GetName(),
			Namespace: jobMetaObj.GetNamespace(),
		})
		if err != nil {
			return err
		}
		klog.V(5).Infof("gang scheduling enabled, gang scheduler name: %s, bind pod to gang for job: %s",
			jc.GangScheduler.PluginName(), jobMetaObj.GetName())
		// bind pod to Gang
		if err = jc.GangScheduler.BindPodToPodGroup(jobMetaObj, podTpl, gangEntity, taskType); err != nil {
			return err
		}

		// delegate the pod's scheduling to this gang scheduler
		if podTpl.Spec.SchedulerName == "" {
			podTpl.Spec.SchedulerName = jc.GangScheduler.SchedulerName()
		}
	}

	// apply to pod template if spot task spec specified
	if taskSpec.SpotTaskSpec != nil {
		idx, _ := strconv.Atoi(taskIdx)
		if idx >= int(*taskSpec.NumTasks)-int(taskSpec.SpotTaskSpec.NumSpotTasks) {
			podTpl.Spec.PriorityClassName = taskSpec.SpotTaskSpec.PriorityClassName
			if podTpl.Labels == nil {
				podTpl.Labels = make(map[string]string)
			}
			for k, v := range taskSpec.SpotTaskSpec.Labels {
				podTpl.Labels[k] = v
			}
		}
	}

	// All settled. Now let's create the pod.

	jobKey, err := GetJobKey(jobMetaObj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for job object %#v: %v", job, err))
		return err
	}
	expectationPodsKey := genExpectationPodsKey(jobKey, taskType)
	err = jc.Expectations.ExpectCreations(expectationPodsKey, 1)
	if err != nil {
		return err
	}

	// set general pod name
	podTpl.Name = utils.GenGeneralName(jobMetaObj.GetName(), taskType, taskIdx)

	// create
	err = jc.PodControl.CreatePods(jobMetaObj.GetNamespace(), podTpl, jobRuntimeObj, jc.GenerateOwnerReference(jobMetaObj))
	if err != nil && k8serrors.IsTimeout(err) {
		// Pod is created but its initialization has timed out.
		// If the initialization is successful eventually, the
		// controller will observe the creation via the informer.
		// If the initialization fails, or if the pod keeps
		// uninitialized for a long time, the informer will not
		// receive any update, and the controller will create a new
		// pod when the expectation expires.
		return nil
	} else if err != nil {
		return err
	}

	return nil
}

// reconcileOnePod reconciles the given pod to its expectation.
func (jc *JobController) reconcileOnePod(ctx context.Context, job client.Object, jobStatus *commonapis.JobStatus,
	taskSpec *commonapis.TaskSpec, pod *corev1.Pod, taskIndex, numTasks int, taskType commonapis.TaskType) (failOver bool, exitCode int32, err error) {

	const initialExitCode int32 = 0xbeef
	exitCode = initialExitCode

	// Check if the index is in the valid range, otherwise we should scale down (delete) the pod
	// since the expected tasks of job role has been adjusted by user.
	if taskIndex < 0 || taskIndex >= numTasks {
		klog.Infof("pod %s.%s has a out of range index %v and should be cleaned", pod.Namespace, pod.Name, taskIndex)
		return false, exitCode, jc.PodControl.DeletePod(pod.Namespace, pod.Name, job)
	}

	// Record event for those successfully exited pods.
	for _, status := range pod.Status.ContainerStatuses {
		state := status.State
		if status.Name == jc.Controller.GetDefaultContainerName() && state.Terminated != nil {
			exitCode = state.Terminated.ExitCode
			klog.Infof("Pod: %v.%v exited with code %v, terminated reason: %v, message: %v",
				pod.Namespace, pod.Name, exitCode, state.Terminated.Reason, state.Terminated.Message)
			jc.Recorder.Eventf(job, corev1.EventTypeNormal, exitedWithCodeReason, "Pod: %v.%v exited with code %v", pod.Namespace, pod.Name, exitCode)
			break
		}
	}

	// Get and pass its container port by context if pod enables hostnetwork mode.
	if EnableHostNetwork(job) {
		storeHostNetworkPortToContext(ctx, strings.ToLower(string(taskType)), strconv.Itoa(taskIndex),
			getContainerHostNetworkPort(
				pod,
				jc.Controller.GetDefaultContainerName(),
				jc.Controller.GetDefaultContainerPortName(),
			))
	}

	// Check if failed pod or exited main container is retryable and triggers failover action if necessary.
	if pod.Status.Phase == corev1.PodFailed || exitCode != initialExitCode {
		if shouldPodFailover(taskSpec, pod, exitCode) {
			klog.Infof("Pod %s/%s should be failovered, failed reason: %s, message: %s, exitcode: %d",
				pod.Namespace, pod.Name, pod.Status.Reason, pod.Status.Message, exitCode)
			failOver = true
		}
	}

	updateJobTaskStatuses(jobStatus, taskType, pod)

	return failOver, exitCode, nil
}

// updateJobTaskStatuses updates the job's status with the given job task pod.
func updateJobTaskStatuses(jobStatus *commonapis.JobStatus, taskType commonapis.TaskType, pod *corev1.Pod) {
	switch pod.Status.Phase {
	case corev1.PodPending:
		if pod.Spec.NodeName != "" && allInitContainersPassed(pod) {
			jobStatus.TaskStatuses[taskType].Active++
		}
	case corev1.PodRunning:
		jobStatus.TaskStatuses[taskType].Active++
	case corev1.PodSucceeded:
		jobStatus.TaskStatuses[taskType].Succeeded++
	case corev1.PodFailed:
		jobStatus.TaskStatuses[taskType].Failed++
	}
}

func allInitContainersPassed(pod *corev1.Pod) bool {
	for ic := range pod.Status.InitContainerStatuses {
		cs := &pod.Status.InitContainerStatuses[ic]
		passed := cs.State.Terminated != nil || cs.State.Running != nil
		if !passed {
			return false
		}
	}
	return true
}

// AdoptAndClaimPods adopts and claims pods for the given job.
func (jc *JobController) AdoptAndClaimPods(job metav1.Object, podList *corev1.PodList) ([]*corev1.Pod, error) {
	selector, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{
		MatchLabels: jc.GenerateLabels(job.GetName()),
	})
	if err != nil {
		return nil, err
	}

	pods := make([]*corev1.Pod, 0, len(podList.Items))
	for idx := range podList.Items {
		pods = append(pods, &podList.Items[idx])
	}

	// If any adoptions are attempted, we should first recheck for deletion
	// with an uncached quorum read sometime after listing Pods (see #42639).
	canAdoptFunc := recheckDeletionTimestamp(func() (metav1.Object, error) {
		freshJob, err := jc.Controller.GetJobFromAPIClient(job.GetNamespace(), job.GetName())
		if err != nil {
			return nil, err
		}
		if freshJob.GetUID() != job.GetUID() {
			return nil, fmt.Errorf("original job %v/%v is gone: got uid %v, wanted %v", job.GetNamespace(), job.GetName(), freshJob.GetUID(), job.GetUID())
		}
		return freshJob, nil
	})

	refManager := k8scontroller.NewPodControllerRefManager(jc.PodControl, job, selector, jc.Controller.GetAPIGroupVersionKind(), canAdoptFunc)
	return refManager.ClaimPods(pods)
}

// failedReasonToPods collects failed reasons while with its exit codes of failed pods.
// key is {reason}-{exit code}, value is a slice of related pods' name.
type failedReasonToPods map[string][]string

func (fc failedReasonToPods) Add(pod *corev1.Pod, exitCode int32) {
	key := fmt.Sprintf("%s-%d", pod.Status.Reason, exitCode)
	fc[key] = append(fc[key], pod.Name)
}

func (fc failedReasonToPods) String() string {
	if fc == nil {
		return ""
	}
	bytes, _ := json.Marshal(&fc)
	return string(bytes)
}

/* Aggregate errors util. */

func newAggregatedErrors() aggregatedErrors {
	return make(aggregatedErrors, 0)
}

type aggregatedErrors []error

func (agg *aggregatedErrors) Collect(err error) {
	if err == nil {
		return
	}
	*agg = append(*agg, err)
}

func (agg *aggregatedErrors) Errors() []error {
	return *agg
}

func (agg *aggregatedErrors) Error() error {
	return errors.NewAggregate(*agg)
}

func (agg *aggregatedErrors) Count() int {
	return len(*agg)
}
