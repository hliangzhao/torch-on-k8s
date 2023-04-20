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

package train

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/hliangzhao/torch-on-k8s/apis"
	trainv1alpha1 "github.com/hliangzhao/torch-on-k8s/apis/train/v1alpha1"
	commonapis "github.com/hliangzhao/torch-on-k8s/pkg/common/apis/v1alpha1"
	"github.com/hliangzhao/torch-on-k8s/pkg/utils"
	concurrentutils "github.com/hliangzhao/torch-on-k8s/pkg/utils/concurrent"
	patchutils "github.com/hliangzhao/torch-on-k8s/pkg/utils/patch"
	kruisev1alpha1 "github.com/openkruise/kruise/apis/apps/v1alpha1"
	"html/template"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/klog"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/utils/pointer"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strconv"
	"strings"
)

/* Elastic scaling related controls. */

const (
	AnnotationCheckpointRequestedVersion = commonapis.ProjectPrefix + "/ckpt-requested-version"
	AnnotationCheckpointCompletedVersion = commonapis.ProjectPrefix + "/ckpt-completed-version"
	AnnotationReadyToStartWorker         = commonapis.ProjectPrefix + "/ready-to-start-worker"
	AnnotationImmediatelyStartWorker     = commonapis.ProjectPrefix + "/immediately-start-worker"
	AnnotationWorldSize                  = commonapis.ProjectPrefix + "/world-size"
)

const (
	CheckpointStartReason    = "CheckpointStarted"
	CheckpointFinishedReason = "CheckpointSucceeded"
	CheckpointFailedReason   = "CheckpointFailed"
)

type ckptVersion struct {
	Version   int32       `json:"version"`
	Status    string      `json:"status"`
	Context   string      `json:"context"`
	Timestamp metav1.Time `json:"timestamp"`
}

const (
	checkpointInProgress = "InProgress"
	checkpointSucceeded  = "Succeeded"
	checkpointFailed     = "Failed"
)

func init() {
	apis.AddToSchemes = append(apis.AddToSchemes, kruisev1alpha1.AddToScheme)
}

func (r *TorchJobReconciler) EnableElasticScaling(job metav1.Object, runPolicy *commonapis.RunPolicy) bool {
	return job.GetAnnotations()[commonapis.AnnotationEnableElasticTraining] == "true"
}

func (r *TorchJobReconciler) ScaleOut(job interface{}, tasks map[commonapis.TaskType]*commonapis.TaskSpec, activePods []*corev1.Pod, activeServices []*corev1.Service) error {
	pytorchJob, ok := job.(*trainv1alpha1.TorchJob)
	if !ok {
		return fmt.Errorf("%+v is not a type of TorchJob", job)
	}

	log.Info("start to scale out pytorch job", "key", pytorchJob.Namespace+"/"+pytorchJob.Name)

	_, err := r.scale(pytorchJob, tasks, activePods, activeServices)
	return err
}

func (r *TorchJobReconciler) ScaleIn(job interface{}, tasks map[commonapis.TaskType]*commonapis.TaskSpec, activePods []*corev1.Pod, activeServices []*corev1.Service) error {
	pytorchJob, ok := job.(*trainv1alpha1.TorchJob)
	if !ok {
		return fmt.Errorf("%+v is not a type of TorchJob", job)
	}

	taskCountByType := make(map[string]int64, len(tasks))
	for taskType, taskSpec := range tasks {
		taskCountByType[strings.ToLower(string(taskType))] = int64(*taskSpec.NumTasks)
	}

	filteredPods := make([]*corev1.Pod, 0, len(activePods))
	filteredServices := make([]*corev1.Service, 0, len(activeServices))
	for _, p := range activePods {
		if !isIndexOutOfRange(p, taskCountByType[p.Labels[commonapis.LabelTaskType]]) {
			filteredPods = append(filteredPods, p)
		}
	}
	for _, svc := range activeServices {
		if !isIndexOutOfRange(svc, taskCountByType[svc.Labels[commonapis.LabelTaskType]]) {
			filteredServices = append(filteredServices, svc)
		}
	}

	log.Info("start to scale in pytorch job", "key", pytorchJob.Namespace+"/"+pytorchJob.Name)
	_, err := r.scale(pytorchJob, tasks, filteredPods, filteredServices)

	return err
}

// TriggerCheckpointIfNecessary triggers checkpoint when workers are going to be preempted or evicted, notify AIMaster
// to checkpoint and drain out victim pods after succeed.
// Checkpoint requests contains a `version` to distinguish from different progresses, and controller guarantees that
// 'checkpoint-version' <= 'job generation'. When preemption happens controller triggers a new round of checkpoint
// and take job generation as its version, and self-increase generation after checkpoint succeed.
func (r *TorchJobReconciler) TriggerCheckpointIfNecessary(job interface{}, pods []*corev1.Pod) (completed bool, err error) {
	pytorchJob, ok := job.(*trainv1alpha1.TorchJob)
	if !ok {
		return false, fmt.Errorf("%+v is not a type of TorchJob", job)
	}

	victims := filterVictimPods(pods, pytorchJob.Generation)

	// start to notify aimaster executing checkpointing and wait for response.
	ckptReqVersion, err := getCkptVersion(pytorchJob.Annotations, AnnotationCheckpointRequestedVersion)
	if err != nil {
		return false, err
	}
	ckptCompletedVersion, err := getCkptVersion(pytorchJob.Annotations, AnnotationCheckpointCompletedVersion)
	if err != nil {
		return false, err
	}

	syncCheckpoint := ckptReqVersion == nil || (ckptCompletedVersion != nil && ckptReqVersion.Version == ckptCompletedVersion.Version)
	if syncCheckpoint {
		// SyncCheckpoint contains a 2-stage checkpoint transaction:
		// 1. complete.Version == request.Version indicates that job checkpoint has completed.
		// 2. request.Status (InProgress|Succeeded) tracks the whole progress, and job checkpoint
		//    is a sub-procedure of it. When complete.Version == request.Version satisfied in InProgress
		//    status, kubedl will clean up victim pods and increase job generation to windup whole progress
		//    and marks checkpoint as Succeeded. Newly observed victim pod events will be merged when status
		//    is InProgress, that is, a new round checkpoint will be triggered only when new victim pods
		//    observed in Succeeded status.

		// 1. Never execute checkpoint before then trigger first one.
		// 2. Previous checkpoint completed but new victim pod(s) observed, need to trigger
		// a new round of checkpoint.
		if ckptReqVersion == nil || ckptReqVersion.Status == checkpointSucceeded {
			if len(victims) == 0 {
				// no preemption happened, no need to checkpoint.
				log.Info("no preemption happened, no need to checkpoint")
				return true, nil
			}
			log.Info("need to trigger new checkpoint and wait for response",
				"key", pytorchJob.Namespace+"/"+pytorchJob.Name, "version", pytorchJob.Generation)
			r.recorder.Eventf(pytorchJob, corev1.EventTypeNormal, CheckpointStartReason,
				"start to checkpoint due to %d pod(s) is going to be evicted or drained out, version: %v", len(victims), pytorchJob.Generation)
			return false, r.triggerJobCheckpoint(pytorchJob)
		} else if ckptReqVersion.Status == checkpointInProgress {
			// A checkpoint has completed, cleanup victim pods, self-increase generation
			// and mark checkpoint as Succeeded to wind up.
			if err = r.cleanupVictimPods(pytorchJob, victims); err != nil {
				return false, err
			}
			if err = r.increaseGenerationAndMarkAsSucceeded(pytorchJob, ckptReqVersion); err != nil {
				return false, nil
			}
			log.Info("finish executing checkpoint for torch job", "job", pytorchJob.Name,
				"version", ckptCompletedVersion.Version, "completed timestamp", ckptCompletedVersion.Timestamp)
			r.recorder.Eventf(pytorchJob, corev1.EventTypeNormal, CheckpointFinishedReason,
				"finish executing checkpoint for pytorch job %s/%s, version: %v, context: %s",
				pytorchJob.Namespace, pytorchJob.Name, ckptCompletedVersion.Version, ckptCompletedVersion.Context)
			return true, nil
		}
	}

	log.Info("pytorch job checkpoint has not completed yet ...",
		"version", ckptReqVersion.Version, "start timestamp", ckptReqVersion.Timestamp)
	return false, nil
}

// scale handles elastic scaling driven by AIMaster, in general the steps of scale in/out workflow can be
// described as follows:
//
// 1) AIMaster updates expected replicas, then ElasticScaler scales out new replicas or scales in extra replicas.
// 2) refresh master service to the latest generation, at this point no master endpoints will be selected,
//    and newly scaled pod will be injected an init container waiting for master service endpoint ready.
// 3) wait util AnnotationReadyToStartWorker is 'true', which represents worker checkpoint finished (controlled by AIMaster).
// 4) refresh stale master pod to the latest generation, after that master service will be available.
// 5) after 4), newly scaled pods will step into running state, then refresh stale worker pods one by one.
// 6) eventually no stale pods can be gathered, mark AnnotationReadyToStartWorker as false to end current round of scaling.
//
// The order of the above steps cannot be reversed.
func (r *TorchJobReconciler) scale(job *trainv1alpha1.TorchJob, tasks map[commonapis.TaskType]*commonapis.TaskSpec,
	activePods []*corev1.Pod, activeServices []*corev1.Service) (finished bool, err error) {

	// Refresh master svc to the latest generation.
	masterSvc := filterMasterService(activeServices)
	if masterSvc != nil {
		if err = r.refreshStaleService(masterSvc, job.Generation); err != nil {
			return false, err
		}
	}

	// Wait until all worker checkpointing completes.
	if job.Annotations[AnnotationReadyToStartWorker] != "true" && job.Annotations[AnnotationImmediatelyStartWorker] != "true" {
		log.Info("pytorch has not ready to restart workers")
		return false, nil
	}

	// Update elastic scaling state as 'inflight' and it will be marked as 'done' when
	// process finishes.
	if job.Annotations[commonapis.AnnotationElasticScaleState] != commonapis.ElasticScaleStateInflight {
		patch := patchutils.NewMergePatch()
		patch.InsertAnnotation(commonapis.AnnotationElasticScaleState, commonapis.ElasticScaleStateInflight)
		if err = r.Client.Patch(context.Background(), job, patch); err != nil {
			return false, err
		}
	}

	totalTasks := utils.GetTotalExcludedTasks(tasks, commonapis.TaskTypeAIMaster)
	total, stalePods := filterStalePodsByTaskType(activePods, job.Generation, commonapis.TaskType(strings.ToLower(string(commonapis.TaskTypeAIMaster))))
	staleWorkers := stalePods[strings.ToLower(string(trainv1alpha1.TorchTaskTypeWorker))]
	staleMasters := stalePods[strings.ToLower(string(trainv1alpha1.TorchTaskTypeMaster))]

	// Refresh stale master pod.
	masterCompleted := true
	for _, p := range staleMasters { // TODO: Support multi-master
		if completed, err := r.restartStalePod(job, p, int64(totalTasks), job.Generation); err != nil {
			return false, err
		} else {
			masterCompleted = masterCompleted && completed
		}
	}
	if !masterCompleted {
		log.Info("refresh stale master has not completed yet", "key", job.Namespace+"/"+job.Name)
		return false, nil
	}
	total -= len(staleMasters)

	// Refresh stale worker pods concurrently.
	tickets := 100
	if len(staleWorkers) < 100 {
		tickets = len(staleWorkers)
	}
	sema := concurrentutils.NewSemaphore(tickets)
	for _, p := range staleWorkers {
		sema.Acquire()

		go func(worker *corev1.Pod) {
			defer sema.Release()

			if completed, err := r.restartStalePod(job, worker, int64(totalTasks), job.Generation); err != nil {
				klog.Errorf("failed to refresh stale worker, pod: %s, expected generation: %v, err: %v",
					worker.Name, job.Generation, err)
			} else if completed {
				total--
			}
		}(p)
	}
	sema.Wait()

	// Mark the elastic scaling completes.
	if len(stalePods) == 0 || total == 0 {
		log.Info("all pods are in latest generation, mark ready-to-start-worker as false")
		patch := patchutils.NewMergePatch()
		patch.InsertAnnotation(AnnotationReadyToStartWorker, "false")
		patch.InsertAnnotation(commonapis.AnnotationElasticScaleState, commonapis.ElasticScaleStateDone)
		if job.Annotations[AnnotationImmediatelyStartWorker] == "true" {
			patch.InsertAnnotation(AnnotationImmediatelyStartWorker, "false")
		}
		if err = r.Client.Patch(context.Background(), job, patch); err != nil {
			return false, err
		}
		r.recorder.Eventf(job, corev1.EventTypeNormal, "ScaleSucceed",
			"pytorch job %s/%s elastic scaling successfully finished, total replicas: %v", job.Namespace, job.Name, totalTasks)
		finished = true
	}

	return finished, nil
}

// restartStalePod refreshes stale pod to the latest generation, patch after-scaled-worldsize to
// pod annotation and triggers containers inplace restart, as for elastic scaling enabled pod,
// env WORLD-SIZE is referenced by annotation AnnotationWorldSize, new world-size value will be
// assigned to restarted pod.
func (r *TorchJobReconciler) restartStalePod(job *trainv1alpha1.TorchJob, pod *corev1.Pod, worldSize,
	generation int64) (completed bool, err error) {

	expectedWorldSize := strconv.FormatInt(worldSize, 10)
	expectedGeneration := strconv.FormatInt(generation, 10)
	podKey := pod.Namespace + "/" + pod.Name

	if job.Annotations[AnnotationImmediatelyStartWorker] == "true" && !controller.IsPodActive(pod) {
		err = r.Client.Delete(context.Background(), pod)
		return err == nil, nil
	}

	if pod.Labels[commonapis.LabelGeneration] == expectedGeneration {
		if ts := getLastRestartFinishTimestamp(pod, r.GetDefaultContainerName()); ts != nil {
			klog.Infof("pod %s/%s finished restart at %v", pod.Namespace, pod.Name, ts.String())
		}
		return true, nil
	}

	log.Info("refresh stale pod to latest generation", "pod", podKey, "generation", generation)

	// inplace restart the pod by Kruise
	completed, err = r.restartPodInKruiseProtocol(job, pod, expectedWorldSize, expectedGeneration)
	if !completed {
		return false, err
	}

	// Finally, incremental generation for current worker and mark refreshment done.
	patch := patchutils.NewStrategicPatch()
	patch.InsertLabel(commonapis.LabelGeneration, expectedGeneration)
	err = r.Client.Patch(context.Background(), pod, patch)
	if err != nil {
		return false, err
	}
	klog.Infof("succeed to refresh pod to generation: %v", generation)
	r.recorder.Eventf(pod, corev1.EventTypeNormal, "RefreshPodSucceed", "succeed to refresh pod to generation: %v", generation)
	return true, nil
}

func (r *TorchJobReconciler) restartPodInKruiseProtocol(job *trainv1alpha1.TorchJob, pod *corev1.Pod,
	expectedWorldSize, expectedGeneration string) (completed bool, err error) {

	podKey := pod.Namespace + "/" + pod.Name

	if curWorldSize, ok := pod.Annotations[AnnotationWorldSize]; !ok || curWorldSize != expectedWorldSize {
		log.Info("update latest world size of pytorch",
			"key", podKey, "current world size", curWorldSize, "target world size", expectedWorldSize)
		patch := patchutils.NewStrategicPatch()
		patch.InsertAnnotation(AnnotationWorldSize, expectedWorldSize)
		if err = r.Client.Patch(context.Background(), pod, patch); err != nil {
			log.Error(err, "failed to refresh world-size of stale worker", "pod", podKey, "world size", expectedWorldSize)
			return false, err
		}
		return false, nil
	}

	crr := kruisev1alpha1.ContainerRecreateRequest{}
	if err = r.Client.Get(context.Background(), types.NamespacedName{Name: pod.Name, Namespace: pod.Namespace}, &crr); err != nil {
		if k8serrors.IsNotFound(err) {
			return false, r.recreatePodContainers(job, pod, expectedGeneration)
		}

		log.Error(err, "failed to get latest container-recreate-request for stale worker",
			"pod", podKey)
		return false, err
	}
	// crr created in previous round, clean it.
	if crr.Labels[commonapis.LabelGeneration] != expectedGeneration {
		if err = r.Client.Delete(context.Background(), &crr); err != nil {
			return false, err
		}
		return false, r.recreatePodContainers(job, pod, expectedGeneration)
	}
	if crr.Status.Phase == kruisev1alpha1.ContainerRecreateRequestFailed {
		r.recorder.Eventf(pod, corev1.EventTypeWarning, "FailedRestartContainer",
			"failed to restart containers of pod %s/%s, fallback to recreate pod", pod.Namespace, pod.Name)
		err = r.Client.Delete(context.Background(), pod)
		return err == nil, err
	}

	recreateDone := crr.Status.Phase == kruisev1alpha1.ContainerRecreateRequestCompleted || crr.Status.Phase == kruisev1alpha1.ContainerRecreateRequestSucceeded
	if !recreateDone {
		log.Info("container recreate request has not completed yet", "pod", podKey)
		return false, nil
	}

	// Finalize container-recreate-request object once it completes, because elastic scaling is repeatable
	// and 'crr' request will be re-initiated.
	defer func() {
		_ = r.Client.Delete(context.Background(), &crr)
	}()

	r.recorder.Eventf(pod, corev1.EventTypeNormal, "ContainerRecreateSucceed", "succeed to recreate containers in stale worker: %s", podKey)
	return true, nil
}

// refreshStaleService refreshes stale service, both in generation label and label selector
// embedded in spec, to the latest generation, only pods labeled with the latest generation
// will be selected by latest generated service.
func (r *TorchJobReconciler) refreshStaleService(svc *corev1.Service, generation int64) error {
	expectedGen := strconv.FormatInt(generation, 10)
	if svc.Labels[commonapis.LabelGeneration] == expectedGen && svc.Spec.Selector[commonapis.LabelGeneration] == expectedGen {
		return nil
	}

	log.Info("refresh stale service to latest generation", "service", svc.Namespace+"/"+svc.Name, "generation", generation)

	// set labels and selector to the latest, then update through patch
	svcCopy := svc.DeepCopy()
	svcCopy.Labels[commonapis.LabelGeneration] = expectedGen
	if svcCopy.Spec.Selector == nil {
		svcCopy.Spec.Selector = make(map[string]string)
	}
	svcCopy.Spec.Selector[commonapis.LabelGeneration] = expectedGen
	if err := r.Client.Patch(context.Background(), svcCopy, client.MergeFrom(svc)); err != nil {
		log.Error(err, "failed to refresh stale service", "service", svc.Namespace+"/"+svc.Name, "generation", generation)
		return err
	}

	r.recorder.Eventf(svc, corev1.EventTypeNormal, "RefreshServiceSucceed", "succeed to refresh service to generation: %v", generation)
	return nil
}

// recreatePodContainers creates the pod of the given generation job by Kruise API.
func (r *TorchJobReconciler) recreatePodContainers(job *trainv1alpha1.TorchJob, pod *corev1.Pod, generation string) error {
	crr := kruisev1alpha1.ContainerRecreateRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pod.Name,
			Namespace: pod.Namespace,
			Labels: map[string]string{
				commonapis.LabelGeneration: generation,
			},
			// the ContainerRecreateRequest is controlled by the to-be-created pod and the job that controls the pod
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion:         "v1",
					Kind:               "Pod",
					Name:               pod.Name,
					UID:                pod.UID,
					Controller:         pointer.BoolPtr(false),
					BlockOwnerDeletion: pointer.BoolPtr(true),
				},
				{
					APIVersion:         job.APIVersion,
					Kind:               job.Kind,
					Name:               job.Name,
					UID:                job.UID,
					Controller:         pointer.BoolPtr(false),
					BlockOwnerDeletion: pointer.BoolPtr(true),
				},
			},
		},
		Spec: kruisev1alpha1.ContainerRecreateRequestSpec{
			PodName:  pod.Name,
			Strategy: &kruisev1alpha1.ContainerRecreateRequestStrategy{OrderedRecreate: false},
		},
	}

	for ci := range pod.Spec.Containers {
		container := &pod.Spec.Containers[ci]
		crr.Spec.Containers = append(crr.Spec.Containers, kruisev1alpha1.ContainerRecreateRequestContainer{Name: container.Name})
	}
	return r.Client.Create(context.Background(), &crr)
}

// triggerJobCheckpoint starts checkpointing for the given job.
func (r *TorchJobReconciler) triggerJobCheckpoint(job *trainv1alpha1.TorchJob) error {
	version := &ckptVersion{
		Version:   int32(job.Generation),
		Context:   fmt.Sprintf("torch job starts to request for checkpoint"),
		Timestamp: metav1.Now(),
		Status:    checkpointInProgress,
	}

	versionStr, err := json.Marshal(version)
	if err != nil {
		return err
	}
	patch := patchutils.NewMergePatch()
	patch.InsertAnnotation(AnnotationCheckpointRequestedVersion, string(versionStr))
	if err = r.Client.Patch(context.Background(), job, patch); err != nil {
		return err
	}

	return nil
}

// cleanupVictimPods deletes the to-be-cleanup pods concurrently.
func (r *TorchJobReconciler) cleanupVictimPods(job *trainv1alpha1.TorchJob, victims []*corev1.Pod) error {
	sema := concurrentutils.NewSemaphore(10)
	errs := make(chan error, 10)
	for _, v := range victims {
		sema.Acquire()
		go func(p *corev1.Pod) {
			defer sema.Release()
			log.Info("pods to cleanup", "key", p.Namespace+"/"+p.Name)
			if err := r.jobController.PodControl.DeletePod(p.Namespace, p.Name, job); err != nil {
				errs <- err
			}
		}(v)
	}
	sema.Wait()
	close(errs)

	if len(errs) == 0 {
		return nil
	}
	errStr := ""
	for err := range errs {
		errStr += fmt.Sprintf("%v\n", err)
	}
	return errors.New(errStr)
}

// increaseGenerationAndMarkSucceeded updates a trivial field in job spec to initiative update
// its generation.
func (r *TorchJobReconciler) increaseGenerationAndMarkAsSucceeded(job *trainv1alpha1.TorchJob, ckptReqVersion *ckptVersion) error {
	// update annotations
	if ckptReqVersion != nil {
		ckptReqVersion.Status = checkpointSucceeded
		anno, err := setCkptVersion(job.Annotations, AnnotationCheckpointRequestedVersion, ckptReqVersion)
		if err != nil {
			return err
		}
		job.Annotations = anno
	}
	job.Annotations[AnnotationReadyToStartWorker] = "true"

	// update spec
	spec := job.Spec.TorchTaskSpecs[trainv1alpha1.TorchTaskTypeMaster]
	if spec == nil {
		spec = job.Spec.TorchTaskSpecs[trainv1alpha1.TorchTaskTypeWorker]
	}
	if spec.Template.Annotations == nil {
		spec.Template.Annotations = make(map[string]string)
	}
	// TODO: "distributed.io/generation-to-increase" should be defined as a constant
	spec.Template.Annotations["distributed.io/generation-to-increase"] = strconv.FormatInt(job.Generation+1, 10)

	if err := r.Client.Update(context.Background(), job); err != nil {
		return err
	}
	return nil
}

// AddMasterWaiterForWorker adds master waiter container to the init container list of the give master's podTemplate.
func AddMasterWaiterForWorker(podTemplate *corev1.PodTemplateSpec, param InitContainerParam) error {
	containers, err := renderMasterWaiterInitContainer(param)
	if err != nil {
		return err
	}
	podTemplate.Spec.InitContainers = append(podTemplate.Spec.InitContainers, containers...)
	return nil
}

func AddImageWarmupForWorker(podTemplate *corev1.PodTemplateSpec, mainContainerName string) {
	image := ""
	resources := corev1.ResourceRequirements{}
	for i := range podTemplate.Spec.Containers {
		c := podTemplate.Spec.Containers[i]
		if c.Name == mainContainerName {
			image = c.Image
			c.Resources.DeepCopyInto(&resources)
			break
		}
	}

	if image == "" {
		return
	}

	initC := corev1.Container{
		Name:  "warmup",
		Image: image,
		Command: []string{
			"echo",
			"I do nothing bu warmup image of the main container",
		},
		Resources: resources,
	}

	if _, ok := resources.Requests[commonapis.ResourceNvidiaGPU]; ok {
		initC.Env = []corev1.EnvVar{
			{Name: "NVIDIA_VISIBLE_DEVICES", Value: ""},
			{Name: "NVIDIA_DRIVER_CAPABILITIES", Value: "all"},
		}
	}

	podTemplate.Spec.InitContainers = append(podTemplate.Spec.InitContainers, initC)
}

func filterVictimPods(activePods []*corev1.Pod, latestGeneration int64) []*corev1.Pod {
	victims := make([]*corev1.Pod, 0, len(activePods))
	for _, pod := range activePods {
		if isVictimCandidatePod(pod) {
			victims = append(victims, pod)
		}
	}
	return victims
}

func filterMasterService(services []*corev1.Service) *corev1.Service {
	labelSelector := &metav1.LabelSelector{
		MatchLabels: map[string]string{},
	}
	labelSelector.MatchLabels[commonapis.LabelTaskType] = strings.ToLower(string(trainv1alpha1.TorchTaskTypeMaster))
	selector, err := metav1.LabelSelectorAsSelector(labelSelector)
	if err != nil {
		return nil
	}

	for _, svc := range services {
		if selector.Matches(labels.Set(svc.Labels)) {
			return svc
		}
	}

	return nil
}

var initContainerTemplate = `
- name: master-waiter
  image: {{.InitContainerImage}}
  imagePullPolicy: IfNotPresent
  env:
  - name: MASTER_ADDR
    value: {{.MasterAddr}}
  command: ['sh', '-c', 'until ping -c1 $MASTER_ADDR >/dev/null 2>&1; do :; sleep 0.1; done;']`

type InitContainerParam struct {
	MasterAddr         string
	InitContainerImage string
}

// renderMasterWaiterInitContainer parses the initContainerTemplate with the given param to a container instance.
func renderMasterWaiterInitContainer(param InitContainerParam) ([]corev1.Container, error) {
	var buf bytes.Buffer
	tpl, err := template.New("container").Parse(initContainerTemplate)
	if err != nil {
		return nil, err
	}
	if err = tpl.Execute(&buf, param); err != nil {
		return nil, err
	}

	var result []corev1.Container
	if err = yaml.Unmarshal(buf.Bytes(), &result); err != nil {
		return nil, err
	}

	return result, nil
}

func isIndexOutOfRange(obj metav1.Object, numTasks int64) bool {
	index := obj.GetLabels()[commonapis.LabelTaskIndex]
	taskType := obj.GetLabels()[commonapis.LabelTaskType]
	if taskType == "" || index == "" {
		return false
	}
	indexNum, err := strconv.ParseInt(index, 10, 64)
	if err != nil {
		return false
	}
	return indexNum >= numTasks
}

// setCkptVersion saves the serialized version into the annotations.
func setCkptVersion(annotations map[string]string, key string, version *ckptVersion) (map[string]string, error) {
	if annotations == nil {
		annotations = make(map[string]string)
	}
	vBytes, err := json.Marshal(version)
	if err != nil {
		return annotations, err
	}
	annotations[key] = string(vBytes)
	return annotations, nil
}

// getCkptVersion gets the ckptVersion from the annotations.
func getCkptVersion(annotations map[string]string, key string) (*ckptVersion, error) {
	vBytes := annotations[key]
	if len(vBytes) == 0 {
		return nil, nil
	}
	version := &ckptVersion{}
	if err := json.Unmarshal([]byte(vBytes), version); err != nil {
		return nil, err
	}
	return version, nil
}

// getLastRestartFinishTimestamp returns the last termination time of the specified container.
func getLastRestartFinishTimestamp(pod *corev1.Pod, containerName string) *metav1.Time {
	for idx := range pod.Status.ContainerStatuses {
		status := pod.Status.ContainerStatuses[idx]
		if status.Name == containerName && status.LastTerminationState.Terminated != nil {
			return &status.LastTerminationState.Terminated.FinishedAt
		}
	}
	return nil
}

func filterStalePodsByTaskType(pods []*corev1.Pod, generation int64, excludes ...commonapis.TaskType) (total int, stalePods map[string][]*corev1.Pod) {
	excludeTaskTypes := sets.NewString()
	for _, e := range excludes {
		excludeTaskTypes.Insert(string(e))
	}

	stalePods = make(map[string][]*corev1.Pod)
	for _, p := range pods {
		staled := isStalePod(p, generation)
		tt := p.Labels[commonapis.LabelTaskType]
		if staled && !excludeTaskTypes.Has(tt) {
			total++
			stalePods[tt] = append(stalePods[tt], p)
		}
	}
	return total, stalePods
}

func isStalePod(pod *corev1.Pod, generation int64) bool {
	// TODO: This func might not be required
	gen := pod.Labels[commonapis.LabelGeneration]
	if gen == "" {
		return true
	}
	current, err := strconv.ParseInt(gen, 10, 64)
	if err != nil {
		return false
	}
	return current < generation
}

func isVictimCandidatePod(pod *corev1.Pod) bool {
	return pod.DeletionTimestamp != nil &&
		utils.HasFinalizer(pod.Finalizers, commonapis.FinalizerPreemptProtector)
}
