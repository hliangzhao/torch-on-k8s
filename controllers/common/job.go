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
	"fmt"
	modelv1alpha1 "github.com/hliangzhao/torch-on-k8s/apis/model/v1alpha1"
	trainv1alpha1 "github.com/hliangzhao/torch-on-k8s/apis/train/v1alpha1"
	"github.com/hliangzhao/torch-on-k8s/pkg/features"
	"github.com/hliangzhao/torch-on-k8s/pkg/storage/registry"
	"github.com/hliangzhao/torch-on-k8s/pkg/utils"
	log "github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/klog"
	k8scontroller "k8s.io/kubernetes/pkg/controller"
	"reflect"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"strconv"
	"strings"
	"time"
)

// TODO: Use different levels of loggers (job, task, pod) such that we can detect bugs immediately.

/* The job reconcile functions. */

// Reasons for job events.
const (
	FailedDeleteJobReason     = "FailedDeleteJob"
	SuccessfulDeleteJobReason = "SuccessfulDeleteJob"
)

// ReconcileJobs reconciles the given job.
func (jc *JobController) ReconcileJobs(job client.Object, tasks map[trainv1alpha1.TaskType]*trainv1alpha1.TaskSpec,
	minMembers map[trainv1alpha1.TaskType]*int32, jobStatus trainv1alpha1.JobStatus, runPolicy *trainv1alpha1.RunPolicy,
	modelVersion *modelv1alpha1.ModelVersionSpec) (result reconcile.Result, err error) {

	jobName := job.GetName()
	jobKey, err := GetJobKey(job)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("could not get job key for job %v: %v", job, err))
		return result, err
	}

	klog.Infof("Reconciling for job %s", job.GetName())

	defer func() {
		// Add job key into backoff-states queue since it will be retried in
		// next round util reconciling succeeded.
		if result.Requeue || err != nil {
			jc.BackoffStatesQueue.AddRateLimited(jobKey)
			return
		}
		// Job succeeded. Just forget it.
		jc.BackoffStatesQueue.Forget(jobKey)
	}()

	oldJobStatus := jobStatus.DeepCopy()

	// get controlled pods & services
	pods, err := jc.Controller.GetPodsForJob(job)
	if err != nil {
		log.Warnf("GetPodsForJob error %v", err)
		return result, err
	}
	services, err := jc.Controller.GetServicesForJob(job)
	if err != nil {
		log.Warnf("GetServicesForJob error %v", err)
		return result, err
	}

	prevNumRetry := jc.BackoffStatesQueue.NumRequeues(jobKey)
	activePods := filterActivePods(pods)
	numActivePods := int32(len(activePods))
	numFailedPods := filterPodByPhase(pods, corev1.PodFailed)
	numTotalTasks := utils.GetTotalTasks(tasks)
	prevNumFailedTasks := getTotalFailedTasks(jobStatus.TaskStatuses)

	var failureMsg string
	jobExceedsLimit := false
	exceedsBackoffLimit := false
	pastBackoffLimit := false

	if runPolicy.BackoffLimit != nil {
		jobHasNewFailedPods := numFailedPods > prevNumFailedTasks
		// Note that here we can compare numActivePods and numTotalTasks because
		// a task wraps only one pod.
		// TODO: This seems buggy. If we set pg.Spec.MinMember when gang scheduling enabled, we should not
		//  compare numActivePods with numTotalTasks, but pg.Spec.MinMember.
		exceedsBackoffLimit = jobHasNewFailedPods && (numActivePods != numTotalTasks) &&
			(int32(prevNumRetry)+1) > *runPolicy.BackoffLimit
		pastBackoffLimit, err = jc.pastBackoffLimit(jobName, runPolicy, tasks, pods)
		if err != nil {
			return result, err
		}
	}

	if exceedsBackoffLimit || pastBackoffLimit {
		// check if the number of pod restart exceeds backoff (for restart OnFailure only)
		// OR if the number of failed jobs increased since the last syncJob
		jobExceedsLimit = true
		failureMsg = fmt.Sprintf("Job %s has failed because it has reached the specified backoff limit", jobName)
	} else if jc.pastActiveDeadline(runPolicy, jobStatus) {
		jobExceedsLimit = true
		failureMsg = fmt.Sprintf("Job %s has failed because it was active longer than specified deadline", jobName)
		now := metav1.Now()
		jobStatus.CompletionTime = &now
	}

	// Delete all controlled pods and services if the job has terminated.
	// And then, do the related operations, such as output model, etc.
	if utils.IsSucceeded(jobStatus) || utils.IsFailed(jobStatus) || jobExceedsLimit {
		if err = jc.deletePodsAndServices(runPolicy, job, pods); err != nil {
			return result, err
		}

		if result, err = jc.cleanupJob(runPolicy, jobStatus, job); err != nil {
			return result, err
		}

		// If enable gang scheduling, delete the corresponding podgroups
		if jc.Config.EnableGangScheduling {
			jc.Recorder.Event(job, corev1.EventTypeNormal, "JobTerminated", "Job has been terminated. Deleting PodGroup")
			if err = jc.DeletePodGroup(job); err != nil {
				jc.Recorder.Eventf(job, corev1.EventTypeWarning, "FailedDeleteGang", "Error deleting: %v", err)
				return result, err
			} else {
				jc.Recorder.Eventf(job, corev1.EventTypeNormal, "SuccessfulDeleteGang", "Deleted gang: %v", jobName)
			}
		}

		if jobExceedsLimit {
			jc.Recorder.Event(job, corev1.EventTypeNormal, utils.JobFailedReason, failureMsg)
			if jobStatus.CompletionTime == nil {
				now := metav1.Now()
				jobStatus.CompletionTime = &now
			}
			err = utils.UpdateJobConditions(&jobStatus, trainv1alpha1.JobFailed, utils.JobFailedReason, failureMsg)
			if err != nil {
				klog.Infof("Append job condition error: %v", err)
				return result, err
			}
		}

		// At this point the pods may have been deleted.
		// 1) If the job succeeded, we manually set the task status.
		// 2) If any task is still active, set its status to `succeeded`.
		if utils.IsSucceeded(jobStatus) {
			for taskType := range jobStatus.TaskStatuses {
				jobStatus.TaskStatuses[taskType].Succeeded += jobStatus.TaskStatuses[taskType].Active
				jobStatus.TaskStatuses[taskType].Active = 0
			}

			// Output the trained model since the job has succeeded
			if modelVersion != nil {
				err = jc.creteModelVersion(job, modelVersion, pods, &jobStatus)
			}
			if err != nil {
				return reconcile.Result{Requeue: true}, err
			}
		}

		// Update job status.
		if !reflect.DeepEqual(*oldJobStatus, jobStatus) {
			return result, jc.Controller.UpdateJobStatusInAPIServer(job, &jobStatus)
		}
		return result, nil
	}

	// The job is still running. Do the following operations, such as create podgroup, create/update/delete pods & services.

	if jc.Config.EnableGangScheduling {
		log.Infof("gang schedule enabled, start to syncing for job %s", jobKey)
		if _, err = jc.CreatePodGroup(job, tasks, minMembers, runPolicy.SchedulingPolicy); err != nil {
			return result, err
		}
	}

	// Scaling will be triggered when following conditions satisfied:
	// 1. Job is in running state.
	// 2. Elastic scaling is enabled.
	// 3. Generation has incremented, which represents the expected number of tasks changed.
	if utils.IsRunning(*oldJobStatus) && jc.Controller.EnableElasticScaling(job, runPolicy) {
		// Firstly, check necessity of checkpoint, notify aimaster executing checkpoint if it is.
		// Once checkpoint triggered, scale out/in progress will be hold util it completed.
		done, err := jc.Controller.TriggerCheckpointIfNecessary(job, pods)
		if err != nil {
			log.Errorf("failed to trigger checkpoints, err: %v", err)
			return result, err
		}

		// No in-progressing checkpoint and generation has incremented (scale out or scale in happened).
		if done && job.GetGeneration() > 1 {
			// numTotalTasks is the expected total task num while numActiveTasksInNewGen is the actual
			numActiveTasksInNewGen := getNumTasksForLatestGeneration(pods, job.GetGeneration())
			if numTotalTasks > numActiveTasksInNewGen {
				err = jc.Controller.ScaleOut(job, tasks, pods, services)
			} else if numTotalTasks < numActiveTasksInNewGen {
				err = jc.Controller.ScaleIn(job, tasks, pods, services)
			}
			if err != nil {
				log.Errorf("failed to execute elastic scaling, err: %v", err)
				return result, err
			}
		}
	}

	restart := false

	// add moth path to container env
	addModelPathEnv(tasks, modelVersion)

	// reconcile each task in order
	ctx := context.WithValue(context.Background(), trainv1alpha1.ContextHostNetworkPorts, make(map[string]int32))
	for _, taskType := range jc.Controller.GetTaskReconcilerOrders() {
		taskSpec, exist := tasks[taskType]
		if !exist {
			continue
		}

		// non-aimaster tasks should wait until the aimaster task is ready
		if utils.ContainsTaskType(tasks, trainv1alpha1.TaskTypeAIMaster) && taskType != trainv1alpha1.TaskTypeAIMaster &&
			job.GetAnnotations()["aimaster"] != "ready" {
			klog.Infof("Task aimaster is not ready and reconciling is frozen.")
			return reconcile.Result{}, nil
		}

		klog.Infof("reconciles task type: %s", taskType)

		// If DAG scheduling has been enabled, and current task has upstream task,
		// wait util all upstream tasks ready.
		if features.FeatureGates.Enabled(features.DAGScheduling) && len(taskSpec.DependsOn) > 0 &&
			!jc.CheckDAGConditionReady(job, tasks, pods, taskSpec.DependsOn) {
			continue
		}

		// upstream is ready, reconcile pod(s) for current task
		err = jc.ReconcilePods(ctx, job, &jobStatus, pods, taskType, taskSpec, tasks, runPolicy, &restart)
		if err != nil {
			klog.Warningf("ReconcilePods error %v", err)
			return result, err
		}

		// reconcile service(s) for the task if required
		if jc.Controller.GetAPIGroupVersionKind().Kind == trainv1alpha1.TorchJobKind {
			torchJob, ok := job.(*trainv1alpha1.TorchJob)
			if !ok {
				klog.Warningf("job is not a TorchJob")
			}
			if torchJob.Spec.EnableTorchElastic && taskType != trainv1alpha1.TaskTypeTorchMaster {
				continue
			}
		}
		err = jc.ReconcileServices(ctx, job, services, taskType, taskSpec)
		if err != nil {
			klog.Warningf("ReconcileServices error %v", err)
			return result, err
		}
	}

	// Controlled pods & services are reconciled, update the job status
	err = jc.Controller.UpdateJobStatus(job, tasks, &jobStatus, restart)
	if err != nil {
		klog.Warningf("UpdateJobStatus error %v", err)
		return result, err
	}

	// Metering first pod launch delay when job state transit from created to running.
	if utils.IsCreated(*oldJobStatus) && utils.IsRunning(jobStatus) {
		jc.Metrics.FirstPodLaunchDelaySeconds(activePods, job, jobStatus)
	}

	// Metring all pods launch delay when latest pods are all active after reconciled, and previous
	// job status is not the all-active state, including the following cases:
	// 1. job created, successfully create all pods and becomes job running.
	// 2. job created, create some pods while some pods failed, finally becomes job running.
	// 3. job running then some pods failed, job step into restarting state, then pod recreated and
	//    finally return back to running state.
	//
	// Case 3 should be discarded.
	if getTotalActivePods(jobStatus.TaskStatuses) == numTotalTasks &&
		getTotalActivePods(oldJobStatus.TaskStatuses) < numTotalTasks &&
		!utils.IsRestarting(*oldJobStatus) {
		jc.Metrics.AllPodsLaunchDelaySeconds(pods, job, jobStatus)
	}

	// Update job status in API server if changed.
	if !reflect.DeepEqual(*oldJobStatus, jobStatus) {
		if err = jc.Controller.UpdateJobStatusInAPIServer(job, &jobStatus); err != nil {
			if errors.IsConflict(err) {
				// retry later when update operation violates with etcd concurrency control.
				result.Requeue = true
				return result, nil
			}
			return result, err
		}
	}
	return result, nil
}

// filterActivePods returns a slice of active pods from the given pods.
func filterActivePods(pods []*corev1.Pod) []*corev1.Pod {
	var ret []*corev1.Pod
	for _, pod := range pods {
		if k8scontroller.IsPodActive(pod) {
			ret = append(ret, pod)
		} else {
			deletionTimeStamp := "N/A"
			if pod.DeletionTimestamp != nil {
				deletionTimeStamp = pod.DeletionTimestamp.String()
			}
			klog.Infof("Ignoring inactive pod %v/%v in state %v, deletion time %s",
				pod.Namespace, pod.Name, pod.Status.Phase, deletionTimeStamp)
		}
	}
	return ret
}

// filterPodByPhase calculates the number of pods that in the given phase.
func filterPodByPhase(pods []*corev1.Pod, phase corev1.PodPhase) int32 {
	var result int32
	for i := range pods {
		if phase == pods[i].Status.Phase {
			result++
		}
	}
	return result
}

// getTotalFailedTasks returns the number of total failed pods in the given tasks.
func getTotalFailedTasks(tasks map[trainv1alpha1.TaskType]*trainv1alpha1.TaskStatus) int32 {
	ret := int32(0)
	for _, status := range tasks {
		ret += status.Failed
	}
	return ret
}

// pastBackoffLimit checks if job's total number of restarts exceeds BackoffLimit.
// This method applies only to pods with `restartPolicy` is `OnFailure` or `Always`.
// For those pods, any restart of them will be collected to calculate the restart number of the job.
func (jc *JobController) pastBackoffLimit(jobName string, runPolicy *trainv1alpha1.RunPolicy,
	tasks map[trainv1alpha1.TaskType]*trainv1alpha1.TaskSpec, pods []*corev1.Pod) (bool, error) {

	result := int32(0)
	for taskType, taskSpec := range tasks {
		if taskSpec.RestartPolicy != trainv1alpha1.RestartPolicyOnFailure &&
			taskSpec.RestartPolicy != trainv1alpha1.RestartPolicyAlways {
			klog.Warningf("The restart policy of task %v of the job %v is not OnFailure or Always. Not counted in backoff limit.", taskType, jobName)
			continue
		}

		tt := strings.ToLower(string(taskType))
		filteredPods, err := jc.filterPodsForTaskType(pods, tt)
		if err != nil {
			return false, err
		}
		for idx := range filteredPods {
			pod := filteredPods[idx]
			if pod.Status.Phase != corev1.PodRunning {
				continue
			}
			for ci := range pod.Status.InitContainerStatuses {
				result += pod.Status.InitContainerStatuses[ci].RestartCount
			}
			for ci := range pod.Status.ContainerStatuses {
				result += pod.Status.ContainerStatuses[ci].RestartCount
			}
		}
	}

	if *runPolicy.BackoffLimit == 0 {
		return result > 0, nil
	}
	return result >= *runPolicy.BackoffLimit, nil
}

// pastActiveDeadline checks if job has ActiveDeadlineSeconds field set and if it is exceeded.
func (jc *JobController) pastActiveDeadline(runPolicy *trainv1alpha1.RunPolicy, jobStatus trainv1alpha1.JobStatus) bool {
	if runPolicy.ActiveDurations == nil || jobStatus.StartTime == nil {
		return false
	}
	now := metav1.Now()
	start := jobStatus.StartTime.Time
	duration := now.Time.Sub(start)
	return duration >= time.Duration(*runPolicy.ActiveDurations)*time.Second
}

// deletePodsAndServices deletes related pods and services of the given job according to runPolicy.
func (jc *JobController) deletePodsAndServices(runPolicy *trainv1alpha1.RunPolicy, job interface{}, pods []*corev1.Pod) error {
	if len(pods) == 0 {
		return nil
	}

	// no clean pod policy specified, do nothing
	if *runPolicy.CleanPodPolicy == trainv1alpha1.CleanPodPolicyNone {
		return nil
	}

	for _, pod := range pods {
		if *runPolicy.CleanPodPolicy == trainv1alpha1.CleanPodPolicyRunning && pod.Status.Phase != corev1.PodRunning {
			continue
		}
		jobObj, ok := job.(runtime.Object)
		if !ok {
			return fmt.Errorf("%+v is not a job", jobObj)
		}
		if err := jc.PodControl.DeletePod(pod.Namespace, pod.Name, jobObj); err != nil {
			return err
		}
		if err := jc.ServiceControl.DeleteService(pod.Namespace, pod.Name, jobObj); err != nil {
			return err
		}
	}

	return nil
}

// creteModelVersion creates a modelversion resource in cluster for the given job.
func (jc *JobController) creteModelVersion(job metav1.Object, modelVersion *modelv1alpha1.ModelVersionSpec,
	pods []*corev1.Pod, jobStatus *trainv1alpha1.JobStatus) error {

	mv := &modelv1alpha1.ModelVersion{}
	mvName := "mv-" + job.GetName() + "-" + string(job.GetUID())[:5]
	err := jc.Client.Get(context.Background(), types.NamespacedName{
		Namespace: job.GetNamespace(),
		Name:      mvName,
	}, mv)

	if err == nil {
		// modelversion already exists, return directly
		return nil
	} else {
		if !errors.IsNotFound(err) {
			klog.Errorf("failed to get model version %s", mv.Name)
			return err
		}
	}

	// model version not found, create it
	mv = &modelv1alpha1.ModelVersion{}
	mv.Namespace = job.GetNamespace()
	mv.Name = mvName
	mv.Spec = *modelVersion
	mv.Spec.CreatedBy = job.GetName()
	if mv.Spec.Storage != nil && mv.Spec.Storage.LocalStorage != nil {
		if mv.Spec.Storage.LocalStorage.NodeName != "" {
			mv.Spec.Storage.LocalStorage.NodeName = jc.Controller.GetNodeForModelOutput(pods)
		}
	}
	// create modelversion resource in cluster
	err = jc.Client.Create(context.Background(), mv)
	if err != nil {
		klog.Errorf("failed to create model version %s", mv.Name)
		return err
	}

	// update job status
	jobStatus.ModelVersionName = mv.Name
	klog.Infof("created model version %s", mv.Name)

	return nil
}

// cleanupJob will do the job deletion when the TTLSecondsAfterFinished is satisfied.
func (jc *JobController) cleanupJob(runPolicy *trainv1alpha1.RunPolicy, jobStatus trainv1alpha1.JobStatus, job interface{}) (reconcile.Result, error) {
	curTime := time.Now()
	jobObj, _ := job.(metav1.Object)
	res := reconcile.Result{}

	ttl := runPolicy.TTLSecondsAfterFinished
	if ttl == nil {
		return res, nil
	}

	if jobStatus.CompletionTime == nil {
		return res, fmt.Errorf("cleanup Job %s, but job has CompletionTime not set", jobObj.GetName())
	}

	duration := time.Second * time.Duration(*ttl)
	delTime := jobStatus.CompletionTime.Add(duration)
	if curTime.After(delTime) {
		err := jc.Controller.DeleteJob(job)
		if err != nil {
			klog.Warningf("Cleanup job error: %v", err)
			return res, err
		}
		return res, nil
	}

	res.Requeue = true
	res.RequeueAfter = delTime.Sub(curTime)
	return res, nil
}

// getNumTasksForLatestGeneration returns the number of pods which has the same generation as provided.
func getNumTasksForLatestGeneration(pods []*corev1.Pod, generation int64) int32 {
	gen := strconv.FormatInt(generation, 10)
	ret := int32(0)
	for _, pod := range pods {
		if pod.Labels[trainv1alpha1.LabelGeneration] == gen {
			ret++
		}
	}
	return ret
}

// addModelPathEnv adds the model path env var and mounts the model volume for every container in the given tasks.
func addModelPathEnv(tasks map[trainv1alpha1.TaskType]*trainv1alpha1.TaskSpec, modelVersion *modelv1alpha1.ModelVersionSpec) {
	if modelVersion == nil {
		return
	}

	storageProvider := registry.GetStorageProvider(modelVersion.Storage)
	for _, taskSpec := range tasks {
		for i := range taskSpec.Template.Spec.Containers {
			exist := false
			for _, envVar := range taskSpec.Template.Spec.Containers[i].Env {
				if envVar.Name == modelv1alpha1.EnvModelPath {
					exist = true
					break
				}
			}
			if !exist {
				taskSpec.Template.Spec.Containers[i].Env = append(taskSpec.Template.Spec.Containers[i].Env, corev1.EnvVar{
					Name:  modelv1alpha1.EnvModelPath,
					Value: storageProvider.GetModelMountPath(modelVersion.Storage),
				})
			}
		}
		storageProvider.AddModelVolumeToPodSpec(modelVersion.Storage, &taskSpec.Template)
	}
}

// shouldCreateService checks for the given task type, whether we need to create a service for it.
func (jc *JobController) shouldCreateService(taskType trainv1alpha1.TaskType) bool {
	if jc.Controller.GetAPIGroupVersionKind().Kind == trainv1alpha1.TorchJobKind && taskType != trainv1alpha1.TaskTypeTorchMaster {
		return false
	}
	return true
}

// getTotalActivePods returns the total number of active pods for the given tasks.
func getTotalActivePods(taskStatues map[trainv1alpha1.TaskType]*trainv1alpha1.TaskStatus) int32 {
	ret := int32(0)
	for _, status := range taskStatues {
		ret += status.Active
	}
	return ret
}
