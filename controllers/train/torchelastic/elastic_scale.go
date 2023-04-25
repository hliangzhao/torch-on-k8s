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

package torchelastic

import (
	"context"
	trainv1alpha1 "github.com/hliangzhao/torch-on-k8s/apis/train/v1alpha1"
	"github.com/hliangzhao/torch-on-k8s/controllers/train"
	"github.com/hliangzhao/torch-on-k8s/pkg/utils/concurrent"
	patchutils "github.com/hliangzhao/torch-on-k8s/pkg/utils/patch"
	kruisev1alpha1 "github.com/openkruise/kruise/apis/apps/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/utils/pointer"
	"reflect"
	"strconv"
	"strings"
)

// TODO: Try to reuse the functions defined in `elastic_scale.go`
//  Besides, why not develop the torchelastic in the coordinator way?

func (r *ElasticTorchJobReconciler) start(ctx context.Context, cancel context.CancelFunc, name, namespace string) {
	sharedTorchJob := &trainv1alpha1.TorchJob{}
	jobName := name
	jobNamespace := namespace

	// create the metric observation for current job if non-exist
	r.mutex.Lock()
	if _, ok := r.metrics[jobName]; !ok {
		r.metrics[jobName] = make(map[int32][]MetricObservation)
	}
	r.mutex.Unlock()

	if err := r.Client.Get(ctx, types.NamespacedName{
		Namespace: jobNamespace,
		Name:      jobName,
	}, sharedTorchJob); err != nil {
		if errors.IsNotFound(err) {
			log.Info("try to get torchjob but it has been deleted",
				"namespace", jobNamespace, "name", jobName)
		} else {
			log.Error(err, "try to get torchjob but err happens")
		}
		defer cancel()
		return
	}

	torchJob := sharedTorchJob.DeepCopy()

	// if the torchelastic job is not correctly set, just remove this job
	if torchJob.Spec.TorchElasticPolicy.NumMaxReplicas == nil || torchJob.Spec.TorchElasticPolicy.NumMinReplicas == nil {
		log.Info("torchjob %s does not configure the max or min replicas", torchJob.Name)
		defer cancel()
		delete(r.jobs, getElasticTorchJobName(torchJob.Name, torchJob.Namespace))
		return
	}

	// update torchelastic job status
	if torchJob.Status.TorchElasticStatuses == nil {
		initElasticTorchJobStatus(torchJob, trainv1alpha1.TaskTypeTorchWorker)
		if err := r.UpdateJobStatusInAPIServer(torchJob, &torchJob.Status); err != nil {
			if errors.IsConflict(err) {
				// this will happen when the update violates the etcd concurrent control
				log.Info("conflict happens when update torchelasticjob status", "job", torchJob.Name)
			}
		}
		return
	}

	jobStatus := torchJob.Status.DeepCopy()
	oldStatus := jobStatus.DeepCopy()
	if torchJob.Status.CompletionTime != nil || torchJob.DeletionTimestamp != nil {
		log.Info("torchjob %s has already completed (or been deleted)", "job", torchJob.Name)
		defer cancel()
		delete(r.jobs, getElasticTorchJobName(torchJob.Name, torchJob.Namespace))
		delete(r.metrics, getElasticTorchJobName(torchJob.Name, torchJob.Namespace))
		return
	}

	curReplicas := *torchJob.Spec.TorchTaskSpecs[trainv1alpha1.TaskTypeTorchWorker].NumTasks

	// start all task pods
	hasPendingPod, hasFailedPod := r.waitForAllPodsRunning(torchJob)

	// if job has pending pods and current task replicas are more than the min replicas setted,
	// just returns back the last replica status
	if hasPendingPod && curReplicas > *torchJob.Spec.TorchElasticPolicy.NumMinReplicas {
		lastReplicas := jobStatus.TorchElasticStatuses[trainv1alpha1.TaskTypeTorchWorker].LastReplicas
		*torchJob.Spec.TorchTaskSpecs[trainv1alpha1.TaskTypeTorchWorker].NumTasks = lastReplicas

		if err := r.Client.Update(ctx, torchJob); err != nil {
			log.Info("failed to update current task replicas to last task replicas", "job", torchJob.Name)
		}
		// TODO: rename to pending task?
		updateStatusForPendingJob(torchJob, lastReplicas, trainv1alpha1.TaskTypeTorchWorker)
		if err := r.UpdateJobStatusInAPIServer(torchJob, &torchJob.Status); err != nil {
			if errors.IsConflict(err) {
				// this will happen when the update violates the etcd concurrent control
				log.Info("conflict happens when update torchelasticjob status", "job", torchJob.Name)
			}
		}
		return
	} else if (hasPendingPod && curReplicas == *torchJob.Spec.TorchElasticPolicy.NumMinReplicas) || hasFailedPod {
		log.Info("pods reach to running state is less than the min replicas settled, or exists pod failed",
			"job", torchJob.Name)
		defer cancel()
		delete(r.jobs, getElasticTorchJobName(torchJob.Name, torchJob.Namespace))
		// TODO: Why not delete the following?
		//delete(r.metrics, getElasticTorchJobName(torchJob.Name, torchJob.Namespace))
		return
	}

	if !hasPendingPod && jobStatus.TorchElasticStatuses != nil &&
		jobStatus.TorchElasticStatuses[trainv1alpha1.TaskTypeTorchWorker].Continue == false {
		// if job metrics have reached the maximum, restart the stale pods
		if jobStatus.TorchElasticStatuses[trainv1alpha1.TaskTypeTorchWorker].ElasticCondition == trainv1alpha1.TorchElasticMaxMetric {
			pods, err := r.GetPodsForJob(torchJob)
			if err != nil {
				log.Info("Failed to get the controlled pods", "job", torchJob.Name)
			}
			complete := r.restartStalePods(torchJob, pods)
			if !complete {
				log.Info("failed to restart the stale pods", "job", torchJob.Name)
				return
			}
			log.Info("restart stale pods succeeded", "job", torchJob.Name)

			// update job status
			jobStatus.TorchElasticStatuses[trainv1alpha1.TaskTypeTorchWorker].ElasticCondition = trainv1alpha1.TorchElasticStop
			if err = r.UpdateJobStatusInAPIServer(torchJob, jobStatus); err != nil {
				if errors.IsConflict(err) {
					// this will happen when the update violates the etcd concurrent control
					log.Info("conflict happens when update torchelasticjob status", "job", torchJob.Name)
					return
				}
			}
			return
		} else if jobStatus.TorchElasticStatuses[trainv1alpha1.TaskTypeTorchWorker].ElasticCondition == trainv1alpha1.TorchElasticStop ||
			jobStatus.TorchElasticStatuses[trainv1alpha1.TaskTypeTorchWorker].ElasticCondition == trainv1alpha1.TorchElasticMaxReplica {
			// if job replicas have reached the maximum, or the elastic condition is set as stop, just return
			log.Info("stop scaling because torchelastic condition is stop or the maximum replica is reached",
				"job", torchJob.Name)
			return
		}
	}

	// read the torchelastic training job log
	observe, err := getMetricsObservation(r.client, jobNamespace, GetDefaultWorkerName(jobName))
	if err != nil {
		log.Info("failed to read torchelastic training job log", err)
		return
	}

	// update metrics status
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if _, ok := r.metrics[jobName][curReplicas]; !ok {
		r.metrics[jobName][curReplicas] = make([]MetricObservation, 0)
	}
	r.metrics[jobName][curReplicas] = append(r.metrics[jobName][curReplicas], observe)
	numCurMetrics := len(r.metrics[jobName][curReplicas])
	log.Info("current metric length is %d", numCurMetrics, "job", torchJob.Name)

	// if current metrics have reached the metric count, judge the next scaling replicas
	if numCurMetrics >= r.metricCount {
		if curReplicas > *torchJob.Spec.TorchElasticPolicy.NumMinReplicas &&
			curReplicas <= *torchJob.Spec.TorchElasticPolicy.NumMaxReplicas {
			lastReplicas := jobStatus.TorchElasticStatuses[trainv1alpha1.TaskTypeTorchWorker].LastReplicas
			if r.IsSatisfyElasticContinue(jobName, curReplicas, lastReplicas) {
				if curReplicas == *torchJob.Spec.TorchElasticPolicy.NumMaxReplicas {
					updateStatusForMaxReplicaJob(torchJob, trainv1alpha1.TaskTypeTorchWorker)
					r.metrics[jobName][curReplicas] = make([]MetricObservation, 0)
				} else {
					newReplicas := computeNewReplicas(curReplicas)
					*torchJob.Spec.TorchTaskSpecs[trainv1alpha1.TaskTypeTorchWorker].NumTasks = newReplicas
					if err := r.Client.Update(ctx, torchJob); err != nil {
						log.Info("failed to torchjob status", "job", torchJob.Name)
					}
					updateStatusForContinueJob(torchJob, curReplicas, newReplicas, trainv1alpha1.TaskTypeTorchWorker)
					if _, ok := r.metrics[jobName][newReplicas]; !ok {
						r.metrics[jobName][newReplicas] = make([]MetricObservation, 0)
					}
				}
			} else {
				*torchJob.Spec.TorchTaskSpecs[trainv1alpha1.TaskTypeTorchWorker].NumTasks = lastReplicas
				if err := r.Client.Update(ctx, torchJob); err != nil {
					log.Info("failed to torchjob status", "job", torchJob.Name)
				}
				updateStatusForMaxMetricJob(torchJob, curReplicas, lastReplicas, trainv1alpha1.TaskTypeTorchWorker)
				r.metrics[jobName][lastReplicas] = make([]MetricObservation, 0)
				r.metrics[jobName][curReplicas] = make([]MetricObservation, 0)
			}
		} else if curReplicas == *torchJob.Spec.TorchElasticPolicy.NumMinReplicas &&
			curReplicas < *torchJob.Spec.TorchElasticPolicy.NumMaxReplicas {
			newReplicas := computeNewReplicas(curReplicas)
			*torchJob.Spec.TorchTaskSpecs[trainv1alpha1.TaskTypeTorchWorker].NumTasks = newReplicas
			if err := r.Client.Update(ctx, torchJob); err != nil {
				log.Info("failed to torchjob status", "job", torchJob.Name)
			}

			updateStatusForContinueJob(torchJob, curReplicas, newReplicas, trainv1alpha1.TaskTypeTorchWorker)
			if _, ok := r.metrics[jobName][newReplicas]; !ok {
				r.metrics[jobName][newReplicas] = make([]MetricObservation, 0)
			}

		} else if curReplicas == *torchJob.Spec.TorchElasticPolicy.NumMaxReplicas {
			updateStatusForMaxReplicaJob(torchJob, trainv1alpha1.TaskTypeTorchWorker)
			if _, ok := r.metrics[jobName][curReplicas]; ok {
				r.metrics[jobName][curReplicas] = make([]MetricObservation, 0)
			}
		}
	}

	if !reflect.DeepEqual(*oldStatus, torchJob.Status) {
		if err = r.UpdateJobStatusInAPIServer(torchJob, &torchJob.Status); err != nil {
			if errors.IsConflict(err) {
				// this will happen when the update violates the etcd concurrent control
				log.Info("conflict happens when update torchelasticjob status", "job", torchJob.Name)
				return
			}
		}
	}

	return
}

func (r *ElasticTorchJobReconciler) recreatePodContainers(job *trainv1alpha1.TorchJob, pod *corev1.Pod, generation string) error {
	crr := kruisev1alpha1.ContainerRecreateRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pod.Name,
			Namespace: pod.Namespace,
			Labels: map[string]string{
				trainv1alpha1.LabelGeneration: generation,
			},
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
		crr.Spec.Containers = append(crr.Spec.Containers, kruisev1alpha1.ContainerRecreateRequestContainer{
			Name: pod.Spec.Containers[ci].Name,
		})
	}

	return r.Client.Create(context.Background(), &crr)
}

// TODO: This func corresponds to scale()
func (r *ElasticTorchJobReconciler) restartStalePods(job *trainv1alpha1.TorchJob, pods []*corev1.Pod) (completed bool) {
	runningPods := filterRunningPods(pods)
	_, stalePods := train.FilterStalePodsByTaskType(runningPods, job.Generation,
		trainv1alpha1.TaskType(strings.ToLower(string(trainv1alpha1.TaskTypeAIMaster))))
	staleWorkers := stalePods[strings.ToLower(string(trainv1alpha1.TaskTypeTorchWorker))]
	totalReplicas := len(stalePods)
	workerNums := len(staleWorkers)
	log.Info("worker nums: %d", workerNums)

	if job.Annotations[train.AnnotationReadyToRestartWorker] == "false" {
		log.Info("torchjob does not need to restart workers")
		return false
	}

	tickets := 100 // max semaphore tickets limited.
	if len(staleWorkers) < 100 {
		tickets = len(staleWorkers)
	}
	sema := concurrent.NewSemaphore(tickets)
	for _, pod := range staleWorkers {
		sema.Acquire()

		go func(worker *corev1.Pod) {
			defer sema.Release()
			if completed, err := r.restartStalePod(job, worker, int64(totalReplicas), job.Generation); err != nil {
				log.Info("Restart worker %s failed because error %v", worker.Name, err)
			} else if completed {
				workerNums--
			}
		}(pod)
	}
	// block until all semaphore is released.
	sema.Wait()

	if workerNums != 0 {
		log.Info("refresh stale workers has not completed yet", "key", job.Namespace+"/"+job.Name)
		return false
	}

	if len(stalePods) == 0 || workerNums == 0 {
		log.Info("all pods are in latest generation, mark ready-to-start-worker as false")
		patch := patchutils.NewMergePatch()
		patch.InsertAnnotation(train.AnnotationReadyToRestartWorker, "false")

		if err := r.Client.Patch(context.Background(), job, patch); err != nil {
			log.Info("fail to patch torchJob: %v", err)
			return false
		}
		log.Info("torchjob %s/%s elastic scaling successfully finished, total replicas: %v", job.Namespace, job.Name, totalReplicas)
		completed = true
	}

	return completed
}

func (r *ElasticTorchJobReconciler) restartStalePod(job *trainv1alpha1.TorchJob, pod *corev1.Pod, wordSize, generation int64) (completed bool, err error) {
	expWorldSize := strconv.FormatInt(wordSize, 10)
	expGeneration := strconv.FormatInt(generation, 10)
	podKey := pod.Namespace + "/" + pod.Name

	if job.Annotations[train.AnnotationReadyToRestartWorker] == "true" && !controller.IsPodActive(pod) {
		err = r.Client.Delete(context.Background(), pod)
		return err == nil, err
	}

	if pod.Labels[trainv1alpha1.LabelGeneration] == expGeneration {
		return true, nil
	}

	log.Info("refresh stale pod to the latest generation", "pod", podKey, "generation", generation)

	// call kruise API to recreate the stale pod
	completed, err = r.restartPodInKruiseProtocol(job, pod, expWorldSize, expGeneration)
	if !completed {
		return false, err
	}

	// increment generation and mark the refresh complete
	patch := patchutils.NewStrategicPatch()
	patch.InsertLabel(trainv1alpha1.LabelGeneration, expGeneration)
	err = r.Client.Patch(context.Background(), pod, patch)
	if err != nil {
		return false, err
	}

	log.Info("refresh pod generation succeeded", "pod", podKey, "generation", generation)
	return true, nil
}

func (r *ElasticTorchJobReconciler) restartPodInKruiseProtocol(job *trainv1alpha1.TorchJob, pod *corev1.Pod,
	expectedWorldSize, expectedGeneration string) (completed bool, err error) {

	podKey := pod.Namespace + "/" + pod.Name
	crr := kruisev1alpha1.ContainerRecreateRequest{}
	if curWorldSize, ok := pod.Annotations[train.AnnotationWorldSize]; !ok || curWorldSize != expectedWorldSize {
		log.Info("update world size to the latest",
			"pod", podKey, "curWorldSize", curWorldSize, "expWorldSize", expectedWorldSize)
		patch := patchutils.NewStrategicPatch()
		patch.InsertAnnotation(train.AnnotationWorldSize, expectedWorldSize)
		if err = r.Client.Patch(context.Background(), pod, patch); err != nil {
			log.Error(err, "failed to update world size", "pod", podKey)
			return false, err
		}
		return false, nil
	}

	if err = r.Client.Get(context.Background(), types.NamespacedName{
		Namespace: pod.Namespace,
		Name:      pod.Name,
	}, &crr); err != nil {
		if errors.IsNotFound(err) {
			return false, r.recreatePodContainers(job, pod, expectedGeneration)
		}
		log.Error(err, "failed to get latest container-recreate-request for stale worker",
			"pod", podKey)
		return false, err
	}

	// crr created in previous round, clean it.
	if crr.Labels[trainv1alpha1.LabelGeneration] != expectedGeneration {
		if err = r.Client.Delete(context.Background(), &crr); err != nil {
			return false, err
		}
		return false, r.recreatePodContainers(job, pod, expectedGeneration)
	}

	if crr.Status.Phase == kruisev1alpha1.ContainerRecreateRequestFailed {
		log.Info("failed to restart containers of pod %s/%s, fallback to recreate pod", pod.Namespace, pod.Name)
		err = r.Client.Delete(context.Background(), pod)
		return err == nil, err
	}

	recreateDone := crr.Status.Phase == kruisev1alpha1.ContainerRecreateRequestCompleted ||
		crr.Status.Phase == kruisev1alpha1.ContainerRecreateRequestSucceeded
	if !recreateDone {
		klog.Error("container recreate request has not completed yet", "pod", podKey)
		return false, nil
	}

	// Finalize container-recreate-request object once it completes, because elastic scaling is repeatable
	// and 'crr' request will be re-initiated.
	defer func() {
		_ = r.Client.Delete(context.Background(), &crr)
	}()

	klog.Info("ContainerRecreateSucceed", "succeed to recreate containers in stale worker: %s", podKey)
	return true, nil
}

func (r *ElasticTorchJobReconciler) waitForAllPodsRunning(job *trainv1alpha1.TorchJob) (hasPendingPod, hasFailedPod bool) {
	pods, err := r.GetPodsForJob(job)
	if err != nil {
		log.Info("GetPodsForJob error: %v", err)
	}

	err = wait.PollImmediate(interval, podReadyTimeout, func() (done bool, err error) {
		for _, pod := range pods {
			if !podRunning(pod) {
				return false, nil
			}
		}
		return true, nil
	})

	if err != nil {
		log.Info("not all pods reach the running state")
	}

	for _, pod := range pods {
		if pod.Status.Phase == corev1.PodPending {
			hasPendingPod = true
			break
		}
	}

	for _, pod := range pods {
		if pod.Status.Phase == corev1.PodFailed {
			hasFailedPod = true
			break
		}
	}

	return
}

func filterRunningPods(pods []*corev1.Pod) []*corev1.Pod {
	var ret []*corev1.Pod
	for _, p := range pods {
		if podRunning(p) {
			ret = append(ret, p)
		} else {
			deletionTimeStamp := "N/A"
			if p.DeletionTimestamp != nil {
				deletionTimeStamp = p.DeletionTimestamp.String()
			}
			log.Info("Ignoring inactive pod %v/%v in state %v, deletion time %s",
				p.Namespace, p.Name, p.Status.Phase, deletionTimeStamp)
		}
	}
	return ret
}
