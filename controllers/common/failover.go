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
	trainv1alpha1 "github.com/hliangzhao/torch-on-k8s/apis/train/v1alpha1"
	concurrentutils "github.com/hliangzhao/torch-on-k8s/pkg/utils/concurrent"
	patchutils "github.com/hliangzhao/torch-on-k8s/pkg/utils/patch"
	kruisev1alpha1 "github.com/openkruise/kruise/apis/apps/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/utils/pointer"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strconv"
	"time"
)

const (
	AnnotationLastFailoverTimestamp = trainv1alpha1.ProjectPrefix + "/last-failover-timestamp"
)

type FailoverAction string

const (
	FailoverInPlaceRestart FailoverAction = "InPlaceRestart"
	FailoverRecreate       FailoverAction = "Recreate"
)

// shouldPodFailover checks whether the input pod can be fail-overed or not.
// If the pod can be fail-overed, it will be re-created and re-scheduled.
func shouldPodFailover(taskSpec *trainv1alpha1.TaskSpec, pod *corev1.Pod, exitCode int32) bool {
	if pod.Status.Phase == corev1.PodFailed && exitCode == 0 {
		klog.Warning("pod status conflicts: pod failed with exit code 0")
	}

	if taskSpec.RestartPolicy != trainv1alpha1.RestartPolicyOnExitCode {
		return false
	}
	return isRetryableExitCode(exitCode) || isRetryablePodFailedReason(pod.Status.Reason)
}

// isRetryableExitCode returns true if the given exitCode indicates that the pod should be re-created and re-scheduled.
func isRetryableExitCode(exitCode int32) bool {
	if exitCode == 1 || exitCode == 2 || exitCode == 126 ||
		exitCode == 127 || exitCode == 128 || exitCode == 139 {
		// Refers to http://tldp.org/LDP/abs/html/exitcodes.html, we identify the following exit codes
		// as permanent errors:
		//   1: General errors
		//   2: Misuse of shell builtins
		//   126: Command invoked cannot execute
		//   127: Command not found
		//   128: Invalid argument to exit
		//   139 (128+11): terminated by SIGSEGV (Invalid memory reference)
		return false
	}

	if exitCode == 130 || exitCode == 137 || exitCode == 143 {
		// We think it's retryable error if the container exits due to the following sys signals
		// that are usually caused by transient issues(e.g. VM was rescheduled):
		//   130(128+2): Container terminated by Control-C
		//   137(128+9): Container received a SIGKILL
		//   143(128+15): Container received a SIGTERM
		// The exit code of container will be 128 + n for fatal error signals.
		// More info can be found in:
		//   http://tldp.org/LDP/abs/html/exitcodes.html,
		//   https://stackoverflow.com/questions/31297616/what-is-the-authoritative-list-of-docker-run-exit-codes
		return true
	}

	if exitCode == 138 {
		// We allow users to specify exit code for the cases that they think should retry.
		// We decide to take the exit code of SIGUSR1(138 = 128 + 10) for user defined retryable error.
		return true
	}

	// We make no guarantee for other exit status. Currently, handling them same as permanent errors.
	return false
}

// isRetryablePodFailedReason returns true if the corresponding pod is failed due to the specified reasons.
func isRetryablePodFailedReason(reason string) bool {
	return retryablePodFailedReasons.Has(reason)
}

var retryablePodFailedReasons = getRetryablePodFailedReasons()

func getRetryablePodFailedReasons() sets.String {
	return sets.NewString(
		// potential pod failed known reasons that can be fail-overed.
		"OOMKilled", "Killed", "Evicted", "UnexpectedAdmissionError",
	)
}

// doFailover recreates the pods in podsToFailover. Those pods will be deleted and created
// in the next reconciliation, then being assigned to some other node after schedule.
func (jc *JobController) doFailover(job client.Object, podsToFailover []*corev1.Pod) error {
	if err := jc.doFailOverByAction(job, podsToFailover, FailoverRecreate); err != nil {
		return err
	}
	jc.Recorder.Eventf(job, corev1.EventTypeNormal, "FailoverSucceed", "succeed to failover %d pods of job %s", len(podsToFailover), job.GetName())

	// add (update) failover annotation by patch
	patch := patchutils.NewMergePatch()
	patch.InsertAnnotation(AnnotationLastFailoverTimestamp, time.Now().Format(time.RFC3339))
	return jc.Client.Patch(context.Background(), job, patch)
}

// doFailOverByAction triggers failover by specified action, there are two different potential mechanism
// to resume the suspended job:
//
// - Recreate: delete anomalous one and create a new one, schedule to another node.
// - InPlaceRestart: restart containers of anomalous pod in-place and restart the process on same node.
func (jc *JobController) doFailOverByAction(job client.Object, podToFailover []*corev1.Pod, action FailoverAction) (err error) {
	klog.Infof("failover is triggered for job %s/%s, action: %v", job.GetNamespace(), job.GetName(), action)

	switch action {
	case FailoverRecreate:
		err = jc.recreatePods(job, podToFailover)
	case FailoverInPlaceRestart:
		err = jc.restartPods(job, podToFailover)
	}

	return err
}

// recreatePods deletes the pods in podsToFailover concurrently.
// These pods will be created automatically in next conciliation time since the expected status is not satisfied.
func (jc *JobController) recreatePods(job client.Object, podsToRecreate []*corev1.Pod) error {
	klog.Infof("job %s/%s need to recreate %v pods", job.GetNamespace(), job.GetName(), len(podsToRecreate))

	// limit max recreate parallelism as 50.
	tickets := 50
	if len(podsToRecreate) < tickets {
		tickets = len(podsToRecreate)
	}

	sema := concurrentutils.NewSemaphore(tickets)
	for _, pod := range podsToRecreate {
		sema.Acquire()
		go func(p *corev1.Pod) {
			defer sema.Release()
			klog.Infof("pod %s/%s need to be recreated", p.Namespace, p.Name)
			if err := jc.PodControl.DeletePod(p.Namespace, p.Name, job); err != nil {
				klog.Errorf("failed to recreate pod %s/%s, err: %v", p.Namespace, p.Name, err)
			}
		}(pod)
	}
	sema.Wait()

	return nil
}

// restartPods inplace restarts the given pods concurrently.
func (jc *JobController) restartPods(job client.Object, podsToRestart []*corev1.Pod) error {
	klog.Infof("job %s/%s need to restart %v pods", job.GetNamespace(), job.GetName(), len(podsToRestart))

	// limit max restart parallelism as 50.
	tickets := 50
	if len(podsToRestart) < tickets {
		tickets = len(podsToRestart)
	}

	total := len(podsToRestart)
	sema := concurrentutils.NewSemaphore(tickets)
	for _, pod := range podsToRestart {
		if !controller.IsPodActive(pod) {
			klog.Infof("pod %s/%s is not active and skip inplace restart", pod.Namespace, pod.Name)
			continue
		}

		sema.Acquire()
		go func(p *corev1.Pod) {
			defer sema.Release()
			if completed, err := jc.restartPod(job, p); err != nil {
				klog.Errorf("failed to restart pod, pod: %s, err: %v", p.Name, err)
			} else {
				if completed {
					total--
				}
			}
		}(pod)
	}
	sema.Wait()

	return nil
}

// restartPod inplace restarts the given pod of job with kruise API.
func (jc *JobController) restartPod(job client.Object, pod *corev1.Pod) (completed bool, err error) {
	podKey := fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)

	// TODO: Find where openkruise api is registered. Is openkruise necessary? Use default creation if possible.

	// Create crr resource in cluster firstly of non-exist because the inplace pod recreation is executed by crr.
	crr := kruisev1alpha1.ContainerRecreateRequest{}
	expectedGeneration := strconv.FormatInt(job.GetGeneration(), 10)
	err = jc.Client.Get(context.Background(), types.NamespacedName{
		Name:      pod.Name,
		Namespace: pod.Namespace,
	}, &crr)
	if err != nil {
		if errors.IsNotFound(err) {
			return false, jc.createContainerRecreateRequest(job, pod, expectedGeneration)
		}
		klog.Errorf("failed to get latest container-recreate-request for stale worker, pod: %s", podKey)
		return false, err
	}

	// Each inplace pod restart is handled by each crr resource individually. If the crr generation is not correct,
	// we need to create a new crr for this new pod restart.
	if crr.Labels[trainv1alpha1.LabelGeneration] != expectedGeneration {
		if err = jc.Client.Delete(context.Background(), &crr); err != nil {
			return false, err
		}
		return false, jc.createContainerRecreateRequest(job, pod, expectedGeneration)
	}

	// Now the crr is doing the inplace pod restart now...

	// if the inplace restart is failed, just delete the pod
	if crr.Status.Phase == kruisev1alpha1.ContainerRecreateRequestFailed {
		jc.Recorder.Eventf(pod, corev1.EventTypeWarning, "FailedRestartContainer",
			"failed to restart containers of pod %s/%s, fallback to recreate pod", pod.Namespace, pod.Name)
		err = jc.Client.Delete(context.Background(), pod)
		return false, err
	}

	recreateDone := crr.Status.Phase == kruisev1alpha1.ContainerRecreateRequestCompleted ||
		crr.Status.Phase == kruisev1alpha1.ContainerRecreateRequestSucceeded
	if !recreateDone {
		klog.Infof("container recreate request has not completed yet, pod: %v", podKey)
		return false, nil
	}

	// As we have mentioned, each inplace pod restart is handled by a unique crr resource.
	// Thus, when succeeded, just delete the crr resource in cluster.
	defer func() {
		_ = jc.Client.Delete(context.Background(), &crr)
	}()

	jc.Recorder.Eventf(pod, corev1.EventTypeNormal, "ContainerRecreateSucceed", "succeed to recreate containers in stale worker: %s", podKey)
	return true, nil
}

// createContainerRecreateRequest creates a kruise crr resource in cluster for handling the upcoming inplace pod recreation.
func (jc *JobController) createContainerRecreateRequest(job client.Object, pod *corev1.Pod, expectedGeneration string) error {
	crr := kruisev1alpha1.ContainerRecreateRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pod.Name,
			Namespace: pod.Namespace,
			Labels: map[string]string{
				trainv1alpha1.LabelJobName:    job.GetName(),
				trainv1alpha1.LabelGeneration: expectedGeneration,
			},
			// the to-be-created crr resource is controlled by the corresponding job and job pod
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion:         "v1",
					Kind:               "Pod",
					Name:               pod.Name,
					UID:                pod.UID,
					Controller:         pointer.BoolPtr(false),
					BlockOwnerDeletion: pointer.BoolPtr(true),
				},
				*metav1.NewControllerRef(job, job.GetObjectKind().GroupVersionKind()),
			},
		},
		Spec: kruisev1alpha1.ContainerRecreateRequestSpec{
			PodName: pod.Name,
			Strategy: &kruisev1alpha1.ContainerRecreateRequestStrategy{
				OrderedRecreate: false,
			},
		},
	}

	// add the containers int he to-be-created pod to crr
	for ci := range pod.Spec.Containers {
		c := &pod.Spec.Containers[ci]
		crr.Spec.Containers = append(crr.Spec.Containers, kruisev1alpha1.ContainerRecreateRequestContainer{
			Name: c.Name,
		})
	}

	// create crr resource in cluster
	return jc.Client.Create(context.Background(), &crr)
}
