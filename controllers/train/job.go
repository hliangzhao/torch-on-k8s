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
	"context"
	"errors"
	"fmt"
	trainv1alpha1 "github.com/hliangzhao/torch-on-k8s/apis/train/v1alpha1"
	"github.com/hliangzhao/torch-on-k8s/pkg/common"
	commonapis "github.com/hliangzhao/torch-on-k8s/pkg/common/apis/v1alpha1"
	"github.com/hliangzhao/torch-on-k8s/pkg/utils"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

/* Job related controls. */

func (r *TorchJobReconciler) GetJobFromInformerCache(namespace, name string) (metav1.Object, error) {
	job := &trainv1alpha1.TorchJob{}
	err := r.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: name}, job)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			log.Info("pytorch job not found", "namespace", namespace, "name", name)
		} else {
			log.Error(err, "failed to get job from api-server", "namespace", namespace, "name", name)
		}
		return nil, err
	}
	return job, nil
}

func (r *TorchJobReconciler) GetJobFromAPIClient(namespace, name string) (metav1.Object, error) {
	job := &trainv1alpha1.TorchJob{}
	err := r.jobController.APIReader.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: name}, job)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			log.Info("pytorch job not found", "namespace", namespace, "name", name)
		} else {
			log.Error(err, "failed to get job from api-server", "namespace", namespace, "name", name)
		}
		return nil, err
	}
	return job, nil
}

func (r *TorchJobReconciler) DeleteJob(job interface{}) error {
	torchJob, ok := job.(*trainv1alpha1.TorchJob)
	if !ok {
		return fmt.Errorf("%+v is not a TorchJob", torchJob)
	}
	if err := r.Delete(context.Background(), torchJob); err != nil {
		r.recorder.Eventf(torchJob, corev1.EventTypeWarning, common.FailedDeleteJobReason, "Error deleting: %v", err)
		log.Error(err, "failed to delete job", "namespace", torchJob.Namespace, "name", torchJob.Name)
		return err
	}
	r.recorder.Eventf(torchJob, corev1.EventTypeNormal, common.SuccessfulDeleteJobReason, "Deleted job: %v", torchJob.Name)
	log.Info("job deleted", "namespace", torchJob.Namespace, "name", torchJob.Name)
	return nil
}

func (r *TorchJobReconciler) UpdateJobStatus(job interface{}, tasks map[commonapis.TaskType]*commonapis.TaskSpec,
	jobStatus *commonapis.JobStatus, restart bool) error {

	torchJob, ok := job.(*trainv1alpha1.TorchJob)
	if !ok {
		return fmt.Errorf("%+v is not a type of TorchJob", torchJob)
	}
	return r.updateGeneralJobStatus(torchJob, tasks, jobStatus, restart)
}

func (r *TorchJobReconciler) UpdateJobStatusInAPIServer(job interface{}, jobStatus *commonapis.JobStatus) error {
	torchJob, ok := job.(*trainv1alpha1.TorchJob)
	if !ok {
		return fmt.Errorf("%+v is not a type of TorchJob", torchJob)
	}

	var jobCopy *trainv1alpha1.TorchJob
	jobCopy = torchJob.DeepCopy()
	jobCopy.Status = *jobStatus.DeepCopy()
	return r.Status().Update(context.Background(), jobCopy)
}

func (r *TorchJobReconciler) updateGeneralJobStatus(job *trainv1alpha1.TorchJob, taskSpecs map[commonapis.TaskType]*commonapis.TaskSpec,
	jobStatus *commonapis.JobStatus, restart bool) error {

	log.Info("Updating status", "TorchJob name", job.Name, "restart", restart)
	// Set job status start time since this job has acknowledged by controller.
	if jobStatus.StartTime == nil {
		now := metav1.Now()
		jobStatus.StartTime = &now
	}

	previousRestarting := utils.IsRestarting(*jobStatus)
	previousFailed := utils.IsFailed(*jobStatus)
	allWorkersSucceed := false
	workerTaskSpec, workerFound := taskSpecs[trainv1alpha1.TorchTaskTypeWorker]
	if workerFound {
		numSucceed := int32(0)
		if jobStatus.TaskStatuses[trainv1alpha1.TorchTaskTypeWorker] != nil {
			numSucceed = jobStatus.TaskStatuses[trainv1alpha1.TorchTaskTypeWorker].Succeeded
		}
		allWorkersSucceed = *workerTaskSpec.NumTasks == numSucceed
	}

	for taskType, taskSpec := range taskSpecs {
		numTasks := *taskSpec.NumTasks
		// If taskType in replica status not found, there must be a mistyped/invalid taskType in job spec,
		// and it has not been reconciled in previous processes, discard it.
		status, ok := jobStatus.TaskStatuses[taskType]
		if !ok {
			log.Info("skipping invalid replica type", "taskType", taskType)
			continue
		}
		expected := numTasks - status.Succeeded
		running := status.Active
		failed := status.Failed

		log.Info("Update pytorch job status", "PyTorchJob", job.Name,
			"ReplicaType", taskType, "expected", expected, "running", running, "failed", failed)

		if utils.ContainsTaskType(taskSpecs, trainv1alpha1.TorchTaskTypeMaster, commonapis.TaskTypeAIMaster) {
			if taskType == trainv1alpha1.TorchTaskTypeMaster || taskType == commonapis.TaskTypeAIMaster {
				if running > 0 {
					msg := fmt.Sprintf("TorchJob %s is running.", job.Name)
					err := utils.UpdateJobConditions(jobStatus, commonapis.JobRunning, utils.JobRunningReason, msg)
					if err != nil {
						log.Info("Append job condition", " error:", err)
						return err
					}
				}
				// Conditions for marking job as succeeded:
				// 1. master exit successfully with success policy is none.
				// 2. if success policy is AllWorkers, then wait util all workers succeed.
				// 3. aimaster is enabled and it exits successfully.
				succeed := numTasks > 0 && expected == 0
				if taskType != commonapis.TaskTypeAIMaster && workerFound {
					succeed = succeed && allWorkersSucceed
				}
				if succeed {
					msg := fmt.Sprintf("TorchJob %s is successfully completed.", job.Name)
					r.recorder.Event(job, corev1.EventTypeNormal, utils.JobSucceededReason, msg)
					if jobStatus.CompletionTime == nil {
						now := metav1.Now()
						jobStatus.CompletionTime = &now
					}
					err := utils.UpdateJobConditions(jobStatus, commonapis.JobSucceed, utils.JobSucceededReason, msg)
					if err != nil {
						log.Info("Append job condition", "error:", err)
						return err
					}
					r.jobController.Metrics.SuccessInc()
				}
			}
		} else {
			log.Info("Invalid config: Job must contain master task spec")
			return errors.New("invalid config: Job must contain master replica spec")
		}

		if failed > 0 {
			if restart && taskType != commonapis.TaskTypeAIMaster {
				msg := fmt.Sprintf("TorchJob %s is restarting because %d %s task(s) failed.", job.Name, failed, taskType)
				r.recorder.Event(job, corev1.EventTypeWarning, utils.JobRestartingReason, msg)
				err := utils.UpdateJobConditions(jobStatus, commonapis.JobRestarting, utils.JobRestartingReason, msg)
				if err != nil {
					log.Info("Append job condition", "error:", err)
					return err
				}
				if !previousRestarting {
					r.jobController.Metrics.FailureInc()
					r.jobController.Metrics.RestartInc()
				}
			} else {
				msg := fmt.Sprintf("TorchJob %s is failed because %d %s task(s) failed.", job.Name, failed, taskType)
				r.recorder.Event(job, corev1.EventTypeNormal, utils.JobFailedReason, msg)
				if jobStatus.CompletionTime == nil {
					now := metav1.Now()
					jobStatus.CompletionTime = &now
				}
				err := utils.UpdateJobConditions(jobStatus, commonapis.JobFailed, utils.JobFailedReason, msg)
				if err != nil {
					log.Info("Append job condition", "error: ", err)
					return err
				}
				if !previousFailed {
					r.jobController.Metrics.FailureInc()
				}
			}
		}
	}
	return nil
}
