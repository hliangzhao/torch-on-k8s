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
	"fmt"
	trainv1alpha1 "github.com/hliangzhao/torch-on-k8s/apis/train/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func getElasticTorchJobName(name, namespace string) string {
	return name + "-" + namespace
}

func (r *ElasticTorchJobReconciler) UpdateJobStatusInAPIServer(job interface{}, jobStatus *trainv1alpha1.JobStatus) error {
	elastictorchjob, ok := job.(*trainv1alpha1.TorchJob)
	if !ok {
		return fmt.Errorf("%+v is not a type of TorchJob", elastictorchjob)
	}
	var jobCopy *trainv1alpha1.TorchJob
	jobCopy = elastictorchjob.DeepCopy()
	jobCopy.Status = *jobStatus.DeepCopy()
	return r.Status().Update(context.Background(), jobCopy)
}

func initElasticTorchJobStatus(job *trainv1alpha1.TorchJob, taskType trainv1alpha1.TaskType) {
	jobStatus := &job.Status
	if jobStatus.TorchElasticStatuses == nil {
		jobStatus.TorchElasticStatuses = make(map[trainv1alpha1.TaskType]*trainv1alpha1.TorchElasticStatus)
	}

	jobStatus.TorchElasticStatuses[taskType] = &trainv1alpha1.TorchElasticStatus{ElasticCondition: trainv1alpha1.TorchElasticStart}
	jobStatus.TorchElasticStatuses[taskType].CurReplicas = *job.Spec.TorchTaskSpecs[taskType].NumTasks
	jobStatus.TorchElasticStatuses[taskType].Continue = true
	now := metav1.Now()
	jobStatus.TorchElasticStatuses[taskType].LastUpdateTime = &now
}

func updateStatusForPendingJob(job *trainv1alpha1.TorchJob, lastReplicas int32, taskType trainv1alpha1.TaskType) {
	jobStatus := &job.Status
	jobStatus.TorchElasticStatuses[taskType].Continue = false
	jobStatus.TorchElasticStatuses[taskType].LastReplicas = jobStatus.TorchElasticStatuses[trainv1alpha1.TaskTypeTorchWorker].CurReplicas
	jobStatus.TorchElasticStatuses[taskType].CurReplicas = lastReplicas
	jobStatus.TorchElasticStatuses[taskType].Message = "There exists pending pods, return to the last replicas"
	now := metav1.Now()
	jobStatus.TorchElasticStatuses[taskType].LastUpdateTime = &now
	jobStatus.TorchElasticStatuses[taskType].ElasticCondition = trainv1alpha1.TorchElasticStop
}

func updateStatusForContinueJob(job *trainv1alpha1.TorchJob, curReplicas, newReplicas int32, taskType trainv1alpha1.TaskType) {
	jobStatus := &job.Status
	jobStatus.TorchElasticStatuses[taskType].LastReplicas = curReplicas
	jobStatus.TorchElasticStatuses[taskType].CurReplicas = newReplicas
	jobStatus.TorchElasticStatuses[taskType].Message = "Pytorch job continues to be scaled"
	now := metav1.Now()
	jobStatus.TorchElasticStatuses[taskType].LastUpdateTime = &now
	jobStatus.TorchElasticStatuses[taskType].Continue = true
	jobStatus.TorchElasticStatuses[taskType].ElasticCondition = trainv1alpha1.TorchElasticContinue
}

func updateStatusForMaxReplicaJob(job *trainv1alpha1.TorchJob, taskType trainv1alpha1.TaskType) {
	jobStatus := &job.Status
	jobStatus.TorchElasticStatuses[taskType].Message = "Pytorch job has reached the max replicas"
	jobStatus.TorchElasticStatuses[taskType].Continue = false
	jobStatus.TorchElasticStatuses[taskType].ElasticCondition = trainv1alpha1.TorchElasticMaxReplica
}

func updateStatusForMaxMetricJob(job *trainv1alpha1.TorchJob, curReplicas, lastReplicas int32, taskType trainv1alpha1.TaskType) {
	jobStatus := &job.Status
	jobStatus.TorchElasticStatuses[taskType].CurReplicas = lastReplicas
	jobStatus.TorchElasticStatuses[taskType].LastReplicas = curReplicas
	jobStatus.TorchElasticStatuses[taskType].Message = "Pytorch job has reached the max metrics"
	now := metav1.Now()
	jobStatus.TorchElasticStatuses[taskType].LastUpdateTime = &now
	jobStatus.TorchElasticStatuses[taskType].Continue = false
	jobStatus.TorchElasticStatuses[taskType].ElasticCondition = trainv1alpha1.TorchElasticMaxMetric
}

func (r *ElasticTorchJobReconciler) IsSatisfyElasticContinue(jobName string, curReplicas, lastReplicas int32) bool {
	currentLength := r.metricCount
	currentLatency := r.metrics[jobName][curReplicas][currentLength-1].Latency
	lastReplicaLatency := r.metrics[jobName][lastReplicas][currentLength-1].Latency
	// Decide whether the elastic scaling can continue by the ratio of batch training latency and replicas.
	return lastReplicaLatency/float64(lastReplicas) > currentLatency/float64(curReplicas)
}

func computeNewReplicas(curReplicas int32) int32 {
	return curReplicas * 2
}
