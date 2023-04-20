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

package v1alpha1

import (
	commonapis "github.com/hliangzhao/torch-on-k8s/pkg/common/apis/v1alpha1"
	"github.com/hliangzhao/torch-on-k8s/pkg/features"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/pointer"
	"strings"
)

/* Set default values for newly created torchjobs. */

// SetDefaults_TorchJob sets unspecified values to defaults.
func SetDefaults_TorchJob(job *TorchJob) {
	if job.Spec.CleanPodPolicy == nil {
		policy := commonapis.CleanPodPolicyNone
		job.Spec.CleanPodPolicy = &policy
	}

	setDefaults_TorchJobName(job)

	if features.FeatureGates.Enabled(features.DAGScheduling) {
		setDefaults_TorchJobDAGConditions(job)
	}

	for tt, ts := range job.Spec.TorchTaskSpecs {
		if tt == TorchTaskTypeWorker {
			setDefaults_TorchJobWorkerReplicas(ts)
		}
		if tt == TorchTaskTypeMaster {
			setDefaults_TorchJobMasterReplicas(ts)
			setDefaults_TorchJobPort(&ts.Template.Spec)
		}
		setDefaults_TerminationMessagePolicy(&ts.Template.Spec)
	}

	if job.Kind == "" {
		job.Kind = TorchJobKind
	}

	if job.APIVersion == "" {
		job.APIVersion = GroupVersion.String()
	}
}

// setDefaults_TorchJobPort sets default port for the torch container in the given pod.
func setDefaults_TorchJobPort(spec *corev1.PodSpec) {
	// find the torch container
	index := -1
	for idx, c := range spec.Containers {
		if c.Name == TorchJobDefaultContainerName {
			index = idx
			break
		}
	}

	if index < 0 {
		return
	}

	// Check the torch container has port set or not.
	// If not exist, create it.
	hasTorchJobPort := false
	for _, port := range spec.Containers[index].Ports {
		if port.Name == TorchJobDefaultPortName {
			hasTorchJobPort = true
			break
		}
	}
	if !hasTorchJobPort {
		spec.Containers[index].Ports = append(spec.Containers[index].Ports, corev1.ContainerPort{
			Name:          TorchJobDefaultPortName,
			ContainerPort: TorchJobDefaultPort,
		})
	}
}

// setDefaults_TorchJobMasterReplicas sets the default task num for the master pod as 1.
// TODO: Support multi-master training architecture.
func setDefaults_TorchJobMasterReplicas(spec *commonapis.TaskSpec) {
	if spec.NumTasks == nil {
		spec.NumTasks = pointer.Int32Ptr(1)
	}
	if spec.RestartPolicy == "" {
		spec.RestartPolicy = TorchJobDefaultMasterRestartPolicy
	}
}

// setDefaults_TorchJobWorkerReplicas sets the default task num for the worker pod as 1.
func setDefaults_TorchJobWorkerReplicas(spec *commonapis.TaskSpec) {
	if spec.NumTasks == nil {
		spec.NumTasks = pointer.Int32Ptr(1)
	}
	if spec.RestartPolicy == "" {
		spec.RestartPolicy = TorchJobDefaultWorkerRestartPolicy
	}
}

// setDefaults_TorchJobTaskName sets the torchjob task name as the given one ("Master" or "Worker").
func setDefaults_TorchJobTaskName(job *TorchJob, taskType commonapis.TaskType) {
	for tt := range job.Spec.TorchTaskSpecs {
		if strings.EqualFold(string(tt), string(taskType)) && tt != taskType {
			ts := job.Spec.TorchTaskSpecs[tt]
			delete(job.Spec.TorchTaskSpecs, tt)
			job.Spec.TorchTaskSpecs[taskType] = ts
			return
		}
	}
}

// setDefaults_TorchJobName sets the torchjob name as the given one.
func setDefaults_TorchJobName(job *TorchJob) {
	setDefaults_TorchJobTaskName(job, TorchTaskTypeMaster)
	setDefaults_TorchJobTaskName(job, TorchTaskTypeWorker)
}

// setDefaults_TorchJobDAGConditions sets the default DAG workflow dependency.
func setDefaults_TorchJobDAGConditions(job *TorchJob) {
	// DAG scheduling flow for pytorch job:
	//
	//  Master
	//  |--> Worker

	// Master task depends on AIMaster
	if job.Spec.TorchTaskSpecs[commonapis.TaskTypeAIMaster] != nil &&
		job.Spec.TorchTaskSpecs[TorchTaskTypeMaster] != nil {

		job.Spec.TorchTaskSpecs[TorchTaskTypeMaster].DependsOn = []commonapis.DAGCondition{
			{
				UpstreamTaskType: commonapis.TaskTypeAIMaster,
				OnPhase:          corev1.PodRunning,
			},
		}
	}

	// Worker tasks depend on Master task
	if job.Spec.TorchTaskSpecs[TorchTaskTypeWorker] != nil &&
		job.Spec.TorchTaskSpecs[TorchTaskTypeMaster] != nil {

		job.Spec.TorchTaskSpecs[TorchTaskTypeWorker].DependsOn = []commonapis.DAGCondition{
			{
				UpstreamTaskType: TorchTaskTypeMaster,
				OnPhase:          corev1.PodRunning,
			},
		}
	}
}

func setDefaults_TerminationMessagePolicy(podSpec *corev1.PodSpec) {
	for ci := range podSpec.Containers {
		c := &podSpec.Containers[ci]
		if c.TerminationMessagePolicy == "" {
			c.TerminationMessagePolicy = corev1.TerminationMessageFallbackToLogsOnError
		}
	}
}
