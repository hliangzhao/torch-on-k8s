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
	"github.com/hliangzhao/torch-on-k8s/pkg/features"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/pointer"
	"strings"
)

/* Set default values of unspecified fields for newly created torchjobs. */

// SetDefaults_TorchJob sets unspecified values to defaults.
func SetDefaults_TorchJob(job *TorchJob) {
	// set clean pod policy as CleanPodPolicyNone, which means do nothing, if not set
	if job.Spec.CleanPodPolicy == nil {
		policy := CleanPodPolicyNone
		job.Spec.CleanPodPolicy = &policy
	}

	// formalize the task type names as "Master" and "Worker" (mAster, master --> Master, etc.)
	setDefaults_TorchJobName(job)

	if features.FeatureGates.Enabled(features.DAGScheduling) {
		// if DAG scheduling is enabled, set the DAGConditions strictly
		setDefaults_TorchJobDAGConditions(job)
	}

	for tt, ts := range job.Spec.TorchTaskSpecs {
		if tt == TaskTypeTorchWorker {
			// set worker's num as 1 if not set, and set the default restart policy for worker(s) if not set
			setDefaults_TorchJobWorkerNumTasks(ts)
		}
		if tt == TaskTypeTorchMaster {
			// set master's num as 1 if not set, and set the default restart policy for master if not set
			setDefaults_TorchJobMasterNumTasks(ts)
			// set the default port of the default container for the master task (pod) if not set
			setDefaults_TorchJobPort(&ts.Template.Spec)
		}
		// set termination message policy if not set
		setDefaults_TerminationMessagePolicy(&ts.Template.Spec)
	}

	// set default APIVersion and Kind if not set
	if job.APIVersion == "" {
		job.APIVersion = GroupVersion.String()
	}
	if job.Kind == "" {
		job.Kind = TorchJobKind
	}

	// if MinMember is not set, set them as NumTasks
	// this will be used only when both DAG scheduling and Gang scheduling are enabled
	if features.FeatureGates.Enabled(features.DAGScheduling) &&
		features.FeatureGates.Enabled(features.GangScheduling) &&
		job.Spec.MinMembers == nil {
		setDefaults_TorchJobMinMembers(job)
	}
}

// setDefaults_TorchJobTaskName formalizes the torchjob task name as the given taskType.
func setDefaults_TorchJobTaskName(job *TorchJob, taskType TaskType) {
	for tt := range job.Spec.TorchTaskSpecs {
		if strings.EqualFold(string(tt), string(taskType)) && tt != taskType {
			ts := job.Spec.TorchTaskSpecs[tt]
			delete(job.Spec.TorchTaskSpecs, tt)
			job.Spec.TorchTaskSpecs[taskType] = ts
			return
		}
	}
}

// setDefaults_TorchJobName formalizes the torchjob task name to "Master" (or "Worker").
func setDefaults_TorchJobName(job *TorchJob) {
	setDefaults_TorchJobTaskName(job, TaskTypeTorchMaster)
	setDefaults_TorchJobTaskName(job, TaskTypeTorchWorker)
}

// setDefaults_TorchJobDAGConditions sets the default DAG workflow dependency.
func setDefaults_TorchJobDAGConditions(job *TorchJob) {
	// DAG scheduling flow for pytorch job:
	//
	//  Master
	//  |--> Worker

	// Master task depends on the successful running of AIMaster task
	if job.Spec.TorchTaskSpecs[TaskTypeAIMaster] != nil &&
		job.Spec.TorchTaskSpecs[TaskTypeTorchMaster] != nil {

		job.Spec.TorchTaskSpecs[TaskTypeTorchMaster].DependsOn = []DAGCondition{
			{
				UpstreamTaskType: TaskTypeAIMaster,
				OnPhase:          corev1.PodRunning,
			},
		}
	}

	// Worker tasks depend on the successful running of Master task
	if job.Spec.TorchTaskSpecs[TaskTypeTorchWorker] != nil &&
		job.Spec.TorchTaskSpecs[TaskTypeTorchMaster] != nil {

		job.Spec.TorchTaskSpecs[TaskTypeTorchWorker].DependsOn = []DAGCondition{
			{
				UpstreamTaskType: TaskTypeTorchMaster,
				OnPhase:          corev1.PodRunning,
			},
		}
	}
}

// setDefaults_TorchJobMasterNumTasks sets the default task num for the master pod as 1.
// If restart policy is not set for master type, set it as TorchJobDefaultMasterRestartPolicy.
func setDefaults_TorchJobMasterNumTasks(spec *TaskSpec) {
	// TODO: Since the master num must be 1, we can forcibly set it as 1 here.
	if spec.NumTasks == nil {
		spec.NumTasks = pointer.Int32Ptr(1)
	}
	if spec.RestartPolicy == "" {
		spec.RestartPolicy = TorchJobDefaultMasterRestartPolicy
	}
}

// setDefaults_TorchJobWorkerNumTasks sets the default task num for the worker pod as 1.
// If restart policy is not set for worker type, set it as TorchJobDefaultWorkerRestartPolicy.
func setDefaults_TorchJobWorkerNumTasks(spec *TaskSpec) {
	if spec.NumTasks == nil {
		spec.NumTasks = pointer.Int32Ptr(1)
	}
	if spec.RestartPolicy == "" {
		spec.RestartPolicy = TorchJobDefaultWorkerRestartPolicy
	}
}

// setDefaults_TorchJobPort sets default port for the torch container in the given pod.
func setDefaults_TorchJobPort(spec *corev1.PodSpec) {
	// find the default container
	defaultContainerIdx := -1
	for idx, c := range spec.Containers {
		if c.Name == TorchJobDefaultContainerName {
			defaultContainerIdx = idx
			break
		}
	}

	if defaultContainerIdx < 0 {
		return
	}

	// set the port value of the default port of the default container if not set
	hasTorchJobPort := false
	for _, port := range spec.Containers[defaultContainerIdx].Ports {
		if port.Name == TorchJobDefaultPortName {
			hasTorchJobPort = true
			break
		}
	}
	if !hasTorchJobPort {
		spec.Containers[defaultContainerIdx].Ports = append(spec.Containers[defaultContainerIdx].Ports, corev1.ContainerPort{
			Name:          TorchJobDefaultPortName,
			ContainerPort: TorchJobDefaultPort,
		})
	}
}

// setDefaults_TerminationMessagePolicy sets the default termination message policy for the given pod spec.
func setDefaults_TerminationMessagePolicy(podSpec *corev1.PodSpec) {
	for ci := range podSpec.Containers {
		c := &podSpec.Containers[ci]
		if c.TerminationMessagePolicy == "" {
			c.TerminationMessagePolicy = corev1.TerminationMessageFallbackToLogsOnError
		}
	}
}

// setDefaults_TorchJobMinMembers sets the default min member for each task type when both Gang scheduling and
// DAG scheduling are enabled.
func setDefaults_TorchJobMinMembers(job *TorchJob) {
	for tt := range job.Spec.MinMembers {
		minMember := *job.Spec.TorchTaskSpecs[tt].NumTasks
		job.Spec.MinMembers[tt] = &minMember
	}
}
