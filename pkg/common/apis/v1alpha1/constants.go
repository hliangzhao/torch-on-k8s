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

import corev1 "k8s.io/api/core/v1"

// ProjectPrefix is the global prefix
const ProjectPrefix = "distributed.io"

/* Job, task, output trained model, and resource related. */

const (
	TaskTypeAIMaster TaskType = "AIMaster"

	ResourceNvidiaGPU corev1.ResourceName = "nvidia.com/gpu"
)

const (
	LabelJobName   = "job-name"
	LabelGroupName = "group-name"
	LabelTaskIndex = "task-index"
	LabelTaskType  = "task-type"
	LabelTaskRole  = "task-role"

	LabelGangSchedulingJobName = ProjectPrefix + "/gang-job-name"

	LabelModelName            = "model." + ProjectPrefix + "/model-name"
	AnnotationImgBuildPodName = "model." + ProjectPrefix + "/img-build-pod-name"
)

/* Network mode related. */

// NetworkMode defines network mode for intra-job communicating
type NetworkMode string

const (
	AnnotationNetworkMode             = ProjectPrefix + "/network-mode"
	HostNetworkMode       NetworkMode = "host"
	// ContextHostNetworkPorts is the key for passing selected host-ports, value is
	// a map object {taskType-taskIndex: port}.
	ContextHostNetworkPorts = ProjectPrefix + "/hostnetwork-ports"
)

/* Elastic scaling related. */

const (
	AnnotationEnableElasticTraining = ProjectPrefix + "/enable-elastic-training"
	AnnotationElasticScaleState     = ProjectPrefix + "/scale-state"
	ElasticScaleStateInflight       = "inflight"
	ElasticScaleStateDone           = "done"
)

/* Pod deletion and failure related. */

const (
	// ContextFailedPodContents collects failed pod exit codes while with its failed
	// reason if they are not retryable
	ContextFailedPodContents = ProjectPrefix + "/failed-pod-contents"

	// LabelGeneration is used for elastic scaling to differentiate the generations (versions) of pods
	LabelGeneration = ProjectPrefix + "/job-generation"

	// FinalizerPreemptProtector is the finalizer added to the controlled pods by torchjob
	FinalizerPreemptProtector = ProjectPrefix + "/preempt-protector"
)

/* Local storage related. The following labels are used when local storage mode is chosen for model saving. */

type NodeStorageType string

const (
	LabelNodeStorageType     = ProjectPrefix + "/storage-type"
	LabelNodeStorageTypeFast = "fast"
)
