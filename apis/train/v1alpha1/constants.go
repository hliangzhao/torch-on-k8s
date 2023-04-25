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
	corev1 "k8s.io/api/core/v1"
)

// ProjectPrefix is the global prefix。
const ProjectPrefix = "distributed.io"

const (
	// ResourceNvidiaGPU is the resource name of nvidia GPU.
	ResourceNvidiaGPU corev1.ResourceName = "nvidia.com/gpu"
)

/* Constants used for differentiating jobs and tasks. */

const (
	LabelJobName   = "job-name"
	LabelGroupName = "group-name"
	LabelTaskIndex = "task-index"
	LabelTaskType  = "task-type" // TODO: What's the difference between task type and task role?
	LabelTaskRole  = "task-role"
)

/* Gang scheduling related. */

const (
	// LabelGangSchedulingJobName is used to mark the job to be scheduled by custom gang scheduler,
	// rather than the default kube-scheduler.
	LabelGangSchedulingJobName = ProjectPrefix + "/gang-job-name"
)

/* Model output related. */

const (
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

/* Elastic scaling related. This is used for the default scaler, and it has nothing to do with torchelastic. */

const (
	AnnotationEnableElasticTraining = ProjectPrefix + "/enable-elastic-training"
	AnnotationElasticScaleState     = ProjectPrefix + "/scale-state"
	ElasticScaleStateInflight       = "inflight"
	ElasticScaleStateDone           = "done"
	// LabelGeneration is used for elastic scaling to differentiate the generations (versions) of task pods.
	LabelGeneration = ProjectPrefix + "/job-generation"
)

/* Pod deletion and failure related. */

const (
	// ContextFailedPodContents collects failed pod exit codes while with its failed
	// reason if they are not retryable
	ContextFailedPodContents = ProjectPrefix + "/failed-pod-contents"

	// FinalizerPreemptProtector is the finalizer added to the controlled pods by torchjob
	FinalizerPreemptProtector = ProjectPrefix + "/preempt-protector"
)

/* Torch job specified constants. */

const (
	TorchJobKind = "TorchJob"

	// TorchJobDefaultPortName is name of the port used to communicate between master and workers.
	TorchJobDefaultPortName = "torchjob-port"

	// TorchJobDefaultContainerName is the name of the TorchJob container.
	TorchJobDefaultContainerName = "torch"

	// TorchJobDefaultPort is default value of the port.
	TorchJobDefaultPort = 23456

	// TorchJobDefaultMasterRestartPolicy is the default RestartPolicy for master task.
	TorchJobDefaultMasterRestartPolicy = RestartPolicyOnExitCode

	// TorchJobDefaultWorkerRestartPolicy is default RestartPolicy for worker task.
	TorchJobDefaultWorkerRestartPolicy = RestartPolicyOnFailure
)
