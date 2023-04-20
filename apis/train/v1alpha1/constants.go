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
)

const (
	TorchJobKind = "TorchJob"

	// TorchJobDefaultPortName is name of the port used to communicate between master and workers.
	TorchJobDefaultPortName = "torchjob-port"

	// TorchJobDefaultContainerName is the name of the TorchJob container.
	TorchJobDefaultContainerName = "torch"

	// TorchJobDefaultPort is default value of the port.
	TorchJobDefaultPort = 23456

	// TorchJobDefaultMasterRestartPolicy is the default RestartPolicy for master task.
	TorchJobDefaultMasterRestartPolicy = commonapis.RestartPolicyOnExitCode

	// TorchJobDefaultWorkerRestartPolicy is default RestartPolicy for worker task.
	TorchJobDefaultWorkerRestartPolicy = commonapis.RestartPolicyOnFailure

	// TorchTaskTypeMaster is the type of Master of distributed PyTorch training job.
	TorchTaskTypeMaster commonapis.TaskType = "Master"

	// TorchTaskTypeWorker is the type for workers of distributed PyTorch training job.
	TorchTaskTypeWorker commonapis.TaskType = "Worker"
)
