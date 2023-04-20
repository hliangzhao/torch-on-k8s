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
	modelv1alpha1 "github.com/hliangzhao/torch-on-k8s/apis/model/v1alpha1"
	commonapis "github.com/hliangzhao/torch-on-k8s/pkg/common/apis/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TODO: Since we have only TorchJob, merge the common/apis/types.go here.

// TorchJobSpec defines the desired state of TorchJob
type TorchJobSpec struct {
	// RunPolicy encapsulates various runtime policies of the distributed training
	// job, for example how to clean up resources and how long the job can stay
	// active.
	commonapis.RunPolicy `json:",inline"`

	// TorchTaskSpecs is a map of TorchTaskType (type) to TorchTaskSpec (value).
	// It specifies the PyTorch cluster configuration.
	// For example,
	//   {
	//     "Master": TorchTaskSpec,
	//     "Worker": TorchTaskSpec,
	//   }
	TorchTaskSpecs map[commonapis.TaskType]*commonapis.TaskSpec `json:"torchTaskSpecs"`

	// ModelVersion represents the final output model of the training torchjob.
	// +optional
	ModelVersion *modelv1alpha1.ModelVersion `json:"modelVersion,omitempty"`
}

// TorchJobStatus defines the observed state of TorchJob
type TorchJobStatus struct{}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=`.status.conditions[-1:].type`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:printcolumn:name="Model-Version",type=string,JSONPath=`.status.modelVersionName`
// +kubebuilder:printcolumn:name="Max-Lifetime",type=integer,JSONPath=`.spec.activeDeadlineSeconds`
// +kubebuilder:printcolumn:name="TTL-After-Finished",type=integer,JSONPath=`.spec.ttlSecondsAfterFinished`

// TorchJob is the Schema for the torchjobs API
type TorchJob struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TorchJobSpec         `json:"spec,omitempty"`
	Status commonapis.JobStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// TorchJobList contains a list of TorchJob
type TorchJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []TorchJob `json:"items"`
}

func init() {
	SchemeBuilder.Register(&TorchJob{}, &TorchJobList{})
}
