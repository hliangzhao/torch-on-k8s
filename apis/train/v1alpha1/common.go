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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func init() {
	// We only register manually written functions here. The registration of the
	// generated functions takes place in the generated files. The separation
	// makes the code compile even when the generated files are missing.
	SchemeBuilder.SchemeBuilder.Register(addDefaultingFuncs)
}

func addDefaultingFuncs(scheme *runtime.Scheme) error {
	return RegisterDefaults(scheme)
}

// SchemeGroupVersion is provided for generate client.
var SchemeGroupVersion = GroupVersion

// Resource is provided for generated listers.
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

// ExtractMetaFieldsFromObject extracts the tasks, status, and scheduling policy form the given torch job.
// TODO: Should we extract `MinMember` out?
func ExtractMetaFieldsFromObject(obj client.Object) (tasks map[TaskType]*TaskSpec,
	status *JobStatus, schedulingPolicy *SchedulingPolicy) {

	torchJob, ok := obj.(*TorchJob)
	if !ok {
		return nil, nil, nil
	}
	return torchJob.Spec.TorchTaskSpecs, &torchJob.Status, torchJob.Spec.SchedulingPolicy
}
