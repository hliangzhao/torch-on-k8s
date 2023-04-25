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

package resources

import (
	trainv1alpha1 "github.com/hliangzhao/torch-on-k8s/apis/train/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	quotav1 "k8s.io/apiserver/pkg/quota/v1"
)

/* This module provides the calculation of resource quota for jobs and tasks. */

// Multiply multiplies resources with given factor for each named resource.
func Multiply(factor int64, resourceList corev1.ResourceList) corev1.ResourceList {
	ret := corev1.ResourceList{}
	for key, value := range resourceList {
		scaled := value
		scaled.Set(factor * scaled.Value())
		ret[key] = scaled
	}
	return ret
}

// AnyLessThan returns true if there is a key such that a[key] < b[key].
func AnyLessThan(a corev1.ResourceList, b corev1.ResourceList) (bool, []corev1.ResourceName) {
	var (
		resourceNames []corev1.ResourceName
		result        = false
	)
	for key, value := range b {
		if other, found := a[key]; found {
			if other.Cmp(value) < 0 {
				result = true
				resourceNames = append(resourceNames, key)
			}
		}
	}
	return result, resourceNames
}

// ComputePodSpecResourceRequest returns the requested resource of the PodSpec.
func ComputePodSpecResourceRequest(spec *corev1.PodSpec) corev1.ResourceList {
	ret := corev1.ResourceList{}
	for _, container := range spec.Containers {
		ret = quotav1.Add(ret, container.Resources.Requests)
	}
	// take max (sum_pod, any_init_container)
	// this is the init containers are executed sequentially
	for _, container := range spec.InitContainers {
		ret = quotav1.Max(ret, container.Resources.Requests)
	}
	// If overhead is being utilized, add to the total requests for the pod
	if spec.Overhead != nil {
		ret = quotav1.Add(ret, spec.Overhead)
	}
	return ret
}

// TaskResourceRequests returns the resource quota for a collection of tasks of a certain task type.
func TaskResourceRequests(taskSpec *trainv1alpha1.TaskSpec) corev1.ResourceList {
	request := ComputePodSpecResourceRequest(&taskSpec.Template.Spec)
	numTasks := int32(1)
	if taskSpec.NumTasks != nil {
		numTasks = *taskSpec.NumTasks
	}
	return Multiply(int64(numTasks), request)
}

// MinTaskResourceRequests returns the minimal resource quota for a collection of tasks of a certain task type.
func MinTaskResourceRequests(taskSpec *trainv1alpha1.TaskSpec, minMember int32) corev1.ResourceList {
	request := ComputePodSpecResourceRequest(&taskSpec.Template.Spec)
	return Multiply(int64(minMember), request)
}

// JobResourceRequests returns the resource quota of normal tasks and spot tasks for the given job (tasks).
func JobResourceRequests(tasks map[trainv1alpha1.TaskType]*trainv1alpha1.TaskSpec) (normal, spot corev1.ResourceList) {
	for _, ts := range tasks {
		request := ComputePodSpecResourceRequest(&ts.Template.Spec)
		numTasks := int32(1)
		if ts.NumTasks != nil {
			numTasks = *ts.NumTasks
		}
		if ts.SpotTaskSpec != nil && ts.SpotTaskSpec.NumSpotTasks > 0 {
			numTasks -= ts.SpotTaskSpec.NumSpotTasks
			spotDelta := Multiply(int64(ts.SpotTaskSpec.NumSpotTasks), request)
			spot = quotav1.Add(spot, spotDelta)
			if numTasks < 0 {
				numTasks = 0
			}
		}
		request = Multiply(int64(numTasks), request)
		normal = quotav1.Add(normal, request)
	}
	return normal, spot
}
