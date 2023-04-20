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

package common

import (
	"fmt"
	commonapis "github.com/hliangzhao/torch-on-k8s/pkg/common/apis/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"strings"
)

// genExpectationPodsKey returns the expected pod key for the given job task.
func genExpectationPodsKey(jobKey, taskType string) string {
	return jobKey + "/" + strings.ToLower(taskType) + "/pods"
}

// genExpectationServicesKey returns the expected service key for the given job task.
func genExpectationServicesKey(jobKey, taskType string) string {
	return jobKey + "/" + strings.ToLower(taskType) + "/services"
}

// recheckDeletionTimestamp returns a CanAdopt() function to recheck deletion.
//
// The CanAdopt() function calls getObject() to fetch the latest value,
// and denies adoption attempts if that object has a non-nil DeletionTimestamp.
func recheckDeletionTimestamp(getObject func() (metav1.Object, error)) func() error {
	return func() error {
		obj, err := getObject()
		if err != nil {
			return fmt.Errorf("can't recheck DeletionTimestamp: %v", err)
		}
		if obj.GetDeletionTimestamp() != nil {
			return fmt.Errorf("%v/%v has just been deleted at %v", obj.GetNamespace(), obj.GetName(), obj.GetDeletionTimestamp())
		}
		return nil
	}
}

// validateControllerRef checks the provided controllerRef is valid or not.
func validateControllerRef(controllerRef *metav1.OwnerReference) error {
	if controllerRef == nil {
		return fmt.Errorf("controllerRef is nil")
	}
	if len(controllerRef.APIVersion) == 0 {
		return fmt.Errorf("controllerRef has empty APIVersion")
	}
	if len(controllerRef.Kind) == 0 {
		return fmt.Errorf("controllerRef has empty Kind")
	}
	if controllerRef.Controller == nil || !*controllerRef.Controller {
		return fmt.Errorf("controllerRef.Controller is not set to true")
	}
	if controllerRef.BlockOwnerDeletion == nil || !*controllerRef.BlockOwnerDeletion {
		return fmt.Errorf("controllerRef.BlockOwnerDeletion is not set")
	}
	return nil
}

// filterPodsForTaskType filters the pods of the given taskType.
func (jc *JobController) filterPodsForTaskType(pods []*corev1.Pod, taskType string) ([]*corev1.Pod, error) {
	var ret []*corev1.Pod

	// create selector
	labelSelector := &metav1.LabelSelector{
		MatchLabels: map[string]string{},
	}
	labelSelector.MatchLabels[commonapis.LabelTaskType] = taskType
	selector, err := metav1.LabelSelectorAsSelector(labelSelector)

	// select pods by selector
	for _, pod := range pods {
		if err != nil {
			return nil, err
		}
		if !selector.Matches(labels.Set(pod.Labels)) {
			continue
		}
		ret = append(ret, pod)
	}

	return ret, nil
}

// mergeMap returns the merged map from the given maps.
func mergeMap(a, b map[string]string) map[string]string {
	if b == nil {
		return a
	}
	if a == nil {
		a = map[string]string{}
	}
	for k, v := range b {
		a[k] = v
	}
	return a
}
