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
	log "github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SatisfyExpectations returns true if the required adds/dels for the given job have been observed.
// Add/del counts are established by the controller at sync time, and updated as controllees are
// observed by the controller manager.
func (jc *JobController) SatisfyExpectations(job metav1.Object, tasks map[commonapis.TaskType]*commonapis.TaskSpec) bool {
	satisfied := true
	key, err := GetJobKey(job)
	if err != nil {
		log.Error(err, fmt.Sprintf("failed to get the key of job (%s/%s)", job.GetNamespace(), job.GetName()))
		return false
	}

	for taskType := range tasks {
		// Check the expectations of the pods.
		expectationPodsKey := genExpectationPodsKey(key, string(taskType))
		satisfied = satisfied && jc.Expectations.SatisfiedExpectations(expectationPodsKey)
	}

	for taskType := range tasks {
		// Check the expectations of the services, not all kinds of jobs create service for all
		// tasks, so return true when at least one service adds/dels observed.
		expectationServicesKey := genExpectationServicesKey(key, string(taskType))
		satisfied = satisfied || jc.Expectations.SatisfiedExpectations(expectationServicesKey)
	}
	return satisfied
}

func (jc *JobController) DeleteExpectations(job metav1.Object, tasks map[commonapis.TaskType]*commonapis.TaskSpec) {
	key, err := GetJobKey(job)
	if err != nil {
		log.Error(err, fmt.Sprintf("failed to get the key of job (%s/%s)", job.GetNamespace(), job.GetName()))
		return
	}

	for taskType := range tasks {
		expectationPodsKey := genExpectationPodsKey(key, string(taskType))
		expectationServicesKey := genExpectationServicesKey(key, string(taskType))
		jc.Expectations.DeleteExpectations(expectationPodsKey)
		jc.Expectations.DeleteExpectations(expectationServicesKey)
	}
	return
}
