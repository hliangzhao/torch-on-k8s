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

package utils

import (
	trainv1alpha1 "github.com/hliangzhao/torch-on-k8s/apis/train/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"strings"
)

/* Job, task, and pod checking & counting. */

// GetTotalTasks returns the total number of tasks.
func GetTotalTasks(tasks map[trainv1alpha1.TaskType]*trainv1alpha1.TaskSpec) int32 {
	ret := int32(0)
	for _, ts := range tasks {
		ret += *ts.NumTasks
	}
	return ret
}

// ContainsTaskType returns true if the given tasks have at least one task whose type is in taskTypes.
func ContainsTaskType(tasks map[trainv1alpha1.TaskType]*trainv1alpha1.TaskSpec, taskTypes ...trainv1alpha1.TaskType) bool {
	for _, tt := range taskTypes {
		if _, ok := tasks[tt]; ok {
			return true
		}
	}
	return false
}

// GetTotalExcludedTasks returns the total number of tasks with the excluded task types uncounted.
func GetTotalExcludedTasks(tasks map[trainv1alpha1.TaskType]*trainv1alpha1.TaskSpec, excludes ...trainv1alpha1.TaskType) int32 {
	excludeTaskTypes := sets.NewString()
	for _, e := range excludes {
		excludeTaskTypes.Insert(string(e))
	}

	ret := int32(0)
	for tt, ts := range tasks {
		if excludeTaskTypes.Has(string(tt)) {
			continue
		}
		ret += *ts.NumTasks
	}
	return ret
}

// HasFinalizer checks whether the target finalizer exists in the given finalizers.
func HasFinalizer(finalizers []string, target string) bool {
	for _, f := range finalizers {
		if f == target {
			return true
		}
	}
	return false
}

func GenGeneralName(jobName, taskType, taskIndex string) string {
	return strings.Replace(jobName+"-"+taskType+"-"+taskIndex, "/", "-", -1)
}

/* Job status and condition related. */

const (
	// JobCreatedReason is added in a job when it is created.
	JobCreatedReason = "JobCreated"

	// JobSucceededReason is added in a job when it is succeeded.
	JobSucceededReason = "JobSucceeded"

	// JobRunningReason is added in a job when it is running.
	JobRunningReason = "JobRunning"

	// JobFailedReason is added in a job when it is failed.
	JobFailedReason = "JobFailed"

	// JobRestartingReason is added in a job when it is restarting.
	JobRestartingReason = "JobRestarting"

	// JobEnqueuedReason is added in a job when it is queuing and being enqueued.
	JobEnqueuedReason = "JobEnqueued"

	// JobDequeuedReason is added in a job when it is queuing and being dequeued.
	JobDequeuedReason = "JobDequeued"
)

// IsSucceeded checks if the job is succeeded.
func IsSucceeded(status trainv1alpha1.JobStatus) bool {
	return hasCondition(status, trainv1alpha1.JobSucceed)
}

// IsFailed checks if the job is failed.
func IsFailed(status trainv1alpha1.JobStatus) bool {
	return hasCondition(status, trainv1alpha1.JobFailed)
}

// IsRunning checks if the job is running.
func IsRunning(status trainv1alpha1.JobStatus) bool {
	return hasCondition(status, trainv1alpha1.JobRunning)
}

// IsCreated checks if the job has created.
func IsCreated(status trainv1alpha1.JobStatus) bool {
	return hasCondition(status, trainv1alpha1.JobCreated)
}

// IsRestarting checks if the job is restarting.
func IsRestarting(status trainv1alpha1.JobStatus) bool {
	return hasCondition(status, trainv1alpha1.JobRestarting)
}

// UpdateJobConditions adds to the jobStatus a new condition if needed, with the conditionType, reason, and message.
func UpdateJobConditions(jobStatus *trainv1alpha1.JobStatus, conditionType trainv1alpha1.JobConditionType, reason, message string) error {
	condition := newCondition(conditionType, reason, message)
	setCondition(jobStatus, condition)
	return nil
}

// NeedEnqueueToCoordinator checks if the job need to be enqueued into
// coordinator if feature-gate is enabled
func NeedEnqueueToCoordinator(status trainv1alpha1.JobStatus) bool {
	return len(status.Conditions) == 0 || isJustCreated(status) || IsEnqueued(status)
}

// IsEnqueued checks whether the given job is enqueued.
func IsEnqueued(status trainv1alpha1.JobStatus) bool {
	cond := getLastCondition(status, trainv1alpha1.JobQueuing)
	if cond != nil && cond.Reason == JobEnqueuedReason {
		return true
	}
	return false
}

// newCondition creates a new job condition.
func newCondition(conditionType trainv1alpha1.JobConditionType, reason, message string) trainv1alpha1.JobCondition {
	return trainv1alpha1.JobCondition{
		Type:               conditionType,
		Status:             corev1.ConditionTrue,
		LastUpdateTime:     metav1.Now(),
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
		Message:            message,
	}
}

// hasCondition checks whether the job is in the given condition.
func hasCondition(status trainv1alpha1.JobStatus, condType trainv1alpha1.JobConditionType) bool {
	for _, condition := range status.Conditions {
		if condition.Type == condType && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// getCondition returns the condition with the provided type.
func getCondition(status trainv1alpha1.JobStatus, condType trainv1alpha1.JobConditionType) *trainv1alpha1.JobCondition {
	for _, condition := range status.Conditions {
		if condition.Type == condType {
			return &condition
		}
	}
	return nil
}

// setCondition updates the job to include the provided condition.
// If the condition that we are about to add already exists
// and has the same status and reason then we are not going to update.
func setCondition(status *trainv1alpha1.JobStatus, condition trainv1alpha1.JobCondition) {
	// Do nothing if JobStatus have failed condition
	if IsFailed(*status) || IsSucceeded(*status) {
		return
	}

	currentCond := getCondition(*status, condition.Type)

	// Do nothing if condition doesn't change
	if currentCond != nil && currentCond.Status == condition.Status && currentCond.Reason == condition.Reason {
		return
	}

	// Do not update lastTransitionTime if the status of the condition doesn't change.
	if currentCond != nil && currentCond.Status == condition.Status {
		condition.LastTransitionTime = currentCond.LastTransitionTime
	}

	// Append the updated condition to the conditions
	newConditions := filterOutCondition(status.Conditions, condition.Type)
	status.Conditions = append(newConditions, condition)
}

func getLastCondition(status trainv1alpha1.JobStatus, condType trainv1alpha1.JobConditionType) *trainv1alpha1.JobCondition {
	if len(status.Conditions) == 0 {
		return nil
	}
	condition := status.Conditions[len(status.Conditions)-1]
	if condition.Type == condType {
		return &condition
	}
	return nil
}

// filterOutCondition returns a new slice of job conditions without conditions with the provided type.
func filterOutCondition(conditions []trainv1alpha1.JobCondition, condType trainv1alpha1.JobConditionType) []trainv1alpha1.JobCondition {
	var newConditions []trainv1alpha1.JobCondition
	for _, c := range conditions {
		if condType == trainv1alpha1.JobRestarting && c.Type == trainv1alpha1.JobRunning {
			continue
		}
		if condType == trainv1alpha1.JobRunning && c.Type == trainv1alpha1.JobRestarting {
			continue
		}

		if c.Type == condType {
			continue
		}

		// Set the running condition status to be false when current condition failed or succeeded
		if (condType == trainv1alpha1.JobFailed || condType == trainv1alpha1.JobSucceed) && c.Type == trainv1alpha1.JobRunning {
			c.Status = corev1.ConditionFalse
		}

		newConditions = append(newConditions, c)
	}
	return newConditions
}

// isJustCreated checks if the job has created.
func isJustCreated(status trainv1alpha1.JobStatus) bool {
	return getLastCondition(status, trainv1alpha1.JobCreated) != nil
}
