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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

/* Define possible conditions of jobs. A job consists of multiple tasks. */

type JobConditionType string

const (
	JobCreated    JobConditionType = "Created"
	JobQueuing    JobConditionType = "Queuing"
	JobRunning    JobConditionType = "Running"
	JobRestarting JobConditionType = "Restarting"
	JobSucceed    JobConditionType = "Succeeded"
	JobFailed     JobConditionType = "Failed"
)

// JobCondition describes the possible conditions of a job.
// +k8s:deepcopy-gen=true
type JobCondition struct {
	Type JobConditionType `json:"type"`
	// Whether the condition type is True, or False, or Unknown
	Status  corev1.ConditionStatus `json:"status"`
	Reason  string                 `json:"reason,omitempty"`
	Message string                 `json:"message,omitempty"`
	// Last time the condition is updated
	LastUpdateTime metav1.Time `json:"lastUpdateTime,omitempty"`
	// Last time the condition status is transited
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty"`
}

/* Define possible status of tasks. A task consists of multiple pods (actually a task wraps only one pod). */

type TaskType string

type TaskStatus struct {
	Active    int32 `json:"active,omitempty"`
	Succeeded int32 `json:"succeed,omitempty"`
	Failed    int32 `json:"failed,omitempty"`
	// Evicted is a subset of Failed
	Evicted int32 `json:"evicted,omitempty"`
}

/* Define the features that might be used for different scheduling policies. */

type SchedulingPolicy struct {
	// Indicate the min number of tasks (pods) for Gang scheduling of a job
	MinAvailable *int32 `json:"minAvailable,omitempty"`

	// Higher the value, larger the priority
	// +optional
	Priority *int32 `json:"priority,omitempty"`

	// This field is required if you want to specify the priority.
	// You must create a corresponding PriorityClass object.
	// Otherwise, the priority will be zero or default.
	// +optional
	PriorityClassName string `json:"priorityClassName,omitempty"`

	// The name of the queue that this job should be enqueued into
	// +optional
	Queue string `json:"queue,omitempty"`
}

/* Define how the pods are cleaned when the corresponding job is finished. */

type CleanPodPolicy string

const (
	CleanPodPolicyRunning CleanPodPolicy = "Running"
	CleanPodPolicyNone    CleanPodPolicy = "None"
)

/* Define how the tasks (pods) are restarted. */

type RestartPolicy string

const (
	RestartPolicyAlways    RestartPolicy = "Always"
	RestartPolicyOnFailure RestartPolicy = "OnFailure"
	// RestartPolicyOnExitCode will restart tasks (pods) according to the exit code
	// specified by user:
	// - 1-127: permanent error, do not restart.
	// - 128-255: retryable error, do restart.
	RestartPolicyOnExitCode RestartPolicy = "ExitCode"
)

/* Define the runtime polices that might be used for the distributed training job. */

// RunPolicy encapsulates various runtime policies for distributed training jobs.
// +k8s:deepcopy-gen=true
type RunPolicy struct {
	// CleanPodPolicy defines the policy to kill pods after the job completes.
	// Default to CleanPodPolicyRunning.
	CleanPodPolicy *CleanPodPolicy `json:"clenPodPolicy,omitempty"`

	// TTLSecondsAfterFinished is the TTL to clean up jobs. Default value is inf.
	TTLSecondsAfterFinished *int32 `json:"TTLSecondsAfterFinished,omitempty"`

	// ActiveDurations is the duration in seconds since start time for the job to
	// be active before the system tries to terminate it.
	// +optional
	ActiveDurations *int64 `json:"activeDurations,omitempty"`

	// Number of retries before marking it as failed
	// +optional
	BackoffLimit *int32 `json:"backoffLimit,omitempty"`

	// +optional
	SchedulingPolicy *SchedulingPolicy `json:"schedulingPolicy,omitempty"`
}

/* Define how a task is triggered when it is a task in a workflow (DAG).
For example, the successful starting of the master is the trigger to start
the workers. */

type DAGCondition struct {
	// The type of the upstream trigger task
	UpstreamTaskType TaskType `json:"dependsOn"`

	// At which phase the upstream task will trigger this task
	OnPhase corev1.PodPhase `json:"onPhase"`
}

/* Spot tasks are tasks that allow interruptions. They can be preempted because
they have less SLO guarantee. */

type SpotTaskSpec struct {
	// NumSpotTasks is the number of desired spot tasks. Default zero.
	// The tasks whose index is in [NumTasks - NumSpotTasks, NumTasks - 1]
	// will be set as spot tasks.
	NumSpotTasks      int32  `json:"numSpotTasks,omitempty"`
	PriorityClassName string `json:"priorityClassName,omitempty"`
	// Extra labels for spot tasks
	Labels map[string]string `json:"labels,omitempty"`
}

/* The Task Spec. */

// TaskSpec is a description of tasks.
// +k8s:deepcopy-gen=true
type TaskSpec struct {
	NumTasks      *int32                 `json:"numTasks,omitempty"`
	SpotTaskSpec  *SpotTaskSpec          `json:"spotTaskSpec,omitempty"`
	Template      corev1.PodTemplateSpec `json:"template,omitempty"`
	RestartPolicy RestartPolicy          `json:"restartPolicy,omitempty"`
	// DependsOn is a list of upstream conditions for starting this task.
	// If not set, this will be set based on frameworks' requirements.
	// Enabled for default, and can be disabled with feature gate.
	DependsOn []DAGCondition `json:"-"`
}

/* The Job Status. */

// JobStatus represents the current observed state of jobs.
// +k8s:deepcopy-gen=true
type JobStatus struct {
	// Conditions is an array of current observed job conditions.
	Conditions []JobCondition `json:"conditions,omitempty"`

	// TaskStatuses is map of TaskType and TaskStatus,
	// specifies the status of each replica.
	TaskStatuses map[TaskType]*TaskStatus `json:"taskStatuses"`

	// The time the job is acknowledged by controller
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// The time the job completes
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// ModelVersionName represents the model version name output by this job run.
	ModelVersionName string `json:"modelVersionName,omitempty"`
}
