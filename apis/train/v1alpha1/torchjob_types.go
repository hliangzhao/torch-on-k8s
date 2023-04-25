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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

/* A job consists of multiple tasks of different task types. Currently, task types include
"AIMAster", "Master", and "Worker". Tasks of the same type are managed in the same way: They
share the same pod template and restart policy. Different task instances of the same type are
differentiated by indices. A task is a wrapper of only one pod. A distributed torch training
job has only one master task but permitted to have multiple worker tasks. TODO: AIMaster is... */

/* Task type. */

// TaskType describes the type of task, including AIMaster, Master, and Worker.
type TaskType string

const (
	TaskTypeAIMaster TaskType = "AIMaster" // TODO: TaskTypeAIMaster is...?
	// TaskTypeTorchMaster is the type of Master of the distributed PyTorch training job.
	TaskTypeTorchMaster TaskType = "Master"
	// TaskTypeTorchWorker is the type for Worker of the distributed PyTorch training job.
	TaskTypeTorchWorker TaskType = "Worker"
)

/* Task spec. */

// SpotTaskSpec describe the spot tasks, which are tasks that allow interruptions.
// They can be preempted because they have less SLO guarantee.
// TODO: Understand how the preemption is implemented and how the pods resumed from
//  the checkpoints.
type SpotTaskSpec struct {
	// NumSpotTasks is the number of desired spot tasks. Default zero.
	// The tasks whose index is in [NumTasks - NumSpotTasks, NumTasks - 1]
	// will be set as spot tasks.
	NumSpotTasks int32 `json:"numSpotTasks,omitempty"`
	// This field is required if you want to specify the priority.
	// You must create a corresponding PriorityClass object.
	// Otherwise, the priority will be zero or default.
	PriorityClassName string `json:"priorityClassName,omitempty"`
	// Extra labels for spot tasks.
	Labels map[string]string `json:"labels,omitempty"`
}

// RestartPolicy describes how a task (pod) is restarted.
type RestartPolicy string

const (
	RestartPolicyAlways    RestartPolicy = "Always" // TODO: RestartPolicyAlways is ...?
	RestartPolicyOnFailure RestartPolicy = "OnFailure"
	// RestartPolicyOnExitCode will restart the certain task (pod) according
	// to the exit code specified by user:
	// - 1-127: permanent error, do not restart.
	// - 128-255: retryable error, do restart.
	RestartPolicyOnExitCode RestartPolicy = "ExitCode"
)

// DAGCondition describes how a task is triggered when it is a task in
// a workflow (DAG). For example, the successful starting of the master
// is the trigger to start the workers.
type DAGCondition struct {
	// The type of the upstream trigger task.
	UpstreamTaskType TaskType `json:"dependsOn"`
	// Which phase the upstream task reached to trigger this task.
	OnPhase corev1.PodPhase `json:"onPhase"`
}

// TaskSpec describes a collection of tasks for a certain type.
// +k8s:deepcopy-gen=true
type TaskSpec struct {
	// NumTasks is the number of tasks of this type.
	NumTasks *int32 `json:"numTasks,omitempty"`
	// The spot tasks' details of the certain task type.
	SpotTaskSpec *SpotTaskSpec `json:"spotTaskSpec,omitempty"`
	// A task wraps only one pod. We use Template to define
	// the pod template spec of the wrapped pod. For a certain
	// task type, all the tasks share the same pod template spec.
	Template corev1.PodTemplateSpec `json:"template,omitempty"`
	// The restart policy of these tasks. All the tasks share the same
	// restart policy.
	RestartPolicy RestartPolicy `json:"restartPolicy,omitempty"`
	// DependsOn is a list of upstream conditions for starting these tasks.
	// If not set, this will be set based on frameworks' requirements.
	// Enabled for default, and can be disabled with featuregate.
	DependsOn []DAGCondition `json:"-"`
}

/* Run policy of a job. */

// CleanPodPolicy describes how the pod is cleaned when the corresponding controller job completes.
type CleanPodPolicy string

const (
	// CleanPodPolicyRunning will clean up the controlled pods who are still in
	// Running state when the corresponding job completes.
	CleanPodPolicyRunning CleanPodPolicy = "Running"
	// CleanPodPolicyNone will do nothing when the corresponding job completes.
	CleanPodPolicyNone CleanPodPolicy = "None"
)

// SchedulingPolicy describes the scheduling policy for a job.
type SchedulingPolicy struct {
	// Indicate the min number of tasks (pods) for Gang scheduling of a job.
	MinAvailable *int32 `json:"minAvailable,omitempty"`
	// Priority of the job. Higher the value, larger the priority.
	// This value will be used when selecting a job from the queue.
	// +optional
	Priority *int32 `json:"priority,omitempty"`
	// This field is required if you want to specify the priority.
	// You must create a corresponding PriorityClass object.
	// Otherwise, the priority will be zero or default.
	// +optional
	PriorityClassName string `json:"priorityClassName,omitempty"`
	// The name of the queue that this job should be enqueued into.
	// +optional
	Queue string `json:"queue,omitempty"`
}

// RunPolicy encapsulates various runtime policies for a job.
// +k8s:deepcopy-gen=true
type RunPolicy struct {
	// CleanPodPolicy defines the policy to kill pods after the job completes.
	// Default to CleanPodPolicyNone.
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

/* Torchelastic-style elastic scaling policy. */

// TorchElasticPolicy is the native support for torchelastic style auto elastic scaling.
// (https://pytorch.org/docs/stable/elastic/quickstart.html)
type TorchElasticPolicy struct {
	// NumMinReplicas is the minimal number of task replicas of "Worker".
	// Default to nil.
	NumMinReplicas *int32 `json:"numMinReplicas,omitempty"`
	// NumMaxReplicas is the maximal number of task replicas of "Worker".
	// Default to nil.
	NumMaxReplicas *int32 `json:"numMaxReplicas,omitempty"`
	// RendezvousBackend is the backend of the Rendezvous server. For example, etcd.
	RendezvousBackend string `json:"rendezvousBackend"`
	// RendezvousEndpoint is the address of the Rendezvous server. For example, ETCD_HOST:ETCD_PORT.
	RendezvousEndpoint string `json:"rendezvousEndpoint"`
	// NProcPerNode is the number of worker tasks on each single node.
	NProcPerNode *int32 `json:"numWorkersPerNodePolicy,omitempty"`
}

/* TorchJob spec. */

// TorchJobSpec defines the desired state of TorchJob
type TorchJobSpec struct {
	// RunPolicy encapsulates various runtime policies of a job, for example,
	// how to clean up resources and how long the job can stay active, how to
	// schedule the job, etc.
	RunPolicy `json:",inline"`
	// TorchTaskSpecs is a map of TorchTaskType (type) to TorchTaskSpec (value).
	// It specifies the PyTorch cluster configuration.
	// For example,
	//   {
	//     "Master": Master's TaskSpec (number of master task must be 1),
	//     "Worker": Worker's TaskSpec (number of work tasks can > 1),
	//   }
	// All the tasks of the same type share the same pod template and restart policy.
	TorchTaskSpecs map[TaskType]*TaskSpec `json:"torchTaskSpecs"`
	// MinMembers describes the minimal number of successfully running tasks of each type
	// when both DAG scheduling and Gang scheduling are enabled. If not set, MinMembers
	// will be NumTasks for each task type in default.
	// +optional
	MinMembers map[TaskType]*int32 `json:"minMembers,omitempty"`
	// ModelVersion describes the final output model of the training torchjob.
	// +optional
	ModelVersion *modelv1alpha1.ModelVersion `json:"modelVersion,omitempty"`
	// TorchElastic is enabled or not.
	// +optional
	EnableTorchElastic bool `json:"enableTorchElastic,omitempty"`
	// TorchElasticPolicy is the config for torchelastic.
	// +optional
	TorchElasticPolicy *TorchElasticPolicy `json:"torchElasticPolicy,omitempty"`
}

/* Job condition. */

// JobConditionType describes the type of the job conditions.
type JobConditionType string

// These are valid conditions of a job.
const (
	JobCreated    JobConditionType = "Created"
	JobQueuing    JobConditionType = "Queuing"
	JobRunning    JobConditionType = "Running"
	JobRestarting JobConditionType = "Restarting"
	JobSucceed    JobConditionType = "Succeeded"
	JobFailed     JobConditionType = "Failed"
)

// JobCondition describes the state of a job at a certain point.
// Following https://github.com/kubernetes/api/blob/v0.22.6/apps/v1/types.go#L455.
// +k8s:deepcopy-gen=true
type JobCondition struct {
	// Type of job condition.
	Type JobConditionType `json:"type"`
	// Status of the condition, one of True, False, Unknown.
	Status corev1.ConditionStatus `json:"status"`
	// The last time this condition was updated.
	LastUpdateTime metav1.Time `json:"lastUpdateTime,omitempty"`
	// Last time the condition transitioned from one status to another.
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty"`
	// The reason for the condition's last transition.
	Reason string `json:"reason,omitempty"`
	// A human-readable message indicating details about the transition.
	Message string `json:"message,omitempty"`
}

/* Task status. */

// TaskStatus describes the status of tasks of a certain task type.
type TaskStatus struct {
	// Active is the number of active tasks of the certain task type.
	Active int32 `json:"active,omitempty"`
	// Succeeded is the number of succeeded tasks of the certain task type.
	Succeeded int32 `json:"succeed,omitempty"`
	// Failed is the number of failed tasks of the certain task type.
	Failed int32 `json:"failed,omitempty"`
	// Evicted is the number of failed tasks of the certain task type.
	// An Evicted task is also a Failed task.
	Evicted int32 `json:"evicted,omitempty"`
}

/* Torchelastic-style elastic scaling status. */

// TorchElasticConditionType describe the type of the elastic conditions for torchelastic.
type TorchElasticConditionType string

const (
	// TorchElasticStart means the elastic scaling has been started.
	TorchElasticStart TorchElasticConditionType = "Start"
	// TorchElasticStop means the elastic scaling has been stopped.
	TorchElasticStop TorchElasticConditionType = "Stop"
	// TorchElasticContinue means the elastic scaling continues.
	TorchElasticContinue TorchElasticConditionType = "Continue"
	// TorchElasticMaxMetric means the number of collected metrics have reached the maximum.
	TorchElasticMaxMetric TorchElasticConditionType = "ReachMaxMetric"
	// TorchElasticMaxReplica means the number of tasks (replicas) of task type "Worker" have reached the maximum.
	TorchElasticMaxReplica TorchElasticConditionType = "ReachMaxReplicas"
)

// TorchElasticStatus describes status of torchelastic.
// +k8s:deepcopy-gen=true
type TorchElasticStatus struct {
	// Type of elastic condition.
	ElasticCondition TorchElasticConditionType `json:"elasticCondition,omitempty"`
	// Whether the job continue scaling or not.
	Continue bool `json:"continue,omitempty"`
	// The number of current task replicas of "Worker".
	CurReplicas int32 `json:"curReplicas,omitempty"`
	// Last number of task replicas of "Worker".
	LastReplicas int32 `json:"lastReplicas,omitempty"`
	// The last time this status was updated.
	LastUpdateTime *metav1.Time `json:"lastUpdateTime,omitempty"`
	// A human-readable message indicating details about the transition.
	Message string `json:"message,omitempty"`
}

/* TorchJob status. */

// JobStatus describes current observed state of jobs.
// +k8s:deepcopy-gen=true
type JobStatus struct {
	// Conditions is an array of current observed job conditions.
	Conditions []JobCondition `json:"conditions,omitempty"`
	// TaskStatuses is a map of TaskType and TaskStatus,
	// specifies the status of each task type.
	TaskStatuses map[TaskType]*TaskStatus `json:"taskStatuses"`
	// TorchElasticStatuses is a map from TaskType to TorchElasticStatus,
	// specifies the torchelastic status of each task type.
	TorchElasticStatuses map[TaskType]*TorchElasticStatus `json:"elasticScalingStatues,omitempty"`
	// StartTime is the time the job is acknowledged by controller.
	StartTime *metav1.Time `json:"startTime,omitempty"`
	// CompletionTime is the time the job completes.
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`
	// ModelVersionName represents the name of the modelversion output by this (torch training) job.
	ModelVersionName string `json:"modelVersionName,omitempty"`
}

// TorchJobStatus defines the observed state of TorchJob.
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

// TorchJob is the Schema for the torchjobs API.
type TorchJob struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TorchJobSpec `json:"spec,omitempty"`
	Status JobStatus    `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// TorchJobList contains a list of TorchJob.
type TorchJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []TorchJob `json:"items"`
}

func init() {
	SchemeBuilder.Register(&TorchJob{}, &TorchJobList{})
}
