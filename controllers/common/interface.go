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
	"context"
	trainv1alpha1 "github.com/hliangzhao/torch-on-k8s/apis/train/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// ControllerInterface is the interface that the job controller should implement.
type ControllerInterface interface {
	ControllerName() string

	GetAPIGroupVersionKind() schema.GroupVersionKind
	GetAPIGroupVersion() schema.GroupVersion
	GetGroupName() string

	// GetJobFromInformerCache returns the job from Informer Cache.
	GetJobFromInformerCache(namespace, name string) (metav1.Object, error)

	// GetJobFromAPIClient returns the job from API server.
	GetJobFromAPIClient(namespace, name string) (metav1.Object, error)

	// GetPodsForJob returns the pods managed (controlled) by the job. This can
	// be achieved by selecting pods using label key "job-name". All pods created
	// by the job will come with label "job-name=<this_job_name>".
	GetPodsForJob(job interface{}) ([]*corev1.Pod, error)

	// GetServicesForJob returns the services managed (controlled) by the job.
	// This can be achieved by selecting services using label key "job-name".
	// All services created by the job will come with label "job-name=<this_job_name>".
	GetServicesForJob(job interface{}) ([]*corev1.Service, error)

	// GetNodeForModelOutput returns the node name where the output model is saved,
	// in case of local storage.
	GetNodeForModelOutput(pods []*corev1.Pod) (nodeName string)

	// DeleteJob deletes the job.
	DeleteJob(job interface{}) error

	// UpdateJobStatus updates the job status and job conditions.
	UpdateJobStatus(job interface{}, tasks map[trainv1alpha1.TaskType]*trainv1alpha1.TaskSpec, jobStatus *trainv1alpha1.JobStatus, restart bool) error

	// UpdateJobStatusInAPIServer updates the job status in API server.
	UpdateJobStatusInAPIServer(job interface{}, jobStatus *trainv1alpha1.JobStatus) error

	// SetClusterSpec sets the distributed training spec for the given job.
	SetClusterSpec(ctx context.Context, job interface{}, podTemplate *corev1.PodTemplateSpec, taskType, taskIndex string) error

	GetDefaultContainerName() string
	GetDefaultContainerPortName() string
	GetDefaultContainerPortNumber() int32

	GetTaskReconcilerOrders() []trainv1alpha1.TaskType

	// IsMaster checks whether master exists in the given tasks and whether the given
	// taskType is a master type.
	// A master-role pod will have "job-role=master" set in its label.
	IsMaster(tasks map[trainv1alpha1.TaskType]*trainv1alpha1.TaskSpec, taskType trainv1alpha1.TaskType) bool

	ElasticScaling
}

// ElasticScaling defines the methods that an elastic scaler should implement.
// TODO: Add the torchelastic functions here and merge it into the elastic_scale.go.
type ElasticScaling interface {
	EnableElasticScaling(job metav1.Object, runPolicy *trainv1alpha1.RunPolicy) bool

	// ScaleOut defines how to scale out a job instance (i.e. scale workers from n to 2*n),
	// usually the scaling progress is incremental and the implementation guarantees idempotence.
	ScaleOut(job interface{}, tasks map[trainv1alpha1.TaskType]*trainv1alpha1.TaskSpec, activePods []*corev1.Pod, activeServices []*corev1.Service) error

	// ScaleIn defines how to scale in a job instance (i.e. scale workers from 2*n to n),
	// usually the scaling progress is incremental and the implementation guarantees idempotence.
	ScaleIn(job interface{}, replicas map[trainv1alpha1.TaskType]*trainv1alpha1.TaskSpec, activePods []*corev1.Pod, activeServices []*corev1.Service) error

	// TriggerCheckpointIfNecessary triggers job checkpoints when it is necessary, e.g.
	// workers are going to be preempted after a grace termination period.
	TriggerCheckpointIfNecessary(job interface{}, activePods []*corev1.Pod) (completed bool, err error)
}
