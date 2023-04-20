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

package coordinator

import (
	"errors"
	"fmt"
	commonapis "github.com/hliangzhao/torch-on-k8s/pkg/common/apis/v1alpha1"
	"github.com/hliangzhao/torch-on-k8s/pkg/utils/resources"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/pointer"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strings"
	"time"
)

// CoordinateConfiguration gives the config of the coordinator.
type CoordinateConfiguration struct {
	SchedulingPeriod  time.Duration
	TenantPlugin      string
	PreFilterPlugins  []string
	FilterPlugins     []string
	ScorePlugins      []string
	PreDequeuePlugins []string
}

/* Queue unit. */

// QueueUnit wraps a job.
type QueueUnit struct {
	Tenant           string
	Priority         *int32
	Job              client.Object
	SchedulingPolicy *commonapis.SchedulingPolicy
	Tasks            map[commonapis.TaskType]*commonapis.TaskSpec
	JobStatus        *commonapis.JobStatus
	Resources        corev1.ResourceList
	SportResources   corev1.ResourceList
	Owner            workqueue.RateLimitingInterface
}

func (qu *QueueUnit) Key() string {
	return fmt.Sprintf("%s/%s/%s",
		qu.Job.GetObjectKind().GroupVersionKind().Kind, qu.Job.GetNamespace(), qu.Job.GetName())
}

// ToQueueUnit creates a QueueUnit instance with the given job info.
func ToQueueUnit(job client.Object, tasks map[commonapis.TaskType]*commonapis.TaskSpec, jobStatus *commonapis.JobStatus,
	schedulingPolicy *commonapis.SchedulingPolicy) *QueueUnit {

	qu := &QueueUnit{
		Job:              job,
		JobStatus:        jobStatus,
		Tasks:            tasks,
		SchedulingPolicy: schedulingPolicy,
	}
	qu.Resources, qu.SportResources = resources.JobResourceRequests(tasks)
	if schedulingPolicy != nil && schedulingPolicy.Priority != nil {
		qu.Priority = pointer.Int32Ptr(*schedulingPolicy.Priority)
	}
	return qu
}

// QueueUnitScore wraps a QueueUnit and the score of it.
type QueueUnitScore struct {
	QueueUnit *QueueUnit
	Score     int64
}

/* The status of running coordinator plugins. */

type Code int

// These are predefined codes used in a status.
const (
	// Success means that plugin ran correctly and found job schedulable.
	// NOTE: A nil status is also considered as "Success".
	Success Code = iota
	// Error is used for internal plugin errors, unexpected input, etc.
	Error
	// Unschedulable is used when a plugin finds a job unschedulable.
	// The accompanying status message should explain why the pod is unschedulable.
	Unschedulable
	// Wait is used when a Permit plugin finds a pod scheduling should wait.
	Wait
	// Skip is used when a Bind plugin chooses to skip binding.
	Skip
)

// NewPluginStatus makes a status out of the given arguments and returns its pointer.
func NewPluginStatus(code Code, reasons ...string) PluginStatus {
	s := &pluginStatus{
		code:    code,
		reasons: reasons,
	}
	if code == Error {
		s.err = errors.New(s.Message())
	}
	return s
}

// pluginStatus indicates the result of running a plugin. It consists of
// a code, a message and (optionally) an error. When the status code is
// not `Success`, the reasons should explain why.
// NOTE: A nil status is also considered as Success.
type pluginStatus struct {
	code    Code
	reasons []string
	err     error
}

func (s *pluginStatus) Code() Code {
	if s == nil {
		return Success
	}
	return s.code
}

func (s *pluginStatus) Message() string {
	if s == nil {
		return ""
	}
	return strings.Join(s.reasons, ", ")
}

func (s *pluginStatus) Reasons() []string {
	return s.reasons
}

func (s *pluginStatus) AppendReason(reason string) {
	s.reasons = append(s.reasons, reason)
}

func (s *pluginStatus) IsSuccess() bool {
	return s.Code() == Success
}

func (s *pluginStatus) IsUnschedulable() bool {
	code := s.Code()
	return code == Unschedulable
}

func (s *pluginStatus) AsError() error {
	if s.IsSuccess() {
		return nil
	}
	if s.err != nil {
		return s.err
	}
	return errors.New(s.Message())
}
