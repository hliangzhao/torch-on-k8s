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
	"context"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
)

/* Interface of coordinator. */

// Coordinator is designed to coordinate the scheduling of different jobs.
// We have different queues, and a job (wrapped as a QueueUnit) is enqueued
// into a specific queue. Each time the coordinator selects a queue (different
// load balancing algorithms can be implemented, such as RoundRobin, WeightedRoundRobin, etc.),
// and a QueueUnit is popped for processing. Specifically, which QueueUnit
// is popped depends on the used plugins. For example, with priority plugin,
// the QueueUnit with the largest priority will be popped.
type Coordinator interface {
	Run(ctx context.Context)
	EnqueueOrUpdate(qu *QueueUnit)
	Dequeue(uid types.UID)
	IsQueuing(uid types.UID) bool
	SetQueueUnitOwner(uid types.UID, owner workqueue.RateLimitingInterface)
}

/* Interface of coordinator plugins. */

type Plugin interface {
	Name() string
}

type PreFilterPlugin interface {
	PreFilter(ctx context.Context, qu *QueueUnit) PluginStatus
	Plugin
}

type FilterPlugin interface {
	Filter(ctx context.Context, que *QueueUnit) PluginStatus
	Plugin
}

type ScorePlugin interface {
	Score(ctx context.Context, qu *QueueUnit) (int64, PluginStatus)
	Plugin
}

type PreDequeuePlugin interface {
	PreDequeue(ctx context.Context, qu *QueueUnit) PluginStatus
	Plugin
}

type TenantPlugin interface {
	TenantName(qu *QueueUnit) string
	Plugin
}

/* Interface of coordinator plugin running status. */

type PluginStatus interface {
	// Code returns code of the status.
	Code() Code

	// Message returns a concatenated message on reasons of the status.
	Message() string

	// Reasons returns reasons of the status.
	Reasons() []string

	// AppendReason appends given reason to the status.
	AppendReason(reason string)

	// IsSuccess returns true if and only if "status" is nil or Code is "Success".
	IsSuccess() bool

	// IsUnschedulable returns true if "status" is Unschedulable (Unschedulable or UnschedulableAndUnresolvable).
	IsUnschedulable() bool

	// AsError returns nil if the status is a success; otherwise returns an "error" object
	// with a concatenated message on reasons of the status.
	AsError() error
}
