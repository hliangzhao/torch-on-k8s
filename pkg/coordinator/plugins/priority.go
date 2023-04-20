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

package plugins

import (
	"context"
	pkgcoordinator "github.com/hliangzhao/torch-on-k8s/pkg/coordinator"
	schedulingv1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

/* The coordinator plugin built on priority. */

const (
	PriorityPluginName = "Priority"
)

func NewPriorityPlugin(c client.Client, recorder record.EventRecorder) pkgcoordinator.Plugin {
	return &priorityPlugin{client: c}
}

var _ pkgcoordinator.ScorePlugin = &priorityPlugin{}

type priorityPlugin struct {
	client client.Client
}

// Score implements score interface and take priority of QueueUnit as its score,
// which helps to determine the dequeue order.
// The priority is fetched from two places: (1) SchedulingPolicy.Priority, (2) SchedulingPolicy.PriorityClassName.
func (pq *priorityPlugin) Score(ctx context.Context, qu *pkgcoordinator.QueueUnit) (int64, pkgcoordinator.PluginStatus) {
	if qu.Priority == nil && qu.SchedulingPolicy != nil {
		if qu.SchedulingPolicy.Priority != nil {
			v := *qu.SchedulingPolicy.Priority
			qu.Priority = &v
		} else {
			populatePriorityValueFromPriorityClass(pq.client, qu)
		}
	}

	v := int32(0)
	if qu.Priority != nil {
		v = *qu.Priority
	}

	klog.V(2).Infof("priority of queue unit %s is %v", qu.Key(), v)
	return int64(v), pkgcoordinator.NewPluginStatus(pkgcoordinator.Success)
}

func (pq *priorityPlugin) Name() string {
	return PriorityPluginName
}

// populatePriorityValueFromPriorityClass sets the priority value for the given QueueUnit instance
// from the priority class.
func populatePriorityValueFromPriorityClass(client client.Client, qu *pkgcoordinator.QueueUnit) {
	if qu.Priority != nil {
		return
	}
	if qu.Priority == nil && qu.SchedulingPolicy != nil && qu.SchedulingPolicy.PriorityClassName != "" {
		pc := schedulingv1.PriorityClass{}
		if err := client.Get(context.Background(), types.NamespacedName{
			Name: qu.SchedulingPolicy.PriorityClassName,
		}, &pc); err == nil {
			qu.Priority = &pc.Value
		}
	}
}
