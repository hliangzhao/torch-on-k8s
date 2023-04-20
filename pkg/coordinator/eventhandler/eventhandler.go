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

package eventhandler

import (
	pkgcoordinator "github.com/hliangzhao/torch-on-k8s/pkg/coordinator"
	"github.com/hliangzhao/torch-on-k8s/pkg/features"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var _ handler.EventHandler = &EnqueueForObject{}

type EnqueueForObject struct {
	Coordinator pkgcoordinator.Coordinator
}

func (e *EnqueueForObject) Create(evt event.CreateEvent, owner workqueue.RateLimitingInterface) {
	if evt.Object == nil {
		return
	}

	// if the job owner not set, set the workqueue in the first such that it can be correctly reconciled later
	if e.shouldSetOwnerQueueAndBreak(evt.Object.GetUID()) {
		e.Coordinator.SetQueueUnitOwner(evt.Object.GetUID(), owner)
		return
	}

	// add the job to workqueue for reconciliation
	owner.Add(reconcile.Request{NamespacedName: types.NamespacedName{
		Name:      evt.Object.GetName(),
		Namespace: evt.Object.GetNamespace(),
	}})
}

func (e *EnqueueForObject) Update(evt event.UpdateEvent, q workqueue.RateLimitingInterface) {
	if evt.ObjectNew != nil {
		if e.shouldSetOwnerQueueAndBreak(evt.ObjectNew.GetUID()) {
			e.Coordinator.SetQueueUnitOwner(evt.ObjectNew.GetUID(), q)
			return
		}
		q.Add(reconcile.Request{NamespacedName: types.NamespacedName{
			Name:      evt.ObjectNew.GetName(),
			Namespace: evt.ObjectNew.GetNamespace(),
		}})
	}
}

func (e *EnqueueForObject) Delete(evt event.DeleteEvent, q workqueue.RateLimitingInterface) {
	if evt.Object == nil {
		return
	}

	if features.FeatureGates.Enabled(features.JobCoordinator) && e.Coordinator != nil {
		// for delete events, just dequeue it from coordinator.
		e.Coordinator.Dequeue(evt.Object.GetUID())
	}
	q.Add(reconcile.Request{NamespacedName: types.NamespacedName{
		Name:      evt.Object.GetName(),
		Namespace: evt.Object.GetNamespace(),
	}})
}

func (e *EnqueueForObject) Generic(evt event.GenericEvent, q workqueue.RateLimitingInterface) {
	if evt.Object == nil {
		return
	}
	if features.FeatureGates.Enabled(features.JobCoordinator) && e.Coordinator != nil {
		// for generic handler, it is called only when unknown type event arrives,
		// hence we set queue unit owner workqueue when it is queuing, otherwise
		// it will be dequeued.
		if e.Coordinator.IsQueuing(evt.Object.GetUID()) {
			// set queue unit owner workqueue and Coordinator will release it later.
			e.Coordinator.SetQueueUnitOwner(evt.Object.GetUID(), q)
		} else {
			e.Coordinator.Dequeue(evt.Object.GetUID())
		}
	}

	q.Add(reconcile.Request{NamespacedName: types.NamespacedName{
		Name:      evt.Object.GetName(),
		Namespace: evt.Object.GetNamespace(),
	}})
}

// shouldSetOwnerQueueAndBreak checks whether the job (identified by uid) should be set its owner (workqueue).
func (e *EnqueueForObject) shouldSetOwnerQueueAndBreak(uid types.UID) bool {
	return features.FeatureGates.Enabled(features.JobCoordinator) && e.Coordinator != nil && e.Coordinator.IsQueuing(uid)
}
