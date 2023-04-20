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

package core

import (
	pkgcoordinator "github.com/hliangzhao/torch-on-k8s/pkg/coordinator"
	"k8s.io/apimachinery/pkg/types"
	"sync"
	"time"
)

/* The queue struct. Queue is used to store QueueUnits. */

func newQueue(tenantName string) *queue {
	return &queue{
		name:        tenantName,
		createdTime: time.Now().Unix(),
		units:       make(map[types.UID]*pkgcoordinator.QueueUnit),
	}
}

// queue is a simple QueueUnit container implementation, unlike scheduling queue in
// kube-scheduler, the queue in job coordinator does not imply a FIFO model but holds
// all units together and coordinator select which one to pop out.
type queue struct {
	name        string
	createdTime int64 // timestamp in unix
	units       map[types.UID]*pkgcoordinator.QueueUnit
}

func (q *queue) size() int {
	return len(q.units)
}

func (q *queue) get(uid types.UID) *pkgcoordinator.QueueUnit {
	return q.units[uid]
}

func (q *queue) exists(uid types.UID) bool {
	_, ok := q.units[uid]
	return ok
}

func (q *queue) update(qu *pkgcoordinator.QueueUnit) {
	if !q.exists(qu.Job.GetUID()) {
		return
	}
	q.units[qu.Job.GetUID()] = qu
}

func (q *queue) remove(uid types.UID) {
	if !q.exists(uid) {
		return
	}
	delete(q.units, uid)
}

func (q *queue) add(qu *pkgcoordinator.QueueUnit) {
	if q.exists(qu.Job.GetUID()) {
		q.update(qu)
		return
	}
	q.units[qu.Job.GetUID()] = qu
}

func (q *queue) snapshot() []*pkgcoordinator.QueueUnit {
	view := make([]*pkgcoordinator.QueueUnit, 0, len(q.units))
	for _, u := range q.units {
		view = append(view, u)
	}
	return view
}

func (q *queue) iter() *queueIterator {
	return &queueIterator{
		units: q.snapshot(),
	}
}

/* Iterators on the queue. */

// queueIterator provides a thread-safe entry to view read-only elements in queue snapshot.
type queueIterator struct {
	mutex sync.RWMutex
	units []*pkgcoordinator.QueueUnit
	pivot int // where we are now
}

func (it *queueIterator) HasNext() bool {
	it.mutex.RLock()
	defer it.mutex.RUnlock()
	return it.pivot < len(it.units)
}

func (it *queueIterator) Next() *pkgcoordinator.QueueUnit {
	if !it.HasNext() {
		return nil
	}

	it.mutex.Lock()
	cur := it.pivot
	it.pivot++
	it.mutex.Unlock()

	return it.units[cur]
}
