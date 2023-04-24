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

package registry

import (
	"github.com/hliangzhao/torch-on-k8s/pkg/gangscheduler"
	"github.com/hliangzhao/torch-on-k8s/pkg/gangscheduler/volcano"
	"k8s.io/klog"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sync"
)

/* Register the gang scheduler Volcano. More gang schedulers can be added in the future. */

var (
	newGangSchedulers []gangscheduler.NewGangScheduler
	registry          = Registry{registry: make(map[string]gangscheduler.GangScheduler)}
)

func init() {
	// add all the provided Gang schedulers into the registry
	newGangSchedulers = append(newGangSchedulers, volcano.NewVolcano)
}

func Register(manager controllerruntime.Manager) {
	for _, newer := range newGangSchedulers {
		scheduler := newer(manager)
		klog.Infof("register gang scheduler %s", scheduler.SchedulerName())
		registry.Add(scheduler)
	}
}

func Get(name string) gangscheduler.GangScheduler {
	return registry.Get(name)
}

type Registry struct {
	lock     sync.Mutex
	registry map[string]gangscheduler.GangScheduler
}

func (r *Registry) Add(scheduler gangscheduler.GangScheduler) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.registry[scheduler.SchedulerName()] = scheduler
}

func (r *Registry) Get(name string) gangscheduler.GangScheduler {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.registry[name]
}

func (r *Registry) Remove(name string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	delete(r.registry, name)
}
