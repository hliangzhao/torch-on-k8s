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

package gangscheduler

import (
	commonapis "github.com/hliangzhao/torch-on-k8s/pkg/common/apis/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// GangScheduler describes an abstract gang scheduler, which defines the behaviors a specific
// Gang scheduler should obey. Here we support Volcano (volcano.sh) as the default gang scheduler.
type GangScheduler interface {
	// CreatePodGroup creates a PodGroup (the Gang entity) for the given job and its tasks based on the given policy
	CreatePodGroup(job metav1.Object, tasks map[commonapis.TaskType]*commonapis.TaskSpec, schedulingPolicy *commonapis.SchedulingPolicy) (runtime.Object, error)
	BindPodToPodGroup(job metav1.Object, podSpec *corev1.PodTemplateSpec, gangEntity runtime.Object, taskType string) error
	GetPodGroup(name types.NamespacedName) (client.ObjectList, error)
	DeletePodGroup(name types.NamespacedName) error

	// PluginName of the specific Gang scheduler, user selectively enables gang-scheduling implementation
	// by --gang-scheduler-name={PluginName} in startup flags.
	PluginName() string
	// SchedulerName is the name of the scheduler to dispatch pod. `pod.spec.schedulerName` will be
	// overridden when pod binds to Gang scheduler in BindPodToGang.
	SchedulerName() string
}

// NewGangScheduler creates a new gang scheduler based on the given manager.
type NewGangScheduler func(manager controllerruntime.Manager) GangScheduler
