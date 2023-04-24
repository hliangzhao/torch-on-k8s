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
	trainv1alpha1 "github.com/hliangzhao/torch-on-k8s/apis/train/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// GangScheduler describes an abstract gang scheduler, which defines the behaviors a specific
// Gang scheduler should obey. Here we support Volcano (volcano.sh) as the default gang scheduler.
type GangScheduler interface {
	// CreatePodGroup creates a PodGroup (the Gang entity) for the given job and its tasks based on the given minMember and schedulingPolicy.
	CreatePodGroup(job metav1.Object, tasks map[trainv1alpha1.TaskType]*trainv1alpha1.TaskSpec, minMembers map[trainv1alpha1.TaskType]*int32, schedulingPolicy *trainv1alpha1.SchedulingPolicy) (runtime.Object, error)

	// BindPodToPodGroup binds the correct podgroup for the given pod of some job task.
	BindPodToPodGroup(job metav1.Object, podSpec *corev1.PodTemplateSpec, gangEntity runtime.Object, taskType string) error

	// GetPodGroup returns the list of podgroups for the given job.
	GetPodGroup(name types.NamespacedName) (client.ObjectList, error)

	// DeletePodGroup deletes the podgroups of the job.
	DeletePodGroup(name types.NamespacedName) error

	// SchedulerName is the name of the scheduler to dispatch pod. `pod.spec.schedulerName` will be
	// overridden when pod binds to Gang scheduler in BindPodToGang.
	SchedulerName() string
}

// NewGangScheduler creates a new gang scheduler based on the given manager.
type NewGangScheduler func(mgr ctrl.Manager) GangScheduler
