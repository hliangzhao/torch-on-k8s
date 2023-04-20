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
	"fmt"
	"github.com/go-logr/logr"
	commonapis "github.com/hliangzhao/torch-on-k8s/pkg/common/apis/v1alpha1"
	pkgcoordinator "github.com/hliangzhao/torch-on-k8s/pkg/coordinator"
	"github.com/hliangzhao/torch-on-k8s/pkg/features"
	"github.com/hliangzhao/torch-on-k8s/pkg/metrics"
	"github.com/hliangzhao/torch-on-k8s/pkg/utils"
	"k8s.io/apimachinery/pkg/runtime"
	"reflect"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

// FieldExtractFunc extracts the wanted fields of the given job.
type FieldExtractFunc func(job client.Object) (tasks map[commonapis.TaskType]*commonapis.TaskSpec,
	jobStatus *commonapis.JobStatus, schedulingPolicy *commonapis.SchedulingPolicy)

// OnOwnerCreateFunc returns a job create event function to be handled.
func OnOwnerCreateFunc(scheme *runtime.Scheme, fieldExtractFunc FieldExtractFunc,
	logger logr.Logger, coordinator pkgcoordinator.Coordinator, metrics *metrics.JobMetrics) func(event.CreateEvent) bool {

	return func(e event.CreateEvent) bool {
		// when a job is created, set default for it as initialization
		scheme.Default(e.Object)
		msg := fmt.Sprintf("Job %s/%s is created.", e.Object.GetObjectKind().GroupVersionKind().Kind, e.Object.GetName())

		tasks, jobStatus, schedulingPolicy := fieldExtractFunc(e.Object)

		if err := utils.UpdateJobConditions(jobStatus, commonapis.JobCreated, utils.JobCreatedReason, msg); err != nil {
			logger.Error(err, "update job condition error")
			return false
		}

		if features.FeatureGates.Enabled(features.JobCoordinator) && coordinator != nil &&
			utils.NeedEnqueueToCoordinator(*jobStatus) {
			logger.Info("job enqueued into coordinator and wait for being scheduled.", "namespace", e.Object.GetNamespace(), "name", e.Object.GetName())
			coordinator.EnqueueOrUpdate(pkgcoordinator.ToQueueUnit(e.Object, tasks, jobStatus, schedulingPolicy))
			return true
		}

		logger.Info(msg)
		metrics.CreatedInc()
		return true
	}
}

// OnOwnerUpdateFunc returns a job update event function to be handled.
func OnOwnerUpdateFunc(scheme *runtime.Scheme, fieldExtractFunc FieldExtractFunc, logger logr.Logger,
	coordinator pkgcoordinator.Coordinator) func(e event.UpdateEvent) bool {

	return func(e event.UpdateEvent) bool {
		scheme.Default(e.ObjectOld)
		scheme.Default(e.ObjectNew)
		oldTasks, _, oldSchedPolicy := fieldExtractFunc(e.ObjectOld)
		newTasks, newJobStatus, newSchedPolicy := fieldExtractFunc(e.ObjectNew)

		var msg string

		if features.FeatureGates.Enabled(features.JobCoordinator) && coordinator != nil {
			// For a job update event, we take an update-then-enqueue action or dequeue action,
			// based on the new job status.
			if utils.NeedEnqueueToCoordinator(*newJobStatus) {
				if !reflect.DeepEqual(oldTasks, newTasks) || !reflect.DeepEqual(oldSchedPolicy, newSchedPolicy) {
					msg = fmt.Sprintf("Job %s/%s is updated.", e.ObjectNew.GetObjectKind().GroupVersionKind().Kind, e.ObjectNew.GetName())
					coordinator.EnqueueOrUpdate(pkgcoordinator.ToQueueUnit(e.ObjectNew, newTasks, newJobStatus, newSchedPolicy))
				}
			} else if coordinator.IsQueuing(e.ObjectNew.GetUID()) {
				msg = fmt.Sprintf("Job %s/%s is dequeued.", e.ObjectNew.GetObjectKind().GroupVersionKind().Kind, e.ObjectNew.GetName())
				coordinator.Dequeue(e.ObjectNew.GetUID())
			}
		}

		logger.Info(msg)
		return true
	}
}

// OnOwnerDeleteFunc returns a job delete event function to be handled.
func OnOwnerDeleteFunc(jc JobController, fieldExtractFunc FieldExtractFunc, logger logr.Logger) func(e event.DeleteEvent) bool {
	return func(e event.DeleteEvent) bool {
		tasks, _, _ := fieldExtractFunc(e.Object)
		jc.DeleteExpectations(e.Object, tasks)
		logger.V(3).Info("job is deleted", "namespace", e.Object.GetNamespace(), "name", e.Object.GetName())
		return true
	}
}
