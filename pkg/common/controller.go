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
	commonapis "github.com/hliangzhao/torch-on-k8s/pkg/common/apis/v1alpha1"
	"github.com/hliangzhao/torch-on-k8s/pkg/gangscheduler"
	"github.com/hliangzhao/torch-on-k8s/pkg/metrics"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/utils/pointer"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strings"
)

var (
	// GetJobKey is the short name to DeletionHandlingMetaNamespaceKeyFunc.
	// IndexerInformer uses a delta queue, therefore for deletions we have to use this
	// key function, but it should be just fine for non-deletion events.
	GetJobKey = cache.DeletionHandlingMetaNamespaceKeyFunc
)

/* The job controller. */

func NewJobController(manager controllerruntime.Manager,
	controllerImpl commonapis.ControllerInterface,
	config JobControllerConfiguration,
	recorder record.EventRecorder,
	metrics *metrics.JobMetrics,
	scheme *runtime.Scheme) JobController {

	jc := JobController{
		Config:             config,
		Controller:         controllerImpl,
		Expectations:       controller.NewControllerExpectations(),
		BackoffStatesQueue: workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter()),
		Recorder:           recorder,
		Metrics:            metrics,
		Client:             manager.GetClient(),
		APIReader:          manager.GetAPIReader(),
		Scheme:             scheme,
	}

	jc.patcher = func(oldObj, newObj client.Object) error {
		newPatchObj := newObj.DeepCopyObject()
		return jc.Client.Patch(context.Background(), newPatchObj.(client.Object), client.MergeFrom(oldObj))
	}

	jc.PodControl = NewPodControl(jc.Client, recorder)
	jc.ServiceControl = NewServiceControl(jc.Client, recorder)

	return jc
}

// JobController defines the controller struct for jobs. It will be wrapped by
// the torchjob controller as an important part.
type JobController struct {
	Config     JobControllerConfiguration
	Controller commonapis.ControllerInterface

	PodControl     controller.PodControlInterface
	ServiceControl ServiceControlInterface

	GangScheduler gangscheduler.GangScheduler

	// A TTLCache of pod/services creates/deletes each job expects to see.
	// We use Job namespace/name + TaskType + pods/services as an expectation key,
	// TODO: Check: for torchjob, how the expectation looks like?
	Expectations controller.ControllerExpectationsInterface

	// BackoffStatesQueue is a rate limited queue and record backoff counts for
	// those reconciling-failed job instances, and it does not play a role of
	// build-in work queue in controller-runtime.
	BackoffStatesQueue workqueue.RateLimitingInterface

	// Recorder is an event recorder for recording Event resources to the
	// Kubernetes API.
	Recorder record.EventRecorder

	// Metrics is a metrics exporter that export single numerical counter values.
	Metrics *metrics.JobMetrics

	// patcher creates a new patch differentiated from old and new object.
	patcher func(oldObj, newObj client.Object) error

	// Client talks to api-server and knows how to perform CRUD operations on Kubernetes objects.
	Client client.Client

	// Scheme defines methods for serializing and deserializing API objects
	Scheme *runtime.Scheme

	// APIReader knows how to read and list Kubernetes objects bypass cache to avoid retrieving
	// stale status for the reason of etcd slow-watch.
	APIReader client.Reader
}

// GenerateOwnerReference marks the owner of the given job as the job controller jc.
func (jc *JobController) GenerateOwnerReference(job metav1.Object) *metav1.OwnerReference {
	controllerRef := &metav1.OwnerReference{
		APIVersion:         jc.Controller.GetAPIGroupVersion().String(),
		Kind:               jc.Controller.GetAPIGroupVersionKind().Kind,
		Name:               job.GetName(),
		UID:                job.GetUID(),
		BlockOwnerDeletion: pointer.BoolPtr(true),
		Controller:         pointer.BoolPtr(true),
	}
	return controllerRef
}

// jobNameToLabels is an in-memory map from job name to the corresponding labels.
// The labels are used as selectors for selecting the controlled objects of the job.
var jobNameToLabels map[string]map[string]string

func (jc *JobController) GenerateLabels(jobName string) map[string]string {
	labels, ok := jobNameToLabels[jobName]
	if !ok {
		apiGroupName := jc.Controller.GetGroupName()
		jobNameToLabels[jobName] = map[string]string{
			commonapis.LabelGroupName: apiGroupName, // "train.distributed.io"
			commonapis.LabelJobName:   strings.Replace(jobName, "/", "-", -1),
		}
		labels, _ = jobNameToLabels[jobName]
	}
	return labels
}

// CreatePodGroup creates the podgroup resource for the given job in cluster for gang scheduling.
func (jc *JobController) CreatePodGroup(job metav1.Object, tasks map[commonapis.TaskType]*commonapis.TaskSpec, schedulingPolicy *commonapis.SchedulingPolicy) (runtime.Object, error) {
	podgroup, err := jc.GangScheduler.CreatePodGroup(job, tasks, schedulingPolicy)
	if err != nil {
		klog.Errorf("failed to create podgroup, gang scheduler: %s, err: %v", jc.GangScheduler.PluginName(), err)
		return nil, err
	}
	klog.Infof("successfully create gang scheduler for job: %s, scheduler name: %s", job.GetName(), jc.GangScheduler.PluginName())
	return podgroup, nil
}

// DeletePodGroup deletes the podgroup resource for the given job in cluster.
func (jc *JobController) DeletePodGroup(job metav1.Object) error {
	err := jc.GangScheduler.DeletePodGroup(types.NamespacedName{
		Name:      job.GetName(),
		Namespace: job.GetNamespace(),
	})
	if err != nil {
		return err
	}
	klog.Infof("delete Gang scheduler for job %s", job.GetName())
	return nil
}

// resolveControllerRef returns the job referenced by the given controllerRef,
// or nil if the given controllerRef could not be resolved to a matching job of the correct Kind.
// This function will be frequently used to find the corresponding controller job of the controlled pod & service.
func (jc *JobController) resolveControllerRef(namespace string, controllerRef *metav1.OwnerReference) metav1.Object {
	// We can't look up by UID, so look up by Name and then verify UID.
	// Don't even try to look up by Name if it's the wrong Kind.
	if controllerRef.Kind != jc.Controller.GetAPIGroupVersionKind().Kind {
		return nil
	}
	job, err := jc.Controller.GetJobFromInformerCache(namespace, controllerRef.Name)
	if err != nil {
		return nil
	}
	if job.GetUID() != controllerRef.UID {
		// The controller we found with this name is not the same one that the
		// ControllerRef points to.
		return nil
	}
	return job
}
