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
	"fmt"
	commonapis "github.com/hliangzhao/torch-on-k8s/pkg/common/apis/v1alpha1"
	"github.com/hliangzhao/torch-on-k8s/pkg/features"
	"github.com/hliangzhao/torch-on-k8s/pkg/utils"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilserrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/intstr"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog"
	k8scontroller "k8s.io/kubernetes/pkg/controller"
	"reflect"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"strconv"
	"strings"
)

// Compared with pod controls, for service control, we need to define
// the ServiceControlInterface and ServiceControllerRefManager manually.

/* The ServiceControl structs and functions. */

type ServiceControlInterface interface {
	CreateServices(namespace string, service *corev1.Service, job runtime.Object) error
	CreateServicesWithControllerRef(namespace string, service *corev1.Service, job runtime.Object, controllerRef *metav1.OwnerReference) error
	PatchService(namespace, name string, data []byte) error
	DeleteService(namespace, name string, job runtime.Object) error
}

const (
	FailedCreateServiceReason     = "FailedCreateService"
	SuccessfulCreateServiceReason = "SuccessfulCreateService"
	FailedDeleteServiceReason     = "FailedDeleteService"
	SuccessfulDeleteServiceReason = "SuccessfulDeleteService"
)

func NewServiceControl(client client.Client, recorder record.EventRecorder) *ServiceControl {
	return &ServiceControl{
		client:   client,
		recorder: recorder,
	}
}

var _ ServiceControlInterface = &ServiceControl{}

type ServiceControl struct {
	client   client.Client
	recorder record.EventRecorder
}

func (sc *ServiceControl) CreateServices(namespace string, service *corev1.Service, job runtime.Object) error {
	return sc.createServices(namespace, service, job, nil)
}

func (sc *ServiceControl) CreateServicesWithControllerRef(namespace string, service *corev1.Service,
	job runtime.Object, controllerRef *metav1.OwnerReference) error {

	if err := validateControllerRef(controllerRef); err != nil {
		return err
	}
	return sc.createServices(namespace, service, job, controllerRef)
}

func (sc *ServiceControl) PatchService(namespace, name string, data []byte) error {
	service := &corev1.Service{}
	if err := sc.client.Get(context.Background(), types.NamespacedName{Name: name, Namespace: namespace}, service); err != nil {
		return err
	}
	return sc.client.Patch(context.Background(), service, client.RawPatch(types.StrategicMergePatchType, data))
}

func (sc *ServiceControl) DeleteService(namespace, name string, job runtime.Object) error {
	service := &corev1.Service{}
	err := sc.client.Get(context.Background(), types.NamespacedName{Name: name, Namespace: namespace}, service)
	if err != nil {
		return err
	}
	if service.DeletionTimestamp != nil {
		klog.Infof("service %s/%s is terminating, skip deleting", service.Namespace, service.Name)
		return nil
	}
	accessor, err := meta.Accessor(job)
	if err != nil {
		return fmt.Errorf("job does not have ObjectMeta, %v", err)
	}
	klog.Infof("job %v deleting service %v/%v", accessor.GetName(), namespace, name)
	if err = sc.client.Delete(context.Background(), service); err != nil {
		sc.recorder.Eventf(job, corev1.EventTypeWarning, FailedDeleteServiceReason, "Error deleting: %v", err)
		return fmt.Errorf("unable to delete service: %v", err)
	} else {
		sc.recorder.Eventf(job, corev1.EventTypeNormal, SuccessfulDeleteServiceReason, "Deleted service: %v", name)
	}
	return nil
}

func (sc *ServiceControl) createServices(namespace string, service *corev1.Service, job runtime.Object,
	controllerRef *metav1.OwnerReference) error {

	if labels.Set(service.Labels).AsSelectorPreValidated().Empty() {
		return fmt.Errorf("unable to create serivces, no labels")
	}

	// set service controller reference, namespace, and create it
	serviceWithOwner := service.DeepCopy()
	if controllerRef != nil {
		serviceWithOwner.OwnerReferences = append(serviceWithOwner.OwnerReferences, *controllerRef)
	}
	serviceWithOwner.Namespace = namespace
	err := sc.client.Create(context.Background(), serviceWithOwner)
	if err != nil {
		sc.recorder.Eventf(job, corev1.EventTypeWarning, FailedCreateServiceReason, "error creating: %v", err)
		return fmt.Errorf("unable to create services: %v", err)
	}

	// service successfully created, record event
	accessor, err := meta.Accessor(job)
	if err != nil {
		klog.Errorf("job does not have ObjectMeta, %v", err)
		return nil
	}
	klog.Infof("job %v created service %v", accessor.GetName(), serviceWithOwner.Name)
	sc.recorder.Eventf(job, corev1.EventTypeNormal, SuccessfulCreateServiceReason, "Created service: %v", serviceWithOwner.Name)

	return nil
}

/* The service reconcile functions of the job controller. */

// OnServiceCreateFunc returns ture if the creation event should be processed.
func (jc *JobController) OnServiceCreateFunc(e event.CreateEvent) bool {
	service := e.Object.(*corev1.Service)
	if service.DeletionTimestamp != nil {
		return false
	}

	if controllerRef := metav1.GetControllerOf(service); controllerRef != nil {
		job := jc.resolveControllerRef(service.Namespace, controllerRef)
		if job == nil {
			return false
		}
		jobKey, err := GetJobKey(job)
		if err != nil {
			return false
		}
		taskType, ok := service.Labels[commonapis.LabelTaskType]
		if !ok {
			klog.Infof("the service does not have job task type label, it is not created by %v, service name: %s",
				jc.Controller.ControllerName(), service.Name)
			return false
		}
		expSvcKey := genExpectationServicesKey(jobKey, taskType)

		// we only need to create the expectation, the evolution to the expectation status will be handled by k8s mechanism
		jc.Expectations.CreationObserved(expSvcKey)
		return true
	}

	return false
}

// OnServiceUpdateFunc returns ture if the update event should be processed.
func (jc *JobController) OnServiceUpdateFunc(e event.UpdateEvent) bool {
	newSvc := e.ObjectNew.(*corev1.Service)
	oldSvc := e.ObjectNew.(*corev1.Service)
	if newSvc.ResourceVersion == oldSvc.ResourceVersion {
		return false
	}

	newCtrRef := metav1.GetControllerOf(newSvc)
	oldCtrRef := metav1.GetControllerOf(oldSvc)
	ctrRefChanged := !reflect.DeepEqual(newCtrRef, oldCtrRef)

	if ctrRefChanged && oldCtrRef != nil {
		if job := jc.resolveControllerRef(oldSvc.Namespace, oldCtrRef); job != nil {
			return true
		}
	}

	if newCtrRef != nil {
		job := jc.resolveControllerRef(newSvc.Namespace, newCtrRef)
		return job != nil
	}

	return false
}

// OnServiceDeleteFunc returns ture if the deletion event should be processed.
func (jc *JobController) OnServiceDeleteFunc(e event.DeleteEvent) bool {
	service, ok := e.Object.(*corev1.Service)

	// the service turns into a tombstone object
	if e.DeleteStateUnknown {
		klog.Warningf("service %s is in delete unknown state", service.Name)
	}

	ctrRef := metav1.GetControllerOf(service)
	if ctrRef == nil {
		return false
	}
	job := jc.resolveControllerRef(service.Namespace, ctrRef)
	if job == nil {
		return false
	}
	jobKey, err := GetJobKey(job)
	if err != nil {
		return false
	}
	taskType, ok := service.Labels[commonapis.LabelTaskType]
	if !ok {
		klog.Infof("the service does not have job task type label, it is not created by %v, service name: %s",
			jc.Controller.ControllerName(), service.Name)
		return false
	}
	expPodsKey := genExpectationServicesKey(jobKey, taskType)

	// create the delete expectation such that the deletion will be automatically detected and handled by k8s mechanism
	jc.Expectations.DeletionObserved(expPodsKey)

	return true
}

// ReconcileServices reconciles the service of the given job task (identified by task key).
func (jc *JobController) ReconcileServices(ctx context.Context, job metav1.Object, services []*corev1.Service,
	taskType commonapis.TaskType, taskSpec *commonapis.TaskSpec) error {

	// get the to-be-reconciled services of certain task type
	tt := strings.ToLower(string(taskType))
	numTasks := int(*taskSpec.NumTasks)
	services, err := jc.filterServicesForTaskType(services, tt)
	if err != nil {
		return err
	}

	svcSlices := jc.getServiceSlices(services, numTasks)
	for svcIdx, svcSlice := range svcSlices {
		if len(svcSlice) > 1 {
			klog.Warningf("we have too many services for %s %d", tt, svcIdx)
		} else if len(svcSlice) == 0 {
			klog.Infof("need to create new service: %s-%d", tt, svcIdx)
			err = jc.createNewService(ctx, job, taskType, taskSpec, strconv.Itoa(svcIdx))
			if err != nil {
				// We don't need to handle the AlreadyExists error since it is handled by the ReconcilePods() function.
				return err
			}
		} else {
			// svcSlice is of length 1, this is what we expected. Just reconcile this service to its expectation.
			svc := svcSlice[0]
			// Check if index is in valid range, otherwise we should scale in extra pods since
			// the expected replicas has been modified.
			if svcIdx >= numTasks {
				if svc.DeletionTimestamp == nil {
					jc.Recorder.Eventf(job.(runtime.Object), corev1.EventTypeNormal, "DeleteService",
						"service %s/%s with index %v is out of expected replicas %v and should be deleted",
						svc.Namespace, svc.Name, svcIdx, numTasks)
					if err = jc.ServiceControl.DeleteService(svc.Namespace, svc.Name, job.(runtime.Object)); err != nil {
						return err
					}
				}
			}
			if EnableHostNetwork(job) {
				// always use the newest specified host port
				hostPort, ok := GetHostNetworkPortFromContext(ctx, tt, strconv.Itoa(svcIdx))
				if ok && len(svc.Spec.Ports) > 0 && svc.Spec.Ports[0].TargetPort.IntVal != hostPort {
					klog.Infof("update target service: %s-%d, new port: %d",
						tt, svcIdx, hostPort)
					// Update service target port to latest container host port, because replicas may fail over
					// and its host port changed, so we'd ensure that other task replicas can reach it with correct
					// target port.
					newSvc := svc.DeepCopy()
					newSvc.Spec.Ports[0].TargetPort = intstr.FromInt(int(hostPort))
					if err = jc.patcher(svc, newSvc); err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

// filterServicesForTaskType returns the services of the given task type.
func (jc *JobController) filterServicesForTaskType(services []*corev1.Service, taskType string) ([]*corev1.Service, error) {
	var ret []*corev1.Service

	// create selector
	labelSelector := &metav1.LabelSelector{
		MatchLabels: make(map[string]string),
	}
	labelSelector.MatchLabels[commonapis.LabelTaskType] = taskType
	selector, err := metav1.LabelSelectorAsSelector(labelSelector)

	for _, svc := range services {
		if err != nil {
			return nil, err
		}
		if !selector.Matches(labels.Set(svc.Labels)) {
			continue
		}
		ret = append(ret, svc)
	}

	return ret, nil
}

// getServiceSlices separates the given services by task index and returns.
func (jc *JobController) getServiceSlices(services []*corev1.Service, numTasks int) [][]*corev1.Service {
	svcSlices := make([][]*corev1.Service, numTasks)
	for _, svc := range services {
		if _, ok := svc.Labels[commonapis.LabelTaskType]; !ok {
			klog.Warning("The service do not have the index label")
			continue
		}

		index, err := strconv.Atoi(svc.Labels[commonapis.LabelTaskIndex])
		if err != nil {
			klog.Warningf("Error when strconv.Atoi: %v", err)
			continue
		}
		if index < 0 {
			klog.Warningf("The label index is not expected: %d", index)
		} else if index >= numTasks {
			// Service index out of range, which indicates that it is a scale in
			// reconciliation and pod index >= numTasks will be deleted later, so
			// we'd increase capacity of pod slice to collect.
			newSvcSlices := make([][]*corev1.Service, index+1)
			copy(newSvcSlices, svcSlices)
			svcSlices = newSvcSlices
		}

		svcSlices[index] = append(svcSlices[index], svc)
	}

	return svcSlices
}

// createNewService creates a new service for the given task type and index.
// Two steps:
// (1) Set service spec according to the task spec.
// (2) Create the service by the settled service spec.
func (jc *JobController) createNewService(ctx context.Context, job metav1.Object, taskType commonapis.TaskType,
	taskSpec *commonapis.TaskSpec, taskIndex string) error {

	// Set service spec according to the taskSpec. Especially the service port.
	tt := strings.ToLower(string(taskType))
	genLabels := jc.GenerateLabels(job.GetName())
	genLabels[commonapis.LabelTaskType] = tt
	genLabels[commonapis.LabelTaskIndex] = taskIndex

	svcPort, err := jc.getPortFromJob(taskSpec)
	if err != nil {
		return err
	}
	targetPort := svcPort
	clusterIP := "None"

	if !features.FeatureGates.Enabled(features.HostNetWithHeadlessSvc) && EnableHostNetwork(job) {
		// Communications between tasks use headless services by default, as for hostnetwork mode,
		// headless service can not forward traffic from one port to another, so we use normal service
		// when hostnetwork enabled.
		clusterIP = ""
		hostPort, ok := GetHostNetworkPortFromContext(ctx, tt, taskIndex)
		if ok {
			targetPort = hostPort
		}
	}

	service := &corev1.Service{
		Spec: corev1.ServiceSpec{
			ClusterIP: clusterIP,
			Selector:  genLabels,
			Ports: []corev1.ServicePort{
				{
					Name:       jc.Controller.GetDefaultContainerPortName(),
					Port:       svcPort,
					TargetPort: intstr.FromInt(int(targetPort)),
				},
			},
		},
	}
	service.Labels = mergeMap(service.Labels, genLabels)

	// Create.
	return jc.createService(job, taskType, service, taskIndex)
}

// getPortFromJob gets the service port of the given task.
// The service port of a job task is the port of the default port of the default container.
func (jc *JobController) getPortFromJob(taskSpec *commonapis.TaskSpec) (int32, error) {
	containers := taskSpec.Template.Spec.Containers
	for _, c := range containers {
		if c.Name == jc.Controller.GetDefaultContainerName() {
			for _, port := range c.Ports {
				if port.Name == jc.Controller.GetDefaultContainerPortName() {
					return port.ContainerPort, nil
				}
			}
		}
	}
	return -1, fmt.Errorf("failed to find port")
}

// createService creates a new common service with the given task type and index.
func (jc *JobController) createService(job metav1.Object, taskType commonapis.TaskType,
	service *corev1.Service, taskIndex string) error {

	jobKey, err := GetJobKey(job)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for job object %#v: %v", job, err))
		return err
	}

	tt := strings.ToLower(string(taskType))
	expServiceKey := genExpectationServicesKey(jobKey, tt)
	err = jc.Expectations.ExpectCreations(expServiceKey, 1)
	if err != nil {
		return err
	}

	service.Name = utils.GenGeneralName(job.GetName(), tt, taskIndex)
	controllerRef := jc.GenerateOwnerReference(job)

	err = jc.ServiceControl.CreateServicesWithControllerRef(job.GetNamespace(), service,
		job.(runtime.Object), controllerRef)
	if err != nil && errors.IsTimeout(err) {
		// Service is created but its initialization has timed out.
		// If the initialization is successful eventually, the
		// controller will observe the creation via the informer.
		// If the initialization fails, or if the service keeps
		// uninitialized for a long time, the informer will not
		// receive any update, and the controller will create a new
		// service when the expectation expires. Thus, we just return nil.
		return nil
	} else if err != nil {
		return err
	}

	return nil
}

// AdoptAndClaimServices adopts and claims services for the given job.
func (jc *JobController) AdoptAndClaimServices(job metav1.Object, serviceList *corev1.ServiceList) ([]*corev1.Service, error) {
	selector, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{
		MatchLabels: jc.GenerateLabels(job.GetName()),
	})
	if err != nil {
		return nil, err
	}

	services := make([]*corev1.Service, 0, len(serviceList.Items))
	for idx := range serviceList.Items {
		services = append(services, &serviceList.Items[idx])
	}

	// If any adoptions are attempted, we should first recheck for deletion
	// with an uncached quorum read sometime after listing Pods (see #42639).
	canAdoptFunc := recheckDeletionTimestamp(func() (metav1.Object, error) {
		freshJob, err := jc.Controller.GetJobFromAPIClient(job.GetNamespace(), job.GetName())
		if err != nil {
			return nil, err
		}
		if freshJob.GetUID() != job.GetUID() {
			return nil, fmt.Errorf("original Job %v/%v is gone: got uid %v, wanted %v",
				job.GetNamespace(), job.GetName(), freshJob.GetUID(), job.GetUID())
		}
		return freshJob, nil
	})

	refManager := NewServiceControllerRefManager(jc.ServiceControl, job, selector, jc.Controller.GetAPIGroupVersionKind(), canAdoptFunc)
	return refManager.ClaimServices(services)
}

/* ServiceControllerRefManager related. It is used for implementing the AdoptAndClaimServices() functions for job controller. */

// NewServiceControllerRefManager returns a ServiceControllerRefManager that exposes
// methods to manage the controllerRef of services.
//
// The CanAdopt() function can be used to perform a potentially expensive check
// (such as a live GET from the API server) prior to the first adoption.
// It will only be called (at most once) if an adoption is actually attempted.
// If CanAdopt() returns a non-nil error, all adoptions will fail.
//
// NOTE: Once CanAdopt() is called, it will not be called again by the same
//       PodControllerRefManager instance. Create a new instance if it makes
//       sense to check CanAdopt() again (e.g. in a different sync pass).
func NewServiceControllerRefManager(serviceControl ServiceControlInterface, ctr metav1.Object, selector labels.Selector,
	ctrKind schema.GroupVersionKind, canAdopt func() error) *ServiceControllerRefManager {

	return &ServiceControllerRefManager{
		BaseControllerRefManager: k8scontroller.BaseControllerRefManager{
			Controller:   ctr,
			Selector:     selector,
			CanAdoptFunc: canAdopt,
		},
		controllerKind: ctrKind,
		serviceControl: serviceControl,
	}
}

type ServiceControllerRefManager struct {
	k8scontroller.BaseControllerRefManager
	controllerKind schema.GroupVersionKind
	serviceControl ServiceControlInterface
}

// ClaimServices tries to take ownership of a list of Services.
//
// It will reconcile the following:
//   * Adopt orphans if the selector matches.
//   * Release owned objects if the selector no longer matches.
//
// Optional: If one or more filters are specified, a Service will only be claimed if
// all filters return true.
//
// A non-nil error is returned if some form of reconciliation was attempted and
// failed. Usually, controllers should try again later in case reconciliation
// is still needed.
//
// If the error is nil, either the reconciliation succeeded, or no
// reconciliation was necessary. The list of Services that you now own is returned.
func (rm *ServiceControllerRefManager) ClaimServices(services []*corev1.Service,
	filters ...func(service *corev1.Service) bool) ([]*corev1.Service, error) {

	var (
		claimed []*corev1.Service
		errList []error
	)

	// define the match, adopt, and release functions
	match := func(obj metav1.Object) bool {
		service := obj.(*corev1.Service)
		if !rm.Selector.Matches(labels.Set(service.Labels)) {
			return false
		}
		for _, filter := range filters {
			if !filter(service) {
				return false
			}
		}
		return true
	}
	adopt := func(obj metav1.Object) error {
		return rm.AdoptService(obj.(*corev1.Service))
	}
	release := func(obj metav1.Object) error {
		return rm.ReleaseService(obj.(*corev1.Service))
	}

	// claim each service in turn
	for _, svc := range services {
		ok, err := rm.ClaimObject(svc, match, adopt, release)
		if err != nil {
			errList = append(errList, err)
			continue
		}
		if ok {
			claimed = append(claimed, svc)
		}
	}

	return claimed, utilserrors.NewAggregate(errList)
}

// AdoptService sends a patch to take control of the service. It returns the error if
// the patching fails.
func (rm *ServiceControllerRefManager) AdoptService(service *corev1.Service) error {
	if err := rm.CanAdopt(); err != nil {
		return fmt.Errorf("can't adopt Service %v/%v (%v): %v", service.Namespace, service.Name, service.UID, err)
	}
	// Note that ValidateOwnerReferences() will reject this patch if another
	// OwnerReference exists with controller=true.
	addControllerPatch := fmt.Sprintf(
		`{"metadata":{"ownerReferences":[{"apiVersion":"%s","kind":"%s","name":"%s","uid":"%s","controller":true,"blockOwnerDeletion":true}],"uid":"%s"}}`,
		rm.controllerKind.GroupVersion(), rm.controllerKind.Kind,
		rm.Controller.GetName(), rm.Controller.GetUID(), service.UID)
	return rm.serviceControl.PatchService(service.Namespace, service.Name, []byte(addControllerPatch))
}

// ReleaseService sends a patch to free the service from the control of the controller.
// It returns the error if the patching fails. 404 and 422 errors are ignored.
func (rm *ServiceControllerRefManager) ReleaseService(service *corev1.Service) error {
	klog.Infof("patching Service %s/%s to remove its controllerRef to %s/%s:%s",
		service.Namespace, service.Name, rm.controllerKind.GroupVersion(), rm.controllerKind.Kind, rm.Controller.GetName())
	deleteOwnerRefPatch := fmt.Sprintf(
		`{"metadata":{"ownerReferences":[{"$patch":"delete","uid":"%s"}],"uid":"%s"}}`,
		rm.Controller.GetUID(), service.UID)
	err := rm.serviceControl.PatchService(service.Namespace, service.Name, []byte(deleteOwnerRefPatch))
	if err != nil {
		if errors.IsNotFound(err) {
			// If the service no longer exists, ignore it.
			return nil
		}
		if errors.IsInvalid(err) {
			// Invalid error will be returned in two cases: 1. the service
			// has no owner reference, 2. the uid of the service doesn't
			// match, which means the service is deleted and then recreated.
			// In both cases, the error can be ignored.

			// TODO: If the service has owner references, but none of them
			//  has the owner.UID, server will silently ignore the patch.
			//  Investigate why.
			return nil
		}
	}
	return err
}
