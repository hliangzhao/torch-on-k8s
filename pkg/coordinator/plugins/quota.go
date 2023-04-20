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
	"encoding/json"
	"fmt"
	commonapis "github.com/hliangzhao/torch-on-k8s/pkg/common/apis/v1alpha1"
	"github.com/hliangzhao/torch-on-k8s/pkg/coordinator"
	"github.com/hliangzhao/torch-on-k8s/pkg/utils"
	flowcontrolutils "github.com/hliangzhao/torch-on-k8s/pkg/utils/flowcontrol"
	resourcesutils "github.com/hliangzhao/torch-on-k8s/pkg/utils/resources"
	"github.com/tidwall/gjson"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	quotav1 "k8s.io/apiserver/pkg/quota/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sync"
	"time"
)

/* The coordinator plugins built on quota. */

const (
	QuotaPluginName = "Quota"

	// defaultQuotaAssumedTimeoutSeconds represents the default timeout seconds from
	// resources assumed as requested quota to it becomes invalid/expired.
	defaultQuotaAssumedTimeoutSeconds = float64(60)
)

var globalQuotaPlugin *quotaPlugin

func NewQuotaPlugin(cli client.Client, recorder record.EventRecorder) coordinator.Plugin {
	if globalQuotaPlugin != nil {
		return globalQuotaPlugin
	}
	globalQuotaPlugin = &quotaPlugin{
		client:   cli,
		recorder: flowcontrolutils.NewFlowControlRecorder(recorder, 3),
	}
	globalQuotaPlugin.assumedQuotas = assumedQuotas{
		assumed:          map[string]map[string]assumedQuota{},
		latestStatusFunc: globalQuotaPlugin.retrieveLatestStatus,
	}
	return globalQuotaPlugin
}

var _ coordinator.TenantPlugin = &quotaPlugin{}
var _ coordinator.FilterPlugin = &quotaPlugin{}
var _ coordinator.PreDequeuePlugin = &quotaPlugin{}

type quotaPlugin struct {
	client   client.Client
	recorder record.EventRecorder
	// assumedQuotas represents assumed deducted quota when job is allowed to
	// be dequeued, there always has a latency from job dequeued to requested
	// quota updated.
	assumedQuotas assumedQuotas
}

// TenantName returns the tenant (queue) name of the given QueueUnit.
func (qp *quotaPlugin) TenantName(qu *coordinator.QueueUnit) string {
	if qu.SchedulingPolicy != nil && qu.SchedulingPolicy.Queue != "" && qu.Tenant == "" {
		qu.Tenant = qu.SchedulingPolicy.Queue
	}
	if qu.Tenant != "" {
		return qu.Tenant
	}
	// Take namespace as default tenant name since ResourceQuotas is partitioned
	// by namespaces.
	return qu.Job.GetNamespace()
}

// Filter returns success if qu passes the quota filter.
// In this filter, we check the resource request can be satisfied or not.
// Only satisfied qu can pass.
func (qp *quotaPlugin) Filter(ctx context.Context, qu *coordinator.QueueUnit) coordinator.PluginStatus {
	klog.V(3).Infof("filter queue unit: %s, resources: %+v", qu.Key(), qu.Resources)

	// fetch all resource quotas in the job namespace from the cluster
	rqs := corev1.ResourceQuotaList{}
	if err := qp.client.List(ctx, &rqs, client.InNamespace(qu.Job.GetNamespace())); err != nil {
		klog.Errorf("failed to get quota %s, err: %v", qu.Tenant, err)
		return coordinator.NewPluginStatus(coordinator.Error, err.Error())
	}

	for _, rq := range rqs.Items {
		// the resource quota cannot be guaranteed, we have to wait
		available, exceed := availableQuota(&rq)
		if exceed {
			klog.V(2).Infof("quota %s has exceeded guaranteed resources", rq.Name)
			return coordinator.NewPluginStatus(coordinator.Wait,
				fmt.Sprintf("queue %s guaranteed quota has exceed", rq.Name))
		}

		assumed := qp.assumedQuotas.accumulateAssumedQuota(qu.Tenant)
		if assumed != nil {
			available = quotav1.SubtractWithNonNegativeResult(available, assumed)
		}

		if le, resources := resourcesutils.AnyLessThan(available, qu.Resources); le {
			msg := fmt.Sprintf("resource %v request exceeds available quota %s, avaiable: %+v, requests: %+v, assumed: %+v",
				resources, qu.Tenant, jsonDump(available), jsonDump(qu.Resources), jsonDump(assumed))
			klog.V(2).Infof(msg)
			qp.recorder.Event(qu.Job, corev1.EventTypeNormal, "QuotaNotSatisfied", msg)
			return coordinator.NewPluginStatus(coordinator.Wait, msg)
		}
	}

	return coordinator.NewPluginStatus(coordinator.Success)
}

// availableQuota calculates the available resource quota for q.
func availableQuota(q *corev1.ResourceQuota) (corev1.ResourceList, bool) {
	max := q.Spec.Hard
	req := q.Status.Used
	req = quotav1.Mask(req,
		quotav1.Intersection(quotav1.ResourceNames(req), quotav1.ResourceNames(max)),
	)
	exceed, _ := resourcesutils.AnyLessThan(max, req)
	return quotav1.Subtract(max, req), exceed
}

// accumulateAssumedQuota aggregates resource quantities assumed for specified
// quota group, it will release stale assumed quotas first before aggregating.
func (aqs *assumedQuotas) accumulateAssumedQuota(quotaName string) corev1.ResourceList {
	aqs.mutex.Lock()
	aqs.releaseInvalidAssumedQuotas(quotaName)
	aqs.mutex.Unlock()

	aqs.mutex.RLock()
	defer aqs.mutex.RUnlock()

	var accumulated corev1.ResourceList
	for key := range aqs.assumed[quotaName] {
		accumulated = quotav1.Add(accumulated, aqs.assumed[quotaName][key].qu.Resources)
	}
	return accumulated
}

// releaseInvalidAssumedQuotas invalidates and release expired assumed quotas.
func (aqs *assumedQuotas) releaseInvalidAssumedQuotas(quotaName string) {
	for key, aq := range aqs.assumed[quotaName] {
		if aq.expired(aqs.latestStatusFunc) {
			klog.V(3).Infof("assumed queue unit %s expired, release it", aq.qu.Key())
			aqs.cancelWithoutLock(quotaName, key)
		}
	}
}

func (aqs *assumedQuotas) cancelWithoutLock(quotaName, key string) {
	delete(aqs.assumed[quotaName], key)
}

// PreDequeue runs pre-dequeue filter for qu.
func (qp *quotaPlugin) PreDequeue(ctx context.Context, qu *coordinator.QueueUnit) coordinator.PluginStatus {
	// assumeQuota consumed before QueueUnit was dequeued, because used quota will not immediately
	// be updated until pods created and quota-controller calculates total used then apply to cluster.
	qp.assumedQuotas.assume(qu.Tenant, qu)
	return coordinator.NewPluginStatus(coordinator.Success)
}

func (qp *quotaPlugin) Name() string {
	return QuotaPluginName
}

func (qp *quotaPlugin) retrieveLatestStatus(qu *coordinator.QueueUnit) (commonapis.JobStatus, error) {
	copiedObj := qu.Job.DeepCopyObject().(client.Object)
	if err := qp.client.Get(context.Background(), types.NamespacedName{
		Name:      qu.Job.GetName(),
		Namespace: qu.Job.GetNamespace(),
	}, copiedObj); err != nil {
		return commonapis.JobStatus{}, err
	}
	return statusTraitor(copiedObj)
}

// statusTraitor traits workload status field from object scheme.
func statusTraitor(obj metav1.Object) (commonapis.JobStatus, error) {
	bytes, err := json.Marshal(obj)
	if err != nil {
		return commonapis.JobStatus{}, err
	}
	status := commonapis.JobStatus{}
	if err = json.Unmarshal([]byte(gjson.GetBytes(bytes, "status").String()), &status); err != nil {
		return commonapis.JobStatus{}, err
	}
	return status, nil
}

/* Assumed quotas. */

type assumedQuotas struct {
	mutex sync.RWMutex

	// assumed is a map of assumed quantities of requested quota in case that
	// calculation delay leads to dequeue objects more than expected.
	assumed map[string]map[string]assumedQuota

	// latestStatusFunc retrieves the latest QueueUnit status from cluster.
	latestStatusFunc func(qu *coordinator.QueueUnit) (commonapis.JobStatus, error)
}

// assume specifies resource quantity has been changed for this
// quota group and will be invalid after defaultQuotaAssumedTimeoutSeconds.
func (aqs *assumedQuotas) assume(quotaName string, qu *coordinator.QueueUnit) {
	aqs.mutex.Lock()
	defer aqs.mutex.Unlock()

	if aqs.assumed[quotaName] == nil {
		aqs.assumed[quotaName] = make(map[string]assumedQuota)
	}

	klog.V(5).Infof("assume resource %+v of quota %s", qu.Resources, quotaName)
	aqs.assumed[quotaName][qu.Key()] = assumedQuota{
		qu:        qu,
		timestamp: time.Now(),
	}
}

func (aqs *assumedQuotas) cancel(quotaName, key string) {
	aqs.mutex.Lock()
	defer aqs.mutex.Unlock()

	aqs.cancelWithoutLock(quotaName, key)
}

/* The base element: assumed quota. */

type assumedQuota struct {
	qu        *coordinator.QueueUnit
	timestamp time.Time
}

// expired checks whether the QueueUnit of aq is expired or not.
func (aq *assumedQuota) expired(statusFunc func(qu *coordinator.QueueUnit) (commonapis.JobStatus, error)) bool {
	if time.Now().Sub(aq.timestamp).Seconds() > defaultQuotaAssumedTimeoutSeconds {
		klog.V(5).Infof("queue unit %s resources assumed time exceeds threshold [%v], mark it as expired",
			aq.qu.Key(), defaultQuotaAssumedTimeoutSeconds)
		return true
	}

	status, err := statusFunc(aq.qu)
	if err != nil {
		if errors.IsNotFound(err) {
			return true
		}
		klog.Errorf("failed to get latest status for queue unit %s, err: %v", aq.qu.Key(), err)
	}

	klog.V(4).Infof("latest status for queue unit %s: %v", aq.qu.Key(), jsonDump(status))
	if utils.IsRunning(status) || utils.IsFailed(status) || utils.IsSucceeded(status) {
		return true
	}

	return false
}

func jsonDump(obj interface{}) string {
	content, _ := json.Marshal(obj)
	return string(content)
}
