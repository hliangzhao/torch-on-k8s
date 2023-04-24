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

package metrics

import (
	"context"
	trainv1alpha1 "github.com/hliangzhao/torch-on-k8s/apis/train/v1alpha1"
	"github.com/hliangzhao/torch-on-k8s/pkg/utils"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strings"
)

var (
	created = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "torch_on_k8s_jobs_created",
		Help: "Counts number of jobs created",
	}, []string{"kind"})

	deleted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "torch_on_k8s_jobs_deleted",
		Help: "Counts number of jobs deleted",
	}, []string{"kind"})

	success = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "torch_on_k8s_jobs_successful",
		Help: "Counts number of jobs successfully finished",
	}, []string{"kind"})

	failure = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "torch_on_k8s_jobs_failed",
		Help: "Counts number of jobs failed",
	}, []string{"kind"})

	restart = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "torch_on_k8s_jobs_restarted",
		Help: "Counts number of jobs restarted",
	}, []string{"kind"})

	firstPodLaunchDelayHist = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "torch_on_k8s_jobs_first_pod_launch_delay_seconds",
		Help: "Histogram for recording launch delay duration(from job created to first pod running).",
	}, []string{"kind", "name", "namespace", "uid"})

	allPodsLaunchDelayHist = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "torch_on_k8s_jobs_all_pods_launch_delay_seconds",
		Help: "Histogram for recording sync launch delay duration(from job created to all pods running).",
	}, []string{"kind", "name", "namespace", "uid"})
)

// JobMetrics holds the kinds of metrics counter for some type of job workload.
type JobMetrics struct {
	kind                string
	created             prometheus.Counter
	deleted             prometheus.Counter
	success             prometheus.Counter
	failure             prometheus.Counter
	restart             prometheus.Counter
	firstPodLaunchDelay *prometheus.HistogramVec
	allPodsLaunchDelay  *prometheus.HistogramVec
}

// NewJobMetrics creates a new job metric instance for monitoring the resource objects identified by api resource kind.
func NewJobMetrics(kind string, client client.Client) *JobMetrics {
	lowerKind := strings.ToLower(kind)
	label := prometheus.Labels{"kind": lowerKind}
	metrics := &JobMetrics{
		kind:                kind,
		created:             created.With(label),
		deleted:             deleted.With(label),
		success:             success.With(label),
		failure:             failure.With(label),
		restart:             restart.With(label),
		firstPodLaunchDelay: firstPodLaunchDelayHist,
		allPodsLaunchDelay:  allPodsLaunchDelayHist,
	}

	// Register running gauge func on center prometheus demand pull.
	promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Name:        "torch_on_k8s_jobs_running",
		Help:        "Counts number of jobs running currently",
		ConstLabels: label,
	}, func() float64 {
		running, err := jobStatusCounter(kind, client, utils.IsRunning)
		if err != nil {
			return 0
		}
		return float64(running)
	})

	// Register pending gauge func on center prometheus demand pull.
	promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Name:        "torch_on_k8s_jobs_pending",
		Help:        "Counts number of jobs pending currently",
		ConstLabels: label,
	}, func() float64 {
		pending, err := jobStatusCounter(kind, client, func(jobStatus trainv1alpha1.JobStatus) bool {
			return utils.IsCreated(jobStatus) && len(jobStatus.Conditions) == 1
		})
		if err != nil {
			return 0
		}
		return float64(pending)
	})

	return metrics
}

func jobStatusCounter(kind string, reader client.Reader, filter func(status trainv1alpha1.JobStatus) bool) (result int32, err error) {
	var list client.ObjectList
	if obj, ok := listObjectMap[kind]; ok {
		list = obj.DeepCopyObject().(client.ObjectList)
	}
	err = reader.List(context.Background(), list)
	if err != nil {
		return 0, err
	}
	statuses := getJobStatusList(list, kind)
	result = int32(0)
	for _, status := range statuses {
		if filter(*status) {
			result++
		}
	}
	return result, nil
}

var (
	listObjectMap = map[string]client.ObjectList{
		trainv1alpha1.TorchJobKind: &trainv1alpha1.TorchJobList{},
	}
)

func getJobStatusList(obj runtime.Object, kind string) []*trainv1alpha1.JobStatus {
	statuses := make([]*trainv1alpha1.JobStatus, 0)
	switch kind {
	case trainv1alpha1.TorchJobKind:
		pytorchList := obj.(*trainv1alpha1.TorchJobList)
		for idx := range pytorchList.Items {
			statuses = append(statuses, &pytorchList.Items[idx].Status)
		}
	}
	return statuses
}

func (m *JobMetrics) CreatedInc() {
	m.created.Inc()
}

func (m *JobMetrics) DeletedInc() {
	m.deleted.Inc()
}

func (m *JobMetrics) SuccessInc() {
	m.success.Inc()
}

func (m *JobMetrics) FailureInc() {
	m.failure.Inc()
}

func (m *JobMetrics) RestartInc() {
	m.restart.Inc()
}

// FirstPodLaunchDelaySeconds calculates the time duration from the job creation time
// to the time the first pod entering into the running state.
func (m *JobMetrics) FirstPodLaunchDelaySeconds(activePods []*corev1.Pod, job metav1.Object, jobStatus trainv1alpha1.JobStatus) {
	if !utils.IsRunning(jobStatus) {
		return
	}

	var earliestTime *metav1.Time
	for _, pod := range activePods {
		if pod.Status.Phase != corev1.PodRunning {
			continue
		}
		readyCond := getPodCondition(&pod.Status, corev1.PodReady)
		if readyCond == nil {
			continue
		}
		if earliestTime == nil || readyCond.LastTransitionTime.Before(earliestTime) {
			earliestTime = &readyCond.LastTransitionTime
		}
	}
	if earliestTime == nil {
		return
	}
	delay := earliestTime.Time.Sub(job.GetCreationTimestamp().Time).Seconds()

	m.firstPodLaunchDelay.With(prometheus.Labels{
		"kind":      m.kind,
		"name":      job.GetName(),
		"namespace": job.GetNamespace(),
		"uid":       string(job.GetUID()),
	}).Observe(delay)
}

// AllPodsLaunchDelaySeconds calculates the time duration from the job creation time
// to the time all pods entering into the running state.
func (m *JobMetrics) AllPodsLaunchDelaySeconds(pods []*corev1.Pod, job metav1.Object, jobStatus trainv1alpha1.JobStatus) {
	if !utils.IsRunning(jobStatus) || jobStatus.StartTime == nil {
		return
	}

	finalTime := job.GetCreationTimestamp().Time
	for _, pod := range pods {
		if pod.Status.Phase != corev1.PodRunning {
			return
		}
		readyCond := getPodCondition(&pod.Status, corev1.PodReady)
		if readyCond == nil {
			continue
		}
		if readyCond.LastTransitionTime.After(finalTime) {
			finalTime = readyCond.LastTransitionTime.Time
		}
	}
	syncDelay := finalTime.Sub(job.GetCreationTimestamp().Time).Seconds()

	m.allPodsLaunchDelay.With(prometheus.Labels{
		"kind":      m.kind,
		"name":      job.GetName(),
		"namespace": job.GetNamespace(),
		"uid":       string(job.GetUID()),
	}).Observe(syncDelay)
}

func getPodCondition(podStatus *corev1.PodStatus, condType corev1.PodConditionType) *corev1.PodCondition {
	for idx := range podStatus.Conditions {
		if podStatus.Conditions[idx].Type == condType {
			return &podStatus.Conditions[idx]
		}
	}
	return nil
}
