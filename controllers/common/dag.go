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
	trainv1alpha1 "github.com/hliangzhao/torch-on-k8s/apis/train/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog"
	"strings"
)

/* DAG condition checking. */

// CheckDAGConditionReady checks whether the DAG conditions are ready for the given job.
func (jc *JobController) CheckDAGConditionReady(job metav1.Object, tasks map[trainv1alpha1.TaskType]*trainv1alpha1.TaskSpec,
	pods []*corev1.Pod, dagConditions []trainv1alpha1.DAGCondition) bool {

	if len(dagConditions) == 0 {
		return true
	}

	klog.Infof("start to check DAG conditions of job %s/%s.", job.GetNamespace(), job.GetName())

	taskTypes := make([]trainv1alpha1.TaskType, 0, len(tasks))
	for tt := range tasks {
		taskTypes = append(taskTypes, tt)
	}
	sortedPods := jc.sortPodsByTaskType(pods, taskTypes)
	for i := range dagConditions {
		if !jc.upstreamTasksReady(sortedPods, tasks, dagConditions[i]) {
			klog.Infof("DAG condition has not ready, upstream: %s, on phase: %s",
				dagConditions[i].UpstreamTaskType, dagConditions[i].OnPhase)
			return false
		}
	}

	klog.Infof("DAG conditions of job %s/%s has all ready.", job.GetNamespace(), job.GetName())
	return true
}

// sortPodsByTaskType collects the pods by task type.
// The pods of the same task type will be inserted into the same list.
func (jc *JobController) sortPodsByTaskType(pods []*corev1.Pod, taskTypes []trainv1alpha1.TaskType) map[trainv1alpha1.TaskType][]*corev1.Pod {
	var sortedPods = make(map[trainv1alpha1.TaskType][]*corev1.Pod)
	var collectors = make(map[string]func(pod *corev1.Pod))

	for _, tt := range taskTypes {
		taskType := strings.ToLower(string(tt))
		collectors[taskType] = func(pod *corev1.Pod) {
			sortedPods[tt] = append(sortedPods[tt], pod)
		}
	}

	for _, pod := range pods {
		tt := pod.Labels[trainv1alpha1.LabelTaskType]
		if collector := collectors[tt]; collector != nil {
			collector(pod)
		}
	}

	return sortedPods
}

// upstreamTasksReady checks whether the given dagCondition is satisfied.
// The check has two steps:
// (1) The successfully created pods resource number in cluster should not smaller than the number of upstream task pods.
// (2) All the successfully created pods resource should enter the expected phase.
func (jc *JobController) upstreamTasksReady(taskPods map[trainv1alpha1.TaskType][]*corev1.Pod,
	tasks map[trainv1alpha1.TaskType]*trainv1alpha1.TaskSpec, dagCondition trainv1alpha1.DAGCondition) bool {

	upstreamTaskSpec, ok := tasks[dagCondition.UpstreamTaskType]
	if !ok {
		// upstream task does not exist, return true directly
		return true
	}
	upstreamTaskPods := taskPods[dagCondition.UpstreamTaskType]
	numTasks := *upstreamTaskSpec.NumTasks

	// check upstream pods creation
	// TODO: When gang scheduling is enabled and MinMember is set, how this is calculated?
	if len(upstreamTaskPods) < int(numTasks) {
		klog.V(3).Infof("upstream pods has not reach expected replicas, expected: %d, now: %d", numTasks, len(upstreamTaskPods))
		return false
	}

	// check whether all the upstream pods reach expected phase
	for _, pod := range upstreamTaskPods {
		if phaseCodes[pod.Status.Phase]-phaseCodes[dagCondition.OnPhase] < 0 {
			return false
		}
	}

	return true
}

var phaseCodes = map[corev1.PodPhase]int{
	corev1.PodPending:   0,
	corev1.PodRunning:   1,
	corev1.PodSucceeded: 2,
	corev1.PodFailed:    2,
}
