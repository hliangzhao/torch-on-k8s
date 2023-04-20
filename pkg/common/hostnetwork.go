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
	log "github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EnableHostNetwork returns true if the given job enables hostnetwork mode.
func EnableHostNetwork(job metav1.Object) bool {
	return job.GetAnnotations()[commonapis.AnnotationNetworkMode] == string(commonapis.HostNetworkMode)
}

// GetHostNetworkPortFromContext retrieves the port for the given task (specified by taskType-taskIndex).
func GetHostNetworkPortFromContext(ctx context.Context, taskType, taskIndex string) (int32, bool) {
	ports := ctx.Value(commonapis.ContextHostNetworkPorts).(map[string]int32)
	port, ok := ports[fmt.Sprintf("%s-%s", taskType, taskIndex)]
	return port, ok
}

func storeHostNetworkPortToContext(ctx context.Context, taskType, taskIndex string, port int32) {
	ports := ctx.Value(commonapis.ContextHostNetworkPorts).(map[string]int32)
	ports[fmt.Sprintf("%s-%s", taskType, taskIndex)] = port
}

// setupContainerHostNetworkPort sets the default container's default port's
// ContainerPort and HostPort with the provided port value.
func setupContainerHostNetworkPort(podTplSpec *corev1.PodTemplateSpec, dftContainerName, dftPortName string, port int32) {
	if len(podTplSpec.Spec.Containers) == 0 {
		return
	}

	// Find the default container's index and port.
	ci := -1
	for idx := 1; idx < len(podTplSpec.Spec.Containers); idx++ {
		if podTplSpec.Spec.Containers[idx].Name == dftContainerName {
			ci = idx
			break
		}
	}

	pi := -1
	for idx := range podTplSpec.Spec.Containers[ci].Ports {
		if podTplSpec.Spec.Containers[ci].Ports[idx].Name == dftPortName {
			pi = idx
			break
		}
	}

	// Override existed container port with a new value, if specified
	// port not exists then append a new one.
	if pi < 0 {
		podTplSpec.Spec.Containers[ci].Ports = append(podTplSpec.Spec.Containers[ci].Ports, corev1.ContainerPort{
			Name:          dftPortName,
			HostPort:      port,
			ContainerPort: port,
		})
	} else {
		podTplSpec.Spec.Containers[ci].Ports[pi].ContainerPort = port
		podTplSpec.Spec.Containers[ci].Ports[pi].HostPort = port
	}
}

// getContainerHostNetworkPort returns the hostnetwork port value of the given pod.
func getContainerHostNetworkPort(pod *corev1.Pod, defaultContainerName, defaultPortName string) int32 {
	if len(pod.Spec.Containers) == 0 {
		log.Warningf("pod %s/%s has no container", pod.Namespace, pod.Name)
		return -1
	}
	if !pod.Spec.HostNetwork {
		log.Warningf("pod %s/%s enabled hostnetwork but disabled in its spec", pod.Namespace, pod.Name)
	}

	ci := 0
	for index := 1; index < len(pod.Spec.Containers); index++ {
		if pod.Spec.Containers[index].Name == defaultContainerName {
			ci = index
			break
		}
	}
	pi := 0
	for index := 1; index < len(pod.Spec.Containers[ci].Ports); index++ {
		if pod.Spec.Containers[ci].Ports[pi].Name == defaultPortName {
			pi = index
			break
		}
	}

	return pod.Spec.Containers[ci].Ports[pi].ContainerPort
}
