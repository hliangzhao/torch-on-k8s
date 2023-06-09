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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/net"
)

/* Config of the job controller. */

var JobControllerConfig JobControllerConfiguration

// JobControllerConfiguration describes the configuration of the job controller.
type JobControllerConfiguration struct {
	// Gang scheduling is enabled or not. If enabled, Volcano will be used as the gang scheduler.
	EnableGangScheduling bool
	// MaxNumConcurrentReconciles is the maximum number of concurrent go routines of job reconciliation.
	MaxNumConcurrentReconciles int
	// ReconcilerSyncLoopPeriod is the amount of time the reconciler syncs states loop
	// wait between two reconciler sync. It is set to 15 sec by default.
	ReconcilerSyncLoopPeriod metav1.Duration
	// HostNetworkPortRange is the range of ports used for hostnetwork mode.
	HostNetworkPortRange net.PortRange
	// ModelImageBuilder is the Kaniko image name that used for building model image.
	ModelImageBuilder string
}
