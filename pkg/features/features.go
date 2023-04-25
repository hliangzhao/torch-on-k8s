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

package features

import (
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/component-base/featuregate"
)

/* Features that can be enabled and disabled. */

func init() {
	runtime.Must(FeatureGates.Add(supportedFeatures))
}

// TODO: Figure out how the last two are used.
const (
	// GangScheduling enables pluggable gang scheduling for jobs.
	// TODO: What if gang scheduling is disabled in JobControllerConfig?
	GangScheduling featuregate.Feature = "GangScheduling"

	// DAGScheduling enables DAG scheduling workflow between different task types of a job.
	DAGScheduling featuregate.Feature = "DAGScheduling"

	// JobCoordinator enables coordinator to coordinate jobs for better fairness and efficiency.
	JobCoordinator featuregate.Feature = "JobCoordinator"

	// TorchLocalMasterAddr explicitly declares to use localhost as master self listened
	// address, it's usually adopted in version < torch 1.9, in >=1.9 distributed communication
	// style, master address value should be aligned with workers, set by master service name.
	TorchLocalMasterAddr featuregate.Feature = "TorchLocalMasterAddr"

	// HostNetWithHeadlessSvc constructs connections intra pods leveraging headless service
	// instead of normal service with different port/targetPort, it bypasses traffic routing
	// but pod may not find correct host port after failovered. If enable hostnetwork,
	// HostNetWithHeadlessSvc should be disabled.
	HostNetWithHeadlessSvc featuregate.Feature = "HostNetWithHeadlessSvc"
)

var (
	FeatureGates = featuregate.NewFeatureGate()

	supportedFeatures = map[featuregate.Feature]featuregate.FeatureSpec{
		GangScheduling:         {Default: true, PreRelease: featuregate.Beta},
		DAGScheduling:          {Default: true, PreRelease: featuregate.Beta},
		TorchLocalMasterAddr:   {Default: true, PreRelease: featuregate.Beta},
		JobCoordinator:         {Default: true, PreRelease: featuregate.Beta},
		HostNetWithHeadlessSvc: {Default: false, PreRelease: featuregate.Alpha},
	}
)
