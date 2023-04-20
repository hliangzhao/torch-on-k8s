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

package kubeconfig

import (
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"net"
	"os"
)

/* Kubeconfig and client related. */

// RecommendedConfigPathEnvVar is an environment variable for path configuration
const RecommendedConfigPathEnvVar = "KUBECONFIG"

// GetClusterConfig obtain the config from the Kube configuration used by kubeconfig, or from k8s cluster.
func GetClusterConfig() (*rest.Config, error) {
	if len(os.Getenv(RecommendedConfigPathEnvVar)) > 0 {
		// Use the current context in kubeconfig
		// This is very useful for running locally.
		return clientcmd.BuildConfigFromFlags("", os.Getenv(RecommendedConfigPathEnvVar))
	}

	// Work around https://github.com/kubernetes/kubernetes/issues/40973
	// See https://github.com/coreos/etcd-operator/issues/731#issuecomment-283804819
	if len(os.Getenv("KUBERNETES_SERVICE_HOST")) == 0 {
		addrs, err := net.LookupHost("kubernetes.default.svc")
		if err != nil {
			panic(err)
		}
		if err := os.Setenv("KUBERNETES_SERVICE_HOST", addrs[0]); err != nil {
			return nil, err
		}
	}
	if len(os.Getenv("KUBERNETES_SERVICE_PORT")) == 0 {
		if err := os.Setenv("KUBERNETES_SERVICE_PORT", "443"); err != nil {
			panic(err)
		}
	}
	return rest.InClusterConfig()
}
