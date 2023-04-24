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

package main

import (
	"github.com/hliangzhao/torch-on-k8s/apis"
	"github.com/hliangzhao/torch-on-k8s/controllers"
	"github.com/hliangzhao/torch-on-k8s/controllers/common"
	"github.com/hliangzhao/torch-on-k8s/controllers/train/torchelastic"
	"github.com/hliangzhao/torch-on-k8s/pkg/features"
	gangschedulerregistry "github.com/hliangzhao/torch-on-k8s/pkg/gangscheduler/registry"
	"github.com/hliangzhao/torch-on-k8s/pkg/metrics"
	"github.com/spf13/pflag"
	"k8s.io/apimachinery/pkg/util/net"
	"os"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	_ = clientgoscheme.AddToScheme(scheme)
}

func main() {
	// parse the argument values into the following variables
	var (
		enableLeaderElection bool
		metricsAddr          int
		ctrlMetricsAddr      string
		hostPortRange        string
	)
	pflag.StringVar(&ctrlMetricsAddr, "controller-metrics-addr", ":8080", "The address the controller metric endpoint binds to.")
	pflag.IntVar(&metricsAddr, "metrics-addr", 8443, "The address the default endpoints binds to.")
	pflag.BoolVar(&enableLeaderElection, "enable-leader-election", true,
		"Enable leader election for controller manager. Enabling this will ensure there is only one active controller manager.")
	pflag.BoolVar(&common.JobControllerConfig.EnableGangScheduling, "enable-gang-scheduling", true, "Enable gang scheduling or not. If enabled, Volcano will be adopted as the gang scheduler")
	pflag.IntVar(&common.JobControllerConfig.MaxNumConcurrentReconciles, "max-reconciles", 1, "Specify the number of max concurrent reconciles of each controller")
	pflag.StringVar(&common.JobControllerConfig.ModelImageBuilder, "model-image-builder", "kubedl/kaniko:latest", "The image name of container builder for building the model image")
	pflag.StringVar(&hostPortRange, "hostnetwork-port-range", "20000-30000", "Hostnetwork port range for hostnetwork-enabled jobs")
	features.FeatureGates.AddFlag(pflag.CommandLine)
	pflag.Parse()

	common.JobControllerConfig.HostNetworkPortRange = *net.ParsePortRangeOrDie(hostPortRange)
	if common.JobControllerConfig.MaxNumConcurrentReconciles <= 0 {
		common.JobControllerConfig.MaxNumConcurrentReconciles = 1
	}

	ctrl.SetLogger(zap.New(zap.UseDevMode(false)))

	// start the controller manager
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:             scheme,
		MetricsBindAddress: ctrlMetricsAddr,
		Port:               9443,
		LeaderElection:     enableLeaderElection,
		LeaderElectionID:   "torch-on-k8s-election",
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	// setup registered scheme
	setupLog.Info("setting up scheme")
	if err := apis.AddToScheme(mgr.GetScheme()); err != nil {
		setupLog.Error(err, "unable to add APIs to scheme")
		os.Exit(1)
	}

	// setup gang schedulers
	setupLog.Info("setting up gang schedulers")
	gangschedulerregistry.Register(mgr)

	// setup controllers
	if err = controllers.SetupWithManager(mgr, common.JobControllerConfig); err != nil {
		setupLog.Error(err, "unable to create controller")
		os.Exit(1)
	}

	// TODO: Move this to add_controllers.go
	// setup torchelastic controller
	if err = torchelastic.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create torchelastic controller")
	}

	// start metrics monitoring
	metrics.StartMonitoringForDefaultRegistry(metricsAddr)

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
