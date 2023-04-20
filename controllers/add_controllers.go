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

package controllers

import (
	modelv1alpha1 "github.com/hliangzhao/torch-on-k8s/apis/model/v1alpha1"
	trainv1alpha1 "github.com/hliangzhao/torch-on-k8s/apis/train/v1alpha1"
	"github.com/hliangzhao/torch-on-k8s/controllers/model"
	"github.com/hliangzhao/torch-on-k8s/controllers/train"
	"github.com/hliangzhao/torch-on-k8s/pkg/common"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog"
	controllerruntime "sigs.k8s.io/controller-runtime"
)

/* Set up the controllers. */

var (
	SetupWithManagerFactory = make(map[runtime.Object]func(mgr controllerruntime.Manager, config common.JobControllerConfiguration) error)
)

func SetupWithManager(mgr controllerruntime.Manager, config common.JobControllerConfiguration) error {
	for workload, register := range SetupWithManagerFactory {
		if err := register(mgr, config); err != nil {
			return err
		}
		klog.Infof("CustomResource %v's controller has started.", workload.GetObjectKind())
	}
	return nil
}

func init() {
	SetupWithManagerFactory[&trainv1alpha1.TorchJob{}] = func(mgr controllerruntime.Manager, config common.JobControllerConfiguration) error {
		return train.NewTorchJobReconciler(mgr, config).SetupWithManager(mgr)
	}
	SetupWithManagerFactory[&modelv1alpha1.ModelVersion{}] = func(mgr controllerruntime.Manager, config common.JobControllerConfiguration) error {
		return model.NewModelVersionReconciler(mgr, config).SetupWithManager(mgr)
	}
}
