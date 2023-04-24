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

package torchelastic

import (
	"context"
	trainv1alpha1 "github.com/hliangzhao/torch-on-k8s/apis/train/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

func OnOwnerCreateFunc(r *ElasticTorchJobReconciler) func(e event.CreateEvent) bool {
	return func(e event.CreateEvent) bool {
		torchjob, ok := e.Object.(*trainv1alpha1.TorchJob)
		if !ok {
			return true
		}
		if !torchjob.Spec.EnableTorchElastic && torchjob.Spec.TorchElasticPolicy == nil {
			return true
		}
		ctx, cancel := context.WithCancel(context.Background())
		ctx = context.WithValue(ctx, "job", torchjob.Name)
		ctx = context.WithValue(ctx, "namespace", torchjob.Namespace)
		log.Info("create torchelastic-enabled torchjob", "job", torchjob.Name, "namespace", torchjob.Namespace)
		r.jobs[getElasticTorchJobName(torchjob.Name, torchjob.Namespace)] = ElasticTorchJob{
			Name:       torchjob.Name,
			Namespace:  torchjob.Namespace,
			ctx:        ctx,
			cancelFunc: cancel,
		}
		return true
	}
}

func OnOwnerDeleteFunc(r *ElasticTorchJobReconciler) func(e event.DeleteEvent) bool {
	return func(e event.DeleteEvent) bool {
		torchjob, ok := e.Object.(*trainv1alpha1.TorchJob)
		if !ok {
			return true
		}
		if !torchjob.Spec.EnableTorchElastic && torchjob.Spec.TorchElasticPolicy == nil {
			return true
		}
		log.Info("delete torchelastic-enabled torchjob", "job", torchjob.Name, "namespace", torchjob.Namespace)
		if _, ok = r.jobs[getElasticTorchJobName(torchjob.Name, torchjob.Namespace)]; ok {
			cancel := r.jobs[getElasticTorchJobName(torchjob.Name, torchjob.Namespace)].cancelFunc
			defer cancel()
			delete(r.jobs, getElasticTorchJobName(torchjob.Name, torchjob.Namespace))
			delete(r.metrics, getElasticTorchJobName(torchjob.Name, torchjob.Namespace))
		}
		return true
	}
}
