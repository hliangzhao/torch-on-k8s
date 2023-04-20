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

package train

import (
	"context"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

/* Service related controls. */

func (r *TorchJobReconciler) GetServicesForJob(job interface{}) ([]*corev1.Service, error) {
	jobObj, err := meta.Accessor(job)
	if err != nil {
		return nil, err
	}

	selector, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{
		MatchLabels: r.jobController.GenerateLabels(jobObj.GetName()),
	})
	if err != nil {
		return nil, err
	}

	// List all pods to include those that don't match the selector anymore
	// but have a ControllerRef pointing to this controller.
	serviceList := &corev1.ServiceList{}
	err = r.List(context.Background(), serviceList, client.MatchingLabelsSelector{Selector: selector})
	if err != nil {
		return nil, err
	}

	return r.jobController.AdoptAndClaimServices(jobObj, serviceList)
}
