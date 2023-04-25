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

package storage

import (
	modelv1alpha1 "github.com/hliangzhao/torch-on-k8s/apis/model/v1alpha1"
	corev1 "k8s.io/api/core/v1"
)

/* The storage interface. */

type Storage interface {
	// CreatePersistentVolume creates a pv with the provided storage type.
	CreatePersistentVolume(mv *modelv1alpha1.Storage, pvName string) *corev1.PersistentVolume

	// AddModelVolumeToPodSpec creates a volume and mounts the volume into all the containers of the pod.
	AddModelVolumeToPodSpec(mv *modelv1alpha1.Storage, podTplSpec *corev1.PodTemplateSpec)

	// GetModelMountPath returns the mount path where the model artifact will be stored.
	GetModelMountPath(mv *modelv1alpha1.Storage) string
}
