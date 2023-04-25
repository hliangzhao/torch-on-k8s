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

package localstorage

import (
	modelv1alpha1 "github.com/hliangzhao/torch-on-k8s/apis/model/v1alpha1"
	"github.com/hliangzhao/torch-on-k8s/pkg/storage"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func NewLocalStorageProvider() storage.Storage {
	return &Provider{}
}

var _ storage.Storage = &Provider{}

type Provider struct{}

// CreatePersistentVolume creates a pv with the source being local host path.
func (lsp *Provider) CreatePersistentVolume(modelstorage *modelv1alpha1.Storage, pvName string) *corev1.PersistentVolume {
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			// Note that pv is cluster-level, not namespace-level.
			Name: pvName,
		},
		Spec: corev1.PersistentVolumeSpec{
			AccessModes:                   []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				// Use local host path as the pv source.
				Local: &corev1.LocalVolumeSource{
					Path: modelstorage.LocalStorage.Path,
				},
			},
			Capacity: corev1.ResourceList{
				// The 500Mi capacity is not enforced for local path volume.
				// This is specified because api-server validation checks a capacity value to be present.
				corev1.ResourceStorage: resource.MustParse("500Mi"),
			},
			StorageClassName: "",
		},
	}

	// NOTE: I try to design a heuristic-based node selection algorithm to select the best node for model saving.
	// However, this violates the usage of host path. The volume created with host path must be located on the
	// node where the pod who creates it located. And, the following node affinity is necessary.
	pv.Spec.NodeAffinity = &corev1.VolumeNodeAffinity{
		Required: &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{
				{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{
							Key:      "kubernetes.io/hostname",
							Operator: corev1.NodeSelectorOpIn,
							Values:   []string{modelstorage.LocalStorage.NodeName},
						},
					},
				},
			},
		},
	}

	return pv
}

// AddModelVolumeToPodSpec creates a volume with source being local host path,
// and mounts the volume into all the containers of the pod.
func (lsp *Provider) AddModelVolumeToPodSpec(modelstorage *modelv1alpha1.Storage, podTplSpec *corev1.PodTemplateSpec) {
	// create the volume with the source being host path
	podTplSpec.Spec.Volumes = append(podTplSpec.Spec.Volumes,
		corev1.Volume{
			Name: "modelvolume",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: modelstorage.LocalStorage.Path,
				},
			},
		})

	// mount the volume for each container
	for idx := range podTplSpec.Spec.Containers {
		podTplSpec.Spec.Containers[idx].VolumeMounts = append(podTplSpec.Spec.Containers[idx].VolumeMounts,
			corev1.VolumeMount{
				Name:      "modelvolume",
				MountPath: modelstorage.LocalStorage.MountPath,
			})
	}
}

// GetModelMountPath returns the mount path where the model artifact will be stored.
func (lsp *Provider) GetModelMountPath(modelstorage *modelv1alpha1.Storage) string {
	return modelstorage.LocalStorage.MountPath
}
