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

package nfs

import (
	modelv1alpha1 "github.com/hliangzhao/torch-on-k8s/apis/model/v1alpha1"
	"github.com/hliangzhao/torch-on-k8s/pkg/storage"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func NewNFSProvider() storage.Storage {
	return &NFSProvider{}
}

var _ storage.Storage = &NFSProvider{}

type NFSProvider struct {
}

func (np *NFSProvider) AddModelVolumeToPodSpec(modelstorage *modelv1alpha1.Storage, podTplSpec *corev1.PodTemplateSpec) {
	// create the volume with the source being remote nfs server
	podTplSpec.Spec.Volumes = append(podTplSpec.Spec.Volumes,
		corev1.Volume{
			Name: "modelvolume",
			VolumeSource: corev1.VolumeSource{
				NFS: &corev1.NFSVolumeSource{
					Path:   modelstorage.NFS.Path,
					Server: modelstorage.NFS.Server,
				},
			},
		})

	// mount the volume for each container
	for idx := range podTplSpec.Spec.Containers {
		podTplSpec.Spec.Containers[idx].VolumeMounts = append(podTplSpec.Spec.Containers[idx].VolumeMounts,
			corev1.VolumeMount{
				Name: "modelvolume", MountPath: modelstorage.LocalStorage.MountPath,
			})
	}
}

func (np *NFSProvider) CreatePersistentVolume(modelstorage *modelv1alpha1.Storage, pvName string) *corev1.PersistentVolume {
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: pvName,
		},
		Spec: corev1.PersistentVolumeSpec{
			Capacity: corev1.ResourceList{
				// the 1Gi capacity is not enforced.
				// This is specified because api-server validation checks a capacity value to be present.
				corev1.ResourceStorage: resource.MustParse("1Gi"),
			},
			AccessModes:                   []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				NFS: &corev1.NFSVolumeSource{
					Server: modelstorage.NFS.Server,
					Path:   modelstorage.NFS.Path,
				},
			},
			StorageClassName: "",
		},
	}
	return pv
}

func (np *NFSProvider) GetModelMountPath(mv *modelv1alpha1.Storage) string {
	return mv.LocalStorage.MountPath
}
