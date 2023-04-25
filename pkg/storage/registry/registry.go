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

package registry

import (
	modelv1alpha1 "github.com/hliangzhao/torch-on-k8s/apis/model/v1alpha1"
	"github.com/hliangzhao/torch-on-k8s/pkg/storage"
	"github.com/hliangzhao/torch-on-k8s/pkg/storage/localstorage"
	"github.com/hliangzhao/torch-on-k8s/pkg/storage/nfs"
)

var (
	StorageProviders = make(map[string]storage.Storage)
)

func init() {
	StorageProviders["LocalStorage"] = localstorage.NewLocalStorageProvider()
	StorageProviders["NFS"] = nfs.NewNFSProvider()
}

// GetStorageProvider returns the storage provider based on the adopted storage type (local storage or nfs).
func GetStorageProvider(modelstorage *modelv1alpha1.Storage) storage.Storage {
	if modelstorage.NFS != nil {
		return StorageProviders["NFS"]
	}
	if modelstorage.LocalStorage != nil {
		return StorageProviders["LocalStorage"]
	}
	return nil
}
