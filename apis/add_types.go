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

package apis

import (
	modelv1alpha1 "github.com/hliangzhao/torch-on-k8s/apis/model/v1alpha1"
	trainv1alpha1 "github.com/hliangzhao/torch-on-k8s/apis/train/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
)

var AddToSchemes runtime.SchemeBuilder

func init() {
	// Add all the CRDs into scheme
	AddToSchemes = append(AddToSchemes,
		trainv1alpha1.AddToScheme,
		modelv1alpha1.AddToScheme,
	)
}

func AddToScheme(s *runtime.Scheme) error {
	return AddToSchemes.AddToScheme(s)
}
