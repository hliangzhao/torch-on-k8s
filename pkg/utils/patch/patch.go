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

package patch

import (
	"encoding/json"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strings"
)

const (
	NullHolder    = "NULL_HOLDER"
	NullHolderStr = "\"NULL_HOLDER\""
)

// NewStrategicPatch returns a strategic-merge-patch type patch entity, which applies
// to build-in resource like pods, services...
func NewStrategicPatch() *Patch {
	return &Patch{patchType: types.StrategicMergePatchType}
}

// NewMergePatch returns a merge-patch type patch entity,  which is applied to torchjobs.
func NewMergePatch() *Patch {
	return &Patch{patchType: types.MergePatchType}
}

var _ client.Patch = &Patch{}

// Patch is a patch that can be applied to a Kubernetes object.
type Patch struct {
	patchType types.PatchType
	patchData patchData
}

func (p *Patch) Type() types.PatchType {
	return p.patchType
}

func (p *Patch) Data(obj client.Object) ([]byte, error) {
	return []byte(p.String()), nil
}

func (p *Patch) String() string {
	js, _ := json.Marshal(&p.patchData)
	return strings.Replace(string(js), NullHolderStr, "null", -1)
}

// Append/Remove finalizer only works when patch type is StrategicMergePatchType.

// AddFinalizer appends the given item into the finalizers.
func (p *Patch) AddFinalizer(item string) *Patch {
	if p.patchType != types.StrategicMergePatchType {
		return p
	}

	if p.patchData.Meta == nil {
		p.patchData.Meta = &prunedMetadata{}
	}
	p.patchData.Meta.Finalizers = append(p.patchData.Meta.Finalizers, item)
	return p
}

// RemoveFinalizer appends the given item into the deletePrimitiveFinalizer such that it can be deleted.
func (p *Patch) RemoveFinalizer(item string) *Patch {
	if p.patchType != types.StrategicMergePatchType {
		return p
	}

	if p.patchData.Meta == nil {
		p.patchData.Meta = &prunedMetadata{}
	}
	p.patchData.Meta.DeletePrimitiveFinalizer = append(p.patchData.Meta.DeletePrimitiveFinalizer, item)
	return p
}

// Override finalizer only works when patch type is MergePatchType.

// OverrideFinalizer replaces the finalizers with the given items.
func (p *Patch) OverrideFinalizer(items []string) *Patch {
	if p.patchType != types.MergePatchType {
		return p
	}

	if p.patchData.Meta == nil {
		p.patchData.Meta = &prunedMetadata{}
	}
	p.patchData.Meta.Finalizers = items
	return p
}

// InsertLabel inserts the key-value pair into the labels.
func (p *Patch) InsertLabel(key, value string) *Patch {
	if p.patchData.Meta == nil {
		p.patchData.Meta = &prunedMetadata{}
	}
	if p.patchData.Meta.Labels == nil {
		p.patchData.Meta.Labels = make(map[string]string)
	}
	p.patchData.Meta.Labels[key] = value
	return p
}

// DeleteLabel deletes the label key-value pair for the given key.
func (p *Patch) DeleteLabel(key string) *Patch {
	if p.patchData.Meta == nil {
		p.patchData.Meta = &prunedMetadata{}
	}
	if p.patchData.Meta.Labels == nil {
		p.patchData.Meta.Labels = make(map[string]string)
	}
	p.patchData.Meta.Labels[key] = NullHolder
	return p
}

// InsertAnnotation inserts the key-value pair into the annotations.
func (p *Patch) InsertAnnotation(key, value string) *Patch {
	if p.patchData.Meta == nil {
		p.patchData.Meta = &prunedMetadata{}
	}
	if p.patchData.Meta.Annotations == nil {
		p.patchData.Meta.Annotations = make(map[string]string)
	}
	p.patchData.Meta.Annotations[key] = value
	return p
}

// DeleteAnnotation deletes the annotation key-value pair for the given key.
func (p *Patch) DeleteAnnotation(key string) *Patch {
	if p.patchData.Meta == nil {
		p.patchData.Meta = &prunedMetadata{}
	}
	if p.patchData.Meta.Annotations == nil {
		p.patchData.Meta.Annotations = make(map[string]string)
	}
	p.patchData.Meta.Annotations[key] = NullHolder
	return p
}

type patchData struct {
	Meta *prunedMetadata `json:"metadata,omitempty"`
}

type prunedMetadata struct {
	Labels                   map[string]string `json:"labels,omitempty"`
	Annotations              map[string]string `json:"annotations,omitempty"`
	Finalizers               []string          `json:"finalizers,omitempty"`
	DeletePrimitiveFinalizer []string          `json:"$deleteFromPrimitiveList/finalizers,omitempty"`
}
