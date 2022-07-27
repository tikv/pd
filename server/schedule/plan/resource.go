// Copyright 2022 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

// CoreResourceKind identifies resource kind
type CoreResourceKind int

const (
	// resourceRegion identifies region
	resourceRegion CoreResourceKind = iota
	// resourceStore identifies store
	resourceStore
)

// CoreResource identifies core resource
type CoreResource struct {
	ID   uint64
	Kind CoreResourceKind
}

// NewRegionResource returns a region resource
func NewRegionResource(id uint64) *CoreResource {
	return &CoreResource{
		ID:   id,
		Kind: resourceRegion,
	}
}

// NewStoreResource returns a store resource
func NewStoreResource(id uint64) *CoreResource {
	return &CoreResource{
		ID:   id,
		Kind: resourceStore,
	}
}
