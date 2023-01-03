// Copyright 2023 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package informer

// NewGenericLister creates a new instance for the GenericLister.
func NewGenericLister(store Store) *GenericLister {
	return &GenericLister{store: store}
}

// GenericLister is used to get the information from the store.
type GenericLister struct {
	store Store
}

// List returns the values for a given range.
func (gl *GenericLister) List(start, end string, limit int) (ret []interface{}, err error) {
	return gl.store.List(start, end, limit)
}

// Get returns the value for the given key.
func (gl *GenericLister) Get(key string) (interface{}, error) {
	v, err := gl.store.GetByKey(key)
	if err != nil {
		return nil, err
	}
	return v, nil
}
