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

package endpoint

import (
	"encoding/json"

	"github.com/pingcap/errors"
	"github.com/tikv/pd/pkg/errs"
)

// ComponentStorage defines the storage operations on the component.
type ComponentStorage interface {
	LoadComponent(component interface{}) (bool, error)
	SaveComponent(component interface{}) error
}

var _ ComponentStorage = (*StorageEndpoint)(nil)

// LoadComponent loads components from componentPath then unmarshal it to component.
func (se *StorageEndpoint) LoadComponent(component interface{}) (bool, error) {
	v, err := se.Load(componentPath)
	if err != nil {
		return false, err
	}
	if v == "" {
		return false, nil
	}
	err = json.Unmarshal([]byte(v), component)
	if err != nil {
		return false, errs.ErrJSONUnmarshal.Wrap(err).GenWithStackByArgs()
	}
	return true, nil
}

// SaveComponent stores marshallable components to the componentPath.
func (se *StorageEndpoint) SaveComponent(component interface{}) error {
	value, err := json.Marshal(component)
	if err != nil {
		return errors.WithStack(err)
	}
	return se.Save(componentPath, string(value))
}
