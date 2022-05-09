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

	"github.com/tikv/pd/pkg/errs"
)

// SelfProtectionStorage defines the storage operations on the self protection.
type SelfProtectionStorage interface {
	LoadSelfProtectionConfig(cfg interface{}) (bool, error)
	SaveSelfProtectionConfig(cfg interface{}) error
}

var _ SelfProtectionStorage = (*StorageEndpoint)(nil)

// LoadSelfProtectionConfig loads self protection config from selfProtectionPath then unmarshal it to cfg.
func (se *StorageEndpoint) LoadSelfProtectionConfig(cfg interface{}) (bool, error) {
	value, err := se.Load(selfProtectionPath)
	if err != nil || value == "" {
		return false, err
	}
	err = json.Unmarshal([]byte(value), cfg)
	if err != nil {
		return false, errs.ErrJSONUnmarshal.Wrap(err).GenWithStackByCause()
	}
	return true, nil
}

// SaveSelfProtectionConfig stores marshallable cfg to the selfProtectionPath.
func (se *StorageEndpoint) SaveSelfProtectionConfig(cfg interface{}) error {
	value, err := json.Marshal(cfg)
	if err != nil {
		return errs.ErrJSONMarshal.Wrap(err).GenWithStackByCause()
	}
	return se.Save(selfProtectionPath, string(value))
}
