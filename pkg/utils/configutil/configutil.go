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

package configutil

import (
	"errors"

	"github.com/BurntSushi/toml"
)

// Utility to test if a configuration is defined.
type ConfigMetaData struct {
	meta *toml.MetaData
	path []string
}

func NewConfigMetadata(meta *toml.MetaData) *ConfigMetaData {
	return &ConfigMetaData{meta: meta}
}

func (m *ConfigMetaData) IsDefined(key string) bool {
	if m.meta == nil {
		return false
	}
	keys := append([]string(nil), m.path...)
	keys = append(keys, key)
	return m.meta.IsDefined(keys...)
}

func (m *ConfigMetaData) Child(path ...string) *ConfigMetaData {
	newPath := append([]string(nil), m.path...)
	newPath = append(newPath, path...)
	return &ConfigMetaData{
		meta: m.meta,
		path: newPath,
	}
}

func (m *ConfigMetaData) CheckUndecoded() error {
	if m.meta == nil {
		return nil
	}
	undecoded := m.meta.Undecoded()
	if len(undecoded) == 0 {
		return nil
	}
	errInfo := "Config contains undefined item: "
	for _, key := range undecoded {
		errInfo += key.String() + ", "
	}
	return errors.New(errInfo[:len(errInfo)-2])
}