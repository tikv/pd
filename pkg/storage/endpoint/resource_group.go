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
	"github.com/gogo/protobuf/proto"

	"github.com/tikv/pd/pkg/utils/keypath"
)

// ResourceGroupStorage defines the storage operations on the resource group.
type ResourceGroupStorage interface {
	LoadResourceGroupSettings(f func(k, v string)) error
	SaveResourceGroupSetting(name string, msg proto.Message) error
	DeleteResourceGroupSetting(name string) error
	LoadResourceGroupStates(f func(k, v string)) error
	SaveResourceGroupStates(name string, obj any) error
	DeleteResourceGroupStates(name string) error
	SaveControllerConfig(config any) error
	LoadControllerConfig() (string, error)
}

var _ ResourceGroupStorage = (*StorageEndpoint)(nil)

// SaveResourceGroupSetting stores a resource group to storage.
func (se *StorageEndpoint) SaveResourceGroupSetting(name string, msg proto.Message) error {
	return se.saveProto(keypath.ResourceGroupSettingKeyPath(name), msg)
}

// DeleteResourceGroupSetting removes a resource group from storage.
func (se *StorageEndpoint) DeleteResourceGroupSetting(name string) error {
	return se.Remove(keypath.ResourceGroupSettingKeyPath(name))
}

// LoadResourceGroupSettings loads all resource groups from storage.
func (se *StorageEndpoint) LoadResourceGroupSettings(f func(k, v string)) error {
	return se.loadRangeByPrefix(keypath.ResourceGroupSettingsPath+"/", f)
}

// SaveResourceGroupStates stores a resource group to storage.
func (se *StorageEndpoint) SaveResourceGroupStates(name string, obj any) error {
	return se.saveJSON(keypath.ResourceGroupStateKeyPath(name), obj)
}

// DeleteResourceGroupStates removes a resource group from storage.
func (se *StorageEndpoint) DeleteResourceGroupStates(name string) error {
	return se.Remove(keypath.ResourceGroupStateKeyPath(name))
}

// LoadResourceGroupStates loads all resource groups from storage.
func (se *StorageEndpoint) LoadResourceGroupStates(f func(k, v string)) error {
	return se.loadRangeByPrefix(keypath.ResourceGroupStatesPath+"/", f)
}

// SaveControllerConfig stores the resource controller config to storage.
func (se *StorageEndpoint) SaveControllerConfig(config any) error {
	return se.saveJSON(keypath.ControllerConfigPath, config)
}

// LoadControllerConfig loads the resource controller config from storage.
func (se *StorageEndpoint) LoadControllerConfig() (string, error) {
	return se.Load(keypath.ControllerConfigPath)
}
