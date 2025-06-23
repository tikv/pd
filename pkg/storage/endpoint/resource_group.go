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
	"strconv"

	"github.com/gogo/protobuf/proto"
	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/utils/keypath"
)

// ResourceGroupStorage defines the storage operations on the resource group.
type ResourceGroupStorage interface {
	LoadResourceGroupSettings(f func(keyspaceID uint32, name, rawValue string)) error
	SaveResourceGroupSetting(keyspaceID uint32, name string, msg proto.Message) error
	DeleteResourceGroupSetting(keyspaceID uint32, name string) error
	LoadResourceGroupStates(f func(keyspaceID uint32, name, rawValue string)) error
	SaveResourceGroupStates(keyspaceID uint32, name string, obj any) error
	DeleteResourceGroupStates(keyspaceID uint32, name string) error
	SaveControllerConfig(config any) error
	LoadControllerConfig() (string, error)
	LoadServiceLimit(keyspaceID uint32) (float64, error)
	SaveServiceLimit(keyspaceID uint32, serviceLimit float64) error
	LoadServiceLimits(f func(keyspaceID uint32, serviceLimit float64)) error
}

var _ ResourceGroupStorage = (*StorageEndpoint)(nil)

// SaveResourceGroupSetting stores a resource group to storage.
func (se *StorageEndpoint) SaveResourceGroupSetting(keyspaceID uint32, name string, msg proto.Message) error {
	return se.saveProto(keypath.KeyspaceResourceGroupSettingPath(keyspaceID, name), msg)
}

// DeleteResourceGroupSetting removes a resource group from storage.
func (se *StorageEndpoint) DeleteResourceGroupSetting(keyspaceID uint32, name string) error {
	return se.Remove(keypath.KeyspaceResourceGroupSettingPath(keyspaceID, name))
}

// LoadResourceGroupSettings loads all resource groups from storage.
func (se *StorageEndpoint) LoadResourceGroupSettings(f func(keyspaceID uint32, name string, rawValue string)) error {
	if err := se.loadRangeByPrefix(keypath.ResourceGroupSettingPrefix(), func(key, value string) {
		// Using the null keyspace ID for the resource group settings loaded from the legacy path.
		f(constant.NullKeyspaceID, key, value)
	}); err != nil {
		return err
	}
	return se.loadRangeByPrefix(keypath.KeyspaceResourceGroupSettingPrefix(), func(key, value string) {
		// Parse the key to get the keyspace ID and resource group name respectively.
		keyspaceID, name, err := keypath.ParseKeyspaceResourceGroupPath(key)
		if err != nil {
			log.Error("failed to parse the keyspace ID and resource group name", zap.String("key", key), zap.Error(err))
			return
		}
		f(keyspaceID, name, value)
	})
}

// SaveResourceGroupStates stores a resource group to storage.
func (se *StorageEndpoint) SaveResourceGroupStates(keyspaceID uint32, name string, obj any) error {
	return se.saveJSON(keypath.KeyspaceResourceGroupStatePath(keyspaceID, name), obj)
}

// DeleteResourceGroupStates removes a resource group from storage.
func (se *StorageEndpoint) DeleteResourceGroupStates(keyspaceID uint32, name string) error {
	return se.Remove(keypath.KeyspaceResourceGroupStatePath(keyspaceID, name))
}

// LoadResourceGroupStates loads all resource groups from storage.
func (se *StorageEndpoint) LoadResourceGroupStates(f func(keyspaceID uint32, name string, rawValue string)) error {
	if err := se.loadRangeByPrefix(keypath.ResourceGroupStatePrefix(), func(key, value string) {
		// Using the null keyspace ID for the resource group states loaded from the legacy path.
		f(constant.NullKeyspaceID, key, value)
	}); err != nil {
		return err
	}
	return se.loadRangeByPrefix(keypath.KeyspaceResourceGroupStatePrefix(), func(key, value string) {
		// Parse the key to get the keyspace ID and resource group name respectively.
		keyspaceID, name, err := keypath.ParseKeyspaceResourceGroupPath(key)
		if err != nil {
			log.Error("failed to parse the keyspace ID and resource group name", zap.String("key", key), zap.Error(err))
			return
		}
		f(keyspaceID, name, value)
	})
}

// SaveControllerConfig stores the resource controller config to storage.
func (se *StorageEndpoint) SaveControllerConfig(config any) error {
	return se.saveJSON(keypath.ControllerConfigPath(), config)
}

// LoadControllerConfig loads the resource controller config from storage.
func (se *StorageEndpoint) LoadControllerConfig() (string, error) {
	return se.Load(keypath.ControllerConfigPath())
}

// LoadServiceLimit loads the service limit for the given keyspace.
func (se *StorageEndpoint) LoadServiceLimit(keyspaceID uint32) (float64, error) {
	value, err := se.Load(keypath.KeyspaceServiceLimitPath(keyspaceID))
	if err != nil || value == "" {
		return 0, err
	}
	return loadJSON[float64](se, keypath.KeyspaceServiceLimitPath(keyspaceID))
}

// SaveServiceLimit stores the service limit for the given keyspace.
func (se *StorageEndpoint) SaveServiceLimit(keyspaceID uint32, serviceLimit float64) error {
	return se.saveJSON(keypath.KeyspaceServiceLimitPath(keyspaceID), serviceLimit)
}

// LoadServiceLimits loads all service limits from storage.
func (se *StorageEndpoint) LoadServiceLimits(f func(keyspaceID uint32, serviceLimit float64)) error {
	return se.loadRangeByPrefix(keypath.KeyspaceServiceLimitPrefix(), func(key, value string) {
		keyspaceID, err := strconv.ParseUint(key, 10, 32)
		if err != nil {
			log.Error("failed to parse the keyspace ID from service limit path",
				zap.String("key", key), zap.Error(err))
			return
		}
		var serviceLimit float64
		if err := json.Unmarshal([]byte(value), &serviceLimit); err != nil {
			log.Error("failed to parse service limit value",
				zap.Uint32("keyspace-id", uint32(keyspaceID)), zap.String("value", value), zap.Error(err))
			return
		}
		f(uint32(keyspaceID), serviceLimit)
	})
}
