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
	"strings"

	"github.com/tikv/pd/pkg/errs"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// ConfigStorage defines the storage operations on the config.
type ConfigStorage interface {
	// Persisted config will be stored in the storage.
	LoadConfig(cfg any) (bool, error)
	SaveConfig(cfg any) error
	// Each scheduler has its own customized config, so we need to store them separately.
	LoadAllSchedulerConfigs() ([]string, []string, error)
	LoadSchedulerConfig(schedulerName string) (string, error)
	SaveSchedulerConfig(schedulerName string, data []byte) error
	RemoveSchedulerConfig(schedulerName string) error
}

var _ ConfigStorage = (*StorageEndpoint)(nil)

// LoadConfig loads config from configPath then unmarshal it to cfg.
func (se *StorageEndpoint) LoadConfig(cfg any) (bool, error) {
	value, err := se.Load(configPath)
	if err != nil || value == "" {
		return false, err
	}
	err = json.Unmarshal([]byte(value), cfg)
	if err != nil {
		return false, errs.ErrJSONUnmarshal.Wrap(err).GenWithStackByCause()
	}
	return true, nil
}

// SaveConfig stores marshallable cfg to the configPath.
func (se *StorageEndpoint) SaveConfig(cfg any) error {
	return se.saveJSON(configPath, cfg)
}

// LoadAllSchedulerConfigs loads all schedulers' config.
func (se *StorageEndpoint) LoadAllSchedulerConfigs() ([]string, []string, error) {
	prefix := customSchedulerConfigPath + "/"
	keys, values, err := se.LoadRange(prefix, clientv3.GetPrefixRangeEnd(prefix), MinKVRangeLimit)
	for i, key := range keys {
		keys[i] = strings.TrimPrefix(key, prefix)
	}
	return keys, values, err
}

// LoadSchedulerConfig loads the config of the given scheduler.
func (se *StorageEndpoint) LoadSchedulerConfig(schedulerName string) (string, error) {
	return se.Load(schedulerConfigPath(schedulerName))
}

// SaveSchedulerConfig saves the config of the given scheduler.
func (se *StorageEndpoint) SaveSchedulerConfig(schedulerName string, data []byte) error {
	return se.Save(schedulerConfigPath(schedulerName), string(data))
}

// RemoveSchedulerConfig removes the config of the given scheduler.
func (se *StorageEndpoint) RemoveSchedulerConfig(schedulerName string) error {
	return se.Remove(schedulerConfigPath(schedulerName))
}
