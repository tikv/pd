// Copyright 2021 TiKV Project Authors.
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

package base

import (
	"encoding/json"
	"strings"

	"github.com/tikv/pd/pkg/errs"
	"go.etcd.io/etcd/clientv3"
)

// ConfigStorage defines the storage operations on the config.
type ConfigStorage interface {
	LoadConfig(cfg interface{}) (bool, error)
	SaveConfig(cfg interface{}) error
	LoadAllScheduleConfig() ([]string, []string, error)
	SaveScheduleConfig(scheduleName string, data []byte) error
	RemoveScheduleConfig(scheduleName string) error
}

// LoadConfig loads config from configPath then unmarshal it to cfg.
func (s *Storage) LoadConfig(cfg interface{}) (bool, error) {
	value, err := s.Load(configPath)
	if err != nil {
		return false, err
	}
	if value == "" {
		return false, nil
	}
	err = json.Unmarshal([]byte(value), cfg)
	if err != nil {
		return false, errs.ErrJSONUnmarshal.Wrap(err).GenWithStackByCause()
	}
	return true, nil
}

// SaveConfig stores marshallable cfg to the configPath.
func (s *Storage) SaveConfig(cfg interface{}) error {
	value, err := json.Marshal(cfg)
	if err != nil {
		return errs.ErrJSONMarshal.Wrap(err).GenWithStackByCause()
	}
	return s.Save(configPath, string(value))
}

// LoadAllScheduleConfig loads all schedulers' config.
func (s *Storage) LoadAllScheduleConfig() ([]string, []string, error) {
	prefix := customScheduleConfigPath + "/"
	keys, values, err := s.LoadRange(prefix, clientv3.GetPrefixRangeEnd(prefix), 1000)
	for i, key := range keys {
		keys[i] = strings.TrimPrefix(key, prefix)
	}
	return keys, values, err
}

// SaveScheduleConfig saves the config of scheduler.
func (s *Storage) SaveScheduleConfig(scheduleName string, data []byte) error {
	return s.Save(scheduleConfigPath(scheduleName), string(data))
}

// RemoveScheduleConfig removes the config of scheduler.
func (s *Storage) RemoveScheduleConfig(scheduleName string) error {
	return s.Remove(scheduleConfigPath(scheduleName))
}
