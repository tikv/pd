// Copyright 2023 TiKV Project Authors.
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

package discovery

import (
	"encoding/json"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// ServiceRegistryEntry is the registry entry of a service
type ServiceRegistryEntry struct {
	// The specific value will be assigned only if the startup parameter is added.
	// If not assigned, the default value(service-hostname) will be used.
	Name           string `json:"name"`
	ServiceAddr    string `json:"service-addr"`
	Version        string `json:"version"`
	GitHash        string `json:"git-hash"`
	DeployPath     string `json:"deploy-path"`
	StartTimestamp int64  `json:"start-timestamp"`
}

// Serialize this service registry entry
func (e *ServiceRegistryEntry) Serialize() (serializedValue string, err error) {
	data, err := json.Marshal(e)
	if err != nil {
		log.Error("json marshal the service registry entry failed", zap.Error(err))
		return "", err
	}
	return string(data), nil
}

// Deserialize the data to this service registry entry
func (e *ServiceRegistryEntry) Deserialize(data []byte) error {
	if err := json.Unmarshal(data, e); err != nil {
		log.Error("json unmarshal the service registry entry failed", zap.Error(err))
		return err
	}
	return nil
}
