// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"bytes"
	"strings"
	"sync"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/kvproto/pkg/configpb"
	"github.com/pingcap/pd/server/core"
)

// ComponentsConfig ...
type ComponentsConfig struct {
	sync.RWMutex
	GlobalCfgs map[string]*GlobalConfig
	LocalCfgs  map[string]*LocalConfig
}

// NewComponentsConfig ...
func NewComponentsConfig() *ComponentsConfig {
	return &ComponentsConfig{}
}

// Persist saves the configuration to the storage.
func (c *ComponentsConfig) Persist(storage *core.Storage) error {
	err := storage.SaveClusterConfig(c)
	return err
}

// Reload reloads the configuration from the storage.
func (c *ComponentsConfig) Reload(storage *core.Storage) error {
	_, err := storage.LoadClusterConfig(c)
	if err != nil {
		return err
	}
	return nil
}

// Get ...
func (c *ComponentsConfig) Get(version *configpb.Version, component, componentID string) (*configpb.Version, string, *configpb.Status) {
	c.RLock()
	defer c.RUnlock()
	var config string
	var err error
	var status *configpb.Status
	if lc, ok := c.LocalCfgs[componentID]; ok {
		// request versions
		rlv, rgv := version.GetLocal(), version.GetGlobal()
		// versions stored in PD
		lcv, gcv := lc.GetVersion(), c.GlobalCfgs[component].GetVersion()
		config, err = c.getComponentCfg(component, componentID)
		if err != nil {
			return version, "", &configpb.Status{
				Code:    configpb.Status_FAILED,
				Message: "encode failed",
			}
		}
		if rlv < lcv || rgv < gcv {
			status = &configpb.Status{Code: configpb.Status_STALE_VERSION}
		} else if rlv == lcv && rgv == gcv {
			status = &configpb.Status{Code: configpb.Status_NOT_CHANGE}
			return version, "", status
		} else {
			// TODO: need more specified error message
			status = &configpb.Status{
				Code:    configpb.Status_FAILED,
				Message: "version is illegal",
			}
		}
	} else {
		status = &configpb.Status{
			Code:    configpb.Status_FAILED,
			Message: "component ID is not existed",
		}
	}
	return c.getLatestVersion(component, componentID), config, status
}

// Create ...
func (c *ComponentsConfig) Create(version *configpb.Version, component, componentID, cfg string) (*configpb.Version, string, *configpb.Status) {
	c.Lock()
	defer c.Unlock()
	var status *configpb.Status
	if lc, ok := c.LocalCfgs[componentID]; ok {
		lcv := lc.GetVersion()
		rlv := version.GetLocal()
		if rlv == lcv {
			status = &configpb.Status{Code: configpb.Status_NOT_CHANGE}
			return version, "", status
		} else if rlv < lcv {
			status = &configpb.Status{Code: configpb.Status_STALE_VERSION}
		} else {
			status = &configpb.Status{Code: configpb.Status_FAILED, Message: "version is illegal"}
		}
	} else {
		lc, err := NewLocalConfig(cfg)
		if err != nil {
			status = &configpb.Status{Code: configpb.Status_FAILED, Message: "parse error"}
			return version, "", status
		}
		c.LocalCfgs[componentID] = lc
		status = &configpb.Status{Code: configpb.Status_OK}
	}

	config, err := c.getComponentCfg(component, componentID)
	if err != nil {
		status = &configpb.Status{Code: configpb.Status_FAILED, Message: "encode error"}
		return version, "", status
	}

	return c.getLatestVersion(component, componentID), config, status
}

func (c *ComponentsConfig) getLatestVersion(component, componentID string) *configpb.Version {
	v := &configpb.Version{
		Global: c.GlobalCfgs[component].GetVersion(),
		Local:  c.LocalCfgs[componentID].GetVersion(),
	}
	return v
}

func (c *ComponentsConfig) getComponentCfg(component, componentID string) (string, error) {
	config := c.LocalCfgs[componentID].GetConfigs()
	if _, ok := c.GlobalCfgs[component]; ok {
		globalCfgs := c.GlobalCfgs[component].GetConfigs()
		// TODO: need to loop over
		for k, v := range globalCfgs {
			configName := strings.Split(k, ".")
			update(config, configName, v)
		}
	}

	return encodeConfigs(config)
}

// Update ...
func (c *ComponentsConfig) Update(kind *configpb.ConfigKind, version *configpb.Version, entries []*configpb.ConfigEntry) (*configpb.Version, *configpb.Status) {
	c.Lock()
	defer c.Unlock()
	global := kind.GetGlobal()
	if global != nil {
		component := global.GetComponent()
		// if the global config of the component is existed.
		if gc, ok := c.GlobalCfgs[component]; ok {
			// if the global version of the request is larger than PD, we need to update it.
			rgv := version.GetGlobal()
			globalVersion := gc.GetVersion()
			if rgv > globalVersion {
				for _, entry := range entries {
					c.GlobalCfgs[component].updateConfig(entry)
				}
			} else {
				v := &configpb.Version{
					Global: globalVersion,
					Local:  version.GetLocal(),
				}
				return v, &configpb.Status{
					Code:    configpb.Status_STALE_VERSION,
					Message: "global version is stale",
				}
			}
		} else {
			gc, err := NewGlobalConfig(entries)
			if err != nil {
				return version, &configpb.Status{Code: configpb.Status_FAILED, Message: "parse error"}
			}
			c.GlobalCfgs[component] = gc
		}
		v := &configpb.Version{
			Global: c.GlobalCfgs[component].GetVersion(),
			Local:  version.GetLocal(),
		}
		return v, &configpb.Status{Code: configpb.Status_OK}
	}

	local := kind.GetLocal()
	if local != nil {
		componentID := local.GetComponentId()
		var isExisted bool
		if lc, ok := c.LocalCfgs[componentID]; ok {
			isExisted = true
			rlv := version.GetLocal()
			localVersion := lc.GetVersion()
			if rlv > localVersion {
				for _, entry := range entries {
					c.LocalCfgs[componentID].updateConfig(entry)
				}
			} else {
				v := &configpb.Version{
					Global: version.GetGlobal(),
					Local:  localVersion,
				}
				return v, &configpb.Status{
					Code:    configpb.Status_STALE_VERSION,
					Message: "local version is stale",
				}
			}
		}
		if !isExisted {
			return version, &configpb.Status{Code: configpb.Status_FAILED, Message: "component ID is not existed"}
		}
		v := &configpb.Version{
			Global: version.GetLocal(),
			Local:  c.LocalCfgs[componentID].GetVersion(),
		}
		return v, &configpb.Status{Code: configpb.Status_OK}
	}
	return version, &configpb.Status{Code: configpb.Status_FAILED, Message: "no component is specified"}
}

// GlobalConfig ...
type GlobalConfig struct {
	version uint64
	configs map[string]string
}

// NewGlobalConfig ...
func NewGlobalConfig(entries []*configpb.ConfigEntry) (*GlobalConfig, error) {
	var configs map[string]string
	for _, entry := range entries {
		configs[entry.GetName()] = entry.GetValue()
	}
	return &GlobalConfig{
		version: 0,
		configs: configs,
	}, nil
}

func (gc *GlobalConfig) updateConfig(entry *configpb.ConfigEntry) {
	configs := gc.GetConfigs()
	configs[entry.GetName()] = entry.GetValue()
}

// GetVersion ...
func (gc *GlobalConfig) GetVersion() uint64 {
	return gc.version
}

// GetConfigs ...
func (gc *GlobalConfig) GetConfigs() map[string]string {
	return gc.configs
}

// LocalConfig ...
type LocalConfig struct {
	version uint64
	configs map[string]interface{}
}

// NewLocalConfig ...
func NewLocalConfig(cfg string) (*LocalConfig, error) {
	var configs map[string]interface{}
	if err := decodeConfigs(cfg, configs); err != nil {
		return nil, err
	}
	return &LocalConfig{
		version: 0,
		configs: configs,
	}, nil
}

// GetVersion ...
func (lc *LocalConfig) GetVersion() uint64 {
	return lc.version
}

// GetConfigs ...
func (lc *LocalConfig) GetConfigs() map[string]interface{} {
	return lc.configs
}

func (lc *LocalConfig) updateConfig(entry *configpb.ConfigEntry) {
	config := lc.GetConfigs()
	configName := strings.Split(entry.GetName(), ".")
	update(config, configName, entry.GetValue())
}

// TODO: need to consider the redundant label.
func update(config map[string]interface{}, configName []string, value string) {
	res := config
	for len(configName) >= 1 {
		if _, ok := config[configName[0]]; !ok {
			config[configName[0]] = make(map[string]interface{})
		}
		config = config[configName[0]].(map[string]interface{})
		configName = configName[1:]
		res = config
	}
	res[configName[0]] = value

}

func encodeConfigs(configs map[string]interface{}) (string, error) {
	buf := new(bytes.Buffer)
	if err := toml.NewEncoder(buf).Encode(configs); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func decodeConfigs(cfg string, configs map[string]interface{}) error {
	if _, err := toml.Decode(cfg, &configs); err != nil {
		return err
	}
	return nil
}
