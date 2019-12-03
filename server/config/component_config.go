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
	"reflect"
	"strconv"
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
	LocalCfgs  map[string]map[string]*LocalConfig
}

// NewComponentsConfig ...
func NewComponentsConfig() *ComponentsConfig {
	return &ComponentsConfig{
		GlobalCfgs: make(map[string]*GlobalConfig),
		LocalCfgs:  make(map[string]map[string]*LocalConfig),
	}
}

// Persist saves the configuration to the storage.
func (c *ComponentsConfig) Persist(storage *core.Storage) error {
	err := storage.SaveComponentsConfig(c)
	return err
}

// Reload reloads the configuration from the storage.
func (c *ComponentsConfig) Reload(storage *core.Storage) error {
	_, err := storage.LoadComponentsConfig(c)
	if err != nil {
		return err
	}
	return nil
}

// GetComponent ...
func (c *ComponentsConfig) GetComponent(ComponentID string) string {
	for component := range c.LocalCfgs {
		for componentID := range c.LocalCfgs[component] {
			if componentID == ComponentID {
				return component
			}
		}
	}
	return ""
}

// Get ...
func (c *ComponentsConfig) Get(version *configpb.Version, component, componentID string) (*configpb.Version, string, *configpb.Status) {
	c.RLock()
	defer c.RUnlock()
	var config string
	var err error
	var status *configpb.Status
	if componentsCfg, ok := c.LocalCfgs[component]; ok {
		if cfg, ok := componentsCfg[componentID]; ok {
			config, err = c.getComponentCfg(component, componentID)
			if err != nil {
				return version, "", &configpb.Status{
					Code:    configpb.Status_UNKNOWN,
					Message: "encode failed",
				}
			}
			res := compareVersion(cfg.GetVersion(), version)
			if res == 0 {
				status = &configpb.Status{Code: configpb.Status_NOT_CHANGE}
				return version, "", status
			} else if res == -1 {
				// TODO: need more specified error message
				status = &configpb.Status{
					Code:    configpb.Status_UNKNOWN,
					Message: "version is illegal",
				}
			} else {
				status = &configpb.Status{Code: configpb.Status_STALE_VERSION}
			}
		} else {
			status = &configpb.Status{
				Code:    configpb.Status_UNKNOWN,
				Message: "component ID is not existed",
			}
		}
	} else {
		status = &configpb.Status{
			Code:    configpb.Status_UNKNOWN,
			Message: "component is not existed",
		}
	}
	return c.getLatestVersion(component, componentID), config, status
}

// TODO: needs more consideration
func compareVersion(origin, new *configpb.Version) int {
	if origin.GetGlobal() == new.GetGlobal() && origin.GetLocal() == new.GetLocal() {
		// version is not change
		return 0
	} else if origin.GetGlobal() < new.GetGlobal() || origin.GetLocal() < new.GetLocal() {
		return -1
	} else {
		return 1
	}
}

// Create ...
func (c *ComponentsConfig) Create(version *configpb.Version, component, componentID, cfg string) (*configpb.Version, string, *configpb.Status) {
	c.Lock()
	defer c.Unlock()
	var status *configpb.Status
	latestVersion := c.getLatestVersion(component, componentID)
	if componentsCfg, ok := c.LocalCfgs[component]; ok {
		if _, ok := componentsCfg[componentID]; ok {
			// restart a component
			res := compareVersion(latestVersion, version)
			if res == 0 {
				status = &configpb.Status{Code: configpb.Status_NOT_CHANGE}
				return latestVersion, "", status
			} else if res == 1 {
				status = &configpb.Status{Code: configpb.Status_STALE_VERSION}
			} else {
				status = &configpb.Status{Code: configpb.Status_UNKNOWN, Message: "version is illegal"}
			}
		} else {
			// add a new component
			lc, err := NewLocalConfig(cfg, latestVersion)
			if err != nil {
				status = &configpb.Status{Code: configpb.Status_UNKNOWN, Message: "parse error"}
			} else {
				componentsCfg[componentID] = lc
				status = &configpb.Status{Code: configpb.Status_OK}
			}
		}
	} else {
		c.LocalCfgs[component] = make(map[string]*LocalConfig)
		// start the first component
		lc, err := NewLocalConfig(cfg, latestVersion)
		if err != nil {
			status = &configpb.Status{Code: configpb.Status_UNKNOWN, Message: "parse error"}
		} else {
			c.LocalCfgs[component][componentID] = lc
			status = &configpb.Status{Code: configpb.Status_OK}
		}
	}

	config, err := c.getComponentCfg(component, componentID)
	if err != nil {
		status = &configpb.Status{Code: configpb.Status_UNKNOWN, Message: "encode error"}
		return latestVersion, "", status
	}

	return latestVersion, config, status
}

func (c *ComponentsConfig) getLatestVersion(component, componentID string) *configpb.Version {
	v := &configpb.Version{
		Global: c.GlobalCfgs[component].GetVersion(),
		Local:  c.LocalCfgs[component][componentID].GetVersion().GetLocal(),
	}
	return v
}

func (c *ComponentsConfig) getComponentCfg(component, componentID string) (string, error) {
	config := c.LocalCfgs[component][componentID].GetConfigs()
	updateEntries := make(map[string]*EntryValue)
	// apply the global change to updateEntries
	if _, ok := c.GlobalCfgs[component]; ok {
		globalUpdateEntries := c.GlobalCfgs[component].GetUpdateEntries()
		for k, v := range globalUpdateEntries {
			updateEntries[k] = v
		}
	}
	// apply the local change to updateEntries
	for k1, v1 := range c.LocalCfgs[component][componentID].GetUpdateEntries() {
		if v, ok := updateEntries[k1]; ok {
			// apply conflict
			if v1.Version.GetGlobal() == v.Version.GetGlobal() {
				updateEntries[k1] = v1
			}
		} else {
			updateEntries[k1] = v1
		}
	}

	for k, v := range updateEntries {
		configName := strings.Split(k, ".")
		update(config, configName, v.Value)
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
		if globalCfg, ok := c.GlobalCfgs[component]; ok {
			globalLatestVersion := globalCfg.GetVersion()
			if globalLatestVersion > version.GetGlobal() {
				return &configpb.Version{Global: globalLatestVersion, Local: version.GetLocal()},
					&configpb.Status{
						Code:    configpb.Status_STALE_VERSION,
						Message: "version is wrong",
					}
			} else if globalLatestVersion < version.GetGlobal() {
				return &configpb.Version{Global: globalLatestVersion, Local: version.GetLocal()},
					&configpb.Status{
						Code:    configpb.Status_UNKNOWN,
						Message: "version is wrong",
					}
			}

			newGlobalVersion := version.GetGlobal() + 1
			for _, entry := range entries {
				globalCfg.updateEntry(entry, &configpb.Version{Global: newGlobalVersion, Local: 0})
			}
			globalCfg.Version = newGlobalVersion
			// update all local config version
			for _, LocalCfg := range c.LocalCfgs[component] {
				LocalCfg.Version = &configpb.Version{Global: newGlobalVersion, Local: 0}
			}
		} else {
			// The global version of first global update should be 0.
			if version.GetGlobal() != 0 {
				return &configpb.Version{Global: 0, Local: 0},
					&configpb.Status{
						Code:    configpb.Status_UNKNOWN,
						Message: "version is wrong",
					}
			}
			newGlobalVersion := uint64(1)
			globalCfg := NewGlobalConfig(entries, &configpb.Version{Global: newGlobalVersion, Local: 0})
			c.GlobalCfgs[component] = globalCfg
			// update all local config version
			for _, LocalCfg := range c.LocalCfgs[component] {
				LocalCfg.Version = &configpb.Version{Global: newGlobalVersion, Local: 0}
			}
		}
		return &configpb.Version{Global: c.GlobalCfgs[component].GetVersion(), Local: 0}, &configpb.Status{Code: configpb.Status_OK}
	}

	local := kind.GetLocal()
	if local != nil {
		componentID := local.GetComponentId()
		component := c.GetComponent(componentID)
		if component == "" {
			return &configpb.Version{Global: 0, Local: 0}, &configpb.Status{Code: configpb.Status_UNKNOWN, Message: "no component is specified"}
		}
		if localCfg, ok := c.LocalCfgs[component][componentID]; ok {
			localLatestVersion := localCfg.GetVersion()
			res := compareVersion(localLatestVersion, version)
			if res == 1 {
				return localLatestVersion, &configpb.Status{
					Code:    configpb.Status_STALE_VERSION,
					Message: "version is wrong",
				}
			} else if res == -1 {
				return localLatestVersion, &configpb.Status{
					Code:    configpb.Status_UNKNOWN,
					Message: "version is wrong",
				}
			}
			for _, entry := range entries {
				localCfg.updateEntry(entry, version)
			}
			localCfg.Version = &configpb.Version{Global: version.GetGlobal(), Local: version.GetLocal() + 1}
		} else {
			return version, &configpb.Status{Code: configpb.Status_UNKNOWN, Message: "component ID is not existed"}
		}
		return c.LocalCfgs[component][componentID].GetVersion(), &configpb.Status{Code: configpb.Status_OK}
	}
	return &configpb.Version{Global: 0, Local: 0}, &configpb.Status{Code: configpb.Status_UNKNOWN, Message: "no component is specified"}
}

// Delete ...
func (c *ComponentsConfig) Delete(kind *configpb.ConfigKind, version *configpb.Version) *configpb.Status {
	c.Lock()
	defer c.Unlock()

	global := kind.GetGlobal()
	if global != nil {
		component := global.GetComponent()
		// if the global config of the component is existed.
		if globalCfg, ok := c.GlobalCfgs[component]; ok {
			globalLatestVersion := globalCfg.GetVersion()
			if globalLatestVersion != version.GetGlobal() {
				return &configpb.Status{
					Code:    configpb.Status_STALE_VERSION,
					Message: "version is wrong",
				}
			}
			delete(c.GlobalCfgs, component)
			for _, LocalCfg := range c.LocalCfgs[component] {
				LocalCfg.Version = &configpb.Version{Global: 0, Local: 0}
			}
		} else {
			return &configpb.Status{Code: configpb.Status_UNKNOWN, Message: "no component is specified"}
		}
		return &configpb.Status{Code: configpb.Status_OK}
	}

	local := kind.GetLocal()
	if local != nil {
		componentID := local.GetComponentId()
		component := c.GetComponent(componentID)
		if component == "" {
			return &configpb.Status{Code: configpb.Status_UNKNOWN, Message: "no component is specified"}
		}
		if localCfg, ok := c.LocalCfgs[component][componentID]; ok {
			localLatestVersion := localCfg.GetVersion()
			res := compareVersion(localLatestVersion, version)
			if res != 0 {
				return &configpb.Status{
					Code:    configpb.Status_STALE_VERSION,
					Message: "version is wrong",
				}
			}
			delete(c.LocalCfgs[component], componentID)
		} else {
			return &configpb.Status{Code: configpb.Status_UNKNOWN, Message: "no component id is specified"}
		}
		return &configpb.Status{Code: configpb.Status_OK}
	}
	return &configpb.Status{Code: configpb.Status_UNKNOWN, Message: "kind is specified"}
}

// EntryValue ...
type EntryValue struct {
	Version *configpb.Version
	Value   string
}

// NewEntryValue ...
func NewEntryValue(e *configpb.ConfigEntry, version *configpb.Version) *EntryValue {
	return &EntryValue{
		Version: version,
		Value:   e.GetValue(),
	}
}

// GlobalConfig ...
type GlobalConfig struct {
	Version       uint64
	UpdateEntries map[string]*EntryValue
}

// NewGlobalConfig ...
func NewGlobalConfig(entries []*configpb.ConfigEntry, version *configpb.Version) *GlobalConfig {
	updateEntries := make(map[string]*EntryValue)
	for _, entry := range entries {
		updateEntries[entry.GetName()] = NewEntryValue(entry, version)
	}
	return &GlobalConfig{
		Version:       version.GetGlobal(),
		UpdateEntries: updateEntries,
	}
}

func (gc *GlobalConfig) updateEntry(entry *configpb.ConfigEntry, version *configpb.Version) {
	entries := gc.GetUpdateEntries()
	entries[entry.GetName()] = NewEntryValue(entry, version)
}

// GetVersion ...
func (gc *GlobalConfig) GetVersion() uint64 {
	if gc == nil {
		return 0
	}
	return gc.Version
}

// GetUpdateEntries ...
func (gc *GlobalConfig) GetUpdateEntries() map[string]*EntryValue {
	return gc.UpdateEntries
}

// LocalConfig ...
type LocalConfig struct {
	Version       *configpb.Version
	UpdateEntries map[string]*EntryValue
	Configs       map[string]interface{}
}

// NewLocalConfig ...
func NewLocalConfig(cfg string, version *configpb.Version) (*LocalConfig, error) {
	configs := make(map[string]interface{})
	if err := decodeConfigs(cfg, configs); err != nil {
		return nil, err
	}
	updateEntries := make(map[string]*EntryValue)
	return &LocalConfig{
		Version:       version,
		UpdateEntries: updateEntries,
		Configs:       configs,
	}, nil
}

// GetUpdateEntries ...
func (lc *LocalConfig) GetUpdateEntries() map[string]*EntryValue {
	return lc.UpdateEntries
}

func (lc *LocalConfig) updateEntry(entry *configpb.ConfigEntry, version *configpb.Version) {
	entries := lc.GetUpdateEntries()
	entries[entry.GetName()] = NewEntryValue(entry, version)
}

// GetVersion ...
func (lc *LocalConfig) GetVersion() *configpb.Version {
	if lc == nil {
		return nil
	}
	return lc.Version
}

// GetConfigs ...
func (lc *LocalConfig) GetConfigs() map[string]interface{} {
	return lc.Configs
}

// TODO: need to consider the redundant label.
func update(config map[string]interface{}, configName []string, value string) {
	res := config
	for len(configName) >= 2 {
		if _, ok := config[configName[0]]; !ok {
			config[configName[0]] = make(map[string]interface{})
		}
		config = config[configName[0]].(map[string]interface{})
		configName = configName[1:]
		res = config
	}

	t := reflect.TypeOf(res[configName[0]])
	// TODO: support more types
	// TODO: error handle
	switch t.Kind() {
	case reflect.Bool:
		v, _ := strconv.ParseBool(value)
		res[configName[0]] = v
	case reflect.Int:
		v, _ := strconv.Atoi(value)
		res[configName[0]] = v
	case reflect.Int64:
		v, _ := strconv.ParseInt(value, 10, 64)
		res[configName[0]] = v
	case reflect.Float64:
		v, _ := strconv.ParseFloat(value, 64)
		res[configName[0]] = v
	case reflect.String:
		res[configName[0]] = value
	default:
		res[configName[0]] = value
	}
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
