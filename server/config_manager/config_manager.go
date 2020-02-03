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

package configmanager

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/kvproto/pkg/configpb"
	"github.com/pingcap/pd/server/cluster"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/member"
	"github.com/pkg/errors"
)

var (
	// errUnknownKind is error info for the kind.
	errUnknownKind = func(k *configpb.ConfigKind) string {
		return fmt.Sprintf("unknown kind: %v", k.String())
	}
	// errEncode is error info for the encode process.
	errEncode = func(e error) string {
		return fmt.Sprintf("encode error: %v", e)
	}
	// errDecode is error info for the decode process.
	errDecode = func(e error) string {
		return fmt.Sprintf("decode error: %v", e)
	}
	errNotSupported = "not supported"
)

// Server is the interface for configuration manager.
type Server interface {
	IsClosed() bool
	ClusterID() uint64
	GetRaftCluster() *cluster.RaftCluster
	GetStorage() *core.Storage
	GetMember() *member.Member
}

// ConfigManager is used to manage all components' config.
type ConfigManager struct {
	sync.RWMutex
	svr Server
	// component -> GlobalConfig
	GlobalCfgs map[string]*GlobalConfig
	// component -> componentID -> LocalConfig
	LocalCfgs map[string]map[string]*LocalConfig
}

// NewConfigManager creates a new ConfigManager.
func NewConfigManager(svr Server) *ConfigManager {
	return &ConfigManager{
		svr:        svr,
		GlobalCfgs: make(map[string]*GlobalConfig),
		LocalCfgs:  make(map[string]map[string]*LocalConfig),
	}
}

// Persist saves the configuration to the storage.
func (c *ConfigManager) Persist(storage *core.Storage) error {
	c.Lock()
	defer c.Unlock()
	return storage.SaveComponentsConfig(c)
}

// Reload reloads the configuration from the storage.
func (c *ConfigManager) Reload(storage *core.Storage) error {
	c.Lock()
	defer c.Unlock()
	_, err := storage.LoadComponentsConfig(c)
	return err
}

// getComponent returns the component from a given component ID.
func (c *ConfigManager) getComponent(id string) string {
	for component, cfgs := range c.LocalCfgs {
		if _, ok := cfgs[id]; ok {
			return component
		}
	}
	return ""
}

// GetConfig returns config and the latest version.
func (c *ConfigManager) GetConfig(version *configpb.Version, component, componentID string) (*configpb.Version, string, *configpb.Status) {
	c.RLock()
	defer c.RUnlock()
	var config string
	var err error
	var status *configpb.Status
	var localCfgs map[string]*LocalConfig
	var cfg *LocalConfig
	var ok bool

	if localCfgs, ok = c.LocalCfgs[component]; !ok {
		return c.getLatestVersion(component, componentID), config, &configpb.Status{Code: configpb.StatusCode_COMPONENT_NOT_FOUND}
	}

	if cfg, ok = localCfgs[componentID]; !ok {
		return c.getLatestVersion(component, componentID), config, &configpb.Status{Code: configpb.StatusCode_COMPONENT_ID_NOT_FOUND}
	}

	config, err = c.getComponentCfg(component, componentID)
	if err != nil {
		return version, "", &configpb.Status{
			Code:    configpb.StatusCode_UNKNOWN,
			Message: errEncode(err),
		}
	}
	if versionEqual(cfg.getVersion(), version) {
		status = &configpb.Status{Code: configpb.StatusCode_OK}
	} else {
		status = &configpb.Status{Code: configpb.StatusCode_WRONG_VERSION}
	}

	return c.getLatestVersion(component, componentID), config, status
}

// CreateConfig is used for registering a component to PD.
func (c *ConfigManager) CreateConfig(version *configpb.Version, component, componentID, cfg string) (*configpb.Version, string, *configpb.Status) {
	c.Lock()
	defer c.Unlock()
	var status *configpb.Status
	latestVersion := c.getLatestVersion(component, componentID)
	initVersion := &configpb.Version{Local: 0, Global: 0}
	if localCfgs, ok := c.LocalCfgs[component]; ok {
		if _, ok := localCfgs[componentID]; ok {
			// restart a component
			if versionEqual(initVersion, version) {
				status = &configpb.Status{Code: configpb.StatusCode_OK}
			} else {
				status = &configpb.Status{Code: configpb.StatusCode_WRONG_VERSION}
			}
		} else {
			// add a new component
			lc, err := NewLocalConfig(cfg, initVersion)
			if err != nil {
				status = &configpb.Status{Code: configpb.StatusCode_UNKNOWN, Message: errDecode(err)}
			} else {
				localCfgs[componentID] = lc
				status = &configpb.Status{Code: configpb.StatusCode_OK}
			}
		}
	} else {
		c.LocalCfgs[component] = make(map[string]*LocalConfig)
		// start the first component
		lc, err := NewLocalConfig(cfg, initVersion)
		if err != nil {
			status = &configpb.Status{Code: configpb.StatusCode_UNKNOWN, Message: errDecode(err)}
		} else {
			c.LocalCfgs[component][componentID] = lc
			status = &configpb.Status{Code: configpb.StatusCode_OK}
		}
	}

	// Apply global config to new component
	globalCfg := c.GlobalCfgs[component]
	if globalCfg != nil {
		entries := globalCfg.GetConfigEntries()
		if err := c.ApplyGlobalConifg(globalCfg, component, globalCfg.getVersion(), entries); err != nil {
			return latestVersion, "", &configpb.Status{Code: configpb.StatusCode_UNKNOWN, Message: err.Error()}
		}
	}

	config, err := c.getComponentCfg(component, componentID)
	if err != nil {
		status = &configpb.Status{Code: configpb.StatusCode_UNKNOWN, Message: errEncode(err)}
		return latestVersion, "", status
	}

	return latestVersion, config, status
}

func (c *ConfigManager) getLatestVersion(component, componentID string) *configpb.Version {
	v := &configpb.Version{
		Global: c.GlobalCfgs[component].getVersion(),
		Local:  c.LocalCfgs[component][componentID].getVersion().GetLocal(),
	}
	return v
}

func (c *ConfigManager) getComponentCfg(component, componentID string) (string, error) {
	config := c.LocalCfgs[component][componentID].getConfigs()
	return encodeConfigs(config)
}

// UpdateConfig is used to update a config with a given config type.
func (c *ConfigManager) UpdateConfig(kind *configpb.ConfigKind, version *configpb.Version, entries []*configpb.ConfigEntry) (*configpb.Version, *configpb.Status) {
	c.Lock()
	defer c.Unlock()

	global := kind.GetGlobal()
	if global != nil {
		return c.updateGlobal(global.GetComponent(), version, entries)
	}

	local := kind.GetLocal()
	if local != nil {
		return c.updateLocal(local.GetComponentId(), version, entries)
	}
	return &configpb.Version{Global: 0, Local: 0}, &configpb.Status{Code: configpb.StatusCode_UNKNOWN, Message: errUnknownKind(kind)}
}

// ApplyGlobalConifg applies the global change to each local component.
func (c *ConfigManager) ApplyGlobalConifg(globalCfg *GlobalConfig, component string, newGlobalVersion uint64, entries []*configpb.ConfigEntry) error {
	// get the global config
	updateEntries := make(map[string]*EntryValue)
	for _, entry := range entries {
		globalCfg.updateEntry(entry, &configpb.Version{Global: newGlobalVersion, Local: 0})
	}

	globalUpdateEntries := c.GlobalCfgs[component].getUpdateEntries()
	for k, v := range globalUpdateEntries {
		updateEntries[k] = v
	}

	// update all local config
	// merge the global config with each local config and update it
	for _, LocalCfg := range c.LocalCfgs[component] {
		if wrongEntry, err := mergeAndUpdateConfig(LocalCfg, updateEntries); err != nil {
			c.deleteEntry(component, wrongEntry)
			return err
		}
		LocalCfg.Version = &configpb.Version{Global: newGlobalVersion, Local: 0}
	}

	// update the global version
	globalCfg.Version = newGlobalVersion
	return nil
}

func (c *ConfigManager) updateGlobal(component string, version *configpb.Version, entries []*configpb.ConfigEntry) (*configpb.Version, *configpb.Status) {
	// if the global config of the component is existed.
	if globalCfg, ok := c.GlobalCfgs[component]; ok {
		globalLatestVersion := globalCfg.getVersion()
		if globalLatestVersion != version.GetGlobal() {
			return &configpb.Version{Global: globalLatestVersion, Local: version.GetLocal()},
				&configpb.Status{Code: configpb.StatusCode_WRONG_VERSION}
		}
		if err := c.ApplyGlobalConifg(globalCfg, component, version.GetGlobal()+1, entries); err != nil {
			return &configpb.Version{Global: globalLatestVersion, Local: version.GetLocal()},
				&configpb.Status{Code: configpb.StatusCode_UNKNOWN, Message: err.Error()}
		}
	} else {
		// The global version of first global update should be 0.
		if version.GetGlobal() != 0 {
			return &configpb.Version{Global: 0, Local: 0},
				&configpb.Status{Code: configpb.StatusCode_WRONG_VERSION}
		}

		globalCfg := NewGlobalConfig(entries, &configpb.Version{Global: 0, Local: 0})
		c.GlobalCfgs[component] = globalCfg

		if err := c.ApplyGlobalConifg(globalCfg, component, 1, entries); err != nil {
			return &configpb.Version{Global: 0, Local: version.GetLocal()},
				&configpb.Status{Code: configpb.StatusCode_UNKNOWN, Message: err.Error()}
		}
	}
	return &configpb.Version{Global: c.GlobalCfgs[component].getVersion(), Local: 0}, &configpb.Status{Code: configpb.StatusCode_OK}
}

func mergeAndUpdateConfig(localCfg *LocalConfig, updateEntries map[string]*EntryValue) (string, error) {
	config := localCfg.getConfigs()
	newUpdateEntries := make(map[string]*EntryValue)
	for k, v := range updateEntries {
		newUpdateEntries[k] = v
	}

	// apply the local change to updateEntries
	for k1, v1 := range localCfg.getUpdateEntries() {
		if v, ok := newUpdateEntries[k1]; ok {
			// apply conflict
			if v1.Version.GetGlobal() == v.Version.GetGlobal() {
				newUpdateEntries[k1] = v1
			}
		} else {
			newUpdateEntries[k1] = v1
		}
	}

	for k, v := range newUpdateEntries {
		configName := strings.Split(k, ".")
		if err := update(config, configName, v.Value); err != nil {
			return k, err
		}
	}
	return "", nil
}

func (c *ConfigManager) updateLocal(componentID string, version *configpb.Version, entries []*configpb.ConfigEntry) (*configpb.Version, *configpb.Status) {
	component := c.getComponent(componentID)
	if component == "" {
		return &configpb.Version{Global: 0, Local: 0}, &configpb.Status{Code: configpb.StatusCode_COMPONENT_NOT_FOUND}
	}
	updateEntries := make(map[string]*EntryValue)
	if _, ok := c.GlobalCfgs[component]; ok {
		globalUpdateEntries := c.GlobalCfgs[component].getUpdateEntries()
		for k, v := range globalUpdateEntries {
			updateEntries[k] = v
		}
	}
	if localCfg, ok := c.LocalCfgs[component][componentID]; ok {
		localLatestVersion := localCfg.getVersion()
		if !versionEqual(localLatestVersion, version) {
			return localLatestVersion, &configpb.Status{Code: configpb.StatusCode_WRONG_VERSION}
		}
		for _, entry := range entries {
			localCfg.updateEntry(entry, version)
		}
		if wrongEntry, err := mergeAndUpdateConfig(localCfg, updateEntries); err != nil {
			c.deleteEntry(component, wrongEntry)
			return localLatestVersion, &configpb.Status{Code: configpb.StatusCode_UNKNOWN, Message: err.Error()}
		}
		localCfg.Version = &configpb.Version{Global: version.GetGlobal(), Local: version.GetLocal() + 1}
	} else {
		return version, &configpb.Status{Code: configpb.StatusCode_COMPONENT_ID_NOT_FOUND}
	}
	return c.LocalCfgs[component][componentID].getVersion(), &configpb.Status{Code: configpb.StatusCode_OK}
}

func (c *ConfigManager) deleteEntry(component, e string) {
	if globalCfg, ok := c.GlobalCfgs[component]; ok {
		delete(globalCfg.UpdateEntries, e)
	}
	for _, localCfg := range c.LocalCfgs[component] {
		delete(localCfg.UpdateEntries, e)
	}
}

// DeleteConfig removes a component from the config manager.
func (c *ConfigManager) DeleteConfig(kind *configpb.ConfigKind, version *configpb.Version) *configpb.Status {
	c.Lock()
	defer c.Unlock()

	global := kind.GetGlobal()
	if global != nil {
		return c.deleteGlobal(global.GetComponent(), version)
	}

	local := kind.GetLocal()
	if local != nil {
		return c.deleteLocal(local.GetComponentId(), version)
	}

	return &configpb.Status{Code: configpb.StatusCode_UNKNOWN, Message: errUnknownKind(kind)}
}

func (c *ConfigManager) deleteGlobal(component string, version *configpb.Version) *configpb.Status {
	// TODO: Add delete global
	return &configpb.Status{Code: configpb.StatusCode_UNKNOWN, Message: errNotSupported}
}

func (c *ConfigManager) deleteLocal(componentID string, version *configpb.Version) *configpb.Status {
	component := c.getComponent(componentID)
	if component == "" {
		return &configpb.Status{Code: configpb.StatusCode_COMPONENT_NOT_FOUND}
	}
	if localCfg, ok := c.LocalCfgs[component][componentID]; ok {
		localLatestVersion := localCfg.getVersion()
		if !versionEqual(localLatestVersion, version) {
			return &configpb.Status{Code: configpb.StatusCode_WRONG_VERSION}
		}
		delete(c.LocalCfgs[component], componentID)
	} else {
		return &configpb.Status{Code: configpb.StatusCode_COMPONENT_ID_NOT_FOUND}
	}
	return &configpb.Status{Code: configpb.StatusCode_OK}
}

// EntryValue is composed by version and value.
type EntryValue struct {
	Version *configpb.Version
	Value   string
}

// NewEntryValue creates a new EntryValue.
func NewEntryValue(e *configpb.ConfigEntry, version *configpb.Version) *EntryValue {
	return &EntryValue{
		Version: version,
		Value:   e.GetValue(),
	}
}

// GlobalConfig is used to manage the global config of components.
type GlobalConfig struct {
	Version       uint64
	UpdateEntries map[string]*EntryValue
}

// NewGlobalConfig create a new GlobalConfig.
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
	entries := gc.getUpdateEntries()
	entries[entry.GetName()] = NewEntryValue(entry, version)
}

// getVersion returns the global version.
func (gc *GlobalConfig) getVersion() uint64 {
	if gc == nil {
		return 0
	}
	return gc.Version
}

// GetUpdateEntries returns a map of global entries which needs to be update.
func (gc *GlobalConfig) getUpdateEntries() map[string]*EntryValue {
	return gc.UpdateEntries
}

// GetConfigEntries returns config entries.
func (gc *GlobalConfig) GetConfigEntries() []*configpb.ConfigEntry {
	var entries []*configpb.ConfigEntry
	for k, v := range gc.UpdateEntries {
		entries = append(entries, &configpb.ConfigEntry{Name: k, Value: v.Value})
	}
	return entries
}

// LocalConfig is used to manage the local config of a component.
type LocalConfig struct {
	Version       *configpb.Version
	UpdateEntries map[string]*EntryValue
	Configs       map[string]interface{}
}

// NewLocalConfig create a new LocalConfig.
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

// GetUpdateEntries returns a map of local entries which needs to be update.
func (lc *LocalConfig) getUpdateEntries() map[string]*EntryValue {
	return lc.UpdateEntries
}

func (lc *LocalConfig) updateEntry(entry *configpb.ConfigEntry, version *configpb.Version) {
	entries := lc.getUpdateEntries()
	entries[entry.GetName()] = NewEntryValue(entry, version)
}

// getVersion return the local config version for a component.
func (lc *LocalConfig) getVersion() *configpb.Version {
	if lc == nil {
		return nil
	}
	return lc.Version
}

func (lc *LocalConfig) getConfigs() map[string]interface{} {
	return lc.Configs
}

// TODO: need to consider the redundant label.
func update(config map[string]interface{}, configName []string, value string) error {
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
	if t == nil {
		return errors.Errorf("config item %s is not existed", configName[0])
	}
	// TODO: support more types
	var v interface{}
	var err error
	switch t.Kind() {
	case reflect.Bool:
		v, err = strconv.ParseBool(value)
	case reflect.Int:
		v, err = strconv.Atoi(value)
	case reflect.Int64:
		v, err = strconv.ParseInt(value, 10, 64)
	case reflect.Float64:
		v, err = strconv.ParseFloat(value, 64)
	case reflect.String:
		v = value
	case reflect.Slice:
		// TODO: make slice work for any other type
		v = strings.Split(value, ",")
	default:
		return errors.Errorf("unsupported type")
	}

	if err != nil {
		return err
	}
	res[configName[0]] = v

	return nil
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

func versionEqual(a, b *configpb.Version) bool {
	return a.GetGlobal() == b.GetGlobal() && a.GetLocal() == b.GetLocal()
}
