// Copyright 2026 TiKV Project Authors.
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

package server

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"sync"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/utils/etcdutil"
)

const (
	resourceGroupWatchPrefix = "resource_group/"

	controllerConfigWatchKey            = "controller"
	legacyResourceGroupSettingsPrefix   = "settings/"
	legacyResourceGroupStatesPrefix     = "states/"
	keyspaceResourceGroupSettingsPrefix = "keyspace/settings/"
	keyspaceResourceGroupStatesPrefix   = "keyspace/states/"
	keyspaceServiceLimitPrefix          = "keyspace/service_limits/"
)

type resourceGroupWatchEntryType uint8

const (
	resourceGroupWatchEntryUnknown resourceGroupWatchEntryType = iota
	resourceGroupWatchEntryController
	resourceGroupWatchEntrySettings
	resourceGroupWatchEntryStates
	resourceGroupWatchEntryServiceLimit
)

type resourceGroupWatchTarget struct {
	entryType  resourceGroupWatchEntryType
	keyspaceID uint32
	groupName  string
}

type metadataLoopWatcher interface {
	StartWatchLoop()
	WaitLoad() error
}

var newMetadataLoopWatcher = func(
	ctx context.Context,
	wg *sync.WaitGroup,
	client *clientv3.Client,
	name, key string,
	preEventsFn func([]*clientv3.Event) error,
	putFn, deleteFn func(*mvccpb.KeyValue) error,
	postEventsFn func([]*clientv3.Event) error,
	isWithPrefix bool,
) metadataLoopWatcher {
	return etcdutil.NewLoopWatcher(
		ctx,
		wg,
		client,
		name,
		key,
		preEventsFn,
		putFn,
		deleteFn,
		postEventsFn,
		isWithPrefix,
	)
}

func parseResourceGroupWatchPath(path string) (resourceGroupWatchTarget, bool) {
	if !strings.HasPrefix(path, resourceGroupWatchPrefix) {
		return resourceGroupWatchTarget{}, false
	}
	trimmed := strings.TrimPrefix(path, resourceGroupWatchPrefix)
	if trimmed == controllerConfigWatchKey {
		return resourceGroupWatchTarget{
			entryType: resourceGroupWatchEntryController,
		}, true
	}
	if strings.HasPrefix(trimmed, legacyResourceGroupSettingsPrefix) {
		name := strings.TrimPrefix(trimmed, legacyResourceGroupSettingsPrefix)
		if name == "" {
			return resourceGroupWatchTarget{}, false
		}
		return resourceGroupWatchTarget{
			entryType:  resourceGroupWatchEntrySettings,
			keyspaceID: constant.NullKeyspaceID,
			groupName:  name,
		}, true
	}
	if strings.HasPrefix(trimmed, legacyResourceGroupStatesPrefix) {
		name := strings.TrimPrefix(trimmed, legacyResourceGroupStatesPrefix)
		if name == "" {
			return resourceGroupWatchTarget{}, false
		}
		return resourceGroupWatchTarget{
			entryType:  resourceGroupWatchEntryStates,
			keyspaceID: constant.NullKeyspaceID,
			groupName:  name,
		}, true
	}
	if strings.HasPrefix(trimmed, keyspaceResourceGroupSettingsPrefix) {
		return parseKeyspaceWatchPath(strings.TrimPrefix(trimmed, keyspaceResourceGroupSettingsPrefix), resourceGroupWatchEntrySettings)
	}
	if strings.HasPrefix(trimmed, keyspaceResourceGroupStatesPrefix) {
		return parseKeyspaceWatchPath(strings.TrimPrefix(trimmed, keyspaceResourceGroupStatesPrefix), resourceGroupWatchEntryStates)
	}
	if strings.HasPrefix(trimmed, keyspaceServiceLimitPrefix) {
		return parseKeyspaceIDWatchPath(strings.TrimPrefix(trimmed, keyspaceServiceLimitPrefix), resourceGroupWatchEntryServiceLimit)
	}
	return resourceGroupWatchTarget{}, false
}

func parseKeyspaceWatchPath(path string, entryType resourceGroupWatchEntryType) (resourceGroupWatchTarget, bool) {
	parts := strings.SplitN(path, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return resourceGroupWatchTarget{}, false
	}
	keyspaceID, err := strconv.ParseUint(parts[0], 10, 32)
	if err != nil {
		return resourceGroupWatchTarget{}, false
	}
	return resourceGroupWatchTarget{
		entryType:  entryType,
		keyspaceID: uint32(keyspaceID),
		groupName:  parts[1],
	}, true
}

func parseKeyspaceIDWatchPath(path string, entryType resourceGroupWatchEntryType) (resourceGroupWatchTarget, bool) {
	if path == "" || strings.Contains(path, "/") {
		return resourceGroupWatchTarget{}, false
	}
	keyspaceID, err := strconv.ParseUint(path, 10, 32)
	if err != nil {
		return resourceGroupWatchTarget{}, false
	}
	return resourceGroupWatchTarget{
		entryType:  entryType,
		keyspaceID: uint32(keyspaceID),
	}, true
}

func (m *Manager) initializeMetadataWatcher(ctx context.Context) error {
	m.Lock()
	m.krgms = make(map[uint32]*keyspaceResourceGroupManager)
	m.Unlock()

	putFn := func(kv *mvccpb.KeyValue) error {
		return m.handleMetadataWatchPut(string(kv.Key), string(kv.Value))
	}
	deleteFn := func(kv *mvccpb.KeyValue) error {
		return m.handleMetadataWatchDelete(string(kv.Key))
	}
	watcher := newMetadataLoopWatcher(
		ctx,
		&m.wg,
		m.srv.GetClient(),
		"resource-manager-metadata-watcher",
		resourceGroupWatchPrefix,
		func([]*clientv3.Event) error { return nil },
		putFn,
		deleteFn,
		func([]*clientv3.Event) error { return nil },
		true, /* withPrefix */
	)
	watcher.StartWatchLoop()
	if err := watcher.WaitLoad(); err != nil {
		return err
	}
	// Ensure reserved default groups exist even if settings were missing in storage.
	m.initReserved()
	return m.loadServiceLimits()
}

func (m *Manager) handleMetadataWatchPut(key, rawValue string) error {
	target, ok := parseResourceGroupWatchPath(key)
	if !ok {
		return nil
	}
	switch target.entryType {
	case resourceGroupWatchEntryController:
		return m.applyControllerConfigFromRaw(rawValue)
	case resourceGroupWatchEntrySettings:
		return m.applyResourceGroupSettingFromRaw(target.keyspaceID, target.groupName, rawValue)
	case resourceGroupWatchEntryStates:
		return m.applyResourceGroupStatesFromRaw(target.keyspaceID, target.groupName, rawValue)
	case resourceGroupWatchEntryServiceLimit:
		return m.applyServiceLimitFromRaw(target.keyspaceID, rawValue)
	default:
		return nil
	}
}

func (m *Manager) handleMetadataWatchDelete(key string) error {
	target, ok := parseResourceGroupWatchPath(key)
	if !ok {
		return nil
	}
	switch target.entryType {
	case resourceGroupWatchEntryServiceLimit:
		krgm := m.getKeyspaceResourceGroupManager(target.keyspaceID)
		if krgm == nil {
			return nil
		}
		krgm.setServiceLimitFromStorage(0)
		return nil
	case resourceGroupWatchEntrySettings:
	default:
		return nil
	}
	// Keep the reserved group alive.
	if target.groupName == DefaultResourceGroupName {
		return nil
	}
	krgm := m.getKeyspaceResourceGroupManager(target.keyspaceID)
	if krgm == nil {
		return nil
	}
	krgm.deleteResourceGroupFromCache(target.groupName)
	return nil
}

func (m *Manager) applyControllerConfigFromRaw(rawValue string) error {
	controllerConfig := &ControllerConfig{}
	if err := json.Unmarshal([]byte(rawValue), controllerConfig); err != nil {
		log.Error("failed to apply controller config from watcher",
			zap.String("raw-value", rawValue),
			zap.Error(err))
		return err
	}
	m.Lock()
	m.controllerConfig = controllerConfig
	m.Unlock()
	return nil
}

func (m *Manager) applyResourceGroupSettingFromRaw(keyspaceID uint32, name, rawValue string) error {
	krgm := m.getOrCreateKeyspaceResourceGroupManager(keyspaceID, false)
	if err := krgm.upsertResourceGroupFromRaw(name, rawValue); err != nil {
		log.Error("failed to apply resource group settings from watcher",
			zap.Uint32("keyspace-id", keyspaceID),
			zap.String("group-name", name),
			zap.String("raw-value", rawValue),
			zap.Error(err))
		return err
	}
	return nil
}

func (m *Manager) applyServiceLimitFromRaw(keyspaceID uint32, rawValue string) error {
	var serviceLimit float64
	if err := json.Unmarshal([]byte(rawValue), &serviceLimit); err != nil {
		log.Error("failed to apply service limit from watcher",
			zap.Uint32("keyspace-id", keyspaceID),
			zap.String("raw-value", rawValue),
			zap.Error(err))
		return err
	}
	m.getOrCreateKeyspaceResourceGroupManager(keyspaceID, false).setServiceLimitFromStorage(serviceLimit)
	return nil
}

func (m *Manager) applyResourceGroupStatesFromRaw(keyspaceID uint32, name, rawValue string) error {
	krgm := m.getKeyspaceResourceGroupManager(keyspaceID)
	if krgm == nil {
		log.Debug("skip applying resource group states without corresponding manager",
			zap.Uint32("keyspace-id", keyspaceID), zap.String("group-name", name))
		return nil
	}
	if err := krgm.setRawStatesIntoResourceGroup(name, rawValue); err != nil {
		log.Error("failed to apply resource group states from watcher",
			zap.Uint32("keyspace-id", keyspaceID),
			zap.String("group-name", name),
			zap.String("raw-value", rawValue),
			zap.Error(err))
		return err
	}
	return nil
}
