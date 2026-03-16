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
	"strconv"
	"strings"
	"sync"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
)

var resourceGroupWatchPrefix = keypath.ResourceGroupPrefix()

type resourceGroupWatchEntryType uint8

const (
	// keypath.ControllerConfigPath()
	resourceGroupWatchEntryController resourceGroupWatchEntryType = iota
	// keypath.ResourceGroupSettingPrefix() and keypath.KeyspaceResourceGroupSettingPrefix()
	resourceGroupWatchEntrySettings
	// keypath.ResourceGroupStatePrefix() and keypath.KeyspaceResourceGroupStatePrefix()
	resourceGroupWatchEntryStates
	// keypath.KeyspaceServiceLimitPrefix()
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
	switch {
	case path == keypath.ControllerConfigPath():
		return resourceGroupWatchTarget{
			entryType: resourceGroupWatchEntryController,
		}, true
	case strings.HasPrefix(path, keypath.ResourceGroupSettingPrefix()):
		return parseLegacyResourceGroupWatchPath(strings.TrimPrefix(path, keypath.ResourceGroupSettingPrefix()), resourceGroupWatchEntrySettings)
	case strings.HasPrefix(path, keypath.ResourceGroupStatePrefix()):
		return parseLegacyResourceGroupWatchPath(strings.TrimPrefix(path, keypath.ResourceGroupStatePrefix()), resourceGroupWatchEntryStates)
	case strings.HasPrefix(path, keypath.KeyspaceResourceGroupSettingPrefix()):
		return parseKeyspaceWatchPath(strings.TrimPrefix(path, keypath.KeyspaceResourceGroupSettingPrefix()), resourceGroupWatchEntrySettings)
	case strings.HasPrefix(path, keypath.KeyspaceResourceGroupStatePrefix()):
		return parseKeyspaceWatchPath(strings.TrimPrefix(path, keypath.KeyspaceResourceGroupStatePrefix()), resourceGroupWatchEntryStates)
	case strings.HasPrefix(path, keypath.KeyspaceServiceLimitPrefix()):
		return parseKeyspaceIDWatchPath(strings.TrimPrefix(path, keypath.KeyspaceServiceLimitPrefix()), resourceGroupWatchEntryServiceLimit)
	default:
		return resourceGroupWatchTarget{}, false
	}
}

func parseLegacyResourceGroupWatchPath(groupName string, entryType resourceGroupWatchEntryType) (resourceGroupWatchTarget, bool) {
	if groupName == "" {
		return resourceGroupWatchTarget{}, false
	}
	return resourceGroupWatchTarget{
		entryType:  entryType,
		keyspaceID: constant.NullKeyspaceID,
		groupName:  groupName,
	}, true
}

func parseKeyspaceWatchPath(path string, entryType resourceGroupWatchEntryType) (resourceGroupWatchTarget, bool) {
	keyspaceID, groupName, err := keypath.ParseKeyspaceResourceGroupPath(path)
	if err != nil {
		log.Warn("failed to parse keyspace resource group watch path",
			zap.String("path", path),
			zap.Error(err))
		return resourceGroupWatchTarget{}, false
	}
	if groupName == "" {
		log.Warn("skip keyspace resource group watch path without group name",
			zap.String("path", path))
		return resourceGroupWatchTarget{}, false
	}
	return resourceGroupWatchTarget{
		entryType:  entryType,
		keyspaceID: keyspaceID,
		groupName:  groupName,
	}, true
}

func parseKeyspaceIDWatchPath(path string, entryType resourceGroupWatchEntryType) (resourceGroupWatchTarget, bool) {
	if path == "" || strings.Contains(path, "/") {
		log.Warn("skip invalid keyspace service limit watch path",
			zap.String("path", path))
		return resourceGroupWatchTarget{}, false
	}
	keyspaceID, err := strconv.ParseUint(path, 10, 32)
	if err != nil {
		log.Warn("failed to parse keyspace service limit watch path",
			zap.String("path", path),
			zap.Error(err))
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
	return nil
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
	if target.entryType == resourceGroupWatchEntryServiceLimit {
		krgm := m.getKeyspaceResourceGroupManager(target.keyspaceID)
		if krgm == nil {
			log.Debug("skip deleting service limit without corresponding manager",
				zap.String("key", key),
				zap.Uint32("keyspace-id", target.keyspaceID))
			return nil
		}
		krgm.setServiceLimitFromStorage(0)
		return nil
	}
	if target.entryType == resourceGroupWatchEntryStates {
		// States cleanup is handled when the settings key is deleted.
		return nil
	}
	if target.entryType != resourceGroupWatchEntrySettings {
		return nil
	}
	// Keep the reserved group alive.
	if target.groupName == DefaultResourceGroupName {
		m.getOrCreateKeyspaceResourceGroupManager(target.keyspaceID, false).restoreDefaultResourceGroupFromReserved()
		return nil
	}
	krgm := m.getKeyspaceResourceGroupManager(target.keyspaceID)
	if krgm == nil {
		log.Debug("skip deleting resource group without corresponding manager",
			zap.String("key", key),
			zap.Uint32("keyspace-id", target.keyspaceID),
			zap.String("group-name", target.groupName))
		return nil
	}
	krgm.deleteResourceGroupFromCache(target.groupName)
	return nil
}
