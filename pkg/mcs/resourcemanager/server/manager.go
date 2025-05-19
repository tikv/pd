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

package server

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/log"

	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/jsonutil"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

// Manager is the manager of resource group.
type Manager struct {
	syncutil.RWMutex
	srv              bs.Server
	controllerConfig *ControllerConfig
	krgms            map[uint32]*keyspaceResourceGroupManager
	storage          endpoint.ResourceGroupStorage
}

type consumptionRecordKey struct {
	name   string
	ruType string
}

// ConfigProvider is used to get resource manager config from the given
// `bs.server` without modifying its interface.
type ConfigProvider interface {
	GetControllerConfig() *ControllerConfig
}

// NewManager returns a new manager base on the given server,
// which should implement the `ConfigProvider` interface.
func NewManager[T ConfigProvider](srv bs.Server) *Manager {
	m := &Manager{
		controllerConfig: srv.(T).GetControllerConfig(),
		krgms:            make(map[uint32]*keyspaceResourceGroupManager),
	}
	// The first initialization after the server is started.
	srv.AddStartCallback(func() {
		log.Info("resource group manager starts to initialize", zap.String("name", srv.Name()))
		m.storage = endpoint.NewStorageEndpoint(
			kv.NewEtcdKVBase(srv.GetClient()),
			nil,
		)
		m.srv = srv
	})
	// The second initialization after becoming serving.
	srv.AddServiceReadyCallback(m.Init)
	return m
}

// GetBasicServer returns the basic server.
func (m *Manager) GetBasicServer() bs.Server {
	return m.srv
}

// GetStorage returns the storage.
func (m *Manager) GetStorage() endpoint.ResourceGroupStorage {
	return m.storage
}

func (m *Manager) getOrCreateKeyspaceResourceGroupManager(keyspaceID uint32) *keyspaceResourceGroupManager {
	krgm := m.getKeyspaceResourceGroupManager(keyspaceID)
	if krgm == nil {
		krgm = newKeyspaceResourceGroupManager(keyspaceID, m)
		m.Lock()
		m.krgms[keyspaceID] = krgm
		m.Unlock()
	}
	return krgm
}

func (m *Manager) getKeyspaceResourceGroupManager(keyspaceID uint32) *keyspaceResourceGroupManager {
	m.RLock()
	defer m.RUnlock()
	return m.krgms[keyspaceID]
}

// Init initializes the resource group manager.
func (m *Manager) Init(ctx context.Context) error {
	v, err := m.storage.LoadControllerConfig()
	if err != nil {
		log.Error("resource controller config load failed", zap.Error(err), zap.String("v", v))
		return err
	}
	if err = json.Unmarshal([]byte(v), &m.controllerConfig); err != nil {
		log.Warn("un-marshall controller config failed, fallback to default", zap.Error(err), zap.String("v", v))
	}

	// re-save the config to make sure the config has been persisted.
	if err := m.storage.SaveControllerConfig(m.controllerConfig); err != nil {
		return err
	}

	// Load keyspace resource groups from the storage.
	if err := m.loadKeyspaceResourceGroups(); err != nil {
		return err
	}

	// Start the background metrics flusher for each keyspace.
	m.RLock()
	defer m.RUnlock()
	for _, krgm := range m.krgms {
		go krgm.backgroundMetricsFlush(ctx)
	}

	go func() {
		defer logutil.LogPanic()
		m.persistLoop(ctx)
	}()
	log.Info("resource group manager finishes initialization")
	return nil
}

func (m *Manager) loadKeyspaceResourceGroups() error {
	// Empty the keyspace resource group manager map before the loading.
	m.Lock()
	m.krgms = make(map[uint32]*keyspaceResourceGroupManager)
	m.Unlock()
	// Load keyspace resource group meta info from the storage.
	if err := m.storage.LoadResourceGroupSettings(func(keyspaceID uint32, name string, rawValue string) {
		err := m.getOrCreateKeyspaceResourceGroupManager(keyspaceID).addResourceGroupFromRaw(name, rawValue)
		if err != nil {
			log.Error("failed to add resource group to the keyspace resource group manager",
				zap.Uint32("keyspace-id", keyspaceID), zap.String("group-name", name), zap.Error(err))
		}
	}); err != nil {
		return err
	}
	// Load keyspace resource group states from the storage.
	if err := m.storage.LoadResourceGroupStates(func(keyspaceID uint32, name string, rawValue string) {
		krgm := m.getKeyspaceResourceGroupManager(keyspaceID)
		if krgm == nil {
			log.Warn("failed to get the corresponding keyspace resource group manager",
				zap.Uint32("keyspace-id", keyspaceID), zap.String("group-name", name))
			return
		}
		err := krgm.setRawStatesIntoResourceGroup(name, rawValue)
		if err != nil {
			log.Error("failed to set resource group state",
				zap.Uint32("keyspace-id", keyspaceID), zap.String("group-name", name), zap.Error(err))
		}
	}); err != nil {
		return err
	}
	// Initialize the reserved keyspace resource group manager and default resource groups.
	m.initReserved()
	return nil
}

func (m *Manager) initReserved() {
	// Initialize the null keyspace resource group manager if it doesn't exist.
	m.getOrCreateKeyspaceResourceGroupManager(constant.NullKeyspaceID)
	// Initialize the default resource group respectively for each keyspace.
	m.RLock()
	defer m.RUnlock()
	for _, krgm := range m.krgms {
		krgm.initDefaultResourceGroup()
	}
}

// UpdateControllerConfigItem updates the controller config item.
func (m *Manager) UpdateControllerConfigItem(key string, value any) error {
	kp := strings.Split(key, ".")
	if len(kp) == 0 {
		return errors.Errorf("invalid key %s", key)
	}
	m.Lock()
	var config any
	switch kp[0] {
	case "request-unit":
		config = &m.controllerConfig.RequestUnit
	default:
		config = m.controllerConfig
	}
	updated, found, err := jsonutil.AddKeyValue(config, kp[len(kp)-1], value)
	if err != nil {
		m.Unlock()
		return err
	}

	if !found {
		m.Unlock()
		return errors.Errorf("config item %s not found", key)
	}
	m.Unlock()
	if updated {
		if err := m.storage.SaveControllerConfig(m.controllerConfig); err != nil {
			log.Error("save controller config failed", zap.Error(err))
		}
		log.Info("updated controller config item", zap.String("key", key), zap.Any("value", value))
	}
	return nil
}

// GetControllerConfig returns the controller config.
func (m *Manager) GetControllerConfig() *ControllerConfig {
	m.RLock()
	defer m.RUnlock()
	return m.controllerConfig
}

// AddResourceGroup puts a resource group.
// NOTE: AddResourceGroup should also be idempotent because tidb depends
// on this retry mechanism.
func (m *Manager) AddResourceGroup(grouppb *rmpb.ResourceGroup) error {
	keyspaceID := constant.NullKeyspaceID
	// TODO: validate the keyspace with the given ID first.
	krgm := m.getOrCreateKeyspaceResourceGroupManager(keyspaceID)
	if krgm == nil {
		return errs.ErrKeyspaceNotExists.FastGenByArgs(keyspaceID)
	}
	return krgm.addResourceGroup(grouppb)
}

// ModifyResourceGroup modifies an existing resource group.
func (m *Manager) ModifyResourceGroup(group *rmpb.ResourceGroup) error {
	keyspaceID := constant.NullKeyspaceID
	krgm := m.getKeyspaceResourceGroupManager(keyspaceID)
	if krgm == nil {
		return errs.ErrKeyspaceNotExists.FastGenByArgs(keyspaceID)
	}
	return krgm.modifyResourceGroup(group)
}

// DeleteResourceGroup deletes a resource group.
func (m *Manager) DeleteResourceGroup(name string) error {
	keyspaceID := constant.NullKeyspaceID
	krgm := m.getKeyspaceResourceGroupManager(keyspaceID)
	if krgm == nil {
		return errs.ErrKeyspaceNotExists.FastGenByArgs(keyspaceID)
	}
	return krgm.deleteResourceGroup(name)
}

// GetResourceGroup returns a copy of a resource group.
func (m *Manager) GetResourceGroup(name string, withStats bool) *ResourceGroup {
	keyspaceID := constant.NullKeyspaceID
	krgm := m.getKeyspaceResourceGroupManager(keyspaceID)
	if krgm == nil {
		return nil
	}
	return krgm.getResourceGroup(name, withStats)
}

// GetMutableResourceGroup returns a mutable resource group.
func (m *Manager) GetMutableResourceGroup(name string) *ResourceGroup {
	keyspaceID := constant.NullKeyspaceID
	krgm := m.getKeyspaceResourceGroupManager(keyspaceID)
	if krgm == nil {
		return nil
	}
	return krgm.getMutableResourceGroup(name)
}

// GetResourceGroupList returns copies of resource group list.
func (m *Manager) GetResourceGroupList(withStats bool) []*ResourceGroup {
	keyspaceID := constant.NullKeyspaceID
	krgm := m.getKeyspaceResourceGroupManager(keyspaceID)
	if krgm == nil {
		return nil
	}
	return krgm.getResourceGroupList(withStats)
}

func (m *Manager) persistLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	failpoint.Inject("fastPersist", func() {
		ticker.Reset(100 * time.Millisecond)
	})
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.persistResourceGroupRunningState()
		}
	}
}

func (m *Manager) persistResourceGroupRunningState() {
	m.RLock()
	krgms := make([]*keyspaceResourceGroupManager, 0, len(m.krgms))
	for _, krgm := range m.krgms {
		krgms = append(krgms, krgm)
	}
	m.RUnlock()
	for _, krgm := range krgms {
		krgm.persistResourceGroupRunningState()
	}
}

func (m *Manager) dispatchConsumption(keyspaceID uint32, item *consumptionItem) {
	krgm := m.getKeyspaceResourceGroupManager(keyspaceID)
	if krgm == nil {
		return
	}
	krgm.consumptionDispatcher <- item
}
