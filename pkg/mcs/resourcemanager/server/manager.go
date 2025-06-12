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
	"fmt"
	"strings"
	"sync"
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

const (
	persistLoopInterval       = time.Minute
	metricsCleanupInterval    = time.Minute
	metricsCleanupTimeout     = 20 * time.Minute
	defaultCollectIntervalSec = 20
	tickPerSecond             = time.Second
)

// Manager is the manager of resource group.
type Manager struct {
	syncutil.RWMutex
	cancel           context.CancelFunc
	wg               sync.WaitGroup
	srv              bs.Server
	controllerConfig *ControllerConfig
	krgms            map[uint32]*keyspaceResourceGroupManager
	storage          interface {
		// Used to store the resource group settings and states.
		endpoint.ResourceGroupStorage
		// Used to get the keyspace meta info.
		endpoint.KeyspaceStorage
	}
	// consumptionChan is used to send the consumption
	// info to the background metrics flusher.
	consumptionDispatcher chan *consumptionItem
	// cached keyspace name for each keyspace ID.
	keyspaceNameLookup map[uint32]string
	// used to get the keyspace ID by name.
	keyspaceIDLookup map[string]uint32
	// metrics is the collection of metrics.
	metrics *metrics
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
		controllerConfig:      srv.(T).GetControllerConfig(),
		krgms:                 make(map[uint32]*keyspaceResourceGroupManager),
		consumptionDispatcher: make(chan *consumptionItem, defaultConsumptionChanSize),
		keyspaceNameLookup:    make(map[uint32]string),
		keyspaceIDLookup:      make(map[string]uint32),
		metrics:               newMetrics(),
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

// This is used for testing only now.
func (m *Manager) close() {
	if m.cancel != nil {
		m.cancel()
	}
	m.wg.Wait()
}

// GetBasicServer returns the basic server.
func (m *Manager) GetBasicServer() bs.Server {
	return m.srv
}

// GetStorage returns the storage.
func (m *Manager) GetStorage() endpoint.ResourceGroupStorage {
	return m.storage
}

// GetKeyspaceServiceLimiter returns the service limit of the keyspace.
func (m *Manager) GetKeyspaceServiceLimiter(keyspaceID uint32) *serviceLimiter {
	krgm := m.getKeyspaceResourceGroupManager(keyspaceID)
	if krgm == nil {
		return nil
	}
	return krgm.getServiceLimiter().Clone()
}

// SetKeyspaceServiceLimit sets the service limit of the keyspace.
func (m *Manager) SetKeyspaceServiceLimit(keyspaceID uint32, serviceLimit float64) {
	// If the keyspace is not found, create a new keyspace resource group manager.
	m.getOrCreateKeyspaceResourceGroupManager(keyspaceID, true).setServiceLimiter(serviceLimit)
}

func (m *Manager) getOrCreateKeyspaceResourceGroupManager(keyspaceID uint32, initDefault bool) *keyspaceResourceGroupManager {
	m.Lock()
	krgm, ok := m.krgms[keyspaceID]
	if !ok {
		krgm = newKeyspaceResourceGroupManager(keyspaceID, m.storage)
		m.krgms[keyspaceID] = krgm
	}
	m.Unlock()
	// Init the default resource group if needed.
	if initDefault {
		krgm.initDefaultResourceGroup()
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

	// This context is derived from the leader/primary context, it will be canceled
	// from the outside loop when the leader/primary step down.
	ctx, m.cancel = context.WithCancel(ctx)
	m.wg.Add(2)
	// Start the background metrics flusher.
	go m.backgroundMetricsFlush(ctx)
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
		// Since the default resource group might be loaded from the storage, we don't need to initialize it here.
		err := m.getOrCreateKeyspaceResourceGroupManager(keyspaceID, false).addResourceGroupFromRaw(name, rawValue)
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
	m.getOrCreateKeyspaceResourceGroupManager(constant.NullKeyspaceID, true)
	// Initialize the default resource group respectively for each keyspace if it doesn't exist.
	for _, krgm := range m.getKeyspaceResourceGroupManagers() {
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
	keyspaceID := ExtractKeyspaceID(grouppb.GetKeyspaceId())
	// If the keyspace is not initialized, it means this is the first resource group created for this keyspace,
	// so we need to initialize the default resource group for the keyspace as well.
	krgm := m.getOrCreateKeyspaceResourceGroupManager(keyspaceID, true)
	if krgm == nil {
		return errs.ErrKeyspaceNotExists.FastGenByArgs(keyspaceID)
	}
	return krgm.addResourceGroup(grouppb)
}

// ModifyResourceGroup modifies an existing resource group.
func (m *Manager) ModifyResourceGroup(grouppb *rmpb.ResourceGroup) error {
	keyspaceID := ExtractKeyspaceID(grouppb.GetKeyspaceId())
	krgm := m.getKeyspaceResourceGroupManager(keyspaceID)
	if krgm == nil {
		return errs.ErrKeyspaceNotExists.FastGenByArgs(keyspaceID)
	}
	return krgm.modifyResourceGroup(grouppb)
}

// DeleteResourceGroup deletes a resource group.
func (m *Manager) DeleteResourceGroup(keyspaceID uint32, name string) error {
	krgm := m.getKeyspaceResourceGroupManager(keyspaceID)
	if krgm == nil {
		return errs.ErrKeyspaceNotExists.FastGenByArgs(keyspaceID)
	}
	return krgm.deleteResourceGroup(name)
}

// GetResourceGroup returns a copy of a resource group.
func (m *Manager) GetResourceGroup(keyspaceID uint32, name string, withStats bool) *ResourceGroup {
	krgm := m.getKeyspaceResourceGroupManager(keyspaceID)
	if krgm == nil {
		return nil
	}
	return krgm.getResourceGroup(name, withStats)
}

// GetMutableResourceGroup returns a mutable resource group.
func (m *Manager) GetMutableResourceGroup(keyspaceID uint32, name string) *ResourceGroup {
	krgm := m.getKeyspaceResourceGroupManager(keyspaceID)
	if krgm == nil {
		return nil
	}
	return krgm.getMutableResourceGroup(name)
}

// GetResourceGroupList returns copies of resource group list.
func (m *Manager) GetResourceGroupList(keyspaceID uint32, withStats bool) []*ResourceGroup {
	krgm := m.getKeyspaceResourceGroupManager(keyspaceID)
	if krgm == nil {
		return nil
	}
	return krgm.getResourceGroupList(withStats, true)
}

func (m *Manager) getRUTracker(keyspaceID uint32, name string) *ruTracker {
	krgm := m.getKeyspaceResourceGroupManager(keyspaceID)
	if krgm == nil {
		return nil
	}
	return krgm.getOrCreateRUTracker(name)
}

func (m *Manager) persistLoop(ctx context.Context) {
	defer m.wg.Done()
	ticker := time.NewTicker(persistLoopInterval)
	failpoint.Inject("fastPersist", func() {
		ticker.Reset(100 * time.Millisecond)
	})
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Info("resource group manager persist loop exits")
			return
		case <-ticker.C:
			for _, krgm := range m.getKeyspaceResourceGroupManagers() {
				krgm.persistResourceGroupRunningState()
			}
		}
	}
}

func (m *Manager) getKeyspaceResourceGroupManagers() []*keyspaceResourceGroupManager {
	m.RLock()
	defer m.RUnlock()
	krgms := make([]*keyspaceResourceGroupManager, 0, len(m.krgms))
	for _, krgm := range m.krgms {
		krgms = append(krgms, krgm)
	}
	return krgms
}

func (m *Manager) dispatchConsumption(req *rmpb.TokenBucketRequest) error {
	isBackground := req.GetIsBackground()
	isTiFlash := req.GetIsTiflash()
	if isBackground && isTiFlash {
		return errors.New("background and tiflash cannot be true at the same time")
	}
	m.consumptionDispatcher <- &consumptionItem{
		keyspaceID:        ExtractKeyspaceID(req.GetKeyspaceId()),
		resourceGroupName: req.GetResourceGroupName(),
		Consumption:       req.GetConsumptionSinceLastRequest(),
		isBackground:      isBackground,
		isTiFlash:         isTiFlash,
	}
	return nil
}

func (m *Manager) getKeyspaceNameByID(ctx context.Context, id uint32) (string, error) {
	if id == constant.NullKeyspaceID {
		return "", nil
	}
	// Try to get the keyspace name from the cache first.
	m.RLock()
	name, ok := m.keyspaceNameLookup[id]
	m.RUnlock()
	if ok {
		return name, nil
	}
	var loadedName string
	// If the keyspace name is not in the cache, try to get it from the storage.
	err := m.storage.RunInTxn(ctx, func(txn kv.Txn) error {
		meta, err := m.storage.LoadKeyspaceMeta(txn, id)
		if err != nil {
			return err
		}
		loadedName = meta.GetName()
		return nil
	})
	if err != nil {
		log.Error("failed to get the keyspace name", zap.Uint32("keyspace-id", id), zap.Error(err))
		return "", err
	}
	if len(loadedName) == 0 {
		return "", fmt.Errorf("got an empty keyspace name by id %d", id)
	}
	// Update the cache.
	m.updateKeyspaceNameLookup(id, loadedName)
	return loadedName, nil
}

func (m *Manager) updateKeyspaceNameLookup(id uint32, name string) {
	m.Lock()
	defer m.Unlock()
	m.keyspaceNameLookup[id] = name
	m.keyspaceIDLookup[name] = id
}

// GetKeyspaceIDByName gets the keyspace ID by name.
func (m *Manager) GetKeyspaceIDByName(ctx context.Context, name string) (*rmpb.KeyspaceIDValue, error) {
	if len(name) == 0 {
		return &rmpb.KeyspaceIDValue{Value: constant.NullKeyspaceID}, nil
	}
	m.RLock()
	id, ok := m.keyspaceIDLookup[name]
	m.RUnlock()
	if ok {
		return &rmpb.KeyspaceIDValue{Value: id}, nil
	}
	var (
		loadedID uint32
		err      error
	)
	err = m.storage.RunInTxn(ctx, func(txn kv.Txn) error {
		ok, loadedID, err = m.storage.LoadKeyspaceID(txn, name)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		log.Error("failed to get the keyspace id", zap.String("keyspace-name", name), zap.Error(err))
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("keyspace not found with name: %s", name)
	}
	// Update the cache.
	m.updateKeyspaceNameLookup(loadedID, name)
	return &rmpb.KeyspaceIDValue{Value: loadedID}, nil
}

func (m *Manager) backgroundMetricsFlush(ctx context.Context) {
	defer logutil.LogPanic()
	defer m.wg.Done()
	cleanUpTicker := time.NewTicker(metricsCleanupInterval)
	defer cleanUpTicker.Stop()
	metricsTicker := time.NewTicker(tickPerSecond)
	defer metricsTicker.Stop()
	failpoint.Inject("fastCleanupTicker", func() {
		cleanUpTicker.Reset(100 * time.Millisecond)
	})

	for {
		select {
		case <-ctx.Done():
			log.Info("resource group manager background metrics flush loop exits")
			return
		case consumptionInfo := <-m.consumptionDispatcher:
			if consumptionInfo == nil || consumptionInfo.Consumption == nil {
				continue
			}
			keyspaceID := consumptionInfo.keyspaceID
			keyspaceName, err := m.getKeyspaceNameByID(ctx, keyspaceID)
			if err != nil {
				continue
			}
			now := time.Now()
			sinceLastRecord := m.metrics.recordConsumption(consumptionInfo, keyspaceName, m.controllerConfig, now)
			resourceGroupName := consumptionInfo.resourceGroupName
			// TODO: maybe we need to distinguish background ru.
			if rg := m.GetMutableResourceGroup(keyspaceID, resourceGroupName); rg != nil {
				rg.UpdateRUConsumption(consumptionInfo.Consumption)
			}
			if rt := m.getRUTracker(keyspaceID, resourceGroupName); rt != nil {
				rt.sample(now, consumptionInfo.RRU+consumptionInfo.WRU, sinceLastRecord)
			}
		case <-cleanUpTicker.C:
			// Clean up the metrics that have not been updated for a long time.
			for r, lastTime := range m.metrics.consumptionRecordMap {
				if time.Since(lastTime) <= metricsCleanupTimeout {
					continue
				}
				keyspaceName, err := m.getKeyspaceNameByID(ctx, r.keyspaceID)
				if err != nil {
					continue
				}
				m.metrics.cleanupAllMetrics(r, keyspaceName)
			}
		case <-metricsTicker.C:
			// Prevent from holding the lock too long when there're many keyspaces and resource groups.
			for _, krgm := range m.getKeyspaceResourceGroupManagers() {
				keyspaceName, err := m.getKeyspaceNameByID(ctx, krgm.keyspaceID)
				if err != nil {
					continue
				}
				for _, group := range krgm.getResourceGroupList(true, true) {
					groupName := group.Name
					// Record the sum of RRU and WRU every second.
					m.metrics.getMaxPerSecTracker(krgm.keyspaceID, keyspaceName, groupName).flushMetrics()
					// Skip the default resource group for the later metrics.
					if groupName == DefaultResourceGroupName {
						continue
					}
					metrics := m.metrics.getGaugeMetrics(krgm.keyspaceID, keyspaceName, groupName)
					metrics.setGroup(group)
					// Record the tracked RU per second.
					if rt := krgm.getRUTracker(groupName); rt != nil {
						metrics.setSampledRUPerSec(rt.getRUPerSec())
					}
				}
			}
		}
	}
}
