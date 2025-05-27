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
	"math"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
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
	persistLoopInterval        = 1 * time.Minute
	metricsCleanupInterval     = time.Minute
	metricsCleanupTimeout      = 20 * time.Minute
	metricsAvailableRUInterval = 1 * time.Second
	defaultCollectIntervalSec  = 20
	tickPerSecond              = time.Second
)

// Manager is the manager of resource group.
type Manager struct {
	syncutil.RWMutex
	srv              bs.Server
	controllerConfig *ControllerConfig
	krgms            map[uint32]*keyspaceResourceGroupManager
	storage          endpoint.ResourceGroupStorage
	// consumptionChan is used to send the consumption
	// info to the background metrics flusher.
	consumptionDispatcher chan *consumptionItem
	// record update time of each resource group
	consumptionRecord map[consumptionRecordKey]time.Time
}

type consumptionRecordKey struct {
	keyspaceID uint32
	name       string
	ruType     string
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
		consumptionRecord:     make(map[consumptionRecordKey]time.Time),
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
	m.Lock()
	defer m.Unlock()
	krgm, ok := m.krgms[keyspaceID]
	if !ok {
		krgm = newKeyspaceResourceGroupManager(keyspaceID, m.storage)
		m.krgms[keyspaceID] = krgm
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
	if grouppb.KeyspaceId != nil {
		keyspaceID = grouppb.KeyspaceId.GetValue()
	}
	krgm := m.getOrCreateKeyspaceResourceGroupManager(keyspaceID)
	if krgm == nil {
		return errs.ErrKeyspaceNotExists.FastGenByArgs(keyspaceID)
	}
	return krgm.addResourceGroup(grouppb)
}

// ModifyResourceGroup modifies an existing resource group.
func (m *Manager) ModifyResourceGroup(grouppb *rmpb.ResourceGroup) error {
	keyspaceID := constant.NullKeyspaceID
	if grouppb.KeyspaceId != nil {
		keyspaceID = grouppb.KeyspaceId.GetValue()
	}
	krgm := m.getKeyspaceResourceGroupManager(keyspaceID)
	if krgm == nil {
		return errs.ErrKeyspaceNotExists.FastGenByArgs(keyspaceID)
	}
	return krgm.modifyResourceGroup(grouppb)
}

// DeleteResourceGroup deletes a resource group.
func (m *Manager) DeleteResourceGroup(keyspaceID uint32, name string) error {
	// TODO: should we allow to delete default resource group?
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

func (m *Manager) persistLoop(ctx context.Context) {
	ticker := time.NewTicker(persistLoopInterval)
	failpoint.Inject("fastPersist", func() {
		ticker.Reset(100 * time.Millisecond)
	})
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
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
	keyspaceID := constant.NullKeyspaceID
	if req.KeyspaceId != nil {
		keyspaceID = req.KeyspaceId.GetValue()
	}
	m.consumptionDispatcher <- &consumptionItem{
		keyspaceID:        keyspaceID,
		resourceGroupName: req.GetResourceGroupName(),
		Consumption:       req.GetConsumptionSinceLastRequest(),
		isBackground:      isBackground,
		isTiFlash:         isTiFlash,
	}
	return nil
}

func (m *Manager) backgroundMetricsFlush(ctx context.Context) {
	defer logutil.LogPanic()
	cleanUpTicker := time.NewTicker(metricsCleanupInterval)
	defer cleanUpTicker.Stop()
	availableRUTicker := time.NewTicker(metricsAvailableRUInterval)
	defer availableRUTicker.Stop()
	recordMaxTicker := time.NewTicker(tickPerSecond)
	defer recordMaxTicker.Stop()

	maxPerSecTrackers := make(map[uint32]map[string]*maxPerSecCostTracker)
	// getMaxPerSecTracker returns the max per sec tracker for the given keyspace ID and name.
	// If the tracker doesn't exist, it will be created.
	getMaxPerSecTracker := func(keyspaceID uint32, name string) *maxPerSecCostTracker {
		trackers := maxPerSecTrackers[keyspaceID]
		if trackers == nil {
			trackers = make(map[string]*maxPerSecCostTracker)
			maxPerSecTrackers[keyspaceID] = trackers
		}
		tracker, ok := trackers[name]
		if !ok {
			tracker = newMaxPerSecCostTracker(name, defaultCollectIntervalSec)
			trackers[name] = tracker
		}
		return tracker
	}
	// deleteMaxPerSecTracker deletes the max per sec tracker for the given keyspace ID and name.
	// If the tracker doesn't exist, it will do nothing.
	deleteMaxPerSecTracker := func(keyspaceID uint32, name string) {
		trackers := maxPerSecTrackers[keyspaceID]
		if trackers == nil {
			return
		}
		delete(trackers, name)
	}
	for {
		select {
		case <-ctx.Done():
			return
		case consumptionInfo := <-m.consumptionDispatcher:
			consumption := consumptionInfo.Consumption
			if consumption == nil {
				continue
			}
			ruLabelType := defaultTypeLabel
			if consumptionInfo.isBackground {
				ruLabelType = backgroundTypeLabel
			}
			if consumptionInfo.isTiFlash {
				ruLabelType = tiflashTypeLabel
			}

			var (
				// TODO: add keyspace name lable to the metrics.
				keyspaceID               = consumptionInfo.keyspaceID
				name                     = consumptionInfo.resourceGroupName
				rruMetrics               = readRequestUnitCost.WithLabelValues(name, name, ruLabelType)
				wruMetrics               = writeRequestUnitCost.WithLabelValues(name, name, ruLabelType)
				sqlLayerRuMetrics        = sqlLayerRequestUnitCost.WithLabelValues(name, name)
				readByteMetrics          = readByteCost.WithLabelValues(name, name, ruLabelType)
				writeByteMetrics         = writeByteCost.WithLabelValues(name, name, ruLabelType)
				kvCPUMetrics             = kvCPUCost.WithLabelValues(name, name, ruLabelType)
				sqlCPUMetrics            = sqlCPUCost.WithLabelValues(name, name, ruLabelType)
				readRequestCountMetrics  = requestCount.WithLabelValues(name, name, readTypeLabel)
				writeRequestCountMetrics = requestCount.WithLabelValues(name, name, writeTypeLabel)
			)
			getMaxPerSecTracker(keyspaceID, name).CollectConsumption(consumption)

			// RU info.
			if consumption.RRU > 0 {
				rruMetrics.Add(consumption.RRU)
			}
			if consumption.WRU > 0 {
				wruMetrics.Add(consumption.WRU)
			}
			// Byte info.
			if consumption.ReadBytes > 0 {
				readByteMetrics.Add(consumption.ReadBytes)
			}
			if consumption.WriteBytes > 0 {
				writeByteMetrics.Add(consumption.WriteBytes)
			}
			// CPU time info.
			if consumption.TotalCpuTimeMs > 0 {
				if consumption.SqlLayerCpuTimeMs > 0 {
					sqlLayerRuMetrics.Add(consumption.SqlLayerCpuTimeMs * m.controllerConfig.RequestUnit.CPUMsCost)
					sqlCPUMetrics.Add(consumption.SqlLayerCpuTimeMs)
				}
				kvCPUMetrics.Add(consumption.TotalCpuTimeMs - consumption.SqlLayerCpuTimeMs)
			}
			// RPC count info.
			if consumption.KvReadRpcCount > 0 {
				readRequestCountMetrics.Add(consumption.KvReadRpcCount)
			}
			if consumption.KvWriteRpcCount > 0 {
				writeRequestCountMetrics.Add(consumption.KvWriteRpcCount)
			}

			m.consumptionRecord[consumptionRecordKey{keyspaceID: keyspaceID, name: name, ruType: ruLabelType}] = time.Now()

			// TODO: maybe we need to distinguish background ru.
			if rg := m.GetMutableResourceGroup(keyspaceID, name); rg != nil {
				rg.UpdateRUConsumption(consumptionInfo.Consumption)
			}
		case <-cleanUpTicker.C:
			// Clean up the metrics that have not been updated for a long time.
			for r, lastTime := range m.consumptionRecord {
				if time.Since(lastTime) <= metricsCleanupTimeout {
					continue
				}
				readRequestUnitCost.DeleteLabelValues(r.name, r.name, r.ruType)
				writeRequestUnitCost.DeleteLabelValues(r.name, r.name, r.ruType)
				sqlLayerRequestUnitCost.DeleteLabelValues(r.name, r.name, r.ruType)
				readByteCost.DeleteLabelValues(r.name, r.name, r.ruType)
				writeByteCost.DeleteLabelValues(r.name, r.name, r.ruType)
				kvCPUCost.DeleteLabelValues(r.name, r.name, r.ruType)
				sqlCPUCost.DeleteLabelValues(r.name, r.name, r.ruType)
				requestCount.DeleteLabelValues(r.name, r.name, readTypeLabel)
				requestCount.DeleteLabelValues(r.name, r.name, writeTypeLabel)
				availableRUCounter.DeleteLabelValues(r.name, r.name)
				delete(m.consumptionRecord, r)
				deleteMaxPerSecTracker(r.keyspaceID, r.name)
				readRequestUnitMaxPerSecCost.DeleteLabelValues(r.name)
				writeRequestUnitMaxPerSecCost.DeleteLabelValues(r.name)
				resourceGroupConfigGauge.DeletePartialMatch(prometheus.Labels{newResourceGroupNameLabel: r.name})
			}
		case <-availableRUTicker.C:
			// Prevent from holding the lock too long when there're many keyspaces and resource groups.
			for _, krgm := range m.getKeyspaceResourceGroupManagers() {
				for _, group := range krgm.getResourceGroupList(true, false) {
					ru := math.Max(group.getRUToken(), 0)
					availableRUCounter.WithLabelValues(group.Name, group.Name).Set(ru)
					resourceGroupConfigGauge.WithLabelValues(group.Name, priorityLabel).Set(group.getPriority())
					resourceGroupConfigGauge.WithLabelValues(group.Name, ruPerSecLabel).Set(group.getFillRate())
					resourceGroupConfigGauge.WithLabelValues(group.Name, ruCapacityLabel).Set(group.getBurstLimit())
				}
			}
		case <-recordMaxTicker.C:
			// Record the sum of RRU and WRU every second.
			for _, krgm := range m.getKeyspaceResourceGroupManagers() {
				names := krgm.getResourceGroupNames(true)
				for _, name := range names {
					getMaxPerSecTracker(krgm.keyspaceID, name).FlushMetrics()
				}
			}
		}
	}
}

type maxPerSecCostTracker struct {
	name          string
	maxPerSecRRU  float64
	maxPerSecWRU  float64
	rruSum        float64
	wruSum        float64
	lastRRUSum    float64
	lastWRUSum    float64
	flushPeriod   int
	cnt           int
	rruMaxMetrics prometheus.Gauge
	wruMaxMetrics prometheus.Gauge
}

func newMaxPerSecCostTracker(name string, flushPeriod int) *maxPerSecCostTracker {
	return &maxPerSecCostTracker{
		name:          name,
		flushPeriod:   flushPeriod,
		rruMaxMetrics: readRequestUnitMaxPerSecCost.WithLabelValues(name),
		wruMaxMetrics: writeRequestUnitMaxPerSecCost.WithLabelValues(name),
	}
}

// CollectConsumption collects the consumption info.
func (t *maxPerSecCostTracker) CollectConsumption(consume *rmpb.Consumption) {
	t.rruSum += consume.RRU
	t.wruSum += consume.WRU
}

// FlushMetrics and set the maxPerSecRRU and maxPerSecWRU to the metrics.
func (t *maxPerSecCostTracker) FlushMetrics() {
	if t.lastRRUSum == 0 && t.lastWRUSum == 0 {
		t.lastRRUSum = t.rruSum
		t.lastWRUSum = t.wruSum
		return
	}
	deltaRRU := t.rruSum - t.lastRRUSum
	deltaWRU := t.wruSum - t.lastWRUSum
	t.lastRRUSum = t.rruSum
	t.lastWRUSum = t.wruSum
	if deltaRRU > t.maxPerSecRRU {
		t.maxPerSecRRU = deltaRRU
	}
	if deltaWRU > t.maxPerSecWRU {
		t.maxPerSecWRU = deltaWRU
	}
	t.cnt++
	// flush to metrics in every flushPeriod.
	if t.cnt%t.flushPeriod == 0 {
		t.rruMaxMetrics.Set(t.maxPerSecRRU)
		t.wruMaxMetrics.Set(t.maxPerSecWRU)
		t.maxPerSecRRU = 0
		t.maxPerSecWRU = 0
	}
}
