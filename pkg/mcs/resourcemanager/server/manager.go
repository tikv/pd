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
	"sort"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/jsonutil"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"go.uber.org/zap"
)

const (
	defaultConsumptionChanSize = 1024
	metricsCleanupInterval     = time.Minute
	metricsCleanupTimeout      = 20 * time.Minute
	metricsAvailableRUInterval = 1 * time.Second
	defaultCollectIntervalSec  = 20
	tickPerSecond              = time.Second

	reservedDefaultGroupName = "default"
	middlePriority           = 8
)

// Manager is the manager of resource group.
type Manager struct {
	syncutil.RWMutex
	srv              bs.Server
	controllerConfig *ControllerConfig
	groups           map[string]*ResourceGroup
	storage          endpoint.ResourceGroupStorage
	// consumptionChan is used to send the consumption
	// info to the background metrics flusher.
	consumptionDispatcher chan struct {
		resourceGroupName string
		*rmpb.Consumption
		isBackground bool
		isTiFlash    bool
	}
	// record update time of each resource group
	consumptionRecord map[consumptionRecordKey]time.Time
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
		groups:           make(map[string]*ResourceGroup),
		consumptionDispatcher: make(chan struct {
			resourceGroupName string
			*rmpb.Consumption
			isBackground bool
			isTiFlash    bool
		}, defaultConsumptionChanSize),
		consumptionRecord: make(map[consumptionRecordKey]time.Time),
	}
	// The first initialization after the server is started.
	srv.AddStartCallback(func() {
		log.Info("resource group manager starts to initialize", zap.String("name", srv.Name()))
		m.storage = endpoint.NewStorageEndpoint(
			kv.NewEtcdKVBase(srv.GetClient(), "resource_group"),
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
	// Load resource group meta info from storage.
	m.Lock()
	m.groups = make(map[string]*ResourceGroup)
	m.Unlock()
	handler := func(k, v string) {
		group := &rmpb.ResourceGroup{}
		if err := proto.Unmarshal([]byte(v), group); err != nil {
			log.Error("failed to parse the resource group", zap.Error(err), zap.String("k", k), zap.String("v", v))
			panic(err)
		}
		m.groups[group.Name] = FromProtoResourceGroup(group)
	}
	if err := m.storage.LoadResourceGroupSettings(handler); err != nil {
		return err
	}
	// Load resource group states from storage.
	tokenHandler := func(k, v string) {
		tokens := &GroupStates{}
		if err := json.Unmarshal([]byte(v), tokens); err != nil {
			log.Error("failed to parse the resource group state", zap.Error(err), zap.String("k", k), zap.String("v", v))
			panic(err)
		}
		if group, ok := m.groups[k]; ok {
			group.SetStatesIntoResourceGroup(tokens)
		}
	}
	if err := m.storage.LoadResourceGroupStates(tokenHandler); err != nil {
		return err
	}

	// Add default group if it's not inited.
	if _, ok := m.groups[reservedDefaultGroupName]; !ok {
		defaultGroup := &ResourceGroup{
			Name: reservedDefaultGroupName,
			Mode: rmpb.GroupMode_RUMode,
			RUSettings: &RequestUnitSettings{
				RU: &GroupTokenBucket{
					Settings: &rmpb.TokenLimitSettings{
						FillRate:   math.MaxInt32,
						BurstLimit: -1,
					},
				},
			},
			Priority: middlePriority,
		}
		if err := m.AddResourceGroup(defaultGroup.IntoProtoResourceGroup()); err != nil {
			log.Warn("init default group failed", zap.Error(err))
		}
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
	// Check the name.
	if len(grouppb.Name) == 0 || len(grouppb.Name) > 32 {
		return errs.ErrInvalidGroup
	}
	// Check the Priority.
	if grouppb.GetPriority() > 16 {
		return errs.ErrInvalidGroup
	}
	group := FromProtoResourceGroup(grouppb)
	m.Lock()
	defer m.Unlock()
	if err := group.persistSettings(m.storage); err != nil {
		return err
	}
	if err := group.persistStates(m.storage); err != nil {
		return err
	}
	m.groups[group.Name] = group
	return nil
}

// ModifyResourceGroup modifies an existing resource group.
func (m *Manager) ModifyResourceGroup(group *rmpb.ResourceGroup) error {
	if group == nil || group.Name == "" {
		return errs.ErrInvalidGroup
	}
	m.Lock()
	curGroup, ok := m.groups[group.Name]
	m.Unlock()
	if !ok {
		return errs.ErrResourceGroupNotExists.FastGenByArgs(group.Name)
	}

	err := curGroup.PatchSettings(group)
	if err != nil {
		return err
	}
	return curGroup.persistSettings(m.storage)
}

// DeleteResourceGroup deletes a resource group.
func (m *Manager) DeleteResourceGroup(name string) error {
	if name == reservedDefaultGroupName {
		return errs.ErrDeleteReservedGroup
	}
	if err := m.storage.DeleteResourceGroupSetting(name); err != nil {
		return err
	}
	m.Lock()
	delete(m.groups, name)
	m.Unlock()
	return nil
}

// GetResourceGroup returns a copy of a resource group.
func (m *Manager) GetResourceGroup(name string, withStats bool) *ResourceGroup {
	m.RLock()
	defer m.RUnlock()
	if group, ok := m.groups[name]; ok {
		return group.Clone(withStats)
	}
	return nil
}

// GetMutableResourceGroup returns a mutable resource group.
func (m *Manager) GetMutableResourceGroup(name string) *ResourceGroup {
	m.RLock()
	defer m.RUnlock()
	if group, ok := m.groups[name]; ok {
		return group
	}
	return nil
}

// GetResourceGroupList returns copies of resource group list.
func (m *Manager) GetResourceGroupList(withStats bool) []*ResourceGroup {
	m.RLock()
	res := make([]*ResourceGroup, 0, len(m.groups))
	for _, group := range m.groups {
		res = append(res, group.Clone(withStats))
	}
	m.RUnlock()
	sort.Slice(res, func(i, j int) bool {
		return res[i].Name < res[j].Name
	})
	return res
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
	keys := make([]string, 0, len(m.groups))
	for k := range m.groups {
		keys = append(keys, k)
	}
	m.RUnlock()
	for idx := range keys {
		m.RLock()
		group, ok := m.groups[keys[idx]]
		if ok {
			if err := group.persistStates(m.storage); err != nil {
				log.Error("persist resource group state failed", zap.Error(err))
			}
		}
		m.RUnlock()
	}
}

// Receive the consumption and flush it to the metrics.
func (m *Manager) backgroundMetricsFlush(ctx context.Context) {
	defer logutil.LogPanic()
	cleanUpTicker := time.NewTicker(metricsCleanupInterval)
	defer cleanUpTicker.Stop()
	availableRUTicker := time.NewTicker(metricsAvailableRUInterval)
	defer availableRUTicker.Stop()
	recordMaxTicker := time.NewTicker(tickPerSecond)
	defer recordMaxTicker.Stop()
	maxPerSecTrackers := make(map[string]*maxPerSecCostTracker)
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
			t, ok := maxPerSecTrackers[name]
			if !ok {
				t = newMaxPerSecCostTracker(name, defaultCollectIntervalSec)
				maxPerSecTrackers[name] = t
			}
			t.CollectConsumption(consumption)

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

			m.consumptionRecord[consumptionRecordKey{name: name, ruType: ruLabelType}] = time.Now()

			// TODO: maybe we need to distinguish background ru.
			if rg := m.GetMutableResourceGroup(name); rg != nil {
				rg.UpdateRUConsumption(consumptionInfo.Consumption)
			}
		case <-cleanUpTicker.C:
			// Clean up the metrics that have not been updated for a long time.
			for r, lastTime := range m.consumptionRecord {
				if time.Since(lastTime) > metricsCleanupTimeout {
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
					delete(maxPerSecTrackers, r.name)
					readRequestUnitMaxPerSecCost.DeleteLabelValues(r.name)
					writeRequestUnitMaxPerSecCost.DeleteLabelValues(r.name)
					resourceGroupConfigGauge.DeletePartialMatch(prometheus.Labels{newResourceGroupNameLabel: r.name})
				}
			}
		case <-availableRUTicker.C:
			m.RLock()
			groups := make([]*ResourceGroup, 0, len(m.groups))
			for name, group := range m.groups {
				if name == reservedDefaultGroupName {
					continue
				}
				groups = append(groups, group)
			}
			m.RUnlock()
			// prevent many groups and hold the lock long time.
			for _, group := range groups {
				ru := group.getRUToken()
				if ru < 0 {
					ru = 0
				}
				availableRUCounter.WithLabelValues(group.Name, group.Name).Set(ru)
				resourceGroupConfigGauge.WithLabelValues(group.Name, priorityLabel).Set(group.getPriority())
				resourceGroupConfigGauge.WithLabelValues(group.Name, ruPerSecLabel).Set(group.getFillRate())
				resourceGroupConfigGauge.WithLabelValues(group.Name, ruCapacityLabel).Set(group.getBurstLimit())
			}
		case <-recordMaxTicker.C:
			// Record the sum of RRU and WRU every second.
			m.RLock()
			names := make([]string, 0, len(m.groups))
			for name := range m.groups {
				names = append(names, name)
			}
			m.RUnlock()
			for _, name := range names {
				if t, ok := maxPerSecTrackers[name]; !ok {
					maxPerSecTrackers[name] = newMaxPerSecCostTracker(name, defaultCollectIntervalSec)
				} else {
					t.FlushMetrics()
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
