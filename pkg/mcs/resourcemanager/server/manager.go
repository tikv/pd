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
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
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
	// resourceGroupStatePersistInterval is the interval to persist resource group running states
	// (e.g. token bucket state and RU consumption) into storage.
	resourceGroupStatePersistInterval = 10 * time.Minute

	reservedDefaultGroupName = "default"
	middlePriority           = 8

	pushMetricsTimeout = 10 * time.Second
	rcuMetricsInterval = 10 * time.Second
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
	consumptionRecordMu syncutil.RWMutex
	consumptionRecord   map[consumptionRecordKey]time.Time

	// async loading state management
	loadingState int32 // atomic access
	// syncLoadedGroups records groups that were loaded synchronously (e.g., by lazy loading)
	syncLoadedGroups map[string]bool
}

// LoadingState represents the current loading state of resource groups
const (
	// LoadingStateNotStarted means resource groups haven't started loading
	LoadingStateNotStarted int32 = iota
	// LoadingStateInProgress means resource groups are being loaded asynchronously
	LoadingStateInProgress
	// LoadingStateCompleted means all resource groups have been loaded
	LoadingStateCompleted
)

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
		loadingState:      LoadingStateNotStarted,
		syncLoadedGroups:  make(map[string]bool),
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
	m.syncLoadedGroups = make(map[string]bool)
	atomic.StoreInt32(&m.loadingState, LoadingStateNotStarted)
	m.Unlock()

	// Start async loading of resource groups
	go m.asyncLoadResourceGroups(ctx)

	// Start the background metrics flusher.
	go m.backgroundMetricsFlush(ctx, m.controllerConfig.PushMetricsAddress, m.controllerConfig.PushMetricsInterval.Duration)
	go func() {
		defer logutil.LogPanic()
		m.persistLoop(ctx)
	}()
	log.Info("resource group manager finishes initialization (async loading started)")
	return nil
}

// asyncLoadResourceGroups loads all resource groups asynchronously
func (m *Manager) asyncLoadResourceGroups(ctx context.Context) {
	defer logutil.LogPanic()

	const retryInterval = 10 * time.Second
	retry := 0

	for {
		select {
		case <-ctx.Done():
			log.Info("async loading resource groups cancelled")
			return
		default:
		}
		if retry > 0 {
			log.Info("retrying async loading resource groups", zap.Int("retry", retry))
			time.Sleep(retryInterval)
		}

		atomic.StoreInt32(&m.loadingState, LoadingStateInProgress)

		log.Info("start async loading resource groups", zap.Int("retry", retry))

		// Start timing the async loading
		startTime := time.Now()

		// Use a separate map to load all resource groups completely
		tempGroups := make(map[string]*ResourceGroup)

		// Load resource group settings from storage
		settingsHandler := func(k, v string) {
			group := &rmpb.ResourceGroup{}
			if err := proto.Unmarshal([]byte(v), group); err != nil {
				log.Error("failed to parse the resource group", zap.Error(err), zap.String("k", k), zap.String("v", v))
				panic(err)
			}
			tempGroups[group.Name] = FromProtoResourceGroup(group)
		}
		if err := m.storage.LoadResourceGroupSettings(settingsHandler); err != nil {
			log.Error("failed to load resource group settings", zap.Error(err), zap.Int("retry", retry))
			atomic.StoreInt32(&m.loadingState, LoadingStateNotStarted)
			retry++
			continue
		}

		// Load resource group states from storage
		statesHandler := func(k, v string) {
			tokens := &GroupStates{}
			if err := json.Unmarshal([]byte(v), tokens); err != nil {
				log.Error("failed to parse the resource group state", zap.Error(err), zap.String("k", k), zap.String("v", v))
				panic(err)
			}
			if group, ok := tempGroups[k]; ok {
				group.SetStatesIntoResourceGroup(tokens)
			}
		}
		if err := m.storage.LoadResourceGroupStates(statesHandler); err != nil {
			log.Error("failed to load resource group states", zap.Error(err), zap.Int("retry", retry))
			atomic.StoreInt32(&m.loadingState, LoadingStateNotStarted)
			retry++
			continue
		}

		// Now update the main groups map with completely loaded resource groups
		m.Lock()
		for name, group := range tempGroups {
			// Skip if the group was loaded synchronously (e.g., by lazy loading)
			// In case lazy loaded group is deleted.
			if !m.syncLoadedGroups[name] {
				m.groups[name] = group
			}
		}
		m.syncLoadedGroups = nil
		m.Unlock()

		err := m.addDefaultGroupIfNeed()
		if err != nil {
			log.Error("failed to persist default group", zap.Error(err))
			continue
		}

		atomic.StoreInt32(&m.loadingState, LoadingStateCompleted)

		// Record the async loading duration
		duration := time.Since(startTime)
		asyncLoadGroupDuration.Observe(duration.Seconds())

		log.Info("async loading resource groups completed", zap.Int("loaded_groups", len(tempGroups)), zap.Duration("duration", duration))
		return
	}
}

func (m *Manager) loadResourceGroup(name string) (*ResourceGroup, error) {
	// Load the specific resource group setting
	group, err := m.storage.LoadResourceGroupSetting(name)
	if err != nil {
		return nil, err
	}

	groupProto := &rmpb.ResourceGroup{}
	if err := proto.Unmarshal([]byte(group), groupProto); err != nil {
		return nil, err
	}

	resourceGroup := FromProtoResourceGroup(groupProto)

	// Load states if available
	states, err := m.storage.LoadResourceGroupState(name)
	if err == nil && states != "" {
		tokens := &GroupStates{}
		if err := json.Unmarshal([]byte(states), tokens); err == nil {
			resourceGroup.SetStatesIntoResourceGroup(tokens)
		}
	}
	return resourceGroup, nil
}

// loadResourceGroupIfNeeded loads a specific resource group if it's not already loaded
func (m *Manager) loadResourceGroupIfNeeded(name string) error {
	if atomic.LoadInt32(&m.loadingState) == LoadingStateCompleted {
		return nil
	}

	// Check if already loaded (for cases where async loading hasn't started yet)
	m.RLock()
	if _, ok := m.groups[name]; ok {
		m.RUnlock()
		return nil
	}
	m.RUnlock()

	group, err := m.loadResourceGroup(name)
	if err != nil && name == reservedDefaultGroupName {
		err = m.addDefaultGroupIfNeed()
	}
	if err != nil {
		return err
	}

	// Double-check if the group was loaded by another goroutine while we were loading
	m.Lock()
	defer m.Unlock()
	if _, exists := m.groups[name]; exists {
		// Another goroutine already loaded it, use the existing one
		return nil
	}
	// Add our loaded group to the map
	m.groups[name] = group
	// Mark this group as synchronously loaded
	m.syncLoadedGroups[name] = true

	// Increment the sync load group counter
	syncLoadGroupCounter.Inc()

	return nil
}

func (m *Manager) addDefaultGroupIfNeed() error {
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
	group := FromProtoResourceGroup(defaultGroup.IntoProtoResourceGroup())
	m.Lock()
	defer m.Unlock()
	if _, ok := m.groups[reservedDefaultGroupName]; ok {
		return nil
	}
	m.groups[group.Name] = group
	if err := group.persistSettings(m.storage); err != nil {
		return err
	}
	return group.persistStates(m.storage)
}

// isResourceGroupLoadingComplete returns true if resource group loading is completed
func (m *Manager) isResourceGroupLoadingComplete() bool {
	return atomic.LoadInt32(&m.loadingState) == LoadingStateCompleted
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
	// Try to load the resource group if not already loaded
	if err := m.loadResourceGroupIfNeeded(grouppb.Name); err != nil {
		log.Debug("failed to load resource group", zap.String("name", grouppb.Name), zap.Error(err))
		// maybe not found, put new one directly.
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
	// Try to load the resource group if not already loaded
	if err := m.loadResourceGroupIfNeeded(group.Name); err != nil {
		log.Debug("failed to load resource group", zap.String("name", group.Name), zap.Error(err))
		return err
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
	// Try to load the resource group if not already loaded
	if err := m.loadResourceGroupIfNeeded(name); err != nil {
		log.Debug("failed to load resource group", zap.String("name", name), zap.Error(err))
		return err
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
	// Try to load the resource group if not already loaded
	if err := m.loadResourceGroupIfNeeded(name); err != nil {
		log.Debug("failed to load resource group", zap.String("name", name), zap.Error(err))
		return nil
	}

	m.RLock()
	defer m.RUnlock()
	if group, ok := m.groups[name]; ok {
		return group.Clone(withStats)
	}
	return nil
}

// GetMutableResourceGroup returns a mutable resource group.
func (m *Manager) GetMutableResourceGroup(name string) *ResourceGroup {
	// Try to load the resource group if not already loaded
	if err := m.loadResourceGroupIfNeeded(name); err != nil {
		log.Debug("failed to load resource group", zap.String("name", name), zap.Error(err))
		return nil
	}

	m.RLock()
	defer m.RUnlock()
	if group, ok := m.groups[name]; ok {
		return group
	}
	return nil
}

// GetResourceGroupList returns copies of resource group list.
// Returns error if resource groups are still being loaded asynchronously.
func (m *Manager) GetResourceGroupList(withStats bool) ([]*ResourceGroup, error) {
	// Check if resource groups are still being loaded
	if !m.isResourceGroupLoadingComplete() {
		log.Debug("resource groups are still being loaded, cannot return list")
		return nil, errs.ErrResourceGroupsLoading
	}

	m.RLock()
	res := make([]*ResourceGroup, 0, len(m.groups))
	for _, group := range m.groups {
		res = append(res, group.Clone(withStats))
	}
	m.RUnlock()
	sort.Slice(res, func(i, j int) bool {
		return res[i].Name < res[j].Name
	})
	return res, nil
}

func (m *Manager) persistLoop(ctx context.Context) {
	ticker := time.NewTicker(resourceGroupStatePersistInterval)
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
	for _, group := range m.getActiveResourceGroups() {
		if err := group.persistStates(m.storage); err != nil {
			log.Error("persist resource group state failed", zap.Error(err))
		}
	}
}

func (m *Manager) updateConsumptionRecord(name string, ruType string) {
	m.consumptionRecordMu.Lock()
	defer m.consumptionRecordMu.Unlock()
	m.consumptionRecord[consumptionRecordKey{name: name, ruType: ruType}] = time.Now()
}

func (m *Manager) getActiveResourceGroups() []*ResourceGroup {
	m.consumptionRecordMu.RLock()
	groupNames := make([]string, 0, len(m.consumptionRecord))
	for k := range m.consumptionRecord {
		if k.name == reservedDefaultGroupName {
			continue
		}
		groupNames = append(groupNames, k.name)
	}
	m.consumptionRecordMu.RUnlock()

	m.RLock()
	defer m.RUnlock()
	groups := make([]*ResourceGroup, 0, len(groupNames))
	for _, name := range groupNames {
		if group, ok := m.groups[name]; ok {
			groups = append(groups, group)
		}
	}
	return groups
}

func (m *Manager) getInactiveResourceGroups() []consumptionRecordKey {
	m.consumptionRecordMu.Lock()
	defer m.consumptionRecordMu.Unlock()
	var keys []consumptionRecordKey
	for k, lastTime := range m.consumptionRecord {
		if k.name == reservedDefaultGroupName {
			continue
		}
		if time.Since(lastTime) > metricsCleanupTimeout {
			delete(m.consumptionRecord, k)
			keys = append(keys, k)
		}
	}
	return keys
}

// Receive the consumption and flush it to the metrics.
func (m *Manager) backgroundMetricsFlush(ctx context.Context, pushMetricsAddr string, pushMetricsInterval time.Duration) {
	defer logutil.LogPanic()
	cleanUpTicker := time.NewTicker(metricsCleanupInterval)
	defer cleanUpTicker.Stop()
	availableRUTicker := time.NewTicker(metricsAvailableRUInterval)
	defer availableRUTicker.Stop()
	recordMaxTicker := time.NewTicker(tickPerSecond)
	defer recordMaxTicker.Stop()
	maxPerSecTrackers := make(map[string]*maxPerSecCostTracker)
	rcuTicker := time.NewTicker(rcuMetricsInterval)
	defer rcuTicker.Stop()
	rcuTrackers := make(map[string]*rcuTracker)

	pushMetricsTickerC := make(<-chan time.Time)
	if pushMetricsAddr != "" && pushMetricsInterval.Seconds() > 0 {
		pushMetricsTicker := time.NewTicker(pushMetricsInterval)
		pushMetricsTickerC = pushMetricsTicker.C
		defer pushMetricsTicker.Stop()
	}
	for {
		select {
		case <-ctx.Done():
			return
		case consumptionInfo := <-m.consumptionDispatcher:
			name := consumptionInfo.resourceGroupName
			ruLabelType := defaultTypeLabel
			if consumptionInfo.isBackground {
				ruLabelType = backgroundTypeLabel
			}
			if consumptionInfo.isTiFlash {
				ruLabelType = tiflashTypeLabel
			}

			m.updateConsumptionRecord(name, ruLabelType)

			consumption := consumptionInfo.Consumption
			if consumption == nil {
				continue
			}

			var (
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

			rt, ok := rcuTrackers[name]
			if !ok {
				rt = newRCUTracker(name)
				rcuTrackers[name] = rt
			}
			rt.CollectConsumption(consumption)

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

			// TODO: maybe we need to distinguish background ru.
			if rg := m.GetMutableResourceGroup(name); rg != nil {
				rg.UpdateRUConsumption(consumptionInfo.Consumption)
			}
		case <-cleanUpTicker.C:
			// Clean up the metrics that have not been updated for a long time.
			for _, r := range m.getInactiveResourceGroups() {
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
				delete(maxPerSecTrackers, r.name)
				delete(rcuTrackers, r.name)
				readRequestUnitMaxPerSecCost.DeleteLabelValues(r.name)
				writeRequestUnitMaxPerSecCost.DeleteLabelValues(r.name)
				resourceGroupConfigGauge.DeletePartialMatch(prometheus.Labels{newResourceGroupNameLabel: r.name})
				requestUnitSumPerSec.DeleteLabelValues(r.name)
				requestUnitConsumeRate.DeleteLabelValues(r.name)
				resourceUnitServiceLimit.DeleteLabelValues(r.name, r.name)
			}

		case <-availableRUTicker.C:
			groups := m.getActiveResourceGroups()
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
			for _, t := range maxPerSecTrackers {
				t.FlushMetrics()
			}

		case <-rcuTicker.C:
			m.RLock()
			names := make([]string, 0, len(rcuTrackers))
			rcuLimit := make([]float64, 0, len(rcuTrackers))
			for name := range rcuTrackers {
				if name == reservedDefaultGroupName {
					continue
				}
				group, ok := m.groups[name]
				if ok {
					names = append(names, name)
					rcuLimit = append(rcuLimit, max(group.getFillRate(), group.getBurstLimit()))
				}
			}
			cpuMsCost := m.controllerConfig.RequestUnit.CPUMsCost
			m.RUnlock()
			for i, name := range names {
				rcuTrackers[name].FlushMetrics(rcuLimit[i], cpuMsCost)
				resourceUnitServiceLimit.WithLabelValues(name, name).Set(rcuLimit[i])
			}

		case <-pushMetricsTickerC:
			podName := os.Getenv("HOSTNAME")
			if podName == "" {
				podName = "default"
			}
			pushCtx, cancel := context.WithTimeout(ctx, pushMetricsTimeout)
			start := time.Now()
			err := push.New(pushMetricsAddr, "resource_group_svc").
				Grouping("pod", podName).
				Collector(readRequestUnitCost).
				Collector(writeRequestUnitCost).
				Collector(sqlLayerRequestUnitCost).
				PushContext(pushCtx)
			cancel()
			if err != nil {
				log.Error("push metrics to Prometheus failed", zap.Error(err))
			}
			pushRUMetricsDuration.Observe(time.Since(start).Seconds())
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

type rcuTracker struct {
	name                    string
	totalRU, totalCPUTimeMs float64
	lastRU, lastCPUTimeMs   float64
	lastFlushTime           time.Time
	rcuMetrics              prometheus.Gauge
	consumeRateMetrics      prometheus.Gauge
}

func newRCUTracker(name string) *rcuTracker {
	return &rcuTracker{
		name:               name,
		rcuMetrics:         requestUnitSumPerSec.WithLabelValues(name),
		consumeRateMetrics: requestUnitConsumeRate.WithLabelValues(name),
	}
}

// CollectConsumption collects the consumption info.
func (t *rcuTracker) CollectConsumption(consume *rmpb.Consumption) {
	t.totalRU += consume.RRU + consume.WRU
	t.totalCPUTimeMs += consume.TotalCpuTimeMs
}

// FlushMetrics calculates the RCU and updates the metrics.
func (t *rcuTracker) FlushMetrics(rcuLimit float64, cpuMsCost float64) {
	if t.lastRU == 0 && t.lastCPUTimeMs == 0 {
		t.lastRU, t.lastCPUTimeMs = t.totalRU, t.totalCPUTimeMs
		t.lastFlushTime = time.Now()
		return
	}

	deltaRU := t.totalRU - t.lastRU + (t.totalCPUTimeMs-t.lastCPUTimeMs)*cpuMsCost
	deltaTime := time.Since(t.lastFlushTime).Seconds()
	if deltaTime <= 0 {
		return
	}
	rcu := deltaRU / deltaTime
	t.lastRU, t.lastCPUTimeMs = t.totalRU, t.totalCPUTimeMs
	t.lastFlushTime = time.Now()

	t.rcuMetrics.Set(rcu)
	if rcuLimit > 0 {
		t.consumeRateMetrics.Set(rcu / rcuLimit)
	}
}
