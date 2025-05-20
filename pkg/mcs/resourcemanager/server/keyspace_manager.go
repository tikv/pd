// Copyright 2025 TiKV Project Authors.
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
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
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
	maxPriority              = 16
	unlimitedRate            = math.MaxInt32
	unlimitedBurstLimit      = -1
)

// centralManager is used to provide some basic functions as a resource group manager.
type centralManager interface {
	GetControllerConfig() *ControllerConfig
	GetStorage() endpoint.ResourceGroupStorage
}

// consumptionItem is used to send the consumption info to the background metrics flusher.
type consumptionItem struct {
	resourceGroupName string
	*rmpb.Consumption
	isBackground bool
	isTiFlash    bool
}

type keyspaceResourceGroupManager struct {
	syncutil.RWMutex
	groups map[string]*ResourceGroup

	keyspaceID uint32
	centralMgr centralManager

	// consumptionChan is used to send the consumption
	// info to the background metrics flusher.
	consumptionDispatcher chan *consumptionItem
	// record update time of each resource group
	consumptionRecord map[consumptionRecordKey]time.Time
}

func newKeyspaceResourceGroupManager(keyspaceID uint32, centralMgr centralManager) *keyspaceResourceGroupManager {
	return &keyspaceResourceGroupManager{
		groups:                make(map[string]*ResourceGroup),
		keyspaceID:            keyspaceID,
		centralMgr:            centralMgr,
		consumptionDispatcher: make(chan *consumptionItem, defaultConsumptionChanSize),
		consumptionRecord:     make(map[consumptionRecordKey]time.Time),
	}
}

func (krgm *keyspaceResourceGroupManager) getControllerConfig() *ControllerConfig {
	return krgm.centralMgr.GetControllerConfig()
}

func (krgm *keyspaceResourceGroupManager) getStorage() endpoint.ResourceGroupStorage {
	return krgm.centralMgr.GetStorage()
}

func (krgm *keyspaceResourceGroupManager) addResourceGroupFromRaw(name string, rawValue string) error {
	group := &rmpb.ResourceGroup{}
	if err := proto.Unmarshal([]byte(rawValue), group); err != nil {
		log.Error("failed to parse the keyspace resource group meta info",
			zap.Uint32("keyspace-id", krgm.keyspaceID), zap.String("name", name), zap.String("raw-value", rawValue), zap.Error(err))
		return err
	}
	krgm.Lock()
	krgm.groups[group.Name] = FromProtoResourceGroup(group)
	krgm.Unlock()
	return nil
}

func (krgm *keyspaceResourceGroupManager) setRawStatesIntoResourceGroup(name string, rawValue string) error {
	tokens := &GroupStates{}
	if err := json.Unmarshal([]byte(rawValue), tokens); err != nil {
		log.Error("failed to parse the keyspace resource group state",
			zap.Uint32("keyspace-id", krgm.keyspaceID), zap.String("name", name), zap.String("raw-value", rawValue), zap.Error(err))
		return err
	}
	krgm.Lock()
	if group, ok := krgm.groups[name]; ok {
		group.SetStatesIntoResourceGroup(tokens)
	}
	krgm.Unlock()
	return nil
}

func (krgm *keyspaceResourceGroupManager) initDefaultResourceGroup() {
	krgm.RLock()
	if _, ok := krgm.groups[reservedDefaultGroupName]; ok {
		krgm.RUnlock()
		return
	}
	krgm.RUnlock()
	defaultGroup := &ResourceGroup{
		Name: reservedDefaultGroupName,
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &RequestUnitSettings{
			RU: &GroupTokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate:   unlimitedRate,
					BurstLimit: unlimitedBurstLimit,
				},
			},
		},
		Priority: middlePriority,
	}
	if err := krgm.addResourceGroup(defaultGroup.IntoProtoResourceGroup()); err != nil {
		log.Warn("init default group failed", zap.Uint32("keyspace-id", krgm.keyspaceID), zap.Error(err))
	}
}

func (krgm *keyspaceResourceGroupManager) addResourceGroup(grouppb *rmpb.ResourceGroup) error {
	// Check the name.
	if len(grouppb.Name) == 0 || len(grouppb.Name) > 32 {
		return errs.ErrInvalidGroup
	}
	// Check the Priority.
	if grouppb.GetPriority() > maxPriority {
		return errs.ErrInvalidGroup
	}
	group := FromProtoResourceGroup(grouppb)
	krgm.Lock()
	defer krgm.Unlock()
	if err := group.persistSettings(krgm.keyspaceID, krgm.getStorage()); err != nil {
		return err
	}
	if err := group.persistStates(krgm.keyspaceID, krgm.getStorage()); err != nil {
		return err
	}
	krgm.groups[group.Name] = group
	return nil
}

func (krgm *keyspaceResourceGroupManager) modifyResourceGroup(group *rmpb.ResourceGroup) error {
	if group == nil || group.Name == "" {
		return errs.ErrInvalidGroup
	}
	krgm.Lock()
	curGroup, ok := krgm.groups[group.Name]
	krgm.Unlock()
	if !ok {
		return errs.ErrResourceGroupNotExists.FastGenByArgs(group.Name)
	}

	err := curGroup.PatchSettings(group)
	if err != nil {
		return err
	}
	return curGroup.persistSettings(krgm.keyspaceID, krgm.getStorage())
}

func (krgm *keyspaceResourceGroupManager) deleteResourceGroup(name string) error {
	if name == reservedDefaultGroupName {
		return errs.ErrDeleteReservedGroup
	}
	if err := krgm.getStorage().DeleteResourceGroupSetting(krgm.keyspaceID, name); err != nil {
		return err
	}
	krgm.Lock()
	delete(krgm.groups, name)
	krgm.Unlock()
	return nil
}

func (krgm *keyspaceResourceGroupManager) getResourceGroup(name string, withStats bool) *ResourceGroup {
	krgm.RLock()
	defer krgm.RUnlock()
	if group, ok := krgm.groups[name]; ok {
		return group.Clone(withStats)
	}
	return nil
}

func (krgm *keyspaceResourceGroupManager) getMutableResourceGroup(name string) *ResourceGroup {
	krgm.Lock()
	defer krgm.Unlock()
	return krgm.groups[name]
}

func (krgm *keyspaceResourceGroupManager) getResourceGroupList(withStats, includeDefault bool) []*ResourceGroup {
	krgm.RLock()
	res := make([]*ResourceGroup, 0, len(krgm.groups))
	for _, group := range krgm.groups {
		if !includeDefault && group.Name == reservedDefaultGroupName {
			continue
		}
		res = append(res, group.Clone(withStats))
	}
	krgm.RUnlock()
	sort.Slice(res, func(i, j int) bool {
		return res[i].Name < res[j].Name
	})
	return res
}

func (krgm *keyspaceResourceGroupManager) getResourceGroupNames(includeDefault bool) []string {
	krgm.RLock()
	defer krgm.RUnlock()
	res := make([]string, 0, len(krgm.groups))
	for name := range krgm.groups {
		if !includeDefault && name == reservedDefaultGroupName {
			continue
		}
		res = append(res, name)
	}
	return res
}

func (krgm *keyspaceResourceGroupManager) persistResourceGroupRunningState() {
	krgm.RLock()
	keys := make([]string, 0, len(krgm.groups))
	for k := range krgm.groups {
		keys = append(keys, k)
	}
	krgm.RUnlock()
	for idx := range keys {
		krgm.RLock()
		group, ok := krgm.groups[keys[idx]]
		if ok {
			if err := group.persistStates(krgm.keyspaceID, krgm.getStorage()); err != nil {
				log.Error("persist keyspace resource group state failed",
					zap.Uint32("keyspace-id", krgm.keyspaceID),
					zap.String("group-name", group.Name),
					zap.Int("index", idx),
					zap.Error(err))
			}
		}
		krgm.RUnlock()
	}
}

// TODO: add keyspace name lable to the metrics.
func (krgm *keyspaceResourceGroupManager) backgroundMetricsFlush(ctx context.Context) {
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
		case consumptionInfo := <-krgm.consumptionDispatcher:
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
					sqlLayerRuMetrics.Add(consumption.SqlLayerCpuTimeMs * krgm.getControllerConfig().RequestUnit.CPUMsCost)
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

			krgm.consumptionRecord[consumptionRecordKey{name: name, ruType: ruLabelType}] = time.Now()

			// TODO: maybe we need to distinguish background ru.
			if rg := krgm.getMutableResourceGroup(name); rg != nil {
				rg.UpdateRUConsumption(consumptionInfo.Consumption)
			}
		case <-cleanUpTicker.C:
			// Clean up the metrics that have not been updated for a long time.
			for r, lastTime := range krgm.consumptionRecord {
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
					delete(krgm.consumptionRecord, r)
					delete(maxPerSecTrackers, r.name)
					readRequestUnitMaxPerSecCost.DeleteLabelValues(r.name)
					writeRequestUnitMaxPerSecCost.DeleteLabelValues(r.name)
					resourceGroupConfigGauge.DeletePartialMatch(prometheus.Labels{newResourceGroupNameLabel: r.name})
				}
			}
		case <-availableRUTicker.C:
			// prevent many groups and hold the lock long time.
			for _, group := range krgm.getResourceGroupList(true, false) {
				ru := math.Max(group.getRUToken(), 0)
				availableRUCounter.WithLabelValues(group.Name, group.Name).Set(ru)
				resourceGroupConfigGauge.WithLabelValues(group.Name, priorityLabel).Set(group.getPriority())
				resourceGroupConfigGauge.WithLabelValues(group.Name, ruPerSecLabel).Set(group.getFillRate())
				resourceGroupConfigGauge.WithLabelValues(group.Name, ruCapacityLabel).Set(group.getBurstLimit())
			}
		case <-recordMaxTicker.C:
			// Record the sum of RRU and WRU every second.
			names := krgm.getResourceGroupNames(true)
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
