// Copyright 2018 TiKV Project Authors.
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

package statistics

import (
	"fmt"
	"strconv"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/statistics/utils"
)

const (
	unknown   = "unknown"
	labelType = "label"
)

var (
	// tikv status counters.
	tikvUpCounter          = clusterStatusGauge.WithLabelValues("store_up_count", "tikv")
	tikvDiconnectedCounter = clusterStatusGauge.WithLabelValues("store_disconnected_count", "tikv")
	tikvDownCounter        = clusterStatusGauge.WithLabelValues("store_down_count", "tikv")
	tikvUnhealthCounter    = clusterStatusGauge.WithLabelValues("store_unhealth_count", "tikv")
	tikvOfflineCounter     = clusterStatusGauge.WithLabelValues("store_offline_count", "tikv")
	tikvTombstoneCounter   = clusterStatusGauge.WithLabelValues("store_tombstone_count", "tikv")
	tikvLowSpaceCounter    = clusterStatusGauge.WithLabelValues("store_low_space_count", "tikv")
	tikvPreparingCounter   = clusterStatusGauge.WithLabelValues("store_preparing_count", "tikv")
	tikvServingCounter     = clusterStatusGauge.WithLabelValues("store_serving_count", "tikv")
	tikvRemovingCounter    = clusterStatusGauge.WithLabelValues("store_removing_count", "tikv")
	tikvRemovedCounter     = clusterStatusGauge.WithLabelValues("store_removed_count", "tikv")

	// tiflash status counters.
	tiflashUpCounter          = clusterStatusGauge.WithLabelValues("store_up_count", "tiflash")
	tiflashDiconnectedCounter = clusterStatusGauge.WithLabelValues("store_disconnected_count", "tiflash")
	tiflashDownCounter        = clusterStatusGauge.WithLabelValues("store_down_count", "tiflash")
	tiflashUnhealthCounter    = clusterStatusGauge.WithLabelValues("store_unhealth_count", "tiflash")
	tiflashOfflineCounter     = clusterStatusGauge.WithLabelValues("store_offline_count", "tiflash")
	tiflashTombstoneCounter   = clusterStatusGauge.WithLabelValues("store_tombstone_count", "tiflash")
	tiflashLowSpaceCounter    = clusterStatusGauge.WithLabelValues("store_low_space_count", "tiflash")
	tiflashPreparingCounter   = clusterStatusGauge.WithLabelValues("store_preparing_count", "tiflash")
	tiflashServingCounter     = clusterStatusGauge.WithLabelValues("store_serving_count", "tiflash")
	tiflashRemovingCounter    = clusterStatusGauge.WithLabelValues("store_removing_count", "tiflash")
	tiflashRemovedCounter     = clusterStatusGauge.WithLabelValues("store_removed_count", "tiflash")

	// Store status metrics.
	storeRegionCountGauge     = clusterStatusGauge.WithLabelValues("region_count", "all")
	storeLeaderCountGauge     = clusterStatusGauge.WithLabelValues("leader_count", "all")
	storeWitnessCountGauge    = clusterStatusGauge.WithLabelValues("witness_count", "all")
	storeLearnerCountGauge    = clusterStatusGauge.WithLabelValues("learner_count", "all")
	storeStorageSizeGauge     = clusterStatusGauge.WithLabelValues("storage_size", "all")
	storeStorageCapacityGauge = clusterStatusGauge.WithLabelValues("storage_capacity", "all")
)

type storeStatistics struct {
	opt             config.ConfProvider
	StorageSize     uint64
	StorageCapacity uint64
	RegionCount     int
	LeaderCount     int
	LearnerCount    int
	WitnessCount    int
	LabelCounter    map[string][]uint64

	engineStatistics map[string]*storeStatusStatistics
}

type storeStatusStatistics struct {
	opt        config.ConfProvider
	Up         int
	Disconnect int
	Unhealthy  int
	Down       int
	Offline    int
	Tombstone  int
	LowSpace   int
	Slow       int
	Preparing  int
	Serving    int
	Removing   int
	Removed    int
}

func (s *storeStatusStatistics) observe(store *core.StoreInfo) {
	// Store state.
	isDown := false
	switch store.GetNodeState() {
	case metapb.NodeState_Preparing, metapb.NodeState_Serving:
		if store.DownTime() >= s.opt.GetMaxStoreDownTime() {
			isDown = true
			s.Down++
		} else if store.IsUnhealthy() {
			s.Unhealthy++
		} else if store.IsDisconnected() {
			s.Disconnect++
		} else if store.IsSlow() {
			s.Slow++
		} else {
			s.Up++
		}
		if store.IsPreparing() {
			s.Preparing++
		} else {
			s.Serving++
		}
	case metapb.NodeState_Removing:
		s.Offline++
		s.Removing++
	case metapb.NodeState_Removed:
		s.Tombstone++
		s.Removed++
		return
	}
	if !isDown && store.IsLowSpace(s.opt.GetLowSpaceRatio()) {
		s.LowSpace++
	}
}

func newStoreStatistics(opt config.ConfProvider) *storeStatistics {
	statistics := make(map[string]*storeStatusStatistics, 1)
	statistics[core.EngineTiKV] = &storeStatusStatistics{opt: opt}
	return &storeStatistics{
		opt:              opt,
		LabelCounter:     make(map[string][]uint64),
		engineStatistics: statistics,
	}
}

func (s *storeStatistics) observe(store *core.StoreInfo) {
	for _, k := range s.opt.GetLocationLabels() {
		v := store.GetLabelValue(k)
		if v == "" {
			v = unknown
		}
		key := fmt.Sprintf("%s:%s", k, v)
		// exclude tombstone
		if !store.IsRemoved() {
			s.LabelCounter[key] = append(s.LabelCounter[key], store.GetID())
		}
	}
	storeAddress := store.GetAddress()
	id := strconv.FormatUint(store.GetID(), 10)
	// Store state.
	var statistics *storeStatusStatistics
	if store.IsTiFlash() {
		statistics = s.engineStatistics[core.EngineTiFlash]
		if statistics == nil {
			s.engineStatistics[core.EngineTiFlash] = &storeStatusStatistics{opt: s.opt}
			statistics = s.engineStatistics[core.EngineTiFlash]
		}
	} else {
		// tikv statistics has been initialized in newStoreStatistics.
		statistics = s.engineStatistics[core.EngineTiKV]
	}
	statistics.observe(store)
	// skip tombstone store avoid to overwrite metrics
	if store.GetNodeState() == metapb.NodeState_Removed {
		return
	}

	// Store stats.
	s.StorageSize += store.StorageSize()
	s.StorageCapacity += store.GetCapacity()
	s.RegionCount += store.GetRegionCount()
	s.LeaderCount += store.GetLeaderCount()
	s.WitnessCount += store.GetWitnessCount()
	s.LearnerCount += store.GetLearnerCount()
	limit, ok := store.GetStoreLimit().(*storelimit.SlidingWindows)
	if ok {
		cap := limit.GetCap()
		storeStatusGauge.WithLabelValues(storeAddress, id, "windows_size").Set(float64(cap))
		for i, use := range limit.GetUsed() {
			priority := constant.PriorityLevel(i).String()
			storeStatusGauge.WithLabelValues(storeAddress, id, "windows_used_level_"+priority).Set(float64(use))
		}
	}

	// TODO: pre-allocate gauge metrics
	storeStatusGauge.WithLabelValues(storeAddress, id, "region_score").Set(store.RegionScore(s.opt.GetRegionScoreFormulaVersion(), s.opt.GetHighSpaceRatio(), s.opt.GetLowSpaceRatio(), 0))
	storeStatusGauge.WithLabelValues(storeAddress, id, "leader_score").Set(store.LeaderScore(s.opt.GetLeaderSchedulePolicy(), 0))
	storeStatusGauge.WithLabelValues(storeAddress, id, "region_size").Set(float64(store.GetRegionSize()))
	storeStatusGauge.WithLabelValues(storeAddress, id, "region_count").Set(float64(store.GetRegionCount()))
	storeStatusGauge.WithLabelValues(storeAddress, id, "leader_size").Set(float64(store.GetLeaderSize()))
	storeStatusGauge.WithLabelValues(storeAddress, id, "leader_count").Set(float64(store.GetLeaderCount()))
	storeStatusGauge.WithLabelValues(storeAddress, id, "witness_count").Set(float64(store.GetWitnessCount()))
	storeStatusGauge.WithLabelValues(storeAddress, id, "learner_count").Set(float64(store.GetLearnerCount()))
	storeStatusGauge.WithLabelValues(storeAddress, id, "store_available").Set(float64(store.GetAvailable()))
	storeStatusGauge.WithLabelValues(storeAddress, id, "store_used").Set(float64(store.GetUsedSize()))
	storeStatusGauge.WithLabelValues(storeAddress, id, "store_capacity").Set(float64(store.GetCapacity()))
	slowTrend := store.GetSlowTrend()
	if slowTrend != nil {
		storeStatusGauge.WithLabelValues(storeAddress, id, "store_slow_trend_cause_value").Set(slowTrend.CauseValue)
		storeStatusGauge.WithLabelValues(storeAddress, id, "store_slow_trend_cause_rate").Set(slowTrend.CauseRate)
		storeStatusGauge.WithLabelValues(storeAddress, id, "store_slow_trend_result_value").Set(slowTrend.ResultValue)
		storeStatusGauge.WithLabelValues(storeAddress, id, "store_slow_trend_result_rate").Set(slowTrend.ResultRate)
	}
}

// ObserveHotStat records the hot region metrics for the store.
func ObserveHotStat(store *core.StoreInfo, stats *StoresStats) {
	// Store flows.
	storeAddress := store.GetAddress()
	id := strconv.FormatUint(store.GetID(), 10)
	storeFlowStats := stats.GetRollingStoreStats(store.GetID())
	if storeFlowStats == nil {
		return
	}

	storeStatusGauge.WithLabelValues(storeAddress, id, "store_write_rate_bytes").Set(storeFlowStats.GetLoad(utils.StoreWriteBytes))
	storeStatusGauge.WithLabelValues(storeAddress, id, "store_read_rate_bytes").Set(storeFlowStats.GetLoad(utils.StoreReadBytes))
	storeStatusGauge.WithLabelValues(storeAddress, id, "store_write_rate_keys").Set(storeFlowStats.GetLoad(utils.StoreWriteKeys))
	storeStatusGauge.WithLabelValues(storeAddress, id, "store_read_rate_keys").Set(storeFlowStats.GetLoad(utils.StoreReadKeys))
	storeStatusGauge.WithLabelValues(storeAddress, id, "store_write_query_rate").Set(storeFlowStats.GetLoad(utils.StoreWriteQuery))
	storeStatusGauge.WithLabelValues(storeAddress, id, "store_read_query_rate").Set(storeFlowStats.GetLoad(utils.StoreReadQuery))
	storeStatusGauge.WithLabelValues(storeAddress, id, "store_cpu_usage").Set(storeFlowStats.GetLoad(utils.StoreCPUUsage))
	storeStatusGauge.WithLabelValues(storeAddress, id, "store_disk_read_rate").Set(storeFlowStats.GetLoad(utils.StoreDiskReadRate))
	storeStatusGauge.WithLabelValues(storeAddress, id, "store_disk_write_rate").Set(storeFlowStats.GetLoad(utils.StoreDiskWriteRate))
	storeStatusGauge.WithLabelValues(storeAddress, id, "store_regions_write_rate_bytes").Set(storeFlowStats.GetLoad(utils.StoreRegionsWriteBytes))
	storeStatusGauge.WithLabelValues(storeAddress, id, "store_regions_write_rate_keys").Set(storeFlowStats.GetLoad(utils.StoreRegionsWriteKeys))

	storeStatusGauge.WithLabelValues(storeAddress, id, "store_write_rate_bytes_instant").Set(storeFlowStats.GetInstantLoad(utils.StoreWriteBytes))
	storeStatusGauge.WithLabelValues(storeAddress, id, "store_read_rate_bytes_instant").Set(storeFlowStats.GetInstantLoad(utils.StoreReadBytes))
	storeStatusGauge.WithLabelValues(storeAddress, id, "store_write_rate_keys_instant").Set(storeFlowStats.GetInstantLoad(utils.StoreWriteKeys))
	storeStatusGauge.WithLabelValues(storeAddress, id, "store_read_rate_keys_instant").Set(storeFlowStats.GetInstantLoad(utils.StoreReadKeys))
	storeStatusGauge.WithLabelValues(storeAddress, id, "store_write_query_rate_instant").Set(storeFlowStats.GetInstantLoad(utils.StoreWriteQuery))
	storeStatusGauge.WithLabelValues(storeAddress, id, "store_read_query_rate_instant").Set(storeFlowStats.GetInstantLoad(utils.StoreReadQuery))
	storeStatusGauge.WithLabelValues(storeAddress, id, "store_regions_write_rate_bytes_instant").Set(storeFlowStats.GetInstantLoad(utils.StoreRegionsWriteBytes))
	storeStatusGauge.WithLabelValues(storeAddress, id, "store_regions_write_rate_keys_instant").Set(storeFlowStats.GetInstantLoad(utils.StoreRegionsWriteKeys))
}

func (s *storeStatistics) collect() {
	placementStatusGauge.Reset()

	// tikv store status metrics.
	tikvStatistics, ok := s.engineStatistics[core.EngineTiKV]
	if ok {
		tikvUpCounter.Set(float64(tikvStatistics.Up))
		tikvDiconnectedCounter.Set(float64(tikvStatistics.Disconnect))
		tikvDownCounter.Set(float64(tikvStatistics.Down))
		tikvUnhealthCounter.Set(float64(tikvStatistics.Unhealthy))
		tikvOfflineCounter.Set(float64(tikvStatistics.Offline))
		tikvTombstoneCounter.Set(float64(tikvStatistics.Tombstone))
		tikvLowSpaceCounter.Set(float64(tikvStatistics.LowSpace))
		tikvPreparingCounter.Set(float64(tikvStatistics.Preparing))
		tikvServingCounter.Set(float64(tikvStatistics.Serving))
		tikvRemovingCounter.Set(float64(tikvStatistics.Removing))
		tikvRemovedCounter.Set(float64(tikvStatistics.Removed))
	}

	// tiflash store status metrics.
	tiflashStatistics, ok := s.engineStatistics[core.EngineTiFlash]
	if ok {
		tiflashUpCounter.Set(float64(tiflashStatistics.Up))
		tiflashDiconnectedCounter.Set(float64(tiflashStatistics.Disconnect))
		tiflashDownCounter.Set(float64(tiflashStatistics.Down))
		tiflashUnhealthCounter.Set(float64(tiflashStatistics.Unhealthy))
		tiflashOfflineCounter.Set(float64(tiflashStatistics.Offline))
		tiflashTombstoneCounter.Set(float64(tiflashStatistics.Tombstone))
		tiflashLowSpaceCounter.Set(float64(tiflashStatistics.LowSpace))
		tiflashPreparingCounter.Set(float64(tiflashStatistics.Preparing))
		tiflashServingCounter.Set(float64(tiflashStatistics.Serving))
		tiflashRemovingCounter.Set(float64(tiflashStatistics.Removing))
		tiflashRemovedCounter.Set(float64(tiflashStatistics.Removed))
	}

	// Store status metrics.
	storeRegionCountGauge.Set(float64(s.RegionCount))
	storeLeaderCountGauge.Set(float64(s.LeaderCount))
	storeWitnessCountGauge.Set(float64(s.WitnessCount))
	storeLearnerCountGauge.Set(float64(s.LearnerCount))
	storeStorageSizeGauge.Set(float64(s.StorageSize))
	storeStorageCapacityGauge.Set(float64(s.StorageCapacity))

	// Current scheduling configurations of the cluster
	configs := make(map[string]float64)
	configs["leader-schedule-limit"] = float64(s.opt.GetLeaderScheduleLimit())
	configs["region-schedule-limit"] = float64(s.opt.GetRegionScheduleLimit())
	configs["merge-schedule-limit"] = float64(s.opt.GetMergeScheduleLimit())
	configs["replica-schedule-limit"] = float64(s.opt.GetReplicaScheduleLimit())
	configs["max-replicas"] = float64(s.opt.GetMaxReplicas())
	configs["high-space-ratio"] = s.opt.GetHighSpaceRatio()
	configs["low-space-ratio"] = s.opt.GetLowSpaceRatio()
	configs["tolerant-size-ratio"] = s.opt.GetTolerantSizeRatio()
	configs["hot-region-schedule-limit"] = float64(s.opt.GetHotRegionScheduleLimit())
	configs["hot-region-cache-hits-threshold"] = float64(s.opt.GetHotRegionCacheHitsThreshold())
	configs["max-pending-peer-count"] = float64(s.opt.GetMaxPendingPeerCount())
	configs["max-snapshot-count"] = float64(s.opt.GetMaxSnapshotCount())
	configs["max-merge-region-size"] = float64(s.opt.GetMaxMergeRegionSize())
	configs["max-merge-region-keys"] = float64(s.opt.GetMaxMergeRegionKeys())
	configs["region-max-size"] = float64(s.opt.GetRegionMaxSize())
	configs["region-split-size"] = float64(s.opt.GetRegionSplitSize())
	configs["region-split-keys"] = float64(s.opt.GetRegionSplitKeys())
	configs["region-max-keys"] = float64(s.opt.GetRegionMaxKeys())

	var enableMakeUpReplica, enableRemoveDownReplica, enableRemoveExtraReplica, enableReplaceOfflineReplica float64
	if s.opt.IsMakeUpReplicaEnabled() {
		enableMakeUpReplica = 1
	}
	if s.opt.IsRemoveDownReplicaEnabled() {
		enableRemoveDownReplica = 1
	}
	if s.opt.IsRemoveExtraReplicaEnabled() {
		enableRemoveExtraReplica = 1
	}
	if s.opt.IsReplaceOfflineReplicaEnabled() {
		enableReplaceOfflineReplica = 1
	}

	configs["enable-makeup-replica"] = enableMakeUpReplica
	configs["enable-remove-down-replica"] = enableRemoveDownReplica
	configs["enable-remove-extra-replica"] = enableRemoveExtraReplica
	configs["enable-replace-offline-replica"] = enableReplaceOfflineReplica

	for typ, value := range configs {
		configStatusGauge.WithLabelValues(typ).Set(value)
	}

	for name, stores := range s.LabelCounter {
		for _, storeID := range stores {
			placementStatusGauge.WithLabelValues(labelType, name, strconv.FormatUint(storeID, 10)).Set(1)
		}
	}

	for storeID, limit := range s.opt.GetStoresLimit() {
		id := strconv.FormatUint(storeID, 10)
		StoreLimitGauge.WithLabelValues(id, "add-peer").Set(limit.AddPeer)
		StoreLimitGauge.WithLabelValues(id, "remove-peer").Set(limit.RemovePeer)
	}
}

// ResetStoreStatistics resets the metrics of store.
func ResetStoreStatistics(storeAddress string, id string) {
	metrics := []string{
		"region_score",
		"leader_score",
		"region_size",
		"region_count",
		"leader_size",
		"leader_count",
		"witness_count",
		"learner_count",
		"store_available",
		"store_used",
		"store_capacity",
		"store_write_rate_bytes",
		"store_read_rate_bytes",
		"store_write_rate_keys",
		"store_read_rate_keys",
		"store_write_query_rate",
		"store_read_query_rate",
		"store_regions_write_rate_bytes",
		"store_regions_write_rate_keys",
		"store_slow_trend_cause_value",
		"store_slow_trend_cause_rate",
		"store_slow_trend_result_value",
		"store_slow_trend_result_rate",
	}
	for _, m := range metrics {
		storeStatusGauge.DeleteLabelValues(storeAddress, id, m)
	}
}

type storeStatisticsMap struct {
	opt   config.ConfProvider
	stats *storeStatistics
}

// NewStoreStatisticsMap creates a new storeStatisticsMap.
func NewStoreStatisticsMap(opt config.ConfProvider) *storeStatisticsMap {
	return &storeStatisticsMap{
		opt:   opt,
		stats: newStoreStatistics(opt),
	}
}

// Observe observes the store.
func (m *storeStatisticsMap) Observe(store *core.StoreInfo) {
	m.stats.observe(store)
}

// Collect collects the metrics.
func (m *storeStatisticsMap) Collect() {
	m.stats.collect()
}

// Reset resets the metrics.
func Reset() {
	storeStatusGauge.Reset()
	clusterStatusGauge.Reset()
	placementStatusGauge.Reset()
	ResetRegionStatsMetrics()
	ResetLabelStatsMetrics()
	ResetHotCacheStatusMetrics()
}
