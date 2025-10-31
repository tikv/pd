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
	"github.com/prometheus/client_golang/prometheus"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/statistics/utils"
)

const (
	unknown   = "unknown"
	labelType = "label"

	ClusterStatusStoreUpCount           = "store_up_count"
	ClusterStatusStoreDisconnectedCount = "store_disconnected_count"
	ClusterStatusStoreSlowCount         = "store_slow_count"
	ClusterStatusStoreDownCount         = "store_down_count"
	ClusterStatusStoreUnhealthCount     = "store_unhealth_count"
	ClusterStatusStoreOfflineCount      = "store_offline_count"
	ClusterStatusStoreTombstoneCount    = "store_tombstone_count"
	ClusterStatusStoreLowSpaceCount     = "store_low_space_count"
	ClusterStatusStorePreparingCount    = "store_preparing_count"
	ClusterStatusStoreServingCount      = "store_serving_count"
	ClusterStatusStoreRemovingCount     = "store_removing_count"
	ClusterStatusStoreRemovedCount      = "store_removed_count"

	ClusterStatusRegionCount     = "region_count"
	ClusterStatusLeaderCount     = "leader_count"
	ClusterStatusWitnessCount    = "witness_count"
	ClusterStatusLearnerCount    = "learner_count"
	ClusterStatusStorageSize     = "storage_size"
	ClusterStatusStorageCapacity = "storage_capacity"
)

type storeStatistics struct {
	opt          config.ConfProvider
	LabelCounter map[string][]uint64
}

func newStoreStatistics(opt config.ConfProvider) *storeStatistics {
	return &storeStatistics{
		opt:          opt,
		LabelCounter: make(map[string][]uint64),
	}
}

func (s *storeStatistics) observeStoreStatus(store *core.StoreInfo) map[string]float64 {
	result := map[string]float64{
		ClusterStatusStoreUpCount:           0,
		ClusterStatusStoreDisconnectedCount: 0,
		ClusterStatusStoreSlowCount:         0,
		ClusterStatusStoreDownCount:         0,
		ClusterStatusStoreUnhealthCount:     0,
		ClusterStatusStoreOfflineCount:      0,
		ClusterStatusStoreTombstoneCount:    0,
		ClusterStatusStoreLowSpaceCount:     0,
		ClusterStatusStorePreparingCount:    0,
		ClusterStatusStoreServingCount:      0,
		ClusterStatusStoreRemovingCount:     0,
		ClusterStatusStoreRemovedCount:      0,
	}

	// Store state.
	isDown := false
	switch store.GetNodeState() {
	case metapb.NodeState_Preparing, metapb.NodeState_Serving:
		if store.DownTime() >= s.opt.GetMaxStoreDownTime() {
			isDown = true
			result[ClusterStatusStoreDownCount]++
		} else if store.IsUnhealthy() {
			result[ClusterStatusStoreUnhealthCount]++
		} else if store.IsDisconnected() {
			result[ClusterStatusStoreDisconnectedCount]++
		} else if store.IsSlow() {
			result[ClusterStatusStoreSlowCount]++
		} else {
			result[ClusterStatusStoreUpCount]++
		}
		if store.IsPreparing() {
			result[ClusterStatusStorePreparingCount]++
		} else {
			result[ClusterStatusStoreServingCount]++
		}
	case metapb.NodeState_Removing:
		result[ClusterStatusStoreOfflineCount]++
		result[ClusterStatusStoreRemovingCount]++
	case metapb.NodeState_Removed:
		result[ClusterStatusStoreTombstoneCount]++
		result[ClusterStatusStoreRemovedCount]++
		return result
	}
	if !isDown && store.IsLowSpace(s.opt.GetLowSpaceRatio()) {
		result[ClusterStatusStoreLowSpaceCount]++
	}
	return result
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
	var engine string
	if store.IsTiFlash() {
		engine = core.EngineTiFlash
	} else {
		engine = core.EngineTiKV
	}
	storeStatusStats := s.observeStoreStatus(store)
	for statusType, value := range storeStatusStats {
		clusterStatusGauge.WithLabelValues(statusType, engine, id).Set(value)
	}
	// skip tombstone store avoid to overwrite metrics
	if store.GetNodeState() == metapb.NodeState_Removed {
		return
	}

	// Store stats.
	clusterStatusGauge.WithLabelValues(ClusterStatusStorageSize, engine, id).Set(float64(store.StorageSize()))
	clusterStatusGauge.WithLabelValues(ClusterStatusStorageCapacity, engine, id).Set(float64(store.GetCapacity()))
	clusterStatusGauge.WithLabelValues(ClusterStatusRegionCount, engine, id).Set(float64(store.GetRegionCount()))
	clusterStatusGauge.WithLabelValues(ClusterStatusLeaderCount, engine, id).Set(float64(store.GetLeaderCount()))
	clusterStatusGauge.WithLabelValues(ClusterStatusWitnessCount, engine, id).Set(float64(store.GetWitnessCount()))
	clusterStatusGauge.WithLabelValues(ClusterStatusLearnerCount, engine, id).Set(float64(store.GetLearnerCount()))
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
	clusterStatusGauge.DeletePartialMatch(prometheus.Labels{"store": id})
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
	placementStatusGauge.Reset()
	clusterStatusGauge.Reset()
	ResetRegionStatsMetrics()
	ResetLabelStatsMetrics()
	ResetHotCacheStatusMetrics()
}
