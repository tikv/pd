// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package statistics

import (
	"fmt"
	"strconv"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/namespace"
)

const (
	unknown   = "unknown"
	labelType = "label"
)

// ScheduleOptions is an interface to access configurations.
// TODO: merge the Options to schedule.Options
type ScheduleOptions interface {
	GetLocationLabels() []string

	GetLowSpaceRatio() float64
	GetHighSpaceRatio() float64
	GetTolerantSizeRatio() float64
	GetStoreBalanceRate() float64

	GetSchedulerMaxWaitingOperator() uint64
	GetLeaderScheduleLimit(name string) uint64
	GetRegionScheduleLimit(name string) uint64
	GetReplicaScheduleLimit(name string) uint64
	GetMergeScheduleLimit(name string) uint64
	GetHotRegionScheduleLimit(name string) uint64
	GetMaxReplicas(name string) int
	GetHotRegionCacheHitsThreshold() int
	GetMaxSnapshotCount() uint64
	GetMaxPendingPeerCount() uint64
	GetMaxMergeRegionSize() uint64
	GetMaxMergeRegionKeys() uint64

	IsRaftLearnerEnabled() bool
	IsMakeUpReplicaEnabled() bool
	IsRemoveExtraReplicaEnabled() bool
	IsRemoveDownReplicaEnabled() bool
	IsReplaceOfflineReplicaEnabled() bool

	GetMaxStoreDownTime() time.Duration
}

type storeStatistics struct {
	opt             ScheduleOptions
	namespace       string
	Up              int
	Disconnect      int
	Unhealth        int
	Down            int
	Offline         int
	Tombstone       int
	LowSpace        int
	StorageSize     uint64
	StorageCapacity uint64
	RegionCount     int
	LeaderCount     int
	LabelCounter    map[string]int
}

func newStoreStatistics(opt ScheduleOptions, namespace string) *storeStatistics {
	return &storeStatistics{
		opt:          opt,
		namespace:    namespace,
		LabelCounter: make(map[string]int),
	}
}

func (s *storeStatistics) Observe(store *core.StoreInfo, stats *StoresStats) {
	for _, k := range s.opt.GetLocationLabels() {
		v := store.GetLabelValue(k)
		if v == "" {
			v = unknown
		}
		key := fmt.Sprintf("%s:%s", k, v)
		s.LabelCounter[key]++
	}
	storeAddress := store.GetAddress()
	id := strconv.FormatUint(store.GetID(), 10)
	// Store state.
	switch store.GetState() {
	case metapb.StoreState_Up:
		if store.DownTime() >= s.opt.GetMaxStoreDownTime() {
			s.Down++
		} else if store.IsUnhealth() {
			s.Unhealth++
		} else if store.IsDisconnected() {
			s.Disconnect++
		} else {
			s.Up++
		}
	case metapb.StoreState_Offline:
		s.Offline++
	case metapb.StoreState_Tombstone:
		s.Tombstone++
		s.resetStoreStatistics(storeAddress, id)
		return
	}
	if store.IsLowSpace(s.opt.GetLowSpaceRatio()) {
		s.LowSpace++
	}

	// Store stats.
	s.StorageSize += store.StorageSize()
	s.StorageCapacity += store.GetCapacity()
	s.RegionCount += store.GetRegionCount()
	s.LeaderCount += store.GetLeaderCount()

	storeStatusGauge.WithLabelValues(s.namespace, storeAddress, id, "region_score").Set(store.RegionScore(s.opt.GetHighSpaceRatio(), s.opt.GetLowSpaceRatio(), 0))
	storeStatusGauge.WithLabelValues(s.namespace, storeAddress, id, "leader_score").Set(store.LeaderScore(0))
	storeStatusGauge.WithLabelValues(s.namespace, storeAddress, id, "region_size").Set(float64(store.GetRegionSize()))
	storeStatusGauge.WithLabelValues(s.namespace, storeAddress, id, "region_count").Set(float64(store.GetRegionCount()))
	storeStatusGauge.WithLabelValues(s.namespace, storeAddress, id, "leader_size").Set(float64(store.GetLeaderSize()))
	storeStatusGauge.WithLabelValues(s.namespace, storeAddress, id, "leader_count").Set(float64(store.GetLeaderCount()))
	storeStatusGauge.WithLabelValues(s.namespace, storeAddress, id, "store_available").Set(float64(store.GetAvailable()))
	storeStatusGauge.WithLabelValues(s.namespace, storeAddress, id, "store_used").Set(float64(store.GetUsedSize()))
	storeStatusGauge.WithLabelValues(s.namespace, storeAddress, id, "store_capacity").Set(float64(store.GetCapacity()))

	// Store flows.
	storeFlowStats := stats.GetRollingStoreStats(store.GetID())
	storeWriteRateBytes, storeReadRateBytes := storeFlowStats.GetBytesRate()
	storeStatusGauge.WithLabelValues(s.namespace, storeAddress, id, "store_write_rate_bytes").Set(float64(storeWriteRateBytes))
	storeStatusGauge.WithLabelValues(s.namespace, storeAddress, id, "store_read_rate_bytes").Set(float64(storeReadRateBytes))
	storeWriteRateKeys, storeReadRateKeys := storeFlowStats.GetKeysWriteRate(), storeFlowStats.GetKeysReadRate()
	storeStatusGauge.WithLabelValues(s.namespace, storeAddress, id, "store_write_rate_keys").Set(float64(storeWriteRateKeys))
	storeStatusGauge.WithLabelValues(s.namespace, storeAddress, id, "store_read_rate_keys").Set(float64(storeReadRateKeys))
}

func (s *storeStatistics) Collect() {
	metrics := make(map[string]float64)
	metrics["store_up_count"] = float64(s.Up)
	metrics["store_disconnected_count"] = float64(s.Disconnect)
	metrics["store_down_count"] = float64(s.Down)
	metrics["store_unhealth_count"] = float64(s.Unhealth)
	metrics["store_offline_count"] = float64(s.Offline)
	metrics["store_tombstone_count"] = float64(s.Tombstone)
	metrics["store_low_space_count"] = float64(s.LowSpace)
	metrics["region_count"] = float64(s.RegionCount)
	metrics["leader_count"] = float64(s.LeaderCount)
	metrics["storage_size"] = float64(s.StorageSize)
	metrics["storage_capacity"] = float64(s.StorageCapacity)

	for typ, value := range metrics {
		clusterStatusGauge.WithLabelValues(typ, s.namespace).Set(value)
	}

	// Current scheduling configurations of the cluster
	configs := make(map[string]float64)
	configs["leader_schedule_limit"] = float64(s.opt.GetLeaderScheduleLimit(s.namespace))
	configs["region_schedule_limit"] = float64(s.opt.GetRegionScheduleLimit(s.namespace))
	configs["merge_schedule_limit"] = float64(s.opt.GetMergeScheduleLimit(s.namespace))
	configs["replica_schedule_limit"] = float64(s.opt.GetReplicaScheduleLimit(s.namespace))
	configs["max_replicas"] = float64(s.opt.GetMaxReplicas(s.namespace))
	configs["high_space_ratio"] = float64(s.opt.GetHighSpaceRatio())
	configs["low_space_ratio"] = float64(s.opt.GetLowSpaceRatio())
	configs["tolerant_size_ratio"] = float64(s.opt.GetTolerantSizeRatio())
	configs["store-balance-rate"] = float64(s.opt.GetStoreBalanceRate())
	configs["hot-region-schedule-limit"] = float64(s.opt.GetHotRegionScheduleLimit(s.namespace))
	configs["hot-region-cache-hits-threshold"] = float64(s.opt.GetHotRegionCacheHitsThreshold())
	configs["max-pending-peer-count"] = float64(s.opt.GetMaxPendingPeerCount())
	configs["max-snapshot-count"] = float64(s.opt.GetMaxSnapshotCount())
	configs["max-merge-region-size"] = float64(s.opt.GetMaxMergeRegionSize())
	configs["max-merge-region-keys"] = float64(s.opt.GetMaxMergeRegionKeys())

	var disableMakeUpReplica, disableLearner, disableRemoveDownReplica, disableRemoveExtraReplica, disableReplaceOfflineReplica float64
	if !s.opt.IsMakeUpReplicaEnabled() {
		disableMakeUpReplica = 1
	}
	if !s.opt.IsRaftLearnerEnabled() {
		disableLearner = 1
	}
	if !s.opt.IsRemoveDownReplicaEnabled() {
		disableRemoveDownReplica = 1
	}
	if !s.opt.IsRemoveExtraReplicaEnabled() {
		disableRemoveExtraReplica = 1
	}
	if !s.opt.IsReplaceOfflineReplicaEnabled() {
		disableReplaceOfflineReplica = 1
	}

	configs["disable_makeup_replica"] = disableMakeUpReplica
	configs["disable_learner"] = disableLearner
	configs["disable_remove_down_replica"] = disableRemoveDownReplica
	configs["disable_remove_extra_replica"] = disableRemoveExtraReplica
	configs["disable_replace_offline_replica"] = disableReplaceOfflineReplica

	for typ, value := range configs {
		configStatusGauge.WithLabelValues(typ, s.namespace).Set(value)
	}

	for name, value := range s.LabelCounter {
		placementStatusGauge.WithLabelValues(labelType, name, s.namespace).Set(float64(value))
	}
}

func (s *storeStatistics) resetStoreStatistics(storeAddress string, id string) {
	storeStatusGauge.WithLabelValues(s.namespace, storeAddress, id, "region_score").Set(0)
	storeStatusGauge.WithLabelValues(s.namespace, storeAddress, id, "leader_score").Set(0)
	storeStatusGauge.WithLabelValues(s.namespace, storeAddress, id, "region_size").Set(0)
	storeStatusGauge.WithLabelValues(s.namespace, storeAddress, id, "region_count").Set(0)
	storeStatusGauge.WithLabelValues(s.namespace, storeAddress, id, "leader_size").Set(0)
	storeStatusGauge.WithLabelValues(s.namespace, storeAddress, id, "leader_count").Set(0)
	storeStatusGauge.WithLabelValues(s.namespace, storeAddress, id, "store_available").Set(0)
	storeStatusGauge.WithLabelValues(s.namespace, storeAddress, id, "store_used").Set(0)
	storeStatusGauge.WithLabelValues(s.namespace, storeAddress, id, "store_capacity").Set(0)
}

type storeStatisticsMap struct {
	opt        ScheduleOptions
	classifier namespace.Classifier
	stats      map[string]*storeStatistics
}

// NewStoreStatisticsMap creates a new storeStatisticsMap.
func NewStoreStatisticsMap(opt ScheduleOptions, classifier namespace.Classifier) *storeStatisticsMap {
	return &storeStatisticsMap{
		opt:        opt,
		classifier: classifier,
		stats:      make(map[string]*storeStatistics),
	}
}

func (m *storeStatisticsMap) Observe(store *core.StoreInfo, stats *StoresStats) {
	namespace := m.classifier.GetStoreNamespace(store)
	stat, ok := m.stats[namespace]
	if !ok {
		stat = newStoreStatistics(m.opt, namespace)
		m.stats[namespace] = stat
	}
	stat.Observe(store, stats)
}

func (m *storeStatisticsMap) Collect() {
	for _, s := range m.stats {
		s.Collect()
	}
}
