// Copyright 2019 TiKV Project Authors.
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
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/movingaverage"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

const (
	storeStatsRollingWindowsSize = 3

	// RegionsStatsObserveInterval is the interval for obtaining statistics from RegionTree
	RegionsStatsObserveInterval = 30 * time.Second
	// RegionsStatsRollingWindowsSize is default size of median filter for data from regionStats
	RegionsStatsRollingWindowsSize = 9
)

// StoresStats is a cache hold hot regions.
type StoresStats struct {
	syncutil.RWMutex
	rollingStoresStats map[uint64]*RollingStoreStats
}

// NewStoresStats creates a new hot spot cache.
func NewStoresStats() *StoresStats {
	return &StoresStats{
		rollingStoresStats: make(map[uint64]*RollingStoreStats),
	}
}

// RemoveRollingStoreStats removes RollingStoreStats with a given store ID.
func (s *StoresStats) RemoveRollingStoreStats(storeID uint64) {
	s.Lock()
	defer s.Unlock()
	delete(s.rollingStoresStats, storeID)
}

// GetRollingStoreStats gets RollingStoreStats with a given store ID.
func (s *StoresStats) GetRollingStoreStats(storeID uint64) *RollingStoreStats {
	s.RLock()
	defer s.RUnlock()
	return s.rollingStoresStats[storeID]
}

// GetOrCreateRollingStoreStats gets or creates RollingStoreStats with a given store ID.
func (s *StoresStats) GetOrCreateRollingStoreStats(storeID uint64) *RollingStoreStats {
	s.Lock()
	defer s.Unlock()
	ret, ok := s.rollingStoresStats[storeID]
	if !ok {
		ret = newRollingStoreStats()
		s.rollingStoresStats[storeID] = ret
	}
	return ret
}

// Observe records the current store status with a given store.
func (s *StoresStats) Observe(storeID uint64, stats *pdpb.StoreStats) {
	rollingStoreStat := s.GetOrCreateRollingStoreStats(storeID)
	rollingStoreStat.Observe(stats)
}

// ObserveRegionsStats records the current stores status from region stats.
func (s *StoresStats) ObserveRegionsStats(storeIDs []uint64, writeBytesRates, writeKeysRates []float64) {
	for i, storeID := range storeIDs {
		rollingStoreStat := s.GetOrCreateRollingStoreStats(storeID)
		rollingStoreStat.ObserveRegionsStats(writeBytesRates[i], writeKeysRates[i])
	}
}

// Set sets the store statistics (for test).
func (s *StoresStats) Set(storeID uint64, stats *pdpb.StoreStats) {
	rollingStoreStat := s.GetOrCreateRollingStoreStats(storeID)
	rollingStoreStat.Set(stats)
}

// SetRegionsStats sets the store statistics from region stats (for test).
func (s *StoresStats) SetRegionsStats(storeIDs []uint64, writeBytesRates, writeKeysRates []float64) {
	for i, storeID := range storeIDs {
		rollingStoreStat := s.GetOrCreateRollingStoreStats(storeID)
		rollingStoreStat.SetRegionsStats(writeBytesRates[i], writeKeysRates[i])
	}
}

// GetStoresLoads returns all stores loads.
func (s *StoresStats) GetStoresLoads() map[uint64][]float64 {
	s.RLock()
	defer s.RUnlock()
	res := make(map[uint64][]float64, len(s.rollingStoresStats))
	for storeID, stats := range s.rollingStoresStats {
		for i := utils.StoreStatKind(0); i < utils.StoreStatCount; i++ {
			res[storeID] = append(res[storeID], stats.GetLoad(i))
		}
	}
	return res
}

// FilterUnhealthyStore filter unhealthy store
func (s *StoresStats) FilterUnhealthyStore(cluster core.StoreSetInformer) {
	s.Lock()
	defer s.Unlock()
	for storeID := range s.rollingStoresStats {
		store := cluster.GetStore(storeID)
		if store == nil || store.IsRemoved() || store.IsUnhealthy() || store.IsPhysicallyDestroyed() {
			delete(s.rollingStoresStats, storeID)
		}
	}
}

// RollingStoreStats are multiple sets of recent historical records with specified windows size.
type RollingStoreStats struct {
	syncutil.RWMutex
	timeMedians []*movingaverage.TimeMedian
	movingAvgs  []movingaverage.MovingAvg
}

// NewRollingStoreStats creates a RollingStoreStats.
func newRollingStoreStats() *RollingStoreStats {
	timeMedians := make([]*movingaverage.TimeMedian, utils.StoreStatCount)
	movingAvgs := make([]movingaverage.MovingAvg, utils.StoreStatCount)

	// from StoreHeartbeat
	interval := utils.StoreHeartBeatReportInterval * time.Second
	timeMedians[utils.StoreReadBytes] = movingaverage.NewTimeMedian(utils.DefaultAotSize, utils.DefaultReadMfSize, interval)
	timeMedians[utils.StoreReadKeys] = movingaverage.NewTimeMedian(utils.DefaultAotSize, utils.DefaultReadMfSize, interval)
	timeMedians[utils.StoreReadQuery] = movingaverage.NewTimeMedian(utils.DefaultAotSize, utils.DefaultReadMfSize, interval)
	timeMedians[utils.StoreWriteBytes] = movingaverage.NewTimeMedian(utils.DefaultAotSize, utils.DefaultWriteMfSize, interval)
	timeMedians[utils.StoreWriteKeys] = movingaverage.NewTimeMedian(utils.DefaultAotSize, utils.DefaultWriteMfSize, interval)
	timeMedians[utils.StoreWriteQuery] = movingaverage.NewTimeMedian(utils.DefaultAotSize, utils.DefaultWriteMfSize, interval)
	movingAvgs[utils.StoreCPUUsage] = movingaverage.NewMedianFilter(storeStatsRollingWindowsSize)
	movingAvgs[utils.StoreDiskReadRate] = movingaverage.NewMedianFilter(storeStatsRollingWindowsSize)
	movingAvgs[utils.StoreDiskWriteRate] = movingaverage.NewMedianFilter(storeStatsRollingWindowsSize)

	// from RegionHeartbeat
	// The data from regionStats is used in TiFlash, so higher tolerance is required
	movingAvgs[utils.StoreRegionsWriteBytes] = movingaverage.NewMedianFilter(RegionsStatsRollingWindowsSize)
	movingAvgs[utils.StoreRegionsWriteKeys] = movingaverage.NewMedianFilter(RegionsStatsRollingWindowsSize)

	return &RollingStoreStats{
		timeMedians: timeMedians,
		movingAvgs:  movingAvgs,
	}
}

func collect(records []*pdpb.RecordPair) float64 {
	var total uint64
	for _, record := range records {
		total += record.GetValue()
	}
	return float64(total)
}

// Observe records current statistics.
func (r *RollingStoreStats) Observe(stats *pdpb.StoreStats) {
	statInterval := stats.GetInterval()
	interval := time.Duration(statInterval.GetEndTimestamp()-statInterval.GetStartTimestamp()) * time.Second
	log.Debug("update store stats",
		zap.Uint64("key-write", stats.KeysWritten),
		zap.Uint64("bytes-write", stats.BytesWritten),
		zap.Uint64("key-read", stats.KeysRead),
		zap.Uint64("bytes-read", stats.BytesRead),
		zap.Uint64("query-write", core.GetWriteQueryNum(stats.QueryStats)),
		zap.Uint64("query-read", core.GetReadQueryNum(stats.QueryStats)),
		zap.Duration("interval", interval),
		zap.Uint64("store-id", stats.GetStoreId()))
	r.Lock()
	defer r.Unlock()
	readQueryNum, writeQueryNum := core.GetReadQueryNum(stats.QueryStats), core.GetWriteQueryNum(stats.QueryStats)
	r.timeMedians[utils.StoreWriteBytes].Add(float64(stats.BytesWritten), interval)
	r.timeMedians[utils.StoreWriteKeys].Add(float64(stats.KeysWritten), interval)
	r.timeMedians[utils.StoreWriteQuery].Add(float64(writeQueryNum), interval)
	r.timeMedians[utils.StoreReadBytes].Add(float64(stats.BytesRead), interval)
	r.timeMedians[utils.StoreReadKeys].Add(float64(stats.KeysRead), interval)
	r.timeMedians[utils.StoreReadQuery].Add(float64(readQueryNum), interval)

	// Updates the cpu usages and disk rw rates of store.
	r.movingAvgs[utils.StoreCPUUsage].Add(collect(stats.GetCpuUsages()))
	r.movingAvgs[utils.StoreDiskReadRate].Add(collect(stats.GetReadIoRates()))
	r.movingAvgs[utils.StoreDiskWriteRate].Add(collect(stats.GetWriteIoRates()))
}

// ObserveRegionsStats records current statistics from region stats.
func (r *RollingStoreStats) ObserveRegionsStats(writeBytesRate, writeKeysRate float64) {
	r.Lock()
	defer r.Unlock()
	r.movingAvgs[utils.StoreRegionsWriteBytes].Add(writeBytesRate)
	r.movingAvgs[utils.StoreRegionsWriteKeys].Add(writeKeysRate)
}

// Set sets the statistics (for test).
func (r *RollingStoreStats) Set(stats *pdpb.StoreStats) {
	statInterval := stats.GetInterval()
	interval := float64(statInterval.GetEndTimestamp() - statInterval.GetStartTimestamp())
	if interval == 0 {
		return
	}
	r.Lock()
	defer r.Unlock()
	readQueryNum, writeQueryNum := core.GetReadQueryNum(stats.QueryStats), core.GetWriteQueryNum(stats.QueryStats)
	r.timeMedians[utils.StoreWriteBytes].Set(float64(stats.BytesWritten) / interval)
	r.timeMedians[utils.StoreReadBytes].Set(float64(stats.BytesRead) / interval)
	r.timeMedians[utils.StoreWriteKeys].Set(float64(stats.KeysWritten) / interval)
	r.timeMedians[utils.StoreReadKeys].Set(float64(stats.KeysRead) / interval)
	r.timeMedians[utils.StoreReadQuery].Set(float64(readQueryNum) / interval)
	r.timeMedians[utils.StoreWriteQuery].Set(float64(writeQueryNum) / interval)
	r.movingAvgs[utils.StoreCPUUsage].Set(collect(stats.GetCpuUsages()))
	r.movingAvgs[utils.StoreDiskReadRate].Set(collect(stats.GetReadIoRates()))
	r.movingAvgs[utils.StoreDiskWriteRate].Set(collect(stats.GetWriteIoRates()))
}

// SetRegionsStats sets the statistics from region stats (for test).
func (r *RollingStoreStats) SetRegionsStats(writeBytesRate, writeKeysRate float64) {
	r.Lock()
	defer r.Unlock()
	r.movingAvgs[utils.StoreRegionsWriteBytes].Set(writeBytesRate)
	r.movingAvgs[utils.StoreRegionsWriteKeys].Set(writeKeysRate)
}

// GetLoad returns store's load.
func (r *RollingStoreStats) GetLoad(k utils.StoreStatKind) float64 {
	r.RLock()
	defer r.RUnlock()
	switch k {
	case utils.StoreReadBytes, utils.StoreReadKeys, utils.StoreReadQuery, utils.StoreWriteBytes, utils.StoreWriteKeys, utils.StoreWriteQuery:
		return r.timeMedians[k].Get()
	case utils.StoreCPUUsage, utils.StoreDiskReadRate, utils.StoreDiskWriteRate, utils.StoreRegionsWriteBytes, utils.StoreRegionsWriteKeys:
		return r.movingAvgs[k].Get()
	}
	return 0
}

// GetInstantLoad returns store's instant load.
func (r *RollingStoreStats) GetInstantLoad(k utils.StoreStatKind) float64 {
	r.RLock()
	defer r.RUnlock()
	switch k {
	case utils.StoreReadBytes, utils.StoreReadKeys, utils.StoreReadQuery, utils.StoreWriteBytes, utils.StoreWriteKeys, utils.StoreWriteQuery:
		return r.timeMedians[k].GetInstantaneous()
	case utils.StoreCPUUsage, utils.StoreDiskReadRate, utils.StoreDiskWriteRate, utils.StoreRegionsWriteBytes, utils.StoreRegionsWriteKeys:
		return r.movingAvgs[k].GetInstantaneous()
	}
	return 0
}
