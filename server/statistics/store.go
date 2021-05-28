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
// See the License for the specific language governing permissions and
// limitations under the License.

package statistics

import (
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/movingaverage"
	"github.com/tikv/pd/server/core"
	"go.uber.org/zap"
)

// StoresStats is a cache hold hot regions.
type StoresStats struct {
	sync.RWMutex
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

// Set sets the store statistics (for test).
func (s *StoresStats) Set(storeID uint64, stats *pdpb.StoreStats) {
	rollingStoreStat := s.GetOrCreateRollingStoreStats(storeID)
	rollingStoreStat.Set(stats)
}

// GetStoresLoads returns all stores loads.
func (s *StoresStats) GetStoresLoads() map[uint64][]float64 {
	s.RLock()
	defer s.RUnlock()
	res := make(map[uint64][]float64, len(s.rollingStoresStats))
	for storeID, stats := range s.rollingStoresStats {
		for i := StoreStatKind(0); i < StoreStatCount; i++ {
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
		if store.IsTombstone() || store.IsUnhealthy() || store.IsPhysicallyDestroyed() {
			delete(s.rollingStoresStats, storeID)
		}
	}
}

// UpdateStoreHeartbeatMetrics is used to update store heartbeat interval metrics
func UpdateStoreHeartbeatMetrics(store *core.StoreInfo) {
	storeHeartbeatIntervalHist.Observe(time.Since(store.GetLastHeartbeatTS()).Seconds())
}

// RollingStoreStats are multiple sets of recent historical records with specified windows size.
type RollingStoreStats struct {
	sync.RWMutex
	timeMedians map[StoreStatKind]*movingaverage.TimeMedian
	movingAvgs  map[StoreStatKind]movingaverage.MovingAvg
}

const (
	storeStatsRollingWindows = 3
	// DefaultAotSize is default size of average over time.
	DefaultAotSize = 2
	// DefaultWriteMfSize is default size of write median filter
	DefaultWriteMfSize = 5
	// defaultReadMfSize is default size of read median filter
	defaultReadMfSize = 3
)

// NewRollingStoreStats creates a RollingStoreStats.
func newRollingStoreStats() *RollingStoreStats {
	timeMedians := make(map[StoreStatKind]*movingaverage.TimeMedian)
	interval := StoreHeartBeatReportInterval * time.Second
	timeMedians[StoreReadBytes] = movingaverage.NewTimeMedian(DefaultAotSize, defaultReadMfSize, interval)
	timeMedians[StoreReadKeys] = movingaverage.NewTimeMedian(DefaultAotSize, defaultReadMfSize, interval)
	timeMedians[StoreReadQuery] = movingaverage.NewTimeMedian(DefaultAotSize, defaultReadMfSize, interval)
	timeMedians[StoreWriteBytes] = movingaverage.NewTimeMedian(DefaultAotSize, DefaultWriteMfSize, interval)
	timeMedians[StoreWriteKeys] = movingaverage.NewTimeMedian(DefaultAotSize, DefaultWriteMfSize, interval)
	timeMedians[StoreWriteQuery] = movingaverage.NewTimeMedian(DefaultAotSize, DefaultWriteMfSize, interval)

	movingAvgs := make(map[StoreStatKind]movingaverage.MovingAvg)
	movingAvgs[StoreCPUUsage] = movingaverage.NewMedianFilter(storeStatsRollingWindows)
	movingAvgs[StoreDiskReadRate] = movingaverage.NewMedianFilter(storeStatsRollingWindows)
	movingAvgs[StoreDiskWriteRate] = movingaverage.NewMedianFilter(storeStatsRollingWindows)

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
	log.Debug("update store stats", zap.Uint64("key-write", stats.KeysWritten), zap.Uint64("bytes-write", stats.BytesWritten), zap.Duration("interval", interval), zap.Uint64("store-id", stats.GetStoreId()))
	r.Lock()
	defer r.Unlock()
	r.timeMedians[StoreWriteBytes].Add(float64(stats.BytesWritten), interval)
	r.timeMedians[StoreWriteKeys].Add(float64(stats.KeysWritten), interval)
	r.timeMedians[StoreWriteQuery].Add(float64(getWriteQueryNum(stats.QueryStats)), interval)
	r.timeMedians[StoreReadBytes].Add(float64(stats.BytesRead), interval)
	r.timeMedians[StoreReadKeys].Add(float64(stats.KeysRead), interval)
	r.timeMedians[StoreReadQuery].Add(float64(getReadQueryNum(stats.QueryStats)), interval)

	// Updates the cpu usages and disk rw rates of store.
	r.movingAvgs[StoreCPUUsage].Add(collect(stats.GetCpuUsages()))
	r.movingAvgs[StoreDiskReadRate].Add(collect(stats.GetReadIoRates()))
	r.movingAvgs[StoreDiskWriteRate].Add(collect(stats.GetWriteIoRates()))
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
	r.timeMedians[StoreWriteBytes].Set(float64(stats.BytesWritten) / interval)
	r.timeMedians[StoreReadBytes].Set(float64(stats.BytesRead) / interval)
	r.timeMedians[StoreWriteKeys].Set(float64(stats.KeysWritten) / interval)
	r.timeMedians[StoreReadKeys].Set(float64(stats.KeysRead) / interval)
	r.timeMedians[StoreReadQuery].Set(float64(getReadQueryNum(stats.QueryStats)) / interval)
	r.timeMedians[StoreWriteQuery].Set(float64(getWriteQueryNum(stats.QueryStats)) / interval)
	r.movingAvgs[StoreCPUUsage].Set(collect(stats.GetCpuUsages()))
	r.movingAvgs[StoreDiskReadRate].Set(collect(stats.GetReadIoRates()))
	r.movingAvgs[StoreDiskWriteRate].Set(collect(stats.GetWriteIoRates()))
}

// GetLoad returns store's load.
func (r *RollingStoreStats) GetLoad(k StoreStatKind) float64 {
	r.RLock()
	defer r.RUnlock()
	switch k {
	case StoreReadBytes, StoreReadKeys, StoreReadQuery, StoreWriteBytes, StoreWriteKeys, StoreWriteQuery:
		return r.timeMedians[k].Get()
	case StoreCPUUsage, StoreDiskReadRate, StoreDiskWriteRate:
		return r.movingAvgs[k].Get()
	}
	return 0
}

// GetInstantLoad returns store's instant load.
// MovingAvgs do not support GetInstantaneous() so they return average values.
func (r *RollingStoreStats) GetInstantLoad(k StoreStatKind) float64 {
	r.RLock()
	defer r.RUnlock()
	switch k {
	case StoreReadBytes, StoreReadKeys, StoreReadQuery, StoreWriteBytes, StoreWriteKeys, StoreWriteQuery:
		return r.timeMedians[k].GetInstantaneous()
	case StoreCPUUsage, StoreDiskReadRate, StoreDiskWriteRate:
		return r.movingAvgs[k].Get()
	}
	return 0
}
