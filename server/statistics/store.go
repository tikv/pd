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
	totalLoads         []float64
}

// NewStoresStats creates a new hot spot cache.
func NewStoresStats() *StoresStats {
	return &StoresStats{
		rollingStoresStats: make(map[uint64]*RollingStoreStats),
		totalLoads:         make([]float64, StoreStatCount),
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
	store := s.GetOrCreateRollingStoreStats(storeID)
	store.Observe(stats)
}

// Set sets the store statistics (for test).
func (s *StoresStats) Set(storeID uint64, stats *pdpb.StoreStats) {
	store := s.GetOrCreateRollingStoreStats(storeID)
	store.Set(stats)
}

// UpdateTotalLoad updates the total loads of all stores.
func (s *StoresStats) UpdateTotalLoad(stores []*core.StoreInfo) {
	s.Lock()
	defer s.Unlock()
	for i := range s.totalLoads {
		s.totalLoads[i] = 0
	}
	for _, store := range stores {
		if !store.IsUp() {
			continue
		}
		stats, ok := s.rollingStoresStats[store.GetID()]
		if !ok {
			continue
		}
		for i := range s.totalLoads {
			s.totalLoads[i] += stats.GetLoad(StoreStatKind(i))
		}
	}
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

func (s *StoresStats) storeIsUnhealthy(cluster core.StoreSetInformer, storeID uint64) bool {
	store := cluster.GetStore(storeID)
	return store == nil || store.IsTombstone() || store.IsUnhealthy() || store.IsPhysicallyDestroyed()
}

// FilterUnhealthyStore filter unhealthy store
func (s *StoresStats) FilterUnhealthyStore(cluster core.StoreSetInformer) {
	s.Lock()
	defer s.Unlock()
	for storeID := range s.rollingStoresStats {
		if s.storeIsUnhealthy(cluster, storeID) {
			delete(s.rollingStoresStats, storeID)
		}
	}
}

// UpdateStoreHeartbeatMetrics is used to update store heartbeat interval metrics
func (s *StoresStats) UpdateStoreHeartbeatMetrics(store *core.StoreInfo) {
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
	// DefaultReadMfSize is default size of read median filter
	DefaultReadMfSize = 3
)

// NewRollingStoreStats creates a RollingStoreStats.
func newRollingStoreStats() *RollingStoreStats {
	timeMedians := make(map[StoreStatKind]*movingaverage.TimeMedian)
	interval := StoreHeartBeatReportInterval * time.Second
	timeMedians[StoreReadBytes] = movingaverage.NewTimeMedian(DefaultAotSize, DefaultReadMfSize, interval)
	timeMedians[StoreReadKeys] = movingaverage.NewTimeMedian(DefaultAotSize, DefaultReadMfSize, interval)
	timeMedians[StoreWriteBytes] = movingaverage.NewTimeMedian(DefaultAotSize, DefaultWriteMfSize, interval)
	timeMedians[StoreWriteKeys] = movingaverage.NewTimeMedian(DefaultAotSize, DefaultWriteMfSize, interval)

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
	interval := statInterval.GetEndTimestamp() - statInterval.GetStartTimestamp()
	log.Debug("update store stats", zap.Uint64("key-write", stats.KeysWritten), zap.Uint64("bytes-write", stats.BytesWritten), zap.Duration("interval", time.Duration(interval)*time.Second), zap.Uint64("store-id", stats.GetStoreId()))
	r.Lock()
	defer r.Unlock()
	r.timeMedians[StoreWriteBytes].Add(float64(stats.BytesWritten), time.Duration(interval)*time.Second)
	r.timeMedians[StoreWriteKeys].Add(float64(stats.KeysWritten), time.Duration(interval)*time.Second)
	r.timeMedians[StoreReadBytes].Add(float64(stats.BytesRead), time.Duration(interval)*time.Second)
	r.timeMedians[StoreReadKeys].Add(float64(stats.KeysRead), time.Duration(interval)*time.Second)

	// Updates the cpu usages and disk rw rates of store.
	r.movingAvgs[StoreCPUUsage].Add(collect(stats.GetCpuUsages()))
	r.movingAvgs[StoreDiskReadRate].Add(collect(stats.GetReadIoRates()))
	r.movingAvgs[StoreDiskWriteRate].Add(collect(stats.GetWriteIoRates()))
}

// Set sets the statistics (for test).
func (r *RollingStoreStats) Set(stats *pdpb.StoreStats) {
	statInterval := stats.GetInterval()
	interval := statInterval.GetEndTimestamp() - statInterval.GetStartTimestamp()
	if interval == 0 {
		return
	}
	r.Lock()
	defer r.Unlock()
	r.timeMedians[StoreWriteBytes].Set(float64(stats.BytesWritten) / float64(interval))
	r.timeMedians[StoreReadBytes].Set(float64(stats.BytesRead) / float64(interval))
	r.timeMedians[StoreWriteKeys].Set(float64(stats.KeysWritten) / float64(interval))
	r.timeMedians[StoreReadKeys].Set(float64(stats.KeysRead) / float64(interval))
	r.movingAvgs[StoreCPUUsage].Set(collect(stats.GetCpuUsages()))
	r.movingAvgs[StoreDiskReadRate].Set(collect(stats.GetReadIoRates()))
	r.movingAvgs[StoreDiskWriteRate].Set(collect(stats.GetWriteIoRates()))
}

// GetLoad returns store's load.
func (r *RollingStoreStats) GetLoad(k StoreStatKind) float64 {
	r.RLock()
	defer r.RUnlock()
	switch k {
	case StoreReadBytes:
		return r.timeMedians[StoreReadBytes].Get()
	case StoreReadKeys:
		return r.timeMedians[StoreReadKeys].Get()
	case StoreWriteBytes:
		return r.timeMedians[StoreWriteBytes].Get()
	case StoreWriteKeys:
		return r.timeMedians[StoreWriteKeys].Get()
	case StoreCPUUsage:
		return r.movingAvgs[StoreCPUUsage].Get()
	case StoreDiskReadRate:
		return r.movingAvgs[StoreDiskReadRate].Get()
	case StoreDiskWriteRate:
		return r.movingAvgs[StoreDiskWriteRate].Get()
	}
	return 0
}

// GetInstantLoad returns store's instant load.
// MovingAvgs do not support GetInstantaneous() so they return average values.
func (r *RollingStoreStats) GetInstantLoad(k StoreStatKind) float64 {
	r.RLock()
	defer r.RUnlock()
	switch k {
	case StoreReadBytes:
		return r.timeMedians[StoreReadBytes].GetInstantaneous()
	case StoreReadKeys:
		return r.timeMedians[StoreReadKeys].GetInstantaneous()
	case StoreWriteBytes:
		return r.timeMedians[StoreWriteBytes].GetInstantaneous()
	case StoreWriteKeys:
		return r.timeMedians[StoreWriteKeys].GetInstantaneous()
	case StoreCPUUsage:
		return r.movingAvgs[StoreCPUUsage].Get()
	case StoreDiskReadRate:
		return r.movingAvgs[StoreDiskReadRate].Get()
	case StoreDiskWriteRate:
		return r.movingAvgs[StoreDiskWriteRate].Get()
	}
	return 0
}
