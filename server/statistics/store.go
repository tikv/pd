// Copyright 2019 PingCAP, Inc.
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
	"github.com/pingcap/pd/v4/server/core"
)

const (
	// StoreHeartBeatReportInterval is the heartbeat report interval of a store.
	StoreHeartBeatReportInterval = 10
)

// StoresStats is a cache hold hot regions.
type StoresStats struct {
	sync.RWMutex
	rollingStoresStats map[uint64]*RollingStoreStats
	bytesReadRate      float64
	bytesWriteRate     float64
}

// NewStoresStats creates a new hot spot cache.
func NewStoresStats() *StoresStats {
	return &StoresStats{
		rollingStoresStats: make(map[uint64]*RollingStoreStats),
	}
}

// CreateRollingStoreStats creates RollingStoreStats with a given store ID.
func (s *StoresStats) CreateRollingStoreStats(storeID uint64) {
	s.Lock()
	defer s.Unlock()
	s.rollingStoresStats[storeID] = newRollingStoreStats()
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

// UpdateTotalBytesRate updates the total bytes write rate and read rate.
func (s *StoresStats) UpdateTotalBytesRate(f func() []*core.StoreInfo) {
	var totalBytesWriteRate float64
	var totalBytesReadRate float64
	var writeRate, readRate float64
	ss := f()
	s.RLock()
	defer s.RUnlock()
	for _, store := range ss {
		if store.IsUp() {
			stats, ok := s.rollingStoresStats[store.GetID()]
			if !ok {
				continue
			}
			writeRate, readRate = stats.GetBytesRate()
			totalBytesWriteRate += writeRate
			totalBytesReadRate += readRate
		}
	}
	s.bytesWriteRate = totalBytesWriteRate
	s.bytesReadRate = totalBytesReadRate
}

// TotalBytesWriteRate returns the total written bytes rate of all StoreInfo.
func (s *StoresStats) TotalBytesWriteRate() float64 {
	return s.bytesWriteRate
}

// TotalBytesReadRate returns the total read bytes rate of all StoreInfo.
func (s *StoresStats) TotalBytesReadRate() float64 {
	return s.bytesReadRate
}

// GetStoreBytesRate returns the bytes write stat of the specified store.
func (s *StoresStats) GetStoreBytesRate(storeID uint64) (writeRate float64, readRate float64) {
	s.RLock()
	defer s.RUnlock()
	if storeStat, ok := s.rollingStoresStats[storeID]; ok {
		return storeStat.GetBytesRate()
	}
	return 0, 0
}

// GetStoreCPUUsage returns the total cpu usages of threads of the specified store.
func (s *StoresStats) GetStoreCPUUsage(storeID uint64) float64 {
	s.RLock()
	defer s.RUnlock()
	if storeStat, ok := s.rollingStoresStats[storeID]; ok {
		return storeStat.GetCPUUsage()
	}
	return 0
}

// GetStoreDiskReadRate returns the total read disk io rate of threads of the specified store.
func (s *StoresStats) GetStoreDiskReadRate(storeID uint64) float64 {
	s.RLock()
	defer s.RUnlock()
	if storeStat, ok := s.rollingStoresStats[storeID]; ok {
		return storeStat.GetDiskReadRate()
	}
	return 0
}

// GetStoreDiskWriteRate returns the total write disk io rate of threads of the specified store.
func (s *StoresStats) GetStoreDiskWriteRate(storeID uint64) float64 {
	s.RLock()
	defer s.RUnlock()
	if storeStat, ok := s.rollingStoresStats[storeID]; ok {
		return storeStat.GetDiskWriteRate()
	}
	return 0
}

// GetStoresCPUUsage returns the cpu usage stat of all StoreInfo.
func (s *StoresStats) GetStoresCPUUsage() map[uint64]float64 {
	s.RLock()
	defer s.RUnlock()
	res := make(map[uint64]float64, len(s.rollingStoresStats))
	for storeID, stats := range s.rollingStoresStats {
		res[storeID] = stats.GetCPUUsage()
	}
	return res
}

// GetStoresDiskReadRate returns the disk read rate stat of all StoreInfo.
func (s *StoresStats) GetStoresDiskReadRate() map[uint64]float64 {
	s.RLock()
	defer s.RUnlock()
	res := make(map[uint64]float64, len(s.rollingStoresStats))
	for storeID, stats := range s.rollingStoresStats {
		res[storeID] = stats.GetDiskReadRate()
	}
	return res
}

// GetStoresDiskWriteRate returns the disk write rate stat of all StoreInfo.
func (s *StoresStats) GetStoresDiskWriteRate() map[uint64]float64 {
	s.RLock()
	defer s.RUnlock()
	res := make(map[uint64]float64, len(s.rollingStoresStats))
	for storeID, stats := range s.rollingStoresStats {
		res[storeID] = stats.GetDiskWriteRate()
	}
	return res
}

// GetStoreBytesWriteRate returns the bytes write stat of the specified store.
func (s *StoresStats) GetStoreBytesWriteRate(storeID uint64) float64 {
	s.RLock()
	defer s.RUnlock()
	if storeStat, ok := s.rollingStoresStats[storeID]; ok {
		return storeStat.GetBytesWriteRate()
	}
	return 0
}

// GetStoreBytesReadRate returns the bytes read stat of the specified store.
func (s *StoresStats) GetStoreBytesReadRate(storeID uint64) float64 {
	s.RLock()
	defer s.RUnlock()
	if storeStat, ok := s.rollingStoresStats[storeID]; ok {
		return storeStat.GetBytesReadRate()
	}
	return 0
}

// GetStoresBytesWriteStat returns the bytes write stat of all StoreInfo.
func (s *StoresStats) GetStoresBytesWriteStat() map[uint64]float64 {
	s.RLock()
	defer s.RUnlock()
	res := make(map[uint64]float64, len(s.rollingStoresStats))
	for storeID, stats := range s.rollingStoresStats {
		writeRate, _ := stats.GetBytesRate()
		res[storeID] = writeRate
	}
	return res
}

// GetStoresBytesReadStat returns the bytes read stat of all StoreInfo.
func (s *StoresStats) GetStoresBytesReadStat() map[uint64]float64 {
	s.RLock()
	defer s.RUnlock()
	res := make(map[uint64]float64, len(s.rollingStoresStats))
	for storeID, stats := range s.rollingStoresStats {
		_, readRate := stats.GetBytesRate()
		res[storeID] = readRate
	}
	return res
}

// GetStoresKeysWriteStat returns the keys write stat of all StoreInfo.
func (s *StoresStats) GetStoresKeysWriteStat() map[uint64]float64 {
	s.RLock()
	defer s.RUnlock()
	res := make(map[uint64]float64, len(s.rollingStoresStats))
	for storeID, stats := range s.rollingStoresStats {
		res[storeID] = stats.GetKeysWriteRate()
	}
	return res
}

// GetStoresKeysReadStat returns the bytes read stat of all StoreInfo.
func (s *StoresStats) GetStoresKeysReadStat() map[uint64]float64 {
	s.RLock()
	defer s.RUnlock()
	res := make(map[uint64]float64, len(s.rollingStoresStats))
	for storeID, stats := range s.rollingStoresStats {
		res[storeID] = stats.GetKeysReadRate()
	}
	return res
}

// RollingStoreStats are multiple sets of recent historical records with specified windows size.
type RollingStoreStats struct {
	sync.RWMutex
	bytesWriteRate          *AvgOverTime
	bytesReadRate           *AvgOverTime
	keysWriteRate           *AvgOverTime
	keysReadRate            *AvgOverTime
	totalCPUUsage           MovingAvg
	totalBytesDiskReadRate  MovingAvg
	totalBytesDiskWriteRate MovingAvg
}

const storeStatsRollingWindows = 3
const storeAvgInterval time.Duration = 3 * StoreHeartBeatReportInterval * time.Second

// NewRollingStoreStats creates a RollingStoreStats.
func newRollingStoreStats() *RollingStoreStats {
	return &RollingStoreStats{
		bytesWriteRate:          NewAvgOverTime(storeAvgInterval),
		bytesReadRate:           NewAvgOverTime(storeAvgInterval),
		keysWriteRate:           NewAvgOverTime(storeAvgInterval),
		keysReadRate:            NewAvgOverTime(storeAvgInterval),
		totalCPUUsage:           NewMedianFilter(storeStatsRollingWindows),
		totalBytesDiskReadRate:  NewMedianFilter(storeStatsRollingWindows),
		totalBytesDiskWriteRate: NewMedianFilter(storeStatsRollingWindows),
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
	r.Lock()
	defer r.Unlock()
	r.bytesWriteRate.Add(float64(stats.BytesWritten), time.Duration(interval)*time.Second)
	r.bytesReadRate.Add(float64(stats.BytesRead), time.Duration(interval)*time.Second)
	r.keysWriteRate.Add(float64(stats.KeysWritten), time.Duration(interval)*time.Second)
	r.keysReadRate.Add(float64(stats.KeysRead), time.Duration(interval)*time.Second)

	// Updates the cpu usages and disk rw rates of store.
	r.totalCPUUsage.Add(collect(stats.GetCpuUsages()))
	r.totalBytesDiskReadRate.Add(collect(stats.GetReadIoRates()))
	r.totalBytesDiskWriteRate.Add(collect(stats.GetWriteIoRates()))
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
	r.bytesWriteRate.Set(float64(stats.BytesWritten) / float64(interval))
	r.bytesReadRate.Set(float64(stats.BytesRead) / float64(interval))
	r.keysWriteRate.Set(float64(stats.KeysWritten) / float64(interval))
	r.keysReadRate.Set(float64(stats.KeysRead) / float64(interval))
}

// GetBytesRate returns the bytes write rate and the bytes read rate.
func (r *RollingStoreStats) GetBytesRate() (writeRate float64, readRate float64) {
	r.RLock()
	defer r.RUnlock()
	return r.bytesWriteRate.Get(), r.bytesReadRate.Get()
}

// GetBytesWriteRate returns the bytes write rate.
func (r *RollingStoreStats) GetBytesWriteRate() float64 {
	r.RLock()
	defer r.RUnlock()
	return r.bytesWriteRate.Get()
}

// GetBytesReadRate returns the bytes read rate.
func (r *RollingStoreStats) GetBytesReadRate() float64 {
	r.RLock()
	defer r.RUnlock()
	return r.bytesReadRate.Get()
}

// GetKeysWriteRate returns the keys write rate.
func (r *RollingStoreStats) GetKeysWriteRate() float64 {
	r.RLock()
	defer r.RUnlock()
	return r.keysWriteRate.Get()
}

// GetKeysReadRate returns the keys read rate.
func (r *RollingStoreStats) GetKeysReadRate() float64 {
	r.RLock()
	defer r.RUnlock()
	return r.keysReadRate.Get()
}

// GetCPUUsage returns the total cpu usages of threads in the store.
func (r *RollingStoreStats) GetCPUUsage() float64 {
	r.RLock()
	defer r.RUnlock()
	return r.totalCPUUsage.Get()
}

// GetDiskReadRate returns the total read disk io rate of threads in the store.
func (r *RollingStoreStats) GetDiskReadRate() float64 {
	r.RLock()
	defer r.RUnlock()
	return r.totalBytesDiskReadRate.Get()
}

// GetDiskWriteRate returns the total write disk io rate of threads in the store.
func (r *RollingStoreStats) GetDiskWriteRate() float64 {
	r.RLock()
	defer r.RUnlock()
	return r.totalBytesDiskWriteRate.Get()
}
