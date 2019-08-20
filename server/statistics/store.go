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

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/server/core"
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

// Observe records the current store status with a given store.
func (s *StoresStats) Observe(storeID uint64, stats *pdpb.StoreStats) {
	s.RLock()
	defer s.RUnlock()
	s.rollingStoresStats[storeID].Observe(stats)
}

// UpdateTotalBytesRate updates the total bytes write rate and read rate.
func (s *StoresStats) UpdateTotalBytesRate(f func() []*core.StoreInfo) {
	s.RLock()
	defer s.RUnlock()
	var totalBytesWriteRate float64
	var totalBytesReadRate float64
	var writeRate, readRate float64
	ss := f()
	for _, store := range ss {
		if store.IsUp() {
			writeRate, readRate = s.rollingStoresStats[store.GetID()].GetBytesRate()
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
func (s *StoresStats) GetStoresCPUUsage() map[uint64]uint64 {
	s.RLock()
	defer s.RUnlock()
	res := make(map[uint64]uint64, len(s.rollingStoresStats))
	for storeID, stats := range s.rollingStoresStats {
		res[storeID] = uint64(stats.GetCPUUsage())
	}
	return res
}

// GetStoresDiskReadRate returns the disk read rate stat of all StoreInfo.
func (s *StoresStats) GetStoresDiskReadRate() map[uint64]uint64 {
	s.RLock()
	defer s.RUnlock()
	res := make(map[uint64]uint64, len(s.rollingStoresStats))
	for storeID, stats := range s.rollingStoresStats {
		res[storeID] = uint64(stats.GetDiskReadRate())
	}
	return res
}

// GetStoresDiskWriteRate returns the disk write rate stat of all StoreInfo.
func (s *StoresStats) GetStoresDiskWriteRate() map[uint64]uint64 {
	s.RLock()
	defer s.RUnlock()
	res := make(map[uint64]uint64, len(s.rollingStoresStats))
	for storeID, stats := range s.rollingStoresStats {
		res[storeID] = uint64(stats.GetDiskWriteRate())
	}
	return res
}

// GetStoresBytesWriteStat returns the bytes write stat of all StoreInfo.
func (s *StoresStats) GetStoresBytesWriteStat() map[uint64]uint64 {
	s.RLock()
	defer s.RUnlock()
	res := make(map[uint64]uint64, len(s.rollingStoresStats))
	for storeID, stats := range s.rollingStoresStats {
		writeRate, _ := stats.GetBytesRate()
		res[storeID] = uint64(writeRate)
	}
	return res
}

// GetStoresBytesReadStat returns the bytes read stat of all StoreInfo.
func (s *StoresStats) GetStoresBytesReadStat() map[uint64]uint64 {
	s.RLock()
	defer s.RUnlock()
	res := make(map[uint64]uint64, len(s.rollingStoresStats))
	for storeID, stats := range s.rollingStoresStats {
		_, readRate := stats.GetBytesRate()
		res[storeID] = uint64(readRate)
	}
	return res
}

// GetStoresKeysWriteStat returns the keys write stat of all StoreInfo.
func (s *StoresStats) GetStoresKeysWriteStat() map[uint64]uint64 {
	s.RLock()
	defer s.RUnlock()
	res := make(map[uint64]uint64, len(s.rollingStoresStats))
	for storeID, stats := range s.rollingStoresStats {
		res[storeID] = uint64(stats.GetKeysWriteRate())
	}
	return res
}

// GetStoresKeysReadStat returns the bytes read stat of all StoreInfo.
func (s *StoresStats) GetStoresKeysReadStat() map[uint64]uint64 {
	s.RLock()
	defer s.RUnlock()
	res := make(map[uint64]uint64, len(s.rollingStoresStats))
	for storeID, stats := range s.rollingStoresStats {
		res[storeID] = uint64(stats.GetKeysReadRate())
	}
	return res
}

// StoreHotRegionInfos : used to get human readable description for hot regions.
type StoreHotRegionInfos struct {
	AsPeer   StoreHotRegionsStat `json:"as_peer"`
	AsLeader StoreHotRegionsStat `json:"as_leader"`
}

// StoreHotRegionsStat used to record the hot region statistics group by store
type StoreHotRegionsStat map[uint64]*HotRegionsStat

// RollingStoreStats are multiple sets of recent historical records with specified windows size.
type RollingStoreStats struct {
	sync.RWMutex
	bytesWriteRate          *RollingStats
	bytesReadRate           *RollingStats
	keysWriteRate           *RollingStats
	keysReadRate            *RollingStats
	totalCPUUsage           *RollingStats
	totalBytesDiskReadRate  *RollingStats
	totalBytesDiskWriteRate *RollingStats
}

const storeStatsRollingWindows = 3

// NewRollingStoreStats creates a RollingStoreStats.
func newRollingStoreStats() *RollingStoreStats {
	return &RollingStoreStats{
		bytesWriteRate:          NewRollingStats(storeStatsRollingWindows),
		bytesReadRate:           NewRollingStats(storeStatsRollingWindows),
		keysWriteRate:           NewRollingStats(storeStatsRollingWindows),
		keysReadRate:            NewRollingStats(storeStatsRollingWindows),
		totalCPUUsage:           NewRollingStats(storeStatsRollingWindows),
		totalBytesDiskReadRate:  NewRollingStats(storeStatsRollingWindows),
		totalBytesDiskWriteRate: NewRollingStats(storeStatsRollingWindows),
	}
}

// Observe records current statistics.
func (r *RollingStoreStats) Observe(stats *pdpb.StoreStats) {
	statInterval := stats.GetInterval()
	interval := statInterval.GetEndTimestamp() - statInterval.GetStartTimestamp()
	if interval == 0 {
		return
	}
	r.Lock()
	defer r.Unlock()
	r.bytesWriteRate.Add(float64(stats.BytesWritten / interval))
	r.bytesReadRate.Add(float64(stats.BytesRead / interval))
	r.keysWriteRate.Add(float64(stats.KeysWritten / interval))
	r.keysReadRate.Add(float64(stats.KeysRead / interval))

	// Updates the cpu usages and disk rw rates of store.
	cpuUsages := stats.GetCpuUsages()
	var totalCPUUsage uint64
	for _, records := range cpuUsages {
		totalCPUUsage += records.GetValue()
	}
	r.totalCPUUsage.Add(float64(totalCPUUsage))

	readIoRates := stats.GetReadIoRates()
	var totalBytesDiskReadRate uint64
	for _, records := range readIoRates {
		totalBytesDiskReadRate += records.GetValue()
	}
	r.totalBytesDiskReadRate.Add(float64(totalBytesDiskReadRate))

	writeIoRates := stats.GetWriteIoRates()
	var totalBytesDiskWriteRate uint64
	for _, records := range writeIoRates {
		totalBytesDiskWriteRate += records.GetValue()
	}
	r.totalBytesDiskWriteRate.Add(float64(totalBytesDiskWriteRate))
}

// GetBytesRate returns the bytes write rate and the bytes read rate.
func (r *RollingStoreStats) GetBytesRate() (writeRate float64, readRate float64) {
	r.RLock()
	defer r.RUnlock()
	return r.bytesWriteRate.Median(), r.bytesReadRate.Median()
}

// GetKeysWriteRate returns the keys write rate.
func (r *RollingStoreStats) GetKeysWriteRate() float64 {
	r.RLock()
	defer r.RUnlock()
	return r.keysWriteRate.Median()
}

// GetKeysReadRate returns the keys read rate.
func (r *RollingStoreStats) GetKeysReadRate() float64 {
	r.RLock()
	defer r.RUnlock()
	return r.keysReadRate.Median()
}

// GetCPUUsage returns the total cpu usages of threads in the store.
func (r *RollingStoreStats) GetCPUUsage() float64 {
	r.RLock()
	defer r.RUnlock()
	return r.totalCPUUsage.Median()
}

// GetDiskReadRate returns the total read disk io rate of threads in the store.
func (r *RollingStoreStats) GetDiskReadRate() float64 {
	r.RLock()
	defer r.RUnlock()
	return r.totalBytesDiskReadRate.Median()
}

// GetDiskWriteRate returns the total write disk io rate of threads in the store.
func (r *RollingStoreStats) GetDiskWriteRate() float64 {
	r.RLock()
	defer r.RUnlock()
	return r.totalBytesDiskWriteRate.Median()
}

// StoreStatInformer provides access to a shared informer of stores statistics.
type StoreStatInformer interface {
	GetStoresStats() *StoresStats
}
