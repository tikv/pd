// Copyright 2020 TiKV Project Authors.
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

package core

import (
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/movingaverage"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
)

type storeStats struct {
	mu       syncutil.RWMutex
	rawStats *pdpb.StoreStats

	// avgAvailable is used to make available smooth, aka no sudden changes.
	avgAvailable *movingaverage.HMA
}

func newStoreStats() *storeStats {
	return &storeStats{
		rawStats:     &pdpb.StoreStats{},
		avgAvailable: movingaverage.NewHMA(60), // take 10 minutes sample under 10s heartbeat rate
	}
}

func (ss *storeStats) updateRawStats(rawStats *pdpb.StoreStats) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.rawStats = rawStats

	if ss.avgAvailable == nil {
		return
	}
	ss.avgAvailable.Add(float64(rawStats.GetAvailable()))
}

// GetStoreStats returns the statistics information of the store.
func (ss *storeStats) GetStoreStats() *pdpb.StoreStats {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	return ss.rawStats
}

// CloneStoreStats returns the statistics information cloned from the store.
func (ss *storeStats) CloneStoreStats() *pdpb.StoreStats {
	ss.mu.RLock()
	stats := typeutil.DeepClone(ss.rawStats, StoreStatsFactory)
	ss.mu.RUnlock()
	return stats
}

// GetCapacity returns the capacity size of the store.
func (ss *storeStats) GetCapacity() uint64 {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	return ss.rawStats.GetCapacity()
}

// GetAvailable returns the available size of the store.
func (ss *storeStats) GetAvailable() uint64 {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	return ss.rawStats.GetAvailable()
}

// GetUsedSize returns the used size of the store.
func (ss *storeStats) GetUsedSize() uint64 {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	return ss.rawStats.GetUsedSize()
}

// GetBytesWritten returns the bytes written for the store during this period.
func (ss *storeStats) GetBytesWritten() uint64 {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	return ss.rawStats.GetBytesWritten()
}

// GetBytesRead returns the bytes read for the store during this period.
func (ss *storeStats) GetBytesRead() uint64 {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	return ss.rawStats.GetBytesRead()
}

// GetKeysWritten returns the keys written for the store during this period.
func (ss *storeStats) GetKeysWritten() uint64 {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	return ss.rawStats.GetKeysWritten()
}

// GetKeysRead returns the keys read for the store during this period.
func (ss *storeStats) GetKeysRead() uint64 {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	return ss.rawStats.GetKeysRead()
}

// IsBusy returns if the store is busy.
func (ss *storeStats) IsBusy() bool {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	return ss.rawStats.GetIsBusy()
}

// GetSendingSnapCount returns the current sending snapshot count of the store.
func (ss *storeStats) GetSendingSnapCount() uint32 {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	return ss.rawStats.GetSendingSnapCount()
}

// GetReceivingSnapCount returns the current receiving snapshot count of the store.
func (ss *storeStats) GetReceivingSnapCount() uint32 {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	return ss.rawStats.GetReceivingSnapCount()
}

// GetAvgAvailable returns available size after the spike changes has been smoothed.
func (ss *storeStats) GetAvgAvailable() uint64 {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	if ss.avgAvailable == nil {
		return ss.rawStats.Available
	}
	return climp0(ss.avgAvailable.Get())
}

func climp0(v float64) uint64 {
	if v <= 0 {
		return 0
	}
	return uint64(v)
}
