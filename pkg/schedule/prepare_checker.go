// Copyright 2022 TiKV Project Authors.
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

package schedule

import (
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

type prepareChecker struct {
	syncutil.RWMutex
	start              time.Time
	totalRegionCountFn func() (int, error)
	prepared           bool
	storesPrepared     bool
}

func newPrepareChecker(totalRegionCountFn func() (int, error)) *prepareChecker {
	return &prepareChecker{
		start:              time.Now(),
		totalRegionCountFn: totalRegionCountFn,
	}
}

// Before starting up the scheduler, we need to take the proportion of the regions on each store into consideration.
func (checker *prepareChecker) check(c *core.BasicCluster) bool {
	checker.Lock()
	defer checker.Unlock()
	if checker.prepared {
		return true
	}
	if time.Since(checker.start) > CollectTimeout {
		c.UpdateAllStoreStatus()
		checker.prepared = true
		return true
	}
	notLoadedFromRegionsCnt := c.GetNotFromStorageRegionsCnt()
	totalRegionCount, err := checker.totalRegionCountFn()
	if err != nil {
		return false
	}
	// Store preparation retains its 90% latch while scheduling waits for full collection.
	if !checker.storesPrepared && float64(totalRegionCount)*core.CollectFactor <= float64(notLoadedFromRegionsCnt) {
		checker.storesPrepared = areStoreRegionsCollected(c, core.CollectFactor)
	}
	// Wait for all regions so incomplete leader counts do not trigger premature balancing.
	if notLoadedFromRegionsCnt < totalRegionCount {
		return false
	}
	if !areStoreRegionsCollected(c, 1) {
		return false
	}
	// Scan for missing leaders and stale subtrees only after the cheaper count
	// checks pass. Synced metadata can be complete without all leaders known.
	if !c.IsRegionLeaderPrepared() {
		return false
	}
	// Store heartbeats can reveal more Regions while the snapshot is scanned.
	if !areStoreRegionsCollected(c, 1) {
		return false
	}
	// Schedulers score StoreInfo snapshots rather than the Region subtrees.
	c.UpdateAllStoreStatus()
	log.Info("not loaded from storage region number is satisfied, finish prepare checker", zap.Int("not-from-storage-region", notLoadedFromRegionsCnt), zap.Int("total-region", totalRegionCount))
	checker.prepared = true
	return true
}

func areStoreRegionsCollected(c *core.BasicCluster, factor float64) bool {
	for _, store := range c.GetStores() {
		if !store.IsPreparing() && !store.IsServing() {
			continue
		}
		storeID := store.GetID()
		collected := c.GetNotFromStorageRegionsCntByStore(storeID)
		if float64(collected) < float64(store.GetStoreStats().GetRegionCount())*factor || float64(collected) < float64(c.GetStoreRegionCount(storeID))*factor {
			return false
		}
	}
	return true
}

// AreStoresPrepared reports whether the original collection thresholds have been met.
func (checker *prepareChecker) AreStoresPrepared() bool {
	if checker == nil {
		return false
	}
	checker.RLock()
	defer checker.RUnlock()
	return checker.prepared || checker.storesPrepared
}

// IsPrepared returns whether the coordinator is prepared.
func (checker *prepareChecker) IsPrepared() bool {
	if checker == nil {
		return false
	}
	checker.RLock()
	defer checker.RUnlock()
	return checker.prepared
}

// SetPrepared is for test purpose
func (checker *prepareChecker) SetPrepared() {
	checker.Lock()
	defer checker.Unlock()
	checker.prepared = true
}
