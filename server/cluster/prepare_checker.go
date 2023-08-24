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

package cluster

import (
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/tikv/pd/pkg/syncutil"
	"github.com/tikv/pd/server/core"
)

type prepareChecker struct {
	syncutil.RWMutex
	reactiveRegions map[uint64]int
	start           time.Time
	sum             int
	prepared        bool
}

func newPrepareChecker() *prepareChecker {
	return &prepareChecker{
		start:           time.Now(),
		reactiveRegions: make(map[uint64]int),
	}
}

// Before starting up the scheduler, we need to take the proportion of the regions on each store into consideration.
func (checker *prepareChecker) check(c *core.BasicCluster) bool {
	checker.Lock()
	defer checker.Unlock()
	if checker.prepared {
		return true
	}
	if time.Since(checker.start) > collectTimeout {
		checker.prepared = true
		return true
	}
	// The number of active regions should be more than total region of all stores * collectFactor
	if float64(c.GetRegionCount())*collectFactor > float64(checker.sum) {
		log.Info("not collect enough regions", zap.Uint64("sum", uint64(checker.sum)), zap.Uint64("region-count", uint64(c.GetRegionCount())))
		logRegion(c, 10)
		return false
	}
	for _, store := range c.GetStores() {
		if !store.IsPreparing() && !store.IsServing() {
			continue
		}
		storeID := store.GetID()
		// For each store, the number of active regions should be more than total region of the store * collectFactor
		if float64(c.GetStoreRegionCount(storeID))*collectFactor > float64(checker.reactiveRegions[storeID]) {
			log.Info("not collect enough regions for store", zap.Uint64("store-id", storeID), zap.Uint64("sum", uint64(checker.reactiveRegions[storeID])),
				zap.Uint64("region-count", uint64(c.GetStoreRegionCount(storeID))))
			return false
		}
	}
	checker.prepared = true
	return true
}

func (checker *prepareChecker) collect(region *core.RegionInfo) {
	checker.Lock()
	defer checker.Unlock()
	for _, p := range region.GetPeers() {
		checker.reactiveRegions[p.GetStoreId()]++
	}
	checker.sum++
}

func (checker *prepareChecker) isPrepared() bool {
	checker.RLock()
	defer checker.RUnlock()
	return checker.prepared
}

func logRegion(c *core.BasicCluster, limit int) {
	for _, region := range c.GetRegions() {
		log.Info("region", zap.Uint64("region-id", region.GetID()), zap.Uint64("store-id", region.GetLeader().GetStoreId()),
			zap.Uint64("leader-id", region.GetLeader().GetId()), zap.Time("update-time", region.GetUpdateTime()))
		if limit--; limit == 0 {
			break
		}
	}
}
