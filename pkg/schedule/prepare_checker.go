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

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"go.uber.org/zap"
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
	notLoadedFromRegionsCnt := c.GetClusterNotFromStorageRegionsCnt()
	totalRegionsCnt := c.GetTotalRegionCount()
	if float64(notLoadedFromRegionsCnt) > float64(totalRegionsCnt)*collectFactor {
		log.Info("meta not loaded from region number is satisfied, finish prepare checker", zap.Int("not-from-storage-region", notLoadedFromRegionsCnt), zap.Int("total-region", totalRegionsCnt))
		checker.prepared = true
		return true
	}
	// The number of active regions should be more than total region of all stores * collectFactor
	if float64(totalRegionsCnt)*collectFactor > float64(checker.sum) {
		return false
	}
	for _, store := range c.GetStores() {
		if !store.IsPreparing() && !store.IsServing() {
			continue
		}
		storeID := store.GetID()
		// For each store, the number of active regions should be more than total region of the store * collectFactor
		if float64(c.GetStoreRegionCount(storeID))*collectFactor > float64(checker.reactiveRegions[storeID]) {
			return false
		}
	}
	checker.prepared = true
	return true
}

func (checker *prepareChecker) Collect(region *core.RegionInfo) {
	checker.Lock()
	defer checker.Unlock()
	for _, p := range region.GetPeers() {
		checker.reactiveRegions[p.GetStoreId()]++
	}
	checker.sum++
}

func (checker *prepareChecker) IsPrepared() bool {
	checker.RLock()
	defer checker.RUnlock()
	return checker.prepared
}

// for test purpose
func (checker *prepareChecker) SetPrepared() {
	checker.Lock()
	defer checker.Unlock()
	checker.prepared = true
}

// for test purpose
func (checker *prepareChecker) GetSum() int {
	checker.RLock()
	defer checker.RUnlock()
	return checker.sum
}
