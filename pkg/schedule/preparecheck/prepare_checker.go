// Copyright 2025 TiKV Project Authors.
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

package preparecheck

import (
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

// collectTimeout is the timeout for collecting regions.
const collectTimeout = 5 * time.Minute

// Checker is used to check if the coordinator has finished cluster information preparation.
type Checker struct {
	syncutil.RWMutex
	start              time.Time
	totalRegionCountFn func() (int, error)
	prepared           bool
}

// NewChecker creates a new Checker.
func NewChecker(totalRegionCountFn func() (int, error)) *Checker {
	return &Checker{
		start:              time.Now(),
		totalRegionCountFn: totalRegionCountFn,
	}
}

// Check is used to check if the coordinator has finished cluster information preparation.
func (checker *Checker) Check(c *core.BasicCluster) bool {
	checker.Lock()
	defer checker.Unlock()
	if checker.prepared {
		return true
	}
	if time.Since(checker.start) > collectTimeout {
		checker.prepared = true
		return true
	}
	notLoadedFromRegionsCnt := c.GetNotFromStorageRegionsCnt()
	totalRegionCount, err := checker.totalRegionCountFn()
	if err != nil {
		return false
	}
	// The number of active regions should be more than total region of all stores * core.CollectFactor
	if float64(totalRegionCount)*core.CollectFactor > float64(notLoadedFromRegionsCnt) {
		return false
	}
	for _, store := range c.GetStores() {
		if !store.IsPreparing() && !store.IsServing() {
			continue
		}
		storeID := store.GetID()
		// It is used to avoid sudden scheduling when scheduling service is just started.
		if float64(store.GetStoreStats().GetRegionCount())*core.CollectFactor > float64(c.GetNotFromStorageRegionsCntByStore(storeID)) {
			return false
		}
		if !c.IsStorePrepared(storeID) {
			return false
		}
	}
	log.Info("not loaded from storage region number is satisfied, finish prepare checker", zap.Int("not-from-storage-region", notLoadedFromRegionsCnt), zap.Int("total-region", totalRegionCount))
	checker.prepared = true
	return true
}

// IsPrepared returns whether the coordinator is prepared.
func (checker *Checker) IsPrepared() bool {
	if checker == nil {
		return false
	}
	checker.RLock()
	defer checker.RUnlock()
	return checker.prepared
}

// RunIfPrepared runs fn while the checker remains prepared.
func (checker *Checker) RunIfPrepared(fn func()) bool {
	if checker == nil {
		return false
	}
	checker.RLock()
	defer checker.RUnlock()
	if !checker.prepared {
		return false
	}
	fn()
	return true
}

// SetPrepared is for test purpose
func (checker *Checker) SetPrepared() {
	checker.Lock()
	defer checker.Unlock()
	checker.prepared = true
}

// ResetPrepared resets the checker so scheduling pauses until cluster
// information is collected again after the region cache is reset.
func (checker *Checker) ResetPrepared() {
	checker.ResetPreparedAndRun(nil)
}

// ResetPreparedAndRun resets the checker and runs fn while scheduling is
// fenced. It is used to keep the prepared state and the region cache reset
// atomic with respect to prepare checks and scheduling.
func (checker *Checker) ResetPreparedAndRun(fn func()) {
	checker.Lock()
	defer checker.Unlock()
	checker.prepared = false
	checker.start = time.Now()
	if fn != nil {
		fn()
	}
}
