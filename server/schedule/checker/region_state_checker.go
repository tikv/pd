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

package checker

import (
	"sync"
	"time"

	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
)

// RegionStateType represents the type of the region's state.
type RegionStateType uint32

// region state type
const (
	DownRegion RegionStateType = 1 << iota
)

// RegionStateChecker ensures regions in abnormal state will be recorded.
type RegionStateChecker struct {
	sync.RWMutex
	opt    *config.PersistOptions
	states map[RegionStateType]map[uint64]*core.RegionInfo
}

// NewRegionStateChecker creates a region state checker.
func NewRegionStateChecker(opt *config.PersistOptions) *RegionStateChecker {
	r := &RegionStateChecker{
		opt:    opt,
		states: make(map[RegionStateType]map[uint64]*core.RegionInfo),
	}
	r.states[DownRegion] = make(map[uint64]*core.RegionInfo)
	return r
}

// GetRegionStatesByType gets the states of the region by types. The regions here need to be cloned, otherwise, it may cause data race problems.
func (r *RegionStateChecker) GetRegionStatesByType(typ RegionStateType) []*core.RegionInfo {
	r.RLock()
	defer r.RUnlock()
	res := make([]*core.RegionInfo, 0, len(r.states[typ]))
	for _, r := range r.states[typ] {
		res = append(res, r.Clone())
	}
	return res
}

// Check verifies a region's state, recording it if need.
func (r *RegionStateChecker) Check(region *core.RegionInfo) {
	r.Lock()
	defer r.Unlock()
	regionID := region.GetID()

	// check down region
	if time.Now().UnixNano()-int64(region.GetInterval().GetEndTimestamp()) >= r.opt.GetMaxStoreDownTime().Nanoseconds() {
		_, exist := r.states[DownRegion][regionID]
		if !exist {
			r.states[DownRegion][regionID] = region
		}
	}
}

// Collect collects the metrics of the regions' states.
func (r *RegionStateChecker) Collect() {
	r.Lock()
	defer r.Unlock()
	regionStatesGauge.WithLabelValues("down-region-count").Set(float64(len(r.states[DownRegion])))
}
