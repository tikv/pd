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

package cluster

import (
	"time"

	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule"
)

// clearThreadhold indicates when we do cleanup operations.
// When the number of times the collect() function is executed is over than this threshold,
// regions that are not in region tree will be deleted.
const clearThreshold = 10

// regionStateType represents the type of region's state.
type regionStateType int

// region state type
const (
	regionStateDown regionStateType = iota
	regionStateTypeLen
)

// regionState is used to record abnormal regions.
type regionState struct {
	cluster schedule.Cluster
	states  [regionStateTypeLen]map[uint64]*core.RegionInfo
	count   int
}

// newRegionState creates a new regionState.
func newRegionState(cluster schedule.Cluster) *regionState {
	r := &regionState{
		cluster: cluster,
		count:   0,
	}
	for rst := regionStateType(0); rst < regionStateTypeLen; rst++ {
		r.states[rst] = map[uint64]*core.RegionInfo{}
	}
	return r
}

// Check verifies a region's state, recording it if need.
func (r *regionState) observe(regions []*core.RegionInfo) {
	now := time.Now().UnixNano()
	expireTime := r.cluster.GetOpts().GetMaxStoreDownTime().Nanoseconds()
	for _, region := range regions {
		regionID := region.GetID()
		// check down region
		if now-int64(region.GetInterval().GetEndTimestamp()) >= expireTime {
			_, exist := r.states[regionStateDown][regionID]
			if !exist {
				r.states[regionStateDown][regionID] = region
			}
		}
	}
}

// Collect collects the metrics of the regions' states.
func (r *regionState) collect() {
	regionStateGauge.WithLabelValues("down-region-count").Set(float64(len(r.states[regionStateDown])))
	r.count++
	if r.count == clearThreshold {
		r.count = 0
		r.clearNotExistRegion()
	}
}

func (r *regionState) clearNotExistRegion() {
	bc := r.cluster.GetBasicCluster()
	for typ := regionStateType(0); typ < regionStateTypeLen; typ++ {
		for _, region := range r.states[typ] {
			regionID := region.GetID()
			if bc.GetRegion(regionID) == nil {
				delete(r.states[typ], regionID)
			}
		}
	}
}
