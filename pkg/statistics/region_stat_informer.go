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

package statistics

import (
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/statistics/utils"
)

// RegionStatInformer provides access to a shared informer of statistics.
type RegionStatInformer interface {
	GetHotPeerStat(rw utils.RWType, regionID, storeID uint64) *HotPeerStat
	IsRegionHot(region *core.RegionInfo) bool
	// GetHotPeerStats return the read or write statistics for hot regions.
	// It returns a map where the keys are store IDs and the values are slices of HotPeerStat.
	// The result only includes peers that are hot enough.
	GetHotPeerStats(rw utils.RWType) map[uint64][]*HotPeerStat
}
