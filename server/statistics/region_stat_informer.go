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

import "github.com/tikv/pd/server/core"

// RegionStatInformer provides access to a shared informer of statistics.
type RegionStatInformer interface {
	IsRegionHot(region *core.RegionInfo) bool
	// RegionWriteStats return the storeID -> write stat of peers on this store.
	// The result only includes peers that are hot enough.
	RegionWriteStats() map[uint64][]*HotPeerStat
	// RegionReadStats return the storeID -> read stat of peers on this store.
	// The result only includes peers that are hot enough.
	RegionReadStats() map[uint64][]*HotPeerStat
}
