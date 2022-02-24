// Copyright 2021 TiKV Project Authors.
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

// storeCollector define the behavior of different engines of stores.
type storeCollector interface {
	// Engine returns the type of Store.
	Engine() string
	// Filter determines whether the Store needs to be handled by itself.
	Filter(info *StoreSummaryInfo, kind core.ResourceKind) bool
	// GetLoads obtains available loads from storeLoads and peerLoadSum according to rwTy and kind.
	GetLoads(storeLoads, peerLoadSum []float64, rwTy RWType, kind core.ResourceKind) (loads []float64)
}

type tikvCollector struct{}

func newTikvCollector() storeCollector {
	return tikvCollector{}
}

func (c tikvCollector) Engine() string {
	return core.EngineTiKV
}

func (c tikvCollector) Filter(info *StoreSummaryInfo, kind core.ResourceKind) bool {
	if info.IsTiFlash() {
		return false
	}
	switch kind {
	case core.LeaderKind:
		return info.AllowLeaderTransfer()
	case core.RegionKind:
		return true
	}
	return false
}

func (c tikvCollector) GetLoads(storeLoads, peerLoadSum []float64, rwTy RWType, kind core.ResourceKind) (loads []float64) {
	loads = make([]float64, DimLen)
	switch rwTy {
	case Read:
		loads[ByteDim] = storeLoads[StoreReadBytes]
		loads[KeyDim] = storeLoads[StoreReadKeys]
		loads[QueryDim] = storeLoads[StoreReadQuery]
	case Write:
		switch kind {
		case core.LeaderKind:
			// Use sum of hot peers to estimate leader-only byte rate.
			// For Write requests, Write{Bytes, Keys} is applied to all Peers at the same time,
			// while the Leader and Follower are under different loads (usually the Leader consumes more CPU).
			// Write{QPS} does not require such processing.
			loads[ByteDim] = peerLoadSum[ByteDim]
			loads[KeyDim] = peerLoadSum[KeyDim]
			loads[QueryDim] = storeLoads[StoreWriteQuery]
		case core.RegionKind:
			loads[ByteDim] = storeLoads[StoreWriteBytes]
			loads[KeyDim] = storeLoads[StoreWriteKeys]
			// The `Write-peer` does not have `QueryDim`
		}
	}
	return
}

type tiflashCollector struct {
	isTraceRegionFlow bool
}

func newTiFlashCollector(isTraceRegionFlow bool) storeCollector {
	return tiflashCollector{isTraceRegionFlow: isTraceRegionFlow}
}

func (c tiflashCollector) Engine() string {
	return core.EngineTiFlash
}

func (c tiflashCollector) Filter(info *StoreSummaryInfo, kind core.ResourceKind) bool {
	switch kind {
	case core.LeaderKind:
		return false
	case core.RegionKind:
		return info.IsTiFlash()
	}
	return false
}

func (c tiflashCollector) GetLoads(storeLoads, peerLoadSum []float64, rwTy RWType, kind core.ResourceKind) (loads []float64) {
	loads = make([]float64, DimLen)
	switch rwTy {
	case Read:
		// TODO: Need TiFlash StoreHeartbeat support
	case Write:
		switch kind {
		case core.LeaderKind:
			// There is no Leader on TiFlash
		case core.RegionKind:
			// TiFlash is currently unable to report statistics in the same unit as Region,
			// so it uses the sum of Regions. If it is not accurate enough, use sum of hot peer.
			if c.isTraceRegionFlow {
				loads[ByteDim] = storeLoads[StoreRegionsWriteBytes]
				loads[KeyDim] = storeLoads[StoreRegionsWriteKeys]
			} else {
				loads[ByteDim] = peerLoadSum[ByteDim]
				loads[KeyDim] = peerLoadSum[KeyDim]
			}
			// The `Write-peer` does not have `QueryDim`
		}
	}
	return
}
