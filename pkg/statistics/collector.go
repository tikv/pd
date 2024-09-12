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

import (
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/statistics/utils"
)

// storeCollector define the behavior of different engines of stores.
type storeCollector interface {
	// engine returns the type of Store.
	engine() string
	// filter determines whether the Store needs to be handled by itself.
	filter(info *StoreSummaryInfo, kind constant.ResourceKind) bool
	// getLoads obtains available loads from storeLoads and peerLoadSum according to rwTy and kind.
	getLoads(storeLoads, peerLoadSum []float64, rwTy utils.RWType, kind constant.ResourceKind) (loads []float64)
}

type tikvCollector struct{}

func newTikvCollector() storeCollector {
	return tikvCollector{}
}

func (tikvCollector) engine() string {
	return core.EngineTiKV
}

func (tikvCollector) filter(info *StoreSummaryInfo, kind constant.ResourceKind) bool {
	if info.IsTiFlash() {
		return false
	}
	switch kind {
	case constant.LeaderKind:
		return info.AllowLeaderTransfer()
	case constant.RegionKind:
		return true
	}
	return false
}

func (tikvCollector) getLoads(storeLoads, peerLoadSum []float64, rwTy utils.RWType, kind constant.ResourceKind) (loads []float64) {
	loads = make([]float64, utils.DimLen)
	switch rwTy {
	case utils.Read:
		loads[utils.ByteDim] = storeLoads[utils.StoreReadBytes]
		loads[utils.KeyDim] = storeLoads[utils.StoreReadKeys]
		loads[utils.QueryDim] = storeLoads[utils.StoreReadQuery]
	case utils.Write:
		switch kind {
		case constant.LeaderKind:
			// Use sum of hot peers to estimate leader-only byte rate.
			// For Write requests, Write{Bytes, Keys} is applied to all Peers at the same time,
			// while the Leader and Follower are under different loads (usually the Leader consumes more CPU).
			// Write{Query} does not require such processing.
			loads[utils.ByteDim] = peerLoadSum[utils.ByteDim]
			loads[utils.KeyDim] = peerLoadSum[utils.KeyDim]
			loads[utils.QueryDim] = storeLoads[utils.StoreWriteQuery]
		case constant.RegionKind:
			loads[utils.ByteDim] = storeLoads[utils.StoreWriteBytes]
			loads[utils.KeyDim] = storeLoads[utils.StoreWriteKeys]
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

func (tiflashCollector) engine() string {
	return core.EngineTiFlash
}

func (tiflashCollector) filter(info *StoreSummaryInfo, kind constant.ResourceKind) bool {
	switch kind {
	case constant.LeaderKind:
		return false
	case constant.RegionKind:
		return info.IsTiFlash()
	}
	return false
}

func (c tiflashCollector) getLoads(storeLoads, peerLoadSum []float64, rwTy utils.RWType, kind constant.ResourceKind) (loads []float64) {
	loads = make([]float64, utils.DimLen)
	switch rwTy {
	case utils.Read:
		// TODO: Need TiFlash StoreHeartbeat support
	case utils.Write:
		switch kind {
		case constant.LeaderKind:
			// There is no Leader on TiFlash
		case constant.RegionKind:
			// TiFlash is currently unable to report statistics in the same unit as Region,
			// so it uses the sum of Regions. If it is not accurate enough, use sum of hot peer.
			if c.isTraceRegionFlow {
				loads[utils.ByteDim] = storeLoads[utils.StoreRegionsWriteBytes]
				loads[utils.KeyDim] = storeLoads[utils.StoreRegionsWriteKeys]
			} else {
				loads[utils.ByteDim] = peerLoadSum[utils.ByteDim]
				loads[utils.KeyDim] = peerLoadSum[utils.KeyDim]
			}
			// The `Write-peer` does not have `QueryDim`
		}
	}
	return
}
