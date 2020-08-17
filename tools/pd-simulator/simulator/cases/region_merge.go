// Copyright 2018 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package cases

import (
	"math/rand"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/tools/pd-simulator/simulator/info"
	"github.com/tikv/pd/tools/pd-simulator/simulator/simutil"
	"go.uber.org/zap"
)

func newRegionMerge() *Case {
	var simCase Case
	// Initialize the cluster
	storeNum, regionNum := getStoreNum(), getRegionNum()
	for i := 1; i <= storeNum; i++ {
		simCase.Stores = append(simCase.Stores, &Store{
			ID:        IDAllocator.nextID(),
			Status:    metapb.StoreState_Up,
			Capacity:  1 * TB,
			Available: 900 * GB,
			Version:   "2.1.0",
		})
	}

	for i := 0; i < storeNum*regionNum/3; i++ {
		storeIDs := rand.Perm(storeNum)
		peers := []*metapb.Peer{
			{Id: IDAllocator.nextID(), StoreId: uint64(storeIDs[0] + 1)},
			{Id: IDAllocator.nextID(), StoreId: uint64(storeIDs[1] + 1)},
			{Id: IDAllocator.nextID(), StoreId: uint64(storeIDs[2] + 1)},
		}
		simCase.Regions = append(simCase.Regions, Region{
			ID:     IDAllocator.nextID(),
			Peers:  peers,
			Leader: peers[0],
			Size:   10 * MB,
			Keys:   100000,
		})
	}
	// Checker description
	threshold := 0.05
	mergeRatio := 4 // when max-merge-region-size is 20, per region will reach 40MB
	simCase.Checker = func(regions *core.RegionsInfo, stats []info.StoreStats) bool {
		sum := 0
		regionCounts := make([]int, 0, storeNum)
		for i := 1; i <= storeNum; i++ {
			regionCount := regions.GetStoreRegionCount(uint64(i))
			regionCounts = append(regionCounts, regionCount)
			sum += regionCount
		}
		simutil.Logger.Info("current counts", zap.Ints("region", regionCounts), zap.Int64("average region size", regions.GetAverageRegionSize()))
		return isUniform(sum, storeNum*regionNum/mergeRatio, threshold)
	}
	return &simCase
}
