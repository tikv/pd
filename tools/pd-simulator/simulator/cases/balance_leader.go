// Copyright 2017 PingCAP, Inc.
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
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/v4/server/core"
	"github.com/pingcap/pd/v4/tools/pd-simulator/simulator/info"
	"github.com/pingcap/pd/v4/tools/pd-simulator/simulator/simutil"
	"go.uber.org/zap"
)

func newBalanceLeader() *Case {
	var simCase Case

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
		peers := []*metapb.Peer{
			{Id: IDAllocator.nextID(), StoreId: uint64(storeNum)},
			{Id: IDAllocator.nextID(), StoreId: uint64((i+1)%(storeNum-1)) + 1},
			{Id: IDAllocator.nextID(), StoreId: uint64((i+2)%(storeNum-1)) + 1},
		}
		simCase.Regions = append(simCase.Regions, Region{
			ID:     IDAllocator.nextID(),
			Peers:  peers,
			Leader: peers[0],
			Size:   96 * MB,
			Keys:   960000,
		})
	}

	threshold := 0.05
	simCase.Checker = func(regions *core.RegionsInfo, stats []info.StoreStats) bool {
		res := true
		leaderCounts := make([]int, 0, storeNum)
		for i := 1; i <= storeNum; i++ {
			leaderCount := regions.GetStoreLeaderCount(uint64(i))
			leaderCounts = append(leaderCounts, leaderCount)
			res = res && isUniform(leaderCount, regionNum/3, threshold)
		}
		simutil.Logger.Info("current counts", zap.Ints("leader", leaderCounts))
		return res
	}
	return &simCase
}
