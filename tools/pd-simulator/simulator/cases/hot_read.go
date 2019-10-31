// Copyright 2018 PingCAP, Inc.
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
	"github.com/pingcap/pd/v4/server/core"
	"github.com/pingcap/pd/v4/tools/pd-simulator/simulator/info"
	"github.com/pingcap/pd/v4/tools/pd-simulator/simulator/simutil"
	"go.uber.org/zap"
)

func newHotRead() *Case {
	var simCase Case

	storeNum, regionNum := getStoreNum(), getRegionNum()

	// Initialize the cluster
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
			Size:   96 * MB,
			Keys:   960000,
		})
	}

	// Events description
	// select regions on store 1 as hot read regions.
	selectRegionNum := 4 * storeNum
	readFlow := make(map[uint64]int64, selectRegionNum)
	for _, r := range simCase.Regions {
		if r.Leader.GetStoreId() == 1 {
			readFlow[r.ID] = 128 * MB
			if len(readFlow) == selectRegionNum {
				break
			}
		}
	}
	e := &ReadFlowOnRegionDescriptor{}
	e.Step = func(tick int64) map[uint64]int64 {
		return readFlow
	}
	simCase.Events = []EventDescriptor{e}
	// Checker description
	simCase.Checker = func(regions *core.RegionsInfo, stats []info.StoreStats) bool {
		leaderCount := make([]int, storeNum)
		for id := range readFlow {
			leaderStore := regions.GetRegion(id).GetLeader().GetStoreId()
			leaderCount[int(leaderStore-1)]++
		}
		simutil.Logger.Info("current hot region counts", zap.Reflect("hot-region", leaderCount))

		// check count diff < 2.
		var min, max int
		for i := range leaderCount {
			if leaderCount[i] > leaderCount[max] {
				max = i
			}
			if leaderCount[i] < leaderCount[min] {
				min = i
			}
		}
		return leaderCount[max]-leaderCount[min] < 2
	}

	return &simCase
}
