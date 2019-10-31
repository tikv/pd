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

func newDeleteNodes() *Case {
	var simCase Case

	storeNum, regionNum := getStoreNum(), getRegionNum()
	noEmptyStoreNum := storeNum - 1
	for i := 1; i <= storeNum; i++ {
		simCase.Stores = append(simCase.Stores, &Store{
			ID:        IDAllocator.nextID(),
			Status:    metapb.StoreState_Up,
			Capacity:  1 * TB,
			Available: 900 * GB,
			Version:   "2.1.0",
		})
	}

	for i := 0; i < regionNum*storeNum/3; i++ {
		peers := []*metapb.Peer{
			{Id: IDAllocator.nextID(), StoreId: uint64(i%storeNum) + 1},
			{Id: IDAllocator.nextID(), StoreId: uint64((i+1)%storeNum) + 1},
			{Id: IDAllocator.nextID(), StoreId: uint64((i+2)%storeNum) + 1},
		}
		simCase.Regions = append(simCase.Regions, Region{
			ID:     IDAllocator.nextID(),
			Peers:  peers,
			Leader: peers[0],
			Size:   96 * MB,
			Keys:   960000,
		})
	}

	ids := make([]uint64, 0, len(simCase.Stores))
	for _, store := range simCase.Stores {
		ids = append(ids, store.ID)
	}

	numNodes := storeNum
	e := &DeleteNodesDescriptor{}
	e.Step = func(tick int64) uint64 {
		if numNodes > noEmptyStoreNum && tick%100 == 0 {
			idx := rand.Intn(numNodes)
			numNodes--
			nodeID := ids[idx]
			ids = append(ids[:idx], ids[idx+1:]...)
			return nodeID
		}
		return 0
	}
	simCase.Events = []EventDescriptor{e}

	threshold := 0.05
	simCase.Checker = func(regions *core.RegionsInfo, stats []info.StoreStats) bool {
		res := numNodes == noEmptyStoreNum
		leaderCounts := make([]int, 0, numNodes)
		regionCounts := make([]int, 0, numNodes)
		for _, i := range ids {
			leaderCount := regions.GetStoreLeaderCount(i)
			regionCount := regions.GetStoreRegionCount(i)
			leaderCounts = append(leaderCounts, leaderCount)
			regionCounts = append(regionCounts, regionCount)
			res = res && leaderAndRegionIsUniform(leaderCount, regionCount, regionNum*storeNum/noEmptyStoreNum, threshold)
		}

		simutil.Logger.Info("current counts", zap.Ints("leader", leaderCounts), zap.Ints("region", regionCounts))
		return res
	}
	return &simCase
}
