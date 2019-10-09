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
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/tools/pd-simulator/simulator/info"
	"github.com/pingcap/pd/tools/pd-simulator/simulator/simutil"
	"go.uber.org/zap"
)

func newRegionMerge() *Case {
	var simCase Case
	// Initialize the cluster
	for i := 1; i <= 4; i++ {
		simCase.Stores = append(simCase.Stores, &Store{
			ID:        IDAllocator.nextID(),
			Status:    metapb.StoreState_Up,
			Capacity:  1 * TB,
			Available: 900 * GB,
			Version:   "2.1.0",
		})
	}

	for i := 0; i < 40; i++ {
		storeIDs := rand.Perm(4)
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
	simCase.Checker = func(regions *core.RegionsInfo, stats []info.StoreStats) bool {
		count1 := regions.GetStoreRegionCount(1)
		count2 := regions.GetStoreRegionCount(2)
		count3 := regions.GetStoreRegionCount(3)
		count4 := regions.GetStoreRegionCount(4)

		sum := count1 + count2 + count3 + count4
		simutil.Logger.Info("current region counts",
			zap.Int("first-store", count1),
			zap.Int("second-store", count2),
			zap.Int("third-store", count3),
			zap.Int("fourth-store", count4))
		return sum == 30
	}
	return &simCase
}
