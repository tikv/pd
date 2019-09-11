// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// //     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package cases

import (
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/tools/pd-simulator/simulator/info"
	"github.com/pingcap/pd/tools/pd-simulator/simulator/simutil"
	"go.uber.org/zap"
)

func newMakeupDownReplicas() *Case {
	var simCase Case

	for i := 1; i <= 4; i++ {
		simCase.Stores = append(simCase.Stores, &Store{
			ID:        IDAllocator.nextID(),
			Status:    metapb.StoreState_Up,
			Capacity:  1 * TB,
			Available: 900 * GB,
			Version:   "2.1.0",
		})
	}

	for i := 0; i < 400; i++ {
		peers := []*metapb.Peer{
			{Id: IDAllocator.nextID(), StoreId: uint64(i)%4 + 1},
			{Id: IDAllocator.nextID(), StoreId: uint64(i+1)%4 + 1},
			{Id: IDAllocator.nextID(), StoreId: uint64(i+2)%4 + 1},
		}
		simCase.Regions = append(simCase.Regions, Region{
			ID:     IDAllocator.nextID(),
			Peers:  peers,
			Leader: peers[0],
			Size:   96 * MB,
			Keys:   960000,
		})
	}

	numNodes := 4
	down := false
	e := &DeleteNodesDescriptor{}
	e.Step = func(tick int64) uint64 {
		if numNodes > 3 && tick%100 == 0 {
			numNodes--
			return uint64(1)
		}
		if tick == 300 {
			down = true
		}
		return 0
	}
	simCase.Events = []EventDescriptor{e}

	simCase.Checker = func(regions *core.RegionsInfo, stats []info.StoreStats) bool {
		sum := 0
		regionCounts := make([]int, 0, 3)
		for i := 1; i <= 4; i++ {
			regionCount := regions.GetStoreRegionCount(uint64(i))
			if i != 1 {
				regionCounts = append(regionCounts, regionCount)
			}
			sum += regionCount
		}
		simutil.Logger.Info("current region counts", zap.Ints("region", regionCounts))

		if down && sum < 1200 {
			// only need to print once
			down = false
			simutil.Logger.Error("making up replicas don't start immediately")
			return false
		}

		for _, regionCount := range regionCounts {
			if regionCount != 400 {
				return false
			}
		}
		return true
	}
	return &simCase
}
