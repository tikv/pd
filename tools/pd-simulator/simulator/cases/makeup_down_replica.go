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
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/v4/server/core"
	"github.com/pingcap/pd/v4/tools/pd-simulator/simulator/info"
	"github.com/pingcap/pd/v4/tools/pd-simulator/simulator/simutil"
	"go.uber.org/zap"
)

func newMakeupDownReplicas() *Case {
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

	for i := 0; i < storeNum*regionNum/3; i++ {
		peers := []*metapb.Peer{
			{Id: IDAllocator.nextID(), StoreId: uint64((i)%storeNum) + 1},
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

	numNodes := storeNum
	down := false
	e := &DeleteNodesDescriptor{}
	e.Step = func(tick int64) uint64 {
		if numNodes > noEmptyStoreNum && tick%100 == 0 {
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
		regionCounts := make([]int, 0, storeNum)
		for i := 1; i <= storeNum; i++ {
			regionCount := regions.GetStoreRegionCount(uint64(i))
			regionCounts = append(regionCounts, regionCount)
			sum += regionCount
		}
		simutil.Logger.Info("current region counts", zap.Ints("region", regionCounts))

		if down && sum < storeNum*regionNum {
			// only need to print once
			down = false
			simutil.Logger.Error("making up replicas don't start immediately")
			return false
		}

		res := true
		threshold := 0.05
		for index, regionCount := range regionCounts {
			if index == 0 { // storeId == 1
				continue
			}
			res = res && isUniform(regionCount, storeNum*regionNum/noEmptyStoreNum, threshold)
		}
		return res
	}
	return &simCase
}
