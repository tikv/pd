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
	"github.com/pingcap/pd/pkg/faketikv/simutil"
	"github.com/pingcap/pd/server/core"
)

func newMakeupDownReplicas() *Conf {
	var conf Conf
	var id idAllocator

	for i := 1; i <= 4; i++ {
		conf.Stores = append(conf.Stores, &Store{
			ID:        id.nextID(),
			Status:    metapb.StoreState_Up,
			Capacity:  1 * TB,
			Available: 900 * GB,
			Version:   "2.1.0",
		})
	}

	for i := 0; i < 400; i++ {
		peers := []*metapb.Peer{
			{Id: id.nextID(), StoreId: uint64(i)%4 + 1},
			{Id: id.nextID(), StoreId: uint64(i+1)%4 + 1},
			{Id: id.nextID(), StoreId: uint64(i+2)%4 + 1},
		}
		conf.Regions = append(conf.Regions, Region{
			ID:     id.nextID(),
			Peers:  peers,
			Leader: peers[0],
			Size:   96 * MB,
			Keys:   960000,
		})
	}
	conf.MaxID = id.maxID

	numNodes := 4
	down := false
	e := &DeleteNodesInner{}
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
	conf.Events = []EventInner{e}

	conf.Checker = func(regions *core.RegionsInfo) bool {
		sum := 0
		regionCounts := make([]int, 0, 3)
		for i := 1; i <= 4; i++ {
			regionCount := regions.GetStoreRegionCount(uint64(i))
			if i != 1 {
				regionCounts = append(regionCounts, regionCount)
			}
			sum += regionCount
		}
		simutil.Logger.Infof("region counts: %v", regionCounts)

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
	return &conf
}
