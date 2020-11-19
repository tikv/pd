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
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/tools/pd-simulator/simulator/info"
	"github.com/tikv/pd/tools/pd-simulator/simulator/simutil"
	"go.uber.org/zap"
)

func newRegionSplit() *Case {
	var simCase Case
	// Initialize the cluster
	storeNum := getStoreNum()
	for i := 1; i <= storeNum; i++ {
		simCase.Stores = append(simCase.Stores, &Store{
			ID:        uint64(i),
			Status:    metapb.StoreState_Up,
			Capacity:  1 * TB,
			Available: 900 * GB,
			Version:   "2.1.0",
		})
	}
	peers := []*metapb.Peer{
		{Id: 4, StoreId: 1},
	}
	simCase.Regions = append(simCase.Regions, Region{
		ID:     5,
		Peers:  peers,
		Leader: peers[0],
		Size:   1 * MB,
		Keys:   10000,
	})

	simCase.RegionSplitSize = 128 * MB
	simCase.RegionSplitKeys = 10000
	// Events description
	e := &WriteFlowOnSpotDescriptor{}
	e.Step = func(tick int64) map[string]int64 {
		return map[string]int64{
			"foobar": 8 * MB,
		}
	}
	simCase.Events = []EventDescriptor{e}

	// Checker description
	simCase.Checker = func(regions *core.RegionsInfo, stats []info.StoreStats) bool {
		res := true
		regionCounts := make([]int, 0, storeNum)
		for i := 1; i <= storeNum; i++ {
			regionCount := regions.GetStoreRegionCount(uint64(i))
			regionCounts = append(regionCounts, regionCount)
			res = res && regionCount > 5
		}
		simutil.Logger.Info("current counts", zap.Ints("region", regionCounts))
		return res
	}
	return &simCase
}
