// Copyright 2019 PingCAP, Inc.
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
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/v4/server/core"
	"github.com/pingcap/pd/v4/tools/pd-simulator/simulator/info"
	"github.com/pingcap/pd/v4/tools/pd-simulator/simulator/simutil"
	"go.uber.org/zap"
)

func newRedundantBalanceRegion() *Case {
	var simCase Case

	storeNum := simutil.CaseConfigure.StoreNum
	regionNum := simutil.CaseConfigure.RegionNum * storeNum / 3
	if storeNum == 0 || regionNum == 0 {
		storeNum, regionNum = 6, 4000
	}

	for i := 0; i < storeNum; i++ {
		if i%2 == 1 {
			simCase.Stores = append(simCase.Stores, &Store{
				ID:        IDAllocator.nextID(),
				Status:    metapb.StoreState_Up,
				Capacity:  1 * TB,
				Available: 980 * GB,
				Version:   "2.1.0",
			})
		} else {
			simCase.Stores = append(simCase.Stores, &Store{
				ID:        IDAllocator.nextID(),
				Status:    metapb.StoreState_Up,
				Capacity:  1 * TB,
				Available: 1 * TB,
				Version:   "2.1.0",
			})
		}
	}

	for i := 0; i < regionNum; i++ {
		peers := []*metapb.Peer{
			{Id: IDAllocator.nextID(), StoreId: uint64(i%storeNum + 1)},
			{Id: IDAllocator.nextID(), StoreId: uint64((i+1)%storeNum + 1)},
			{Id: IDAllocator.nextID(), StoreId: uint64((i+2)%storeNum + 1)},
		}
		simCase.Regions = append(simCase.Regions, Region{
			ID:     IDAllocator.nextID(),
			Peers:  peers,
			Leader: peers[0],
			Size:   96 * MB,
			Keys:   960000,
		})
	}

	storesLastUpdateTime := make([]int64, storeNum+1)
	storeLastAvailable := make([]uint64, storeNum+1)
	simCase.Checker = func(regions *core.RegionsInfo, stats []info.StoreStats) bool {
		res := true
		curTime := time.Now().Unix()
		storesAvailable := make([]uint64, 0, storeNum+1)
		for i := 1; i <= storeNum; i++ {
			available := stats[i].GetAvailable()
			storesAvailable = append(storesAvailable, available)
			if curTime-storesLastUpdateTime[i] > 60 {
				if storeLastAvailable[i] != available {
					res = false
				}
				if stats[i].ToCompactionSize != 0 {
					res = false
				}
				storesLastUpdateTime[i] = curTime
				storeLastAvailable[i] = available
			} else {
				res = false
			}
		}
		simutil.Logger.Info("current counts", zap.Uint64s("storesAvailable", storesAvailable))
		return res
	}
	return &simCase
}
