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
	"github.com/pingcap/pd/pkg/faketikv/simutil"
	"github.com/pingcap/pd/server/core"
)

func newRegionMerge() *Conf {
	var conf Conf
	// Initialize the cluster
	for i := 1; i <= 4; i++ {
		conf.Stores = append(conf.Stores, Store{
			ID:        uint64(i),
			Status:    metapb.StoreState_Up,
			Capacity:  10 * gb,
			Available: 9 * gb,
		})
	}
	var id idAllocator
	id.setMaxID(4)
	for i := 0; i < 40; i++ {
		storeIDs := rand.Perm(4)
		peers := []*metapb.Peer{
			{Id: id.nextID(), StoreId: uint64(storeIDs[0] + 1)},
			{Id: id.nextID(), StoreId: uint64(storeIDs[1] + 1)},
			{Id: id.nextID(), StoreId: uint64(storeIDs[2] + 1)},
		}
		conf.Regions = append(conf.Regions, Region{
			ID:     id.nextID(),
			Peers:  peers,
			Leader: peers[0],
			Size:   10 * mb,
			Keys:   100000,
		})
	}
	conf.MaxID = id.maxID

	// Checker description
	conf.Checker = func(regions *core.RegionsInfo) bool {
		count1 := regions.GetStoreRegionCount(1)
		count2 := regions.GetStoreRegionCount(2)
		count3 := regions.GetStoreRegionCount(3)
		count4 := regions.GetStoreRegionCount(4)

		sum := count1 + count2 + count3 + count4
		simutil.Logger.Infof("region counts: %v %v %v %v", count1, count2, count3, count4)
		return sum == 30
	}
	return &conf
}
