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
	"github.com/pingcap/pd/server/core"
	log "github.com/sirupsen/logrus"
)

func newAddNodes() *Conf {
	var conf Conf

	for i := 1; i <= 3; i++ {
		conf.Stores = append(conf.Stores, Store{
			ID:        uint64(i),
			Status:    metapb.StoreState_Up,
			Capacity:  10 * gb,
			Available: 9 * gb,
		})
	}

	var id idAllocator
	id.setMaxID(100)
	for i := 0; i < 1000; i++ {
		peers := []*metapb.Peer{
			{Id: id.nextID(), StoreId: 1},
			{Id: id.nextID(), StoreId: 2},
			{Id: id.nextID(), StoreId: 3},
		}
		conf.Regions = append(conf.Regions, Region{
			ID:     id.nextID(),
			Peers:  peers,
			Leader: peers[0],
			Size:   96 * mb,
		})
	}
	conf.MaxID = id.maxID
	checkers := make([]CheckerFunc, 2)
	checkers[0] = func(regions *core.RegionsInfo) (ChangeTask, bool) {
		count1 := regions.GetStoreLeaderCount(1)
		count2 := regions.GetStoreLeaderCount(2)
		count3 := regions.GetStoreLeaderCount(3)
		log.Infof("leader counts: %v %v %v", count1, count2, count3)
		task := &AddNode{[]uint64{4, 5}}
		return task, count1 <= 350 &&
			count2 >= 300 &&
			count3 >= 300
	}

	checkers[1] = func(regions *core.RegionsInfo) (ChangeTask, bool) {
		count1 := regions.GetStoreLeaderCount(1)
		count2 := regions.GetStoreLeaderCount(2)
		count3 := regions.GetStoreLeaderCount(3)
		count4 := regions.GetStoreLeaderCount(4)
		count5 := regions.GetStoreLeaderCount(5)
		log.Infof("leader counts: %v %v %v %v %v", count1, count2, count3, count4, count5)

		return nil, count1 <= 220 &&
			count2 >= 190 &&
			count3 >= 190 &&
			count4 >= 190 &&
			count5 >= 190
	}
	index := 0
	conf.Checker = func(regions *core.RegionsInfo) (ChangeTask, bool) {
		if index == 1 {
			return checkers[index](regions)
		}
		task, res := checkers[index](regions)
		if res {
			index++
			return task, false
		}
		return nil, false
	}
	return &conf
}
