// Copyright 2022 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cases

import (
	"fmt"
	"time"

	"github.com/docker/go-units"
	"go.uber.org/zap"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	sc "github.com/tikv/pd/tools/pd-simulator/simulator/config"
	"github.com/tikv/pd/tools/pd-simulator/simulator/info"
	"github.com/tikv/pd/tools/pd-simulator/simulator/simutil"
)

func newLabelNotMatch1(_ *sc.SimConfig) *Case {
	var simCase Case
	simCase.Labels = []string{"host"}

	num1, num2 := 3, 1
	storeNum, regionNum := num1+num2, 200
	allStores := make(map[uint64]struct{}, storeNum+1)
	for range num1 {
		id := IDAllocator.nextID()
		simCase.Stores = append(simCase.Stores, &Store{
			ID:     id,
			Status: metapb.StoreState_Up,
			Labels: []*metapb.StoreLabel{{Key: "host", Value: fmt.Sprintf("host%d", id)}},
		})
		allStores[id] = struct{}{}
	}
	id := IDAllocator.nextID()
	simCase.Stores = append(simCase.Stores, &Store{
		ID:     id,
		Status: metapb.StoreState_Up,
	})
	allStores[id] = struct{}{}

	for i := range regionNum {
		peers := []*metapb.Peer{
			{Id: IDAllocator.nextID(), StoreId: uint64(i%num1 + 1)},
			{Id: IDAllocator.nextID(), StoreId: uint64((i+1)%num1 + 1)},
			{Id: IDAllocator.nextID(), StoreId: uint64((i+2)%num1 + 1)},
		}
		simCase.Regions = append(simCase.Regions, Region{
			ID:     IDAllocator.nextID(),
			Peers:  peers,
			Leader: peers[0],
			Size:   96 * units.MiB,
			Keys:   960000,
		})
	}

	storesLastUpdateTime := make(map[uint64]int64, storeNum+1)
	storeLastAvailable := make(map[uint64]uint64, storeNum+1)
	simCase.Checker = func(stores []*metapb.Store, _ *core.RegionsInfo, stats []info.StoreStats) bool {
		for _, store := range stores {
			if store.NodeState == metapb.NodeState_Removed {
				delete(allStores, store.GetId())
			}
		}

		res := true
		curTime := time.Now().Unix()
		storesAvailable := make([]uint64, 0, storeNum+1)
		for storeID := range allStores {
			available := stats[storeID].GetAvailable()
			storesAvailable = append(storesAvailable, available)
			if curTime-storesLastUpdateTime[storeID] > 360 {
				if storeLastAvailable[storeID] != available {
					res = false
				}
				if stats[storeID].ToCompactionSize != 0 {
					res = false
				}
				storesLastUpdateTime[storeID] = curTime
				storeLastAvailable[storeID] = available
			} else {
				res = false
			}
		}
		simutil.Logger.Info("current counts", zap.Uint64s("storesAvailable", storesAvailable))
		return res
	}
	return &simCase
}

func newLabelIsolation1(_ *sc.SimConfig) *Case {
	var simCase Case
	simCase.Labels = []string{"host"}

	num1, num2 := 2, 2
	storeNum, regionNum := num1+num2, 300
	allStores := make(map[uint64]struct{}, storeNum+1)
	for range num1 {
		id := IDAllocator.nextID()
		simCase.Stores = append(simCase.Stores, &Store{
			ID:     id,
			Status: metapb.StoreState_Up,
			Labels: []*metapb.StoreLabel{{Key: "host", Value: fmt.Sprintf("host%d", id)}},
		})
		allStores[id] = struct{}{}
	}
	for range num2 {
		id := IDAllocator.nextID()
		simCase.Stores = append(simCase.Stores, &Store{
			ID:     id,
			Status: metapb.StoreState_Up,
			Labels: []*metapb.StoreLabel{{Key: "host", Value: fmt.Sprintf("host%d", id)}},
		})
		allStores[id] = struct{}{}
	}

	for i := range regionNum {
		peers := []*metapb.Peer{
			{Id: IDAllocator.nextID(), StoreId: uint64(i%num1 + 1)},
			{Id: IDAllocator.nextID(), StoreId: uint64((i+1)%num1 + 1)},
			{Id: IDAllocator.nextID(), StoreId: uint64(i%num2 + num1 + 1)},
		}
		simCase.Regions = append(simCase.Regions, Region{
			ID:     IDAllocator.nextID(),
			Peers:  peers,
			Leader: peers[0],
			Size:   96 * units.MiB,
			Keys:   960000,
		})
	}

	storesLastUpdateTime := make(map[uint64]int64, storeNum)
	storeLastAvailable := make(map[uint64]uint64, storeNum)
	simCase.Checker = func(stores []*metapb.Store, _ *core.RegionsInfo, stats []info.StoreStats) bool {
		for _, store := range stores {
			if store.NodeState == metapb.NodeState_Removed {
				delete(allStores, store.GetId())
			}
		}

		res := true
		curTime := time.Now().Unix()
		storesAvailable := make([]uint64, 0, storeNum+1)
		for storeID := range allStores {
			available := stats[storeID].GetAvailable()
			storesAvailable = append(storesAvailable, available)
			if curTime-storesLastUpdateTime[storeID] > 360 {
				if storeLastAvailable[storeID] != available {
					res = false
				}
				if stats[storeID].ToCompactionSize != 0 {
					res = false
				}
				storesLastUpdateTime[storeID] = curTime
				storeLastAvailable[storeID] = available
			} else {
				res = false
			}
		}
		simutil.Logger.Info("current counts", zap.Uint64s("storesAvailable", storesAvailable))
		return res
	}
	return &simCase
}

func newLabelIsolation2(_ *sc.SimConfig) *Case {
	var simCase Case
	simCase.Labels = []string{"dc", "zone", "host"}

	storeNum, regionNum := 5, 200
	allStores := make(map[uint64]struct{}, storeNum)
	for range storeNum {
		id := IDAllocator.nextID()
		simCase.Stores = append(simCase.Stores, &Store{
			ID:     id,
			Status: metapb.StoreState_Up,
		})
		allStores[id] = struct{}{}
	}
	simCase.Stores[0].Labels = []*metapb.StoreLabel{{Key: "dc", Value: "dc1"}, {Key: "zone", Value: "zone1"}, {Key: "host", Value: "host1"}}
	simCase.Stores[1].Labels = []*metapb.StoreLabel{{Key: "dc", Value: "dc1"}, {Key: "zone", Value: "zone1"}, {Key: "host", Value: "host2"}}
	simCase.Stores[2].Labels = []*metapb.StoreLabel{{Key: "dc", Value: "dc1"}, {Key: "zone", Value: "zone2"}, {Key: "host", Value: "host3"}}
	simCase.Stores[3].Labels = []*metapb.StoreLabel{{Key: "dc", Value: "dc1"}, {Key: "zone", Value: "zone2"}, {Key: "host", Value: "host4"}}
	simCase.Stores[4].Labels = []*metapb.StoreLabel{{Key: "dc", Value: "dc1"}, {Key: "zone", Value: "zone3"}, {Key: "host", Value: "host5"}}

	for i := range regionNum {
		peers := []*metapb.Peer{
			{Id: IDAllocator.nextID(), StoreId: uint64(i%storeNum + 1)},
			{Id: IDAllocator.nextID(), StoreId: uint64((i+1)%storeNum + 1)},
			{Id: IDAllocator.nextID(), StoreId: uint64((i+2)%storeNum + 1)},
		}
		simCase.Regions = append(simCase.Regions, Region{
			ID:     IDAllocator.nextID(),
			Peers:  peers,
			Leader: peers[0],
			Size:   96 * units.MiB,
			Keys:   960000,
		})
	}

	storesLastUpdateTime := make(map[uint64]int64, storeNum)
	storeLastAvailable := make(map[uint64]uint64, storeNum)
	simCase.Checker = func(stores []*metapb.Store, _ *core.RegionsInfo, stats []info.StoreStats) bool {
		for _, store := range stores {
			if store.NodeState == metapb.NodeState_Removed {
				delete(allStores, store.GetId())
			}
		}

		res := true
		curTime := time.Now().Unix()
		storesAvailable := make([]uint64, 0, storeNum+1)
		for storeID := range allStores {
			available := stats[storeID].GetAvailable()
			storesAvailable = append(storesAvailable, available)
			if curTime-storesLastUpdateTime[storeID] > 360 {
				if storeLastAvailable[storeID] != available {
					res = false
				}
				if stats[storeID].ToCompactionSize != 0 {
					res = false
				}
				storesLastUpdateTime[storeID] = curTime
				storeLastAvailable[storeID] = available
			} else {
				res = false
			}
		}
		simutil.Logger.Info("current counts", zap.Uint64s("storesAvailable", storesAvailable))
		return res
	}
	return &simCase
}
