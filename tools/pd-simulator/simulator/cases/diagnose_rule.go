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
	"time"

	"github.com/docker/go-units"
	"go.uber.org/zap"

	"github.com/pingcap/kvproto/pkg/metapb"

	pdHttp "github.com/tikv/pd/client/http"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/placement"
	sc "github.com/tikv/pd/tools/pd-simulator/simulator/config"
	"github.com/tikv/pd/tools/pd-simulator/simulator/info"
	"github.com/tikv/pd/tools/pd-simulator/simulator/simutil"
)

func newRule1(_ *sc.SimConfig) *Case {
	var simCase Case

	simCase.Rules = make([]*pdHttp.Rule, 0)
	simCase.Rules = append(simCase.Rules, &pdHttp.Rule{
		GroupID:     "test1",
		ID:          "test1",
		StartKeyHex: "",
		EndKeyHex:   "",
		Role:        pdHttp.Learner,
		Count:       1,
		LabelConstraints: []pdHttp.LabelConstraint{
			{
				Key:    "region",
				Op:     "in",
				Values: []string{"region1"},
			},
		},
		LocationLabels: []string{"host"},
	}, &pdHttp.Rule{
		GroupID:     placement.DefaultGroupID,
		ID:          placement.DefaultRuleID,
		StartKeyHex: "",
		EndKeyHex:   "",
		Role:        pdHttp.Voter,
		Count:       5,
		LabelConstraints: []pdHttp.LabelConstraint{
			{
				Key:    "region",
				Op:     "in",
				Values: []string{"region2"},
			},
		},
		LocationLabels: []string{"idc", "host"},
	})

	storeNum, regionNum := 9, 300
	allStores := make(map[uint64]struct{}, storeNum)
	for range storeNum {
		id := IDAllocator.nextID()
		simCase.Stores = append(simCase.Stores, &Store{
			ID:     id,
			Status: metapb.StoreState_Up,
		})
		allStores[id] = struct{}{}
	}
	simCase.Stores[0].Labels = []*metapb.StoreLabel{{Key: "region", Value: "region2"}, {Key: "idc", Value: "idc1"}}
	simCase.Stores[1].Labels = []*metapb.StoreLabel{{Key: "region", Value: "region2"}, {Key: "idc", Value: "idc1"}}
	simCase.Stores[2].Labels = []*metapb.StoreLabel{{Key: "region", Value: "region2"}, {Key: "idc", Value: "idc1"}}
	simCase.Stores[3].Labels = []*metapb.StoreLabel{{Key: "region", Value: "region2"}, {Key: "idc", Value: "idc1"}}
	simCase.Stores[4].Labels = []*metapb.StoreLabel{{Key: "region", Value: "region2"}, {Key: "idc", Value: "idc2"}}
	simCase.Stores[5].Labels = []*metapb.StoreLabel{{Key: "region", Value: "region2"}, {Key: "idc", Value: "idc3"}}
	simCase.Stores[6].Labels = []*metapb.StoreLabel{{Key: "region", Value: "region2"}, {Key: "idc", Value: "idc4"}}
	simCase.Stores[7].Labels = []*metapb.StoreLabel{{Key: "region", Value: "region2"}, {Key: "idc", Value: "idc5"}}
	simCase.Stores[8].Labels = []*metapb.StoreLabel{{Key: "region", Value: "region1"}}

	for i := range regionNum {
		peers := []*metapb.Peer{
			{Id: IDAllocator.nextID(), StoreId: uint64(i%(storeNum-5) + 5)},
			{Id: IDAllocator.nextID(), StoreId: uint64((i+1)%(storeNum-5) + 5)},
			{Id: IDAllocator.nextID(), StoreId: uint64((i+2)%(storeNum-5) + 5)},
			{Id: IDAllocator.nextID(), StoreId: uint64((i+3)%(storeNum-5) + 5)},
			{Id: IDAllocator.nextID(), StoreId: uint64(i%(storeNum-5) + 1)},
			{Id: IDAllocator.nextID(), StoreId: uint64(9), Role: metapb.PeerRole_Learner},
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

func newRule2(_ *sc.SimConfig) *Case {
	var simCase Case

	simCase.Rules = make([]*pdHttp.Rule, 0)
	simCase.Rules = append(simCase.Rules,
		&pdHttp.Rule{
			GroupID:     "test1",
			ID:          "test1",
			StartKeyHex: "",
			EndKeyHex:   "",
			Role:        pdHttp.Leader,
			Count:       1,
			LabelConstraints: []pdHttp.LabelConstraint{
				{
					Key:    "region",
					Op:     "in",
					Values: []string{"region1"},
				},
			},
		})

	storeNum, regionNum := 6, 300
	allStores := make(map[uint64]struct{}, storeNum)
	for range storeNum {
		id := IDAllocator.nextID()
		simCase.Stores = append(simCase.Stores, &Store{
			ID:     id,
			Status: metapb.StoreState_Up,
		})
		allStores[id] = struct{}{}
	}
	simCase.Stores[0].Labels = []*metapb.StoreLabel{{Key: "region", Value: "region1"}}
	simCase.Stores[1].Labels = []*metapb.StoreLabel{{Key: "region", Value: "region1"}}
	simCase.Stores[2].Labels = []*metapb.StoreLabel{{Key: "region", Value: "region1"}}
	simCase.Stores[3].Labels = []*metapb.StoreLabel{{Key: "region", Value: "region2"}}
	simCase.Stores[4].Labels = []*metapb.StoreLabel{{Key: "region", Value: "region2"}}
	simCase.Stores[5].Labels = []*metapb.StoreLabel{{Key: "region", Value: "region2"}}

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

	storesLastUpdateTime := make([]int64, storeNum+1)
	storeLastAvailable := make([]uint64, storeNum+1)
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
