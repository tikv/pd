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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cases

import (
	"fmt"

	"github.com/docker/go-units"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	sc "github.com/tikv/pd/tools/pd-simulator/simulator/config"
	"github.com/tikv/pd/tools/pd-simulator/simulator/info"
	"github.com/tikv/pd/tools/pd-simulator/simulator/simutil"
)

var hotReadStore uint64 = 1

func newHotRead(config *sc.SimConfig) *Case {
	var simCase Case
	totalStore := config.TotalStore
	totalRegion := config.TotalRegion
	replica := int(config.ServerConfig.Replication.MaxReplicas)
	allStores := make(map[uint64]struct{}, totalStore)
	// Initialize the cluster
	for range totalStore {
		id := simutil.IDAllocator.NextID()
		simCase.Stores = append(simCase.Stores, &Store{
			ID:     id,
			Status: metapb.StoreState_Up,
		})
		allStores[id] = struct{}{}
	}

	for i := range totalRegion {
		peers := make([]*metapb.Peer, 0, replica)
		for j := range replica {
			peers = append(peers, &metapb.Peer{
				Id:      simutil.IDAllocator.NextID(),
				StoreId: uint64((i+j)%totalStore + 1),
			})
		}
		simCase.Regions = append(simCase.Regions, Region{
			ID:     simutil.IDAllocator.NextID(),
			Peers:  peers,
			Leader: peers[0],
			Size:   96 * units.MiB,
			Keys:   960000,
		})
	}

	// select the first store as hot read store
	for store := range allStores {
		hotReadStore = store
		break
	}

	// Events description
	// select regions on `hotReadStore` as hot read regions.
	selectRegionNum := 4 * totalStore
	readFlow := make(map[uint64]int64, selectRegionNum)
	for _, r := range simCase.Regions {
		if r.Leader.GetStoreId() == hotReadStore {
			readFlow[r.ID] = 128 * units.MiB
			if len(readFlow) == selectRegionNum {
				break
			}
		}
	}
	e := &ReadFlowOnRegionDescriptor{}
	e.Step = func(int64) map[uint64]int64 {
		return readFlow
	}
	simCase.Events = []EventDescriptor{e}
	// Checker description
	simCase.Checker = func(stores []*metapb.Store, regions *core.RegionsInfo, _ []info.StoreStats) bool {
		for _, store := range stores {
			if store.NodeState == metapb.NodeState_Removed {
				if store.Id == hotReadStore {
					simutil.Logger.Error(fmt.Sprintf("hot store %d is removed", hotReadStore))
					return true
				}
				delete(allStores, store.GetId())
			}
		}

		leaderCount := make(map[uint64]int, len(allStores))
		for id := range readFlow {
			leaderStore := regions.GetRegion(id).GetLeader().GetStoreId()
			leaderCount[leaderStore]++
		}

		// check count diff < 2.
		var min, max uint64
		for i := range leaderCount {
			if leaderCount[i] > leaderCount[max] {
				max = i
			}
			if leaderCount[i] < leaderCount[min] {
				min = i
			}
		}
		return leaderCount[max]-leaderCount[min] < 2
	}

	return &simCase
}
