// Copyright 2019 TiKV Project Authors.
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

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/core"
	sc "github.com/tikv/pd/tools/pd-simulator/simulator/config"
	"github.com/tikv/pd/tools/pd-simulator/simulator/info"
	"github.com/tikv/pd/tools/pd-simulator/simulator/simutil"
)

func newRedundantBalanceRegion(config *sc.SimConfig) *Case {
	var simCase Case

	totalStore := config.TotalStore
	totalRegion := config.TotalRegion
	replica := int(config.ServerConfig.Replication.MaxReplicas)
	allStores := make(map[uint64]struct{}, totalStore)

	for i := range totalStore {
		s := &Store{
			ID:     simutil.IDAllocator.NextID(),
			Status: metapb.StoreState_Up,
		}
		if i%2 == 1 {
			s.HasExtraUsedSpace = true
		}
		simCase.Stores = append(simCase.Stores, s)
		allStores[s.ID] = struct{}{}
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
		})
	}

	storesLastUpdateTime := make(map[uint64]int64, totalStore)
	storeLastAvailable := make(map[uint64]uint64, totalStore)
	simCase.Checker = func(stores []*metapb.Store, _ *core.RegionsInfo, stats []info.StoreStats) bool {
		for _, store := range stores {
			if store.NodeState == metapb.NodeState_Removed {
				delete(allStores, store.GetId())
			}
		}

		curTime := time.Now().Unix()
		for storeID := range allStores {
			available := stats[storeID].GetAvailable()
			if curTime-storesLastUpdateTime[storeID] > 60 {
				if storeLastAvailable[storeID] != available {
					return false
				}
				if stats[storeID].ToCompactionSize != 0 {
					return false
				}
				storesLastUpdateTime[storeID] = curTime
				storeLastAvailable[storeID] = available
			} else {
				return false
			}
		}
		return true
	}
	return &simCase
}
