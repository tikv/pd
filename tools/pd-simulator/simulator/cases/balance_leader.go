// Copyright 2017 TiKV Project Authors.
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
	"github.com/docker/go-units"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/core"
	sc "github.com/tikv/pd/tools/pd-simulator/simulator/config"
	"github.com/tikv/pd/tools/pd-simulator/simulator/info"
	"github.com/tikv/pd/tools/pd-simulator/simulator/simutil"
)

func newBalanceLeader(config *sc.SimConfig) *Case {
	var simCase Case

	totalStore := config.TotalStore
	totalRegion := config.TotalRegion
	allStores := make(map[uint64]struct{}, totalStore)
	replica := int(config.ServerConfig.Replication.MaxReplicas)
	for range totalStore {
		id := simutil.IDAllocator.NextID()
		simCase.Stores = append(simCase.Stores, &Store{
			ID:     id,
			Status: metapb.StoreState_Up,
		})
		allStores[id] = struct{}{}
	}

	leaderStoreID := simCase.Stores[totalStore-1].ID
	for i := range totalRegion {
		peers := make([]*metapb.Peer, 0, replica)
		peers = append(peers, &metapb.Peer{
			Id:      simutil.IDAllocator.NextID(),
			StoreId: leaderStoreID,
		})
		for j := 1; j < replica; j++ {
			peers = append(peers, &metapb.Peer{
				Id:      simutil.IDAllocator.NextID(),
				StoreId: uint64((i+j)%(totalStore-1) + 1),
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

	simCase.Checker = func(stores []*metapb.Store, regions *core.RegionsInfo, _ []info.StoreStats) bool {
		for _, store := range stores {
			if store.NodeState == metapb.NodeState_Removed {
				delete(allStores, store.GetId())
			}
		}
		if len(allStores) == 0 {
			return false
		}
		for storeID := range allStores {
			leaderCount := regions.GetStoreLeaderCount(storeID)
			if !isUniform(leaderCount, totalRegion/len(allStores)) {
				return false
			}
		}
		return true
	}
	return &simCase
}
