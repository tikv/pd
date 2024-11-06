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
	"github.com/docker/go-units"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/core"
	sc "github.com/tikv/pd/tools/pd-simulator/simulator/config"
	"github.com/tikv/pd/tools/pd-simulator/simulator/info"
	"github.com/tikv/pd/tools/pd-simulator/simulator/simutil"
)

func newRegionMerge(config *sc.SimConfig) *Case {
	var simCase Case
	totalStore := config.TotalStore
	totalRegion := config.TotalRegion
	replica := int(config.ServerConfig.Replication.MaxReplicas)
	allStores := make(map[uint64]struct{}, totalStore)

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
			Size:   10 * units.MiB,
			Keys:   100000,
		})
	}
	// Checker description
	mergeRatio := 4 // when max-merge-region-size is 20, per region will reach 40MB
	simCase.Checker = func(stores []*metapb.Store, regions *core.RegionsInfo, _ []info.StoreStats) bool {
		for _, store := range stores {
			if store.NodeState == metapb.NodeState_Removed {
				delete(allStores, store.GetId())
			}
		}

		currentPeerCount := 0
		for storeID := range allStores {
			currentPeerCount += regions.GetStoreRegionCount(storeID)
		}
		return isUniform(currentPeerCount, totalRegion*replica/mergeRatio)
	}
	return &simCase
}
