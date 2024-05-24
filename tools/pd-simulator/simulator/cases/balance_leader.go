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
)

func newBalanceLeader(config *sc.SimConfig) *Case {
	var simCase Case

	totalStore := config.TotalStore
	totalRegion := config.TotalRegion
	replica := int(config.ServerConfig.Replication.MaxReplicas)
	for i := 0; i < totalStore; i++ {
		simCase.Stores = append(simCase.Stores, &Store{
			ID:     IDAllocator.nextID(),
			Status: metapb.StoreState_Up,
		})
	}

	leaderStoreID := simCase.Stores[totalStore-1].ID
	for i := 0; i < totalRegion; i++ {
		peers := make([]*metapb.Peer, 0, replica)
		peers = append(peers, &metapb.Peer{
			Id:      IDAllocator.nextID(),
			StoreId: leaderStoreID,
		})
		for j := 1; j < replica; j++ {
			peers = append(peers, &metapb.Peer{
				Id:      IDAllocator.nextID(),
				StoreId: uint64((i+j)%(totalStore-1) + 1),
			})
		}
		simCase.Regions = append(simCase.Regions, Region{
			ID:     IDAllocator.nextID(),
			Peers:  peers,
			Leader: peers[0],
			Size:   96 * units.MiB,
			Keys:   960000,
		})
	}

	simCase.Checker = func(regions *core.RegionsInfo, _ []info.StoreStats) bool {
		for i := 1; i <= totalStore; i++ {
			leaderCount := regions.GetStoreLeaderCount(uint64(i))
			if !isUniform(leaderCount, totalRegion/totalStore) {
				return false
			}
		}
		return true
	}
	return &simCase
}
