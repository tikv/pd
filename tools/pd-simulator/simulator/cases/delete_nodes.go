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
	"math/rand"

	"github.com/docker/go-units"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/core"
	sc "github.com/tikv/pd/tools/pd-simulator/simulator/config"
	"github.com/tikv/pd/tools/pd-simulator/simulator/info"
)

func newDeleteNodes(config *sc.SimConfig) *Case {
	var simCase Case

	totalStore := config.TotalStore
	totalRegion := config.TotalRegion
	replica := int(config.ServerConfig.Replication.MaxReplicas)
	noEmptyStoreNum := totalStore - 1
	for i := 1; i <= totalStore; i++ {
		simCase.Stores = append(simCase.Stores, &Store{
			ID:     IDAllocator.nextID(),
			Status: metapb.StoreState_Up,
		})
	}

	for i := 0; i < totalRegion; i++ {
		peers := make([]*metapb.Peer, 0, replica)
		for j := 0; j < replica; j++ {
			peers = append(peers, &metapb.Peer{
				Id:      IDAllocator.nextID(),
				StoreId: uint64((i+j)%totalStore + 1),
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

	ids := make([]uint64, 0, len(simCase.Stores))
	for _, store := range simCase.Stores {
		ids = append(ids, store.ID)
	}

	currentStoreCount := totalStore
	e := &DeleteNodesDescriptor{}
	e.Step = func(tick int64) uint64 {
		if currentStoreCount > noEmptyStoreNum && tick%100 == 0 {
			idx := rand.Intn(currentStoreCount)
			currentStoreCount--
			nodeID := ids[idx]
			ids = append(ids[:idx], ids[idx+1:]...)
			return nodeID
		}
		return 0
	}
	simCase.Events = []EventDescriptor{e}

	simCase.Checker = func(regions *core.RegionsInfo, _ []info.StoreStats) bool {
		if currentStoreCount != noEmptyStoreNum {
			return false
		}
		for _, i := range ids {
			leaderCount := regions.GetStoreLeaderCount(i)
			peerCount := regions.GetStoreRegionCount(i)
			if !isUniform(leaderCount, totalRegion/noEmptyStoreNum) {
				return false
			}
			if !isUniform(peerCount, totalRegion*replica/noEmptyStoreNum) {
				return false
			}
		}
		return true
	}
	return &simCase
}
