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

func newHotWrite(config *sc.SimConfig) *Case {
	var simCase Case
	totalStore := config.TotalStore
	totalRegion := config.TotalRegion
	replica := int(config.ServerConfig.Replication.MaxReplicas)

	// Initialize the cluster
	for i := 0; i < totalStore; i++ {
		simCase.Stores = append(simCase.Stores, &Store{
			ID:     simutil.IDAllocator.NextID(),
			Status: metapb.StoreState_Up,
		})
	}

	for i := 0; i < totalRegion; i++ {
		peers := make([]*metapb.Peer, 0, replica)
		for j := 0; j < replica; j++ {
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

	// Events description
	// select regions on store 1 as hot write regions.
	selectStoreNum := totalStore
	writeFlow := make(map[uint64]int64, selectStoreNum)
	for _, r := range simCase.Regions {
		if r.Leader.GetStoreId() == 1 {
			writeFlow[r.ID] = 2 * units.MiB
			if len(writeFlow) == selectStoreNum {
				break
			}
		}
	}
	e := &WriteFlowOnRegionDescriptor{}
	e.Step = func(int64) map[uint64]int64 {
		return writeFlow
	}

	simCase.Events = []EventDescriptor{e}

	// Checker description
	simCase.Checker = func(regions *core.RegionsInfo, _ []info.StoreStats) bool {
		leaderCount := make([]int, totalStore)
		peerCount := make([]int, totalStore)
		for id := range writeFlow {
			region := regions.GetRegion(id)
			leaderCount[int(region.GetLeader().GetStoreId()-1)]++
			for _, p := range region.GetPeers() {
				peerCount[int(p.GetStoreId()-1)]++
			}
		}

		// check count diff <= 2.
		var minLeader, maxLeader, minPeer, maxPeer int
		for i := range leaderCount {
			if leaderCount[i] > leaderCount[maxLeader] {
				maxLeader = i
			}
			if leaderCount[i] < leaderCount[minLeader] {
				minLeader = i
			}
			if peerCount[i] > peerCount[maxPeer] {
				maxPeer = i
			}
			if peerCount[i] < peerCount[minPeer] {
				minPeer = i
			}
		}
		return leaderCount[maxLeader]-leaderCount[minLeader] <= 2 && peerCount[maxPeer]-peerCount[minPeer] <= 2
	}

	return &simCase
}
