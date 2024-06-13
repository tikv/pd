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

func newHotRead(config *sc.SimConfig) *Case {
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
	// select regions on store 1 as hot read regions.
	selectRegionNum := 4 * totalStore
	readFlow := make(map[uint64]int64, selectRegionNum)
	for _, r := range simCase.Regions {
		if r.Leader.GetStoreId() == 1 {
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
	simCase.Checker = func(regions *core.RegionsInfo, _ []info.StoreStats) bool {
		leaderCount := make([]int, totalStore)
		for id := range readFlow {
			leaderStore := regions.GetRegion(id).GetLeader().GetStoreId()
			leaderCount[int(leaderStore-1)]++
		}

		// check count diff < 2.
		var min, max int
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
