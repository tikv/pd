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

func newMakeupDownReplicas(config *sc.SimConfig) *Case {
	var simCase Case
	totalStore := config.TotalStore
	totalRegion := config.TotalRegion
	replica := int(config.ServerConfig.Replication.MaxReplicas)

	noEmptyStoreNum := totalStore - 1
	for range totalStore {
		simCase.Stores = append(simCase.Stores, &Store{
			ID:     simutil.IDAllocator.NextID(),
			Status: metapb.StoreState_Up,
		})
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

	numNodes := totalStore
	down := false
	e := &DeleteNodesDescriptor{}
	e.Step = func(tick int64) uint64 {
		if numNodes > noEmptyStoreNum && tick%100 == 0 {
			numNodes--
			return uint64(1)
		}
		if tick == 300 {
			down = true
		}
		return 0
	}
	simCase.Events = []EventDescriptor{e}

	simCase.Checker = func(_ []*metapb.Store, regions *core.RegionsInfo, _ []info.StoreStats) bool {
		if !down {
			return false
		}
		for i := 1; i <= totalStore; i++ {
			peerCount := regions.GetStoreRegionCount(uint64(i))
			if isUniform(peerCount, replica*totalRegion/noEmptyStoreNum) {
				return false
			}
		}
		return true
	}
	return &simCase
}
