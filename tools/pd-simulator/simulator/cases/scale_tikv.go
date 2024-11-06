// Copyright 2024 TiKV Project Authors.
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
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/core"
	sc "github.com/tikv/pd/tools/pd-simulator/simulator/config"
	"github.com/tikv/pd/tools/pd-simulator/simulator/info"
	"github.com/tikv/pd/tools/pd-simulator/simulator/simutil"
)

func newScaleInOut(config *sc.SimConfig) *Case {
	var simCase Case

	totalStore := config.TotalStore
	totalRegion := config.TotalRegion
	replica := int(config.ServerConfig.Replication.MaxReplicas)
	if totalStore == 0 || totalRegion == 0 {
		totalStore, totalRegion = 6, 4000
	}

	for i := range totalStore {
		s := &Store{
			ID:     IDAllocator.nextID(),
			Status: metapb.StoreState_Up,
		}
		if i%2 == 1 {
			s.HasExtraUsedSpace = true
		}
		simCase.Stores = append(simCase.Stores, s)
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
			ID:     IDAllocator.nextID(),
			Peers:  peers,
			Leader: peers[0],
		})
	}

	scaleInTick := int64(totalRegion * 3 / totalStore)
	addEvent := &AddNodesDescriptor{}
	addEvent.Step = func(tick int64) uint64 {
		if tick == scaleInTick {
			return uint64(totalStore + 1)
		}
		return 0
	}

	removeEvent := &DeleteNodesDescriptor{}
	removeEvent.Step = func(tick int64) uint64 {
		if tick == scaleInTick*2 {
			return uint64(totalStore + 1)
		}
		return 0
	}
	simCase.Events = []EventDescriptor{addEvent, removeEvent}

	simCase.Checker = func([]*metapb.Store, *core.RegionsInfo, []info.StoreStats) bool {
		return false
	}
	return &simCase
}
