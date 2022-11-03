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

	for i := 0; i < totalStore; i++ {
		s := &Store{
			ID:     simutil.IDAllocator.NextID(),
			Status: metapb.StoreState_Up,
		}
		if i%2 == 1 {
			s.HasExtraUsedSpace = true
		}
		simCase.Stores = append(simCase.Stores, s)
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
		})
	}

	storesLastUpdateTime := make([]int64, totalStore+1)
	storeLastAvailable := make([]uint64, totalStore+1)
	simCase.Checker = func(_ *core.RegionsInfo, stats []info.StoreStats) bool {
		curTime := time.Now().Unix()
		for i := 1; i <= totalStore; i++ {
			available := stats[i].GetAvailable()
			if curTime-storesLastUpdateTime[i] > 60 {
				if storeLastAvailable[i] != available {
					return false
				}
				if stats[i].ToCompactionSize != 0 {
					return false
				}
				storesLastUpdateTime[i] = curTime
				storeLastAvailable[i] = available
			} else {
				return false
			}
		}
		return true
	}
	return &simCase
}
