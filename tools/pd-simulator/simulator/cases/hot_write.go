// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package cases

import (
	"math/rand"

	"go.uber.org/zap"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/tools/pd-simulator/simulator/info"
	"github.com/pingcap/pd/tools/pd-simulator/simulator/simutil"
)

func newHotWrite() *Case {
	var simCase Case

	// Initialize the cluster
	for i := 1; i <= 10; i++ {
		simCase.Stores = append(simCase.Stores, &Store{
			ID:        IDAllocator.nextID(),
			Status:    metapb.StoreState_Up,
			Capacity:  1 * TB,
			Available: 900 * GB,
			Version:   "2.1.0",
		})
	}

	for i := 0; i < 500; i++ {
		storeIDs := rand.Perm(10)
		peers := []*metapb.Peer{
			{Id: IDAllocator.nextID(), StoreId: uint64(storeIDs[0] + 1)},
			{Id: IDAllocator.nextID(), StoreId: uint64(storeIDs[1] + 1)},
			{Id: IDAllocator.nextID(), StoreId: uint64(storeIDs[2] + 1)},
		}
		simCase.Regions = append(simCase.Regions, Region{
			ID:     IDAllocator.nextID(),
			Peers:  peers,
			Leader: peers[0],
			Size:   96 * MB,
			Keys:   960000,
		})
	}

	// Events description
	// select 5 regions on store 1 as hot write regions.
	writeFlow := make(map[uint64]int64, 5)
	for _, r := range simCase.Regions {
		if r.Leader.GetStoreId() == 1 {
			writeFlow[r.ID] = 2 * MB
			if len(writeFlow) == 5 {
				break
			}
		}
	}
	e := &WriteFlowOnRegionDescriptor{}
	e.Step = func(tick int64) map[uint64]int64 {
		return writeFlow
	}

	simCase.Events = []EventDescriptor{e}

	// Checker description
	simCase.Checker = func(regions *core.RegionsInfo, stats []info.StoreStats) bool {
		var leaderCount, peerCount [10]int
		for id := range writeFlow {
			region := regions.GetRegion(id)
			leaderCount[int(region.GetLeader().GetStoreId()-1)]++
			for _, p := range region.GetPeers() {
				peerCount[int(p.GetStoreId()-1)]++
			}
		}
		simutil.Logger.Info("current hot region counts", zap.Reflect("leader", leaderCount), zap.Reflect("peer", peerCount))

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
