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
	"math/rand"

	"github.com/docker/go-units"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/tools/pd-simulator/simulator/info"
	"github.com/tikv/pd/tools/pd-simulator/simulator/simutil"
)

func newRegionHeartbeat() *Case {
	var simCase Case

	storeNum := simutil.CaseConfigure.StoreNum
	regionNum := simutil.CaseConfigure.RegionNum
	if storeNum == 0 || regionNum == 0 {
		storeNum, regionNum = 100, 2000000
	}

	for i := 0; i < storeNum; i++ {
		simCase.Stores = append(simCase.Stores, &Store{
			ID:     IDAllocator.nextID(),
			Status: metapb.StoreState_Up,
		})
	}

	for i := 0; i < regionNum; i++ {
		peers := []*metapb.Peer{
			{Id: IDAllocator.nextID(), StoreId: uint64(i%storeNum + 1)},
			{Id: IDAllocator.nextID(), StoreId: uint64((i+1)%storeNum + 1)},
			{Id: IDAllocator.nextID(), StoreId: uint64((i+2)%storeNum + 1)},
		}
		simCase.Regions = append(simCase.Regions, Region{
			ID:              IDAllocator.nextID(),
			Peers:           peers,
			Leader:          peers[0],
			ApproximateSize: 96 * units.MiB,
			ApproximateKeys: 960000,
		})
	}
	rand.Seed(0) // Ensure consistent behavior multiple times
	slice := make([]int, regionNum)
	for i := range slice {
		slice[i] = i
	}
	pick := func(ratio float64) []int {
		rand.Shuffle(regionNum, func(i, j int) {
			slice[i], slice[j] = slice[j], slice[i]
		})
		return append(slice[:0:0], slice[0:int(float64(regionNum)*ratio)]...)
	}
	var bytesUnit uint64 = 1 << 23 // 8MB
	var keysUint uint64 = 1 << 13  // 8K
	updateSpace := pick(0.6)
	updateFlow := pick(0.4)

	e := &RandomSchedulingDescriptor{}
	e.Step = func(tick int64) map[uint64]Region {
		updateRes := make(map[uint64]Region)
		// update space
		for _, i := range updateSpace {
			region := simCase.Regions[i]
			region.ApproximateSize += bytesUnit
			region.ApproximateKeys += keysUint
			updateRes[region.ID] = region
		}
		// update flow
		for _, i := range updateFlow {
			region := simCase.Regions[i]
			region.BytesWritten += bytesUnit
			region.BytesRead += bytesUnit
			region.KeysWritten += keysUint
			region.KeysRead += keysUint
			updateRes[region.ID] = region
		}
		return updateRes
	}
	simCase.Events = []EventDescriptor{e}
	simCase.Checker = func(regions *core.RegionsInfo, stats []info.StoreStats) bool {
		return false
	}
	return &simCase
}
