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
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/tools/pd-simulator/simulator/info"
	"github.com/tikv/pd/tools/pd-simulator/simulator/simutil"
)

func newScaleInOut() *Case {
	var simCase Case

	storeNum := simutil.CaseConfigure.StoreNum
	regionNum := simutil.CaseConfigure.RegionNum
	if storeNum == 0 || regionNum == 0 {
		storeNum, regionNum = 6, 4000
	}

	for i := 0; i < storeNum; i++ {
		s := &Store{
			ID:     IDAllocator.nextID(),
			Status: metapb.StoreState_Up,
		}
		if i%2 == 1 {
			s.HasExtraUsedSpace = true
		}
		simCase.Stores = append(simCase.Stores, s)
	}

	for i := 0; i < regionNum; i++ {
		peers := []*metapb.Peer{
			{Id: IDAllocator.nextID(), StoreId: uint64(i%storeNum + 1)},
			{Id: IDAllocator.nextID(), StoreId: uint64((i+1)%storeNum + 1)},
			{Id: IDAllocator.nextID(), StoreId: uint64((i+2)%storeNum + 1)},
		}
		simCase.Regions = append(simCase.Regions, Region{
			ID:     IDAllocator.nextID(),
			Peers:  peers,
			Leader: peers[0],
		})
	}

	scaleInTick := int64(regionNum * 3 / storeNum)
	addEvent := &AddNodesDescriptor{}
	addEvent.Step = func(tick int64) uint64 {
		if tick == scaleInTick {
			return uint64(storeNum + 1)
		}
		return 0
	}

	removeEvent := &DeleteNodesDescriptor{}
	removeEvent.Step = func(tick int64) uint64 {
		if tick == scaleInTick*2 {
			return uint64(storeNum + 1)
		}
		return 0
	}
	simCase.Events = []EventDescriptor{addEvent, removeEvent}

	simCase.Checker = func(regions *core.RegionsInfo, stats []info.StoreStats) bool {
		return false
	}
	return &simCase
}
