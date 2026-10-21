// Copyright 2023 TiKV Project Authors.
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

package statistics

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/statistics/utils"
)

func TestHistoryLoads(t *testing.T) {
	re := require.New(t)
	historyLoads := NewStoreHistoryLoads(DefaultHistorySampleDuration, 0)
	loads := Loads{1.0, 2.0, 3.0}
	rwTp := utils.Read
	kind := constant.LeaderKind
	historyLoads.Add(1, rwTp, kind, loads)
	re.Len(historyLoads.Get(1, rwTp, kind)[0], 10)

	var expectLoads HistoryLoads
	for i := range loads {
		expectLoads[i] = make([]float64, 10)
	}
	for i := range 10 {
		historyLoads.Add(1, rwTp, kind, loads)
		expectLoads[utils.ByteDim][i] = 1.0
		expectLoads[utils.KeyDim][i] = 2.0
		expectLoads[utils.QueryDim][i] = 3.0
	}
	re.Equal(expectLoads, historyLoads.Get(1, rwTp, kind))

	historyLoads = NewStoreHistoryLoads(time.Millisecond, time.Millisecond)
	historyLoads.Add(1, rwTp, kind, loads)
	re.Len(historyLoads.Get(1, rwTp, kind)[0], 1)

	historyLoads = NewStoreHistoryLoads(time.Millisecond, time.Second)
	historyLoads.Add(1, rwTp, kind, loads)
	re.Empty(historyLoads.Get(1, rwTp, kind)[0])

	historyLoads = NewStoreHistoryLoads(0, time.Second)
	historyLoads.Add(1, rwTp, kind, loads)
	re.Empty(historyLoads.Get(1, rwTp, kind)[0])

	historyLoads = NewStoreHistoryLoads(0, 0)
	historyLoads.Add(1, rwTp, kind, loads)
	re.Empty(historyLoads.Get(1, rwTp, kind)[0])
}

func TestHistoryGCDoesNotReaddRemovedStore(t *testing.T) {
	re := require.New(t)
	history := NewStoreHistoryLoads(time.Minute, 0)
	rw := utils.Read
	kind := constant.RegionKind
	storeID := uint64(1)

	live := core.NewStoreInfo(&metapb.Store{
		Id:        storeID,
		NodeState: metapb.NodeState_Serving,
	})
	infos := map[uint64]*StoreSummaryInfo{
		storeID: {StoreInfo: live},
	}
	loads := map[uint64]StoreKindLoads{
		storeID: {1, 0, 0, 0, 0},
	}

	SummaryStoresLoad(infos, loads, history, nil, false, rw, kind)
	re.NotEmpty(history.Get(storeID, rw, kind)[0])

	removed := live.Clone(core.SetStoreState(metapb.StoreState_Tombstone))
	history.GC([]*core.StoreInfo{removed})
	re.Empty(history.Get(storeID, rw, kind)[0])

	SummaryStoresLoad(
		map[uint64]*StoreSummaryInfo{storeID: {StoreInfo: removed}},
		loads, history, nil, false, rw, kind,
	)
	re.Empty(history.Get(storeID, rw, kind)[0])
}
