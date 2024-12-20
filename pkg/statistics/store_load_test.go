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

	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/statistics/utils"
)

func TestHistoryLoads(t *testing.T) {
	re := require.New(t)
	historyLoads := NewStoreHistoryLoads(utils.DimLen, DefaultHistorySampleDuration, 0)
	loads := []float64{1.0, 2.0, 3.0}
	rwTp := utils.Read
	kind := constant.LeaderKind
	historyLoads.Add(1, rwTp, kind, loads)
	re.Len(historyLoads.Get(1, rwTp, kind)[0], 10)

	expectLoads := make([][]float64, utils.DimLen)
	for i := range loads {
		expectLoads[i] = make([]float64, 10)
	}
	for i := range 10 {
		historyLoads.Add(1, rwTp, kind, loads)
		expectLoads[utils.ByteDim][i] = 1.0
		expectLoads[utils.KeyDim][i] = 2.0
		expectLoads[utils.QueryDim][i] = 3.0
	}
	re.EqualValues(expectLoads, historyLoads.Get(1, rwTp, kind))

	historyLoads = NewStoreHistoryLoads(utils.DimLen, time.Millisecond, time.Millisecond)
	historyLoads.Add(1, rwTp, kind, loads)
	re.Len(historyLoads.Get(1, rwTp, kind)[0], 1)

	historyLoads = NewStoreHistoryLoads(utils.DimLen, time.Millisecond, time.Second)
	historyLoads.Add(1, rwTp, kind, loads)
	re.Empty(historyLoads.Get(1, rwTp, kind)[0])

	historyLoads = NewStoreHistoryLoads(utils.DimLen, 0, time.Second)
	historyLoads.Add(1, rwTp, kind, loads)
	re.Empty(historyLoads.Get(1, rwTp, kind)[0])

	historyLoads = NewStoreHistoryLoads(utils.DimLen, 0, 0)
	historyLoads.Add(1, rwTp, kind, loads)
	re.Empty(historyLoads.Get(1, rwTp, kind)[0])
}
