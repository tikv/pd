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

package schedulers

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/versioninfo"
)

func TestRandomMergeSchedule(t *testing.T) {
	re := require.New(t)
	checkRandomMergeSchedule(re, false /* disable placement rules */)
	checkRandomMergeSchedule(re, true /* enable placement rules */)
}

func checkRandomMergeSchedule(re *require.Assertions, enablePlacementRules bool) {
	cancel, _, tc, oc := prepareSchedulersTest(true /* need to run stream*/)
	defer cancel()
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.SetEnablePlacementRules(enablePlacementRules)
	tc.SetMaxReplicasWithLabel(enablePlacementRules, 3)
	tc.SetMergeScheduleLimit(1)

	mb, err := CreateScheduler(types.RandomMergeScheduler, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.RandomMergeScheduler, []string{"", ""}))
	re.NoError(err)

	tc.AddRegionStore(1, 4)
	tc.AddLeaderRegion(1, 1)
	tc.AddLeaderRegion(2, 1)
	tc.AddLeaderRegion(3, 1)
	tc.AddLeaderRegion(4, 1)

	re.True(mb.IsScheduleAllowed(tc))
	ops, _ := mb.Schedule(tc, false)
	re.Empty(ops) // regions are not fully replicated

	tc.SetMaxReplicasWithLabel(enablePlacementRules, 1)
	ops, _ = mb.Schedule(tc, false)
	re.Len(ops, 2)
	re.NotZero(ops[0].Kind() & operator.OpMerge)
	re.NotZero(ops[1].Kind() & operator.OpMerge)

	oc.AddWaitingOperator(ops...)
	re.False(mb.IsScheduleAllowed(tc))
}
