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

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/keyutil"
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

// fixedRandomMergeSource leaves target selection to the scheduler.
type fixedRandomMergeSource struct {
	*mockcluster.Cluster
}

func (c fixedRandomMergeSource) GetStores() []*core.StoreInfo {
	return []*core.StoreInfo{c.GetStore(1)}
}

func TestRandomMergeRetriesPlacementRoleRejection(t *testing.T) {
	for _, mode := range []string{"legacy", "joint"} {
		t.Run(mode, func(t *testing.T) {
			re := require.New(t)
			cancel, _, tc, oc := prepareSchedulersTest()
			defer cancel()
			tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.SwitchWitness))
			tc.SetEnableUseJointConsensus(mode == "joint")
			tc.SetEnablePlacementRules(true)
			for i, zone := range []string{"A", "B", "B", "A"} {
				tc.AddLabelsStore(uint64(i+1), 1, map[string]string{"zone": zone})
			}
			tc.SetStoreEvictLeader(4, true)
			re.NoError(tc.GetRuleManager().SetRules([]*placement.Rule{
				{
					GroupID: "pd", ID: "default", Role: placement.Leader, Count: 1,
					LabelConstraints: []placement.LabelConstraint{{Key: "zone", Op: placement.In, Values: []string{"A"}}},
				},
				{
					GroupID: "pd", ID: "voters-b", Index: 1, Role: placement.Voter, Count: 2,
					LabelConstraints: []placement.LabelConstraint{{Key: "zone", Op: placement.In, Values: []string{"B"}}},
				},
			}))
			tc.AddLeaderRegionWithRange(1, "", "a", 1, 2, 3)
			tc.AddLeaderRegionWithRange(2, "a", "b", 1, 2, 3)
			tc.AddLeaderRegionWithRange(3, "b", "c", 4, 2, 3)
			prev, source, next := tc.GetRegion(1), tc.GetRegion(2), tc.GetRegion(3)
			re.True(allowMerge(tc, source, prev))
			re.True(allowMerge(tc, source, next))
			_, err := operator.CreateMergeRegionOperator("test", tc, source, next, operator.OpMerge)
			re.ErrorIs(err, operator.ErrTargetRole)

			cluster := fixedRandomMergeSource{Cluster: tc}
			scheduler := newRandomMergeScheduler(oc, &randomMergeSchedulerConfig{
				Ranges: []keyutil.KeyRange{{StartKey: []byte("a"), EndKey: []byte("b")}},
			})
			// Two-way scheduling can always use the previous Region, whichever
			// neighbor the random selection tries first.
			tc.SetEnableOneWayMerge(false)
			for range 32 {
				ops, _ := scheduler.Schedule(cluster, false)
				re.Len(ops, 2)
				re.Equal(source.GetID(), ops[0].RegionID())
				re.Equal(prev.GetID(), ops[1].RegionID())
			}

			tc.SetEnableOneWayMerge(true)
			ops, _ := scheduler.Schedule(cluster, false)
			re.Empty(ops)

			// The other neighbor remains subject to the normal merge filters.
			tc.SetEnableOneWayMerge(false)
			tc.PutRegion(prev.Clone(core.WithPendingPeers([]*metapb.Peer{prev.GetStorePeer(2)})))
			for range 32 {
				ops, _ = scheduler.Schedule(cluster, false)
				re.Empty(ops)
			}
		})
	}
}
