// Copyright 2026 TiKV Project Authors.
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

package diagnostics

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

type testCluster struct {
	*mockcluster.Cluster
	halted   bool
	scanHook func()
}

func (c *testCluster) IsSchedulingHalted() bool {
	return c.halted
}

func (c *testCluster) ScanRegions(startKey, endKey []byte, limit int) []*core.RegionInfo {
	if c.scanHook != nil {
		c.scanHook()
	}
	return c.Cluster.ScanRegions(startKey, endKey, limit)
}

func newTestCluster(t *testing.T) *testCluster {
	t.Helper()
	opts := mockconfig.NewTestOptions()
	opts.SetPlacementRuleEnabled(true)
	opts.SetAffinityScheduleLimit(4)
	cluster := &testCluster{Cluster: mockcluster.NewCluster(t.Context(), opts)}
	for i := uint64(1); i <= 5; i++ {
		cluster.AddLabelsStore(i, 0, map[string]string{"role": "full"})
		cluster.PutStore(cluster.GetStore(i).Clone(core.SetNodeState(metapb.NodeState_Serving)))
	}
	return cluster
}

func createGroup(t *testing.T, cluster *testCluster, id string, ranges []keyutil.KeyRange, leader uint64, voters []uint64) {
	t.Helper()
	re := require.New(t)
	re.NoError(cluster.AffinityManager.CreateAffinityGroups([]affinity.GroupKeyRanges{{GroupID: id, KeyRanges: ranges}}))
	if leader != 0 {
		_, err := cluster.AffinityManager.UpdateAffinityGroupPeers(id, leader, voters)
		re.NoError(err)
	}
}

type peerSpec struct {
	store   uint64
	learner bool
	witness bool
	pending bool
}

func putRegion(
	cluster *testCluster,
	id uint64,
	start, end string,
	leaderStore uint64,
	size int64,
	specs ...peerSpec,
) {
	peers := make([]*metapb.Peer, 0, len(specs))
	pending := make([]*metapb.Peer, 0)
	var leader *metapb.Peer
	for i, spec := range specs {
		peer := &metapb.Peer{Id: id*10 + uint64(i), StoreId: spec.store, IsWitness: spec.witness}
		if spec.learner {
			peer.Role = metapb.PeerRole_Learner
		}
		peers = append(peers, peer)
		if spec.pending {
			pending = append(pending, peer)
		}
		if spec.store == leaderStore && leader == nil {
			leader = peer
		}
	}
	region := core.NewRegionInfo(&metapb.Region{
		Id:       id,
		StartKey: []byte(start),
		EndKey:   []byte(end),
		Peers:    peers,
	}, leader, core.SetApproximateSize(size), core.WithPendingPeers(pending))
	cluster.PutRegion(region)
}

func fullRange() []keyutil.KeyRange {
	return []keyutil.KeyRange{{StartKey: []byte("a"), EndKey: []byte("z")}}
}

func collect(t *testing.T, cluster *testCluster, operators *operator.Controller, ids ...string) map[string]*affinity.GroupState {
	t.Helper()
	states, err := CollectGroupStates(t.Context(), cluster.AffinityManager, cluster, operators, ids)
	require.NoError(t, err)
	return states
}

func reasonCodes(reasons []affinity.BlockedReason) []string {
	codes := make([]string, len(reasons))
	for i, reason := range reasons {
		codes[i] = reason.Code
	}
	return codes
}

func TestCollectLayoutsSizesAndOperators(t *testing.T) {
	re := require.New(t)
	cluster := newTestCluster(t)
	createGroup(t, cluster, "g", fullRange(), 1, []uint64{1, 2, 3})
	putRegion(cluster, 1, "a", "j", 3, 64,
		peerSpec{store: 3}, peerSpec{store: 2}, peerSpec{store: 1})
	putRegion(cluster, 2, "j", "p", 4, 32,
		peerSpec{store: 4}, peerSpec{store: 2}, peerSpec{store: 1})
	putRegion(cluster, 3, "p", "z", 0, 16,
		peerSpec{store: 3}, peerSpec{store: 1}, peerSpec{store: 2})

	controller := operator.NewController(t.Context(), cluster.GetBasicCluster(), cluster.GetSharedConfig(), nil)
	runningRegion := operator.NewTestOperator(1, &metapb.RegionEpoch{}, operator.OpAffinity|operator.OpRegion)
	runningRegion.SetDesc("affinity-move-region")
	re.True(runningRegion.Start())
	controller.SetOperator(runningRegion)
	runningLeader := operator.NewTestOperator(2, &metapb.RegionEpoch{}, operator.OpLeader)
	runningLeader.SetDesc("move-leader")
	re.True(runningLeader.Start())
	controller.SetOperator(runningLeader)
	created := operator.NewTestOperator(3, &metapb.RegionEpoch{}, operator.OpRegion)
	controller.SetOperator(created)

	d := collect(t, cluster, controller, "g")["g"].Diagnostics
	re.Equal([]uint64{3, 4}, d.ActualLeaderStoreIDs)
	re.Equal([][]uint64{{1, 2, 3}, {1, 2, 4}}, d.ActualVoterStoreIDs)
	re.Equal(uint64(112), *d.ApproximateSizeMiB)
	re.Equal(uint64(32), *d.PendingMoveMiB)
	re.Equal([]affinity.OperatorCount{{Kind: "affinity-move-region", Count: 1}, {Kind: "move-leader", Count: 1}}, d.RunningOperators)
	re.Equal([]string{"region_without_leader"}, reasonCodes(d.BlockedReasons))
	re.Nil(d.EffectivePolicy)
	re.Regexp(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$`, d.ObservedAt)
	observedAt, err := time.Parse(observedAtLayout, d.ObservedAt)
	re.NoError(err)
	re.Equal(time.UTC, observedAt.Location())
}

func TestCoverageFailures(t *testing.T) {
	for _, test := range []struct {
		name     string
		populate func(*testCluster)
		code     string
	}{
		{name: "no heartbeat", populate: func(*testCluster) {}, code: "coverage_gap"},
		{name: "hole", populate: func(cluster *testCluster) {
			putRegion(cluster, 1, "a", "m", 1, 10, peerSpec{store: 1})
			putRegion(cluster, 2, "n", "z", 1, 10, peerSpec{store: 1})
		}, code: "coverage_gap"},
		{name: "cross end", populate: func(cluster *testCluster) {
			putRegion(cluster, 1, "a", "zz", 1, 10, peerSpec{store: 1})
		}, code: "cross_boundary_region"},
		{name: "cross start", populate: func(cluster *testCluster) {
			putRegion(cluster, 1, "0", "z", 1, 10, peerSpec{store: 1})
		}, code: "cross_boundary_region"},
	} {
		t.Run(test.name, func(t *testing.T) {
			re := require.New(t)
			cluster := newTestCluster(t)
			createGroup(t, cluster, "g", fullRange(), 1, []uint64{1})
			test.populate(cluster)
			d := collect(t, cluster, nil, "g")["g"].Diagnostics
			re.Nil(d.ActualLeaderStoreIDs)
			re.Nil(d.ActualVoterStoreIDs)
			re.Nil(d.ApproximateSizeMiB)
			re.Nil(d.PendingMoveMiB)
			re.Nil(d.RunningOperators)
			re.Contains(reasonCodes(d.BlockedReasons), test.code)
		})
	}
}

func TestMissingAffinityLabelIsIncomplete(t *testing.T) {
	re := require.New(t)
	cluster := newTestCluster(t)
	createGroup(t, cluster, "g", fullRange(), 1, []uint64{1})
	putRegion(cluster, 1, "a", "z", 1, 10, peerSpec{store: 1})
	re.NoError(cluster.GetRegionLabeler().DeleteLabelRule(affinity.GetLabelRuleID("g")))
	d := collect(t, cluster, nil, "g")["g"].Diagnostics
	re.Nil(d.ActualLeaderStoreIDs)
	re.Nil(d.ApproximateSizeMiB)
	re.Contains(reasonCodes(d.BlockedReasons), "cross_boundary_region")
}

func TestPendingMoveRoles(t *testing.T) {
	for _, test := range []struct {
		name     string
		specs    []peerSpec
		expected uint64
	}{
		{name: "missing voter", specs: []peerSpec{{store: 1}, {store: 2}}, expected: 64},
		{name: "pending voter", specs: []peerSpec{{store: 1}, {store: 2}, {store: 3, pending: true}}, expected: 64},
		{name: "complete learner promotion", specs: []peerSpec{{store: 1}, {store: 2}, {store: 3, learner: true}}, expected: 0},
		{name: "pending learner", specs: []peerSpec{{store: 1}, {store: 2}, {store: 3, learner: true, pending: true}}, expected: 64},
		{name: "witness becomes full voter", specs: []peerSpec{{store: 1}, {store: 2}, {store: 3, witness: true}}, expected: 64},
		{name: "leader transfer only", specs: []peerSpec{{store: 2}, {store: 1}, {store: 3}}, expected: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			re := require.New(t)
			cluster := newTestCluster(t)
			createGroup(t, cluster, "g", fullRange(), 1, []uint64{1, 2, 3})
			putRegion(cluster, 1, "a", "z", 2, 64, test.specs...)
			d := collect(t, cluster, nil, "g")["g"].Diagnostics
			re.Equal(test.expected, *d.PendingMoveMiB)
		})
	}

	t.Run("target witness", func(t *testing.T) {
		re := require.New(t)
		cluster := newTestCluster(t)
		cluster.PutStoreWithLabels(1, "role", "full")
		cluster.PutStoreWithLabels(2, "role", "full")
		cluster.PutStoreWithLabels(3, "role", "witness")
		cluster.GetSharedConfig().SetEnableWitness(true)
		re.NoError(cluster.SetRule(&placement.Rule{
			GroupID: placement.DefaultGroupID,
			ID:      placement.DefaultRuleID,
			Role:    placement.Voter,
			Count:   2,
			LabelConstraints: []placement.LabelConstraint{{
				Key: "role", Op: placement.In, Values: []string{"full"},
			}},
		}))
		re.NoError(cluster.SetRule(&placement.Rule{
			GroupID:   placement.DefaultGroupID,
			ID:        "witness",
			Role:      placement.Voter,
			Count:     1,
			IsWitness: true,
			LabelConstraints: []placement.LabelConstraint{{
				Key: "role", Op: placement.In, Values: []string{"witness"},
			}},
		}))
		createGroup(t, cluster, "g", fullRange(), 1, []uint64{1, 2, 3})
		putRegion(cluster, 1, "a", "z", 1, 64,
			peerSpec{store: 1}, peerSpec{store: 2}, peerSpec{store: 3, witness: true})
		d := collect(t, cluster, nil, "g")["g"].Diagnostics
		re.Zero(*d.PendingMoveMiB)

		cluster.RemoveRegion(cluster.GetRegion(1))
		putRegion(cluster, 1, "a", "z", 1, 64, peerSpec{store: 1}, peerSpec{store: 2})
		d = collect(t, cluster, nil, "g")["g"].Diagnostics
		re.Zero(*d.PendingMoveMiB)
	})

	t.Run("ambiguous target role preserves size", func(t *testing.T) {
		re := require.New(t)
		cluster := newTestCluster(t)
		cluster.GetSharedConfig().SetEnableWitness(true)
		re.NoError(cluster.SetRule(&placement.Rule{
			GroupID: placement.DefaultGroupID, ID: placement.DefaultRuleID, Role: placement.Voter, Count: 3,
		}))
		re.NoError(cluster.SetRule(&placement.Rule{
			GroupID: placement.DefaultGroupID, ID: "witness", Role: placement.Voter, Count: 1, IsWitness: true,
		}))
		createGroup(t, cluster, "g", fullRange(), 1, []uint64{1, 2, 3})
		putRegion(cluster, 1, "a", "z", 1, 64, peerSpec{store: 1}, peerSpec{store: 2})
		d := collect(t, cluster, nil, "g")["g"].Diagnostics
		re.Equal(uint64(64), *d.ApproximateSizeMiB)
		re.Nil(d.PendingMoveMiB)
	})
}

func TestSpreadUsesAllPartitionsAndCommittedTargets(t *testing.T) {
	re := require.New(t)
	cluster := newTestCluster(t)
	cluster.PutStoreWithLabels(1, "role", "full")
	cluster.PutStoreWithLabels(2, "role", "full")
	cluster.PutStoreWithLabels(3, "role", "old")
	re.NoError(cluster.SetRule(&placement.Rule{
		GroupID: placement.DefaultGroupID,
		ID:      placement.DefaultRuleID,
		Role:    placement.Voter,
		Count:   1,
		LabelConstraints: []placement.LabelConstraint{{
			Key: "role", Op: placement.In, Values: []string{"full"},
		}},
	}))
	ids := []string{"_tidb_pt_41_p1", "_tidb_pt_41_p2", "_tidb_pt_41_p3"}
	ranges := [][]keyutil.KeyRange{
		{{StartKey: []byte("a"), EndKey: []byte("b")}},
		{{StartKey: []byte("c"), EndKey: []byte("d")}},
		{{StartKey: []byte("e"), EndKey: []byte("f")}},
	}
	for i, id := range ids {
		createGroup(t, cluster, id, ranges[i], 3, []uint64{3})
		putRegion(cluster, uint64(i+1), string(ranges[i][0].StartKey), string(ranges[i][0].EndKey), 3, 10, peerSpec{store: 3})
	}

	states := collect(t, cluster, nil, ids[0])
	re.Equal(uint64(3), *states[ids[0]].Diagnostics.LeaderSpread)
	re.Equal(uint64(3), *states[ids[0]].Diagnostics.VoterSpread)

	for _, id := range []string{"_tidb_ks7_pt_41_p1", "_tidb_ks8_pt_41_p1"} {
		keyRange := []keyutil.KeyRange{{StartKey: []byte(id), EndKey: []byte(id + "0")}}
		createGroup(t, cluster, id, keyRange, 3, []uint64{3})
		putRegion(cluster, 10, string(keyRange[0].StartKey), string(keyRange[0].EndKey), 3, 10, peerSpec{store: 3})
		state := collect(t, cluster, nil, id)[id]
		re.Equal(uint64(1), *state.Diagnostics.LeaderSpread)
		re.Equal(uint64(1), *state.Diagnostics.VoterSpread)
		cluster.RemoveRegion(cluster.GetRegion(10))
	}

	createGroup(t, cluster, "_tidb_t_41", []keyutil.KeyRange{{StartKey: []byte("x"), EndKey: []byte("y")}}, 3, []uint64{3})
	putRegion(cluster, 20, "x", "y", 3, 10, peerSpec{store: 3})
	tableState := collect(t, cluster, nil, "_tidb_t_41")["_tidb_t_41"]
	re.Nil(tableState.Diagnostics.LeaderSpread)
	re.Nil(tableState.Diagnostics.VoterSpread)
}

func TestSpreadCandidateBoundaries(t *testing.T) {
	re := require.New(t)
	three := spreadValue(map[uint64]struct{}{1: {}}, map[uint64]uint64{3: 3})
	re.Equal(uint64(3), *three)
	zero := spreadValue(map[uint64]struct{}{1: {}}, map[uint64]uint64{1: 3})
	re.Zero(*zero)
	re.Nil(spreadValue(map[uint64]struct{}{}, map[uint64]uint64{3: 3}))
	three = spreadValue(map[uint64]struct{}{1: {}, 2: {}}, map[uint64]uint64{3: 3})
	re.Equal(uint64(3), *three)

	cluster := newTestCluster(t)
	store := cluster.GetStore(1)
	re.True(matchesPlacementRole([]*placement.Rule{{Role: placement.Voter}}, store, leaderRole))
	re.True(matchesPlacementRole([]*placement.Rule{{Role: placement.Voter}}, store, voterRole))
	re.True(matchesPlacementRole([]*placement.Rule{{Role: placement.Leader}}, store, leaderRole))
	re.False(matchesPlacementRole([]*placement.Rule{{Role: placement.Learner}}, store, leaderRole))
	re.False(matchesPlacementRole([]*placement.Rule{{Role: placement.Learner}}, store, voterRole))
	cluster.SetStoreBusy(1, true)
	collector := &collector{cluster: cluster}
	re.True(collector.healthyStore(store, voterRole))
	re.False(collector.healthyStore(store, leaderRole))
	for _, id := range []string{"_tidb_pt_41_p01", "_tidb_pt_041_p1", "_tidb_ks01_pt_41_p1"} {
		_, ok := groupTableKey(id)
		re.False(ok, id)
	}
}

func TestSpreadIndependentFromRegionCoverage(t *testing.T) {
	re := require.New(t)
	cluster := newTestCluster(t)
	p1, p2 := "_tidb_pt_41_p1", "_tidb_pt_41_p2"
	createGroup(t, cluster, p1, []keyutil.KeyRange{{StartKey: []byte("a"), EndKey: []byte("c")}}, 1, []uint64{1, 2})
	createGroup(t, cluster, p2, []keyutil.KeyRange{{StartKey: []byte("c"), EndKey: []byte("g")}}, 2, []uint64{2, 3})
	putRegion(cluster, 1, "a", "c", 1, 10, peerSpec{store: 1}, peerSpec{store: 2})
	// p2 has a coverage hole, but its committed targets remain valid metadata.
	putRegion(cluster, 2, "c", "e", 2, 10, peerSpec{store: 2}, peerSpec{store: 3})

	both := collect(t, cluster, nil, p1, p2)
	single := collect(t, cluster, nil, p1)
	for _, id := range []string{p1, p2} {
		d := both[id].Diagnostics
		re.NotNil(d.LeaderSpread, id)
		re.NotNil(d.VoterSpread, id)
		re.Equal(single[p1].Diagnostics.LeaderSpread, d.LeaderSpread, id)
		re.Equal(single[p1].Diagnostics.VoterSpread, d.VoterSpread, id)
	}
	re.Contains(reasonCodes(both[p2].Diagnostics.BlockedReasons), "coverage_gap")
}

func TestSamplingRetryAndPersistentChange(t *testing.T) {
	t.Run("retry succeeds", func(t *testing.T) {
		re := require.New(t)
		cluster := newTestCluster(t)
		createGroup(t, cluster, "g", fullRange(), 1, []uint64{1})
		putRegion(cluster, 1, "a", "z", 1, 20, peerSpec{store: 1})
		scans := 0
		cluster.scanHook = func() {
			scans++
			if scans == 1 {
				_, err := cluster.AffinityManager.UpdateAffinityGroupPeers("g", 2, []uint64{1, 2})
				re.NoError(err)
			}
		}
		d := collect(t, cluster, nil, "g")["g"].Diagnostics
		re.Equal(2, scans)
		re.NotNil(d.ActualLeaderStoreIDs)
		re.Equal(uint64(20), *d.PendingMoveMiB)
		re.NotContains(reasonCodes(d.BlockedReasons), "sampling_changed")
	})

	t.Run("persistent change", func(t *testing.T) {
		re := require.New(t)
		cluster := newTestCluster(t)
		createGroup(t, cluster, "g", fullRange(), 1, []uint64{1})
		putRegion(cluster, 1, "a", "z", 1, 20, peerSpec{store: 1})
		scans := 0
		cluster.scanHook = func() {
			scans++
			_, err := cluster.AffinityManager.UpdateAffinityGroupPeers("g", uint64(scans%2+1), []uint64{1, 2})
			re.NoError(err)
		}
		d := collect(t, cluster, nil, "g")["g"].Diagnostics
		re.Equal(2, scans)
		re.Nil(d.ActualLeaderStoreIDs)
		re.Nil(d.ActualVoterStoreIDs)
		re.Nil(d.ApproximateSizeMiB)
		re.Nil(d.PendingMoveMiB)
		re.Nil(d.RunningOperators)
		re.Equal([]string{"sampling_changed"}, reasonCodes(d.BlockedReasons))
	})
}

func TestLargeGroupDeduplicatesLayouts(t *testing.T) {
	re := require.New(t)
	cluster := newTestCluster(t)
	createGroup(t, cluster, "g", fullRange(), 1, []uint64{1})
	const regionCount = 10000
	key := func(i int) string { return fmt.Sprintf("a%05d", i) }
	for i := range regionCount {
		start := key(i)
		if i == 0 {
			start = "a"
		}
		end := key(i + 1)
		if i == regionCount-1 {
			end = "z"
		}
		putRegion(cluster, uint64(i+1), start, end, 1, 1, peerSpec{store: 1})
	}
	d := collect(t, cluster, nil, "g")["g"].Diagnostics
	re.Equal([]uint64{1}, d.ActualLeaderStoreIDs)
	re.Equal([][]uint64{{1}}, d.ActualVoterStoreIDs)
	re.Equal(uint64(regionCount), *d.ApproximateSizeMiB)
	re.Zero(*d.PendingMoveMiB)
}

func TestKnownEmptyAndUnknownJSON(t *testing.T) {
	re := require.New(t)
	cluster := newTestCluster(t)
	createGroup(t, cluster, "empty", nil, 0, nil)
	d := collect(t, cluster, nil, "empty")["empty"].Diagnostics
	re.NotNil(d.ActualLeaderStoreIDs)
	re.NotNil(d.ActualVoterStoreIDs)
	re.Empty(d.ActualLeaderStoreIDs)
	re.Empty(d.ActualVoterStoreIDs)
	re.Zero(*d.ApproximateSizeMiB)
	re.Nil(d.PendingMoveMiB)
	re.NotNil(d.RunningOperators)
	re.Equal([]string{"no_key_ranges", "target_not_determined", "target_store_degraded"}, reasonCodes(d.BlockedReasons))

	data, err := json.Marshal(d)
	re.NoError(err)
	fields := make(map[string]json.RawMessage)
	re.NoError(json.Unmarshal(data, &fields))
	for _, name := range []string{"actual_leader_store_ids", "actual_voter_store_ids", "running_operators"} {
		re.JSONEq("[]", string(fields[name]), name)
	}
	for _, name := range []string{"effective_policy", "pending_move_mib", "leader_spread", "voter_spread"} {
		re.JSONEq("null", string(fields[name]), name)
	}
	re.JSONEq("0", string(fields["approximate_size_mib"]))

	createGroup(t, cluster, "complete", []keyutil.KeyRange{{StartKey: []byte("x"), EndKey: []byte("y")}}, 1, []uint64{1})
	putRegion(cluster, 1, "x", "y", 1, 1, peerSpec{store: 1})
	complete := collect(t, cluster, nil, "complete")["complete"].Diagnostics
	re.NotNil(complete.BlockedReasons)
	re.Empty(complete.BlockedReasons)
	data, err = json.Marshal(complete)
	re.NoError(err)
	re.Contains(string(data), `"blocked_reasons":[]`)
	re.True(regexp.MustCompile(`"observed_at":"[^"]+\.\d{6}Z"`).Match(data))

	createGroup(t, cluster, "no-target", []keyutil.KeyRange{{StartKey: []byte("n"), EndKey: []byte("o")}}, 0, nil)
	putRegion(cluster, 2, "n", "o", 1, 7, peerSpec{store: 1})
	noTarget := collect(t, cluster, nil, "no-target")["no-target"].Diagnostics
	re.Equal(uint64(7), *noTarget.ApproximateSizeMiB)
	re.Nil(noTarget.PendingMoveMiB)
}

func TestBlockedReasonOrderingAndContext(t *testing.T) {
	re := require.New(t)
	cluster := newTestCluster(t)
	cluster.halted = true
	cluster.SetAffinityScheduleLimit(0)
	state := &affinity.GroupState{Phase: affinity.PhasePreparing}
	state.ID = "g"
	sample := &regionSample{
		snapshot: &affinity.GroupSamplingSnapshot{
			State:         state,
			Availability:  "degraded",
			RangeConflict: true,
		},
		stable:        true,
		coverageGap:   true,
		crossBoundary: true,
		withoutLeader: true,
	}
	c := &collector{cluster: cluster}
	d := c.newDiagnostics(sample)
	expected := []string{
		"affinity_scheduling_paused", "coverage_gap", "cross_boundary_region",
		"no_key_ranges", "range_conflict", "region_without_leader", "target_not_determined", "target_store_degraded",
	}
	re.Equal(expected, reasonCodes(d.BlockedReasons))
	re.True(slices.IsSorted(reasonCodes(d.BlockedReasons)))
	for _, reason := range d.BlockedReasons {
		re.NotEmpty(reason.Text)
	}

	createGroup(t, cluster, "g", fullRange(), 1, []uint64{1})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := CollectGroupStates(ctx, cluster.AffinityManager, cluster, nil, []string{"g"})
	re.ErrorIs(err, context.Canceled)
}
