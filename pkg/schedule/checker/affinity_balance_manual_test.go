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

package checker

import (
	"bytes"
	"context"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/storage/endpoint"
)

func TestManualBalanceSubmitsWholeTableBeforeConvergence(t *testing.T) {
	for _, changeRanges := range []bool{false, true} {
		name := "converged"
		if changeRanges {
			name = "range conflict retains submitted targets"
		}
		t.Run(name, func(t *testing.T) {
			groups := make([]manualBalanceFixtureGroup, 2*endpoint.MaxAffinityTxnBatchOps+1)
			for i := range groups {
				groups[i] = manualBalanceFixtureGroup{leader: 1, voters: []uint64{1, 2, 3}}
			}
			checker, manager, initial := newManualBalanceFixture(t, 3, groups)
			tc := checker.cluster.(*mockcluster.Cluster)
			heartbeatObserved := make(chan struct{})
			heartbeatSent := false
			checker.cluster = &affinityBalanceScanCluster{CheckerCluster: tc, beforeScan: func([]byte) {
				if heartbeatSent {
					return
				}
				for _, group := range initial.Groups {
					target := manager.GetAffinityGroupState(group.ID)
					if target.LeaderStoreID == group.LeaderStoreID {
						continue
					}
					region := tc.GetRegionByKey(initial.Ranges[group.ID].KeyRanges[0].StartKey)
					heartbeat := region.Clone(core.WithLeader(region.GetStorePeer(target.LeaderStoreID)))
					tc.PutRegion(heartbeat)
					manager.ObserveBalanceRegion(heartbeat)
					manager.GetAndCacheRegionAffinityGroupState(heartbeat)
					heartbeatSent = true
					close(heartbeatObserved)
					return
				}
			}}
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			type result struct {
				plans []affinity.BalancePlan
				err   error
			}
			results := make(chan result, 1)
			go func() {
				plans, err := manager.RebalanceAffinityGroup(ctx, initial.Groups[0].ID, true)
				results <- result{plans, err}
			}()
			require.Eventually(t, func() bool {
				changed := 0
				for _, group := range initial.Groups {
					if manager.GetAffinityGroupState(group.ID).LeaderStoreID != group.LeaderStoreID {
						changed++
					}
				}
				return changed > endpoint.MaxAffinityTxnBatchOps
			}, 10*time.Second, time.Millisecond)
			select {
			case <-heartbeatObserved:
			default:
				t.Fatal("expected a previous batch heartbeat while validating the next batch")
			}
			require.True(t, manager.IsManualBalanceRunning(initial.TableID))
			select {
			case done := <-results:
				t.Fatalf("apply returned before Region convergence: %v", done.err)
			default:
			}

			if changeRanges {
				require.NoError(t, manager.UpdateAffinityGroupKeyRanges(nil, []affinity.GroupKeyRanges{initial.Ranges[initial.Groups[0].ID]}))
			} else {
				for _, group := range initial.Groups {
					target := manager.GetAffinityGroupState(group.ID)
					region := tc.GetRegionByKey(initial.Ranges[group.ID].KeyRanges[0].StartKey)
					heartbeat := region.Clone(core.WithLeader(region.GetStorePeer(target.LeaderStoreID)))
					tc.PutRegion(heartbeat)
					manager.ObserveBalanceRegion(heartbeat)
					_, aligned := manager.GetAndCacheRegionAffinityGroupState(heartbeat)
					require.True(t, aligned)
				}
			}
			select {
			case done := <-results:
				if changeRanges {
					require.ErrorIs(t, done.err, affinity.ErrBalanceConflict)
				} else {
					require.NoError(t, done.err)
				}
				require.Len(t, done.plans, len(initial.Groups))
				changed := 0
				for _, plan := range done.plans {
					if !plan.Changed {
						continue
					}
					changed++
					if changeRanges {
						require.Contains(t, []string{"submitted", "applied"}, plan.Status)
					} else {
						require.Equal(t, "applied", plan.Status)
					}
					require.Equal(t, plan.LeaderStoreID, manager.GetAffinityGroupState(plan.GroupID).LeaderStoreID)
					require.NotEmpty(t, plan.Revision)
				}
				require.Greater(t, changed, endpoint.MaxAffinityTxnBatchOps)
			case <-ctx.Done():
				t.Fatal("manual balance did not complete or detect changed ranges")
			}
			require.False(t, manager.IsManualBalanceRunning(initial.TableID))
		})
	}
}

func TestManualBalanceRechecksSubmissionGateBetweenBatches(t *testing.T) {
	for _, test := range []struct {
		name string
		stop func(*AffinityChecker, *mockcluster.Cluster)
	}{
		{"paused", func(checker *AffinityChecker, _ *mockcluster.Cluster) { checker.PauseOrResume(60) }},
		{"halted", func(_ *AffinityChecker, tc *mockcluster.Cluster) { tc.SetHaltScheduling(true, "test") }},
		{"limit zero", func(_ *AffinityChecker, tc *mockcluster.Cluster) { tc.SetAffinityScheduleLimit(0) }},
		{"placement disabled", func(_ *AffinityChecker, tc *mockcluster.Cluster) {
			config := tc.GetReplicationConfig().Clone()
			config.EnablePlacementRules = false
			tc.SetReplicationConfig(config)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			groups := make([]manualBalanceFixtureGroup, 2*endpoint.MaxAffinityTxnBatchOps+1)
			for i := range groups {
				groups[i] = manualBalanceFixtureGroup{leader: 1, voters: []uint64{1, 2, 3}}
			}
			checker, manager, initial := newManualBalanceFixture(t, 3, groups)
			tc := checker.cluster.(*mockcluster.Cluster)
			stopped := false
			checker.cluster = &affinityBalanceScanCluster{CheckerCluster: tc, beforeScan: func([]byte) {
				if stopped {
					return
				}
				for _, group := range initial.Groups {
					if manager.GetAffinityGroupState(group.ID).LeaderStoreID != group.LeaderStoreID {
						test.stop(checker, tc)
						stopped = true
						return
					}
				}
			}}

			plans, err := manager.RebalanceAffinityGroup(t.Context(), initial.Groups[0].ID, true)

			require.ErrorIs(t, err, affinity.ErrBalanceNotReady)
			require.True(t, stopped)
			submitted, pending := 0, 0
			for _, plan := range plans {
				switch plan.Status {
				case "submitted":
					submitted++
					require.Equal(t, plan.LeaderStoreID, manager.GetAffinityGroupState(plan.GroupID).LeaderStoreID)
				case "not_submitted":
					pending++
					require.Equal(t, uint64(1), manager.GetAffinityGroupState(plan.GroupID).LeaderStoreID)
				}
			}
			require.Equal(t, endpoint.MaxAffinityTxnBatchOps, submitted)
			require.Positive(t, pending)
		})
	}
}

func TestManualBalanceRechecksEarlierGroupAfterConvergenceHeartbeat(t *testing.T) {
	for _, test := range []struct {
		name               string
		restoreImmediately bool
		observeHeartbeat   bool
	}{
		{name: "remains off target", observeHeartbeat: true},
		{name: "returns to target in the same round", restoreImmediately: true, observeHeartbeat: true},
		{name: "checker observation delayed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			checker, manager, initial := newManualBalanceFixture(t, 3, []manualBalanceFixtureGroup{
				{leader: 1, voters: []uint64{1, 2, 3}},
				{leader: 1, voters: []uint64{1, 2, 3}},
				{leader: 1, voters: []uint64{1, 2, 3}},
			})
			preview, err := manager.RebalanceAffinityGroup(t.Context(), initial.Groups[0].ID, false)
			require.NoError(t, err)
			changed := slices.DeleteFunc(slices.Clone(preview), func(plan affinity.BalancePlan) bool { return !plan.Changed })
			require.Len(t, changed, 2)
			tc := checker.cluster.(*mockcluster.Cluster)
			firstKey := initial.Ranges[changed[0].GroupID].KeyRanges[0].StartKey
			secondKey := initial.Ranges[changed[1].GroupID].KeyRanges[0].StartKey
			updateRegion := func(key []byte, leader uint64, observe bool) {
				region := tc.GetRegionByKey(key)
				heartbeat := region.Clone(core.WithLeader(region.GetStorePeer(leader)))
				tc.PutRegion(heartbeat)
				if observe {
					manager.ObserveBalanceRegion(heartbeat)
					manager.GetAndCacheRegionAffinityGroupState(heartbeat)
				}
			}
			var armed atomic.Bool
			firstChecked, injected, rescanned := false, false, false
			var generationBefore, generationAfter uint64
			invalidated, rechecked := make(chan struct{}), make(chan struct{})
			checker.cluster = &affinityBalanceScanCluster{CheckerCluster: tc, beforeScan: func(key []byte) {
				if !armed.Load() {
					return
				}
				if bytes.Equal(key, firstKey) {
					firstChecked = true
					if injected && !rescanned {
						rescanned = true
						close(rechecked)
					}
				}
				if firstChecked && !injected && bytes.Equal(key, secondKey) {
					version, _ := manager.GetBalanceVersion(initial.TableID)
					generationBefore = version.Table
					updateRegion(firstKey, changed[0].OldLeaderStoreID, test.observeHeartbeat)
					if test.restoreImmediately {
						updateRegion(firstKey, changed[0].LeaderStoreID, test.observeHeartbeat)
					}
					version, _ = manager.GetBalanceVersion(initial.TableID)
					generationAfter = version.Table
					injected = true
					close(invalidated)
				}
			}}
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			type result struct {
				plans []affinity.BalancePlan
				err   error
			}
			results := make(chan result, 1)
			go func() {
				plans, err := manager.RebalanceAffinityGroup(ctx, initial.Groups[0].ID, true)
				results <- result{plans, err}
			}()
			require.Eventually(t, func() bool {
				for _, plan := range changed {
					if manager.GetAffinityGroupState(plan.GroupID).LeaderStoreID != plan.LeaderStoreID {
						return false
					}
				}
				return true
			}, 3*time.Second, time.Millisecond)
			for _, plan := range preview {
				updateRegion(initial.Ranges[plan.GroupID].KeyRanges[0].StartKey, plan.LeaderStoreID, true)
			}
			armed.Store(true)
			select {
			case <-invalidated:
			case done := <-results:
				t.Fatalf("apply returned before the convergence interleaving: %v", done.err)
			case <-ctx.Done():
				t.Fatal("convergence interleaving was not reached")
			}
			if !test.observeHeartbeat {
				require.Equal(t, generationBefore, generationAfter, "RegionTree changes must be checked before checker observation")
			}
			select {
			case <-rechecked:
			case done := <-results:
				t.Fatalf("apply returned without rechecking the invalidated group: %v", done.err)
			case <-ctx.Done():
				t.Fatal("invalidated convergence round was not retried")
			}
			if !test.restoreImmediately {
				updateRegion(firstKey, changed[0].LeaderStoreID, test.observeHeartbeat)
			}
			select {
			case done := <-results:
				require.NoError(t, done.err)
				for _, plan := range done.plans {
					if plan.Changed {
						require.Equal(t, "applied", plan.Status)
					}
				}
			case <-ctx.Done():
				t.Fatal("manual balance did not finish after convergence")
			}
		})
	}
}

func TestManualBalanceTargetSubmissionIgnoresOperatorCapacity(t *testing.T) {
	checker, manager, initial := newManualBalanceFixture(t, 3, []manualBalanceFixtureGroup{{leader: 1, voters: []uint64{1, 2, 3}}})
	tc := checker.cluster.(*mockcluster.Cluster)
	tc.SetAffinityScheduleLimit(1)
	tc.AddLeaderRegion(100, 1, 2, 3)
	checker.opController = operator.NewController(t.Context(), tc.GetBasicCluster(), tc.GetSharedConfig(), nil)
	op, err := operator.CreateTransferLeaderOperator("affinity-test", tc, tc.GetRegion(100), 2, nil, operator.OpAffinity)
	require.NoError(t, err)
	require.True(t, op.Start())
	checker.opController.SetOperator(op)
	require.False(t, checker.balanceSchedulingAllowed())

	plans, err := manager.RebalanceAffinityGroup(t.Context(), initial.Groups[0].ID, false)

	require.NoError(t, err)
	require.Len(t, plans, 1)
	require.False(t, plans[0].Changed)
}

func TestManualBalanceRefreshReusesFingerprintAfterHeartbeat(t *testing.T) {
	checker, manager, snapshot := newManualBalanceFixture(t, 3, []manualBalanceFixtureGroup{{leader: 1, voters: []uint64{1, 2, 3}}})
	tc := checker.cluster.(*mockcluster.Cluster)
	scanCluster := &affinityBalanceScanCluster{CheckerCluster: tc}
	checker.cluster = scanCluster
	require.NoError(t, manager.PrepareBalanceCommit(t.Context(), snapshot))
	fingerprint := checker.balanceInputFingerprint(snapshot)
	require.Equal(t, 1, scanCluster.labelReads)
	region := tc.GetRegion(1)
	manager.ObserveBalanceRegion(region.Clone(core.WithLeader(region.GetStorePeer(2))))

	current, err := checker.refreshManualBalance(t.Context(), snapshot, fingerprint)

	require.NoError(t, err)
	require.Greater(t, current.Version.Table, snapshot.Version.Table)
	require.Equal(t, 1, scanCluster.labelReads, "normal migration needs no metadata rescan")
	require.NoError(t, tc.GetRegionLabeler().SetLabelRule(&labeler.LabelRule{
		ID: "unrelated-manual-metadata", RuleType: labeler.KeyRange,
		Labels: []labeler.RegionLabel{{Key: "balance-test", Value: "unrelated"}}, Data: labeler.MakeKeyRanges("79", "7a"),
	}))
	_, err = checker.refreshManualBalance(t.Context(), current, fingerprint)
	require.NoError(t, err)
	require.Equal(t, 2, scanCluster.labelReads, "metadata changes must still recheck relevance")
}

func TestManualBalanceReadsCurrentApproximateSize(t *testing.T) {
	checker, manager, snapshot := newManualBalanceFixture(t, 4, []manualBalanceFixtureGroup{
		{leader: 1, voters: []uint64{1, 2, 3}},
	})
	tc := checker.cluster.(*mockcluster.Cluster)
	target := snapshot.Groups[0].Group
	target.VoterStoreIDs = []uint64{1, 2, 4}
	for _, size := range []int64{2, 7} {
		region := tc.GetRegion(1).Clone(core.SetApproximateSize(size))
		tc.PutRegion(region)
		manager.GetAndCacheRegionAffinityGroupState(region)
		status, bytes, err := checker.validateManualBalanceTarget(t.Context(), snapshot, snapshot.Groups[0], &target)
		require.NoError(t, err)
		require.Equal(t, balanceAllowed, status)
		require.Equal(t, uint64(size)*1024*1024, bytes)
	}
	require.Equal(t, []uint64{1, 2, 3}, snapshot.Groups[0].VoterStoreIDs)
	require.False(t, slices.Equal(target.VoterStoreIDs, snapshot.Groups[0].VoterStoreIDs))
}
