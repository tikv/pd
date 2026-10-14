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
	"context"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/schedule/affinity"
)

func TestManualBalanceSubmitsWholeTableBeforeConvergence(t *testing.T) {
	for _, changeRanges := range []bool{false, true} {
		name := "converged"
		if changeRanges {
			name = "range conflict retains submitted targets"
		}
		t.Run(name, func(t *testing.T) {
			checker, manager, initial := newManualBalanceFixture(t, 3, []manualBalanceFixtureGroup{
				{leader: 1, voters: []uint64{1, 2, 3}},
				{leader: 1, voters: []uint64{1, 2, 3}},
				{leader: 1, voters: []uint64{1, 2, 3}},
				{leader: 1, voters: []uint64{1, 2, 3}},
			})
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
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
				return changed >= 2
			}, 3*time.Second, time.Millisecond)
			require.True(t, manager.IsManualBalanceRunning(initial.TableID))
			select {
			case done := <-results:
				t.Fatalf("apply returned before Region convergence: %v", done.err)
			default:
			}

			if changeRanges {
				require.NoError(t, manager.UpdateAffinityGroupKeyRanges(nil, []affinity.GroupKeyRanges{initial.Ranges[initial.Groups[0].ID]}))
			} else {
				tc := checker.cluster.(*mockcluster.Cluster)
				for i, group := range initial.Groups {
					target := manager.GetAffinityGroupState(group.ID)
					region := tc.GetRegion(uint64(i + 1))
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
						require.Equal(t, "submitted", plan.Status)
					} else {
						require.Equal(t, "applied", plan.Status)
					}
					require.Equal(t, plan.LeaderStoreID, manager.GetAffinityGroupState(plan.GroupID).LeaderStoreID)
					require.NotEmpty(t, plan.Revision)
				}
				require.GreaterOrEqual(t, changed, 2)
			case <-ctx.Done():
				t.Fatal("manual balance did not complete or detect changed ranges")
			}
			require.False(t, manager.IsManualBalanceRunning(initial.TableID))
		})
	}
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
