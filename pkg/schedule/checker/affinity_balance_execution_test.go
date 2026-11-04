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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/utils/testutil"
)

type balanceExecutionTargetWriter struct {
	write func([]affinity.GroupTarget) ([]string, error)
}

func (w balanceExecutionTargetWriter) WriteGroupTargets(_ context.Context, targets []affinity.GroupTarget) ([]string, error) {
	return w.write(targets)
}

func TestAffinityBalanceExecutionAfterRegionChanges(t *testing.T) {
	for _, event := range []string{"split", "merge"} {
		for _, gate := range []string{"allowed", "placement", "best-location"} {
			t.Run(event+"/"+gate, func(t *testing.T) {
				re := require.New(t)
				f := newAffinityBalanceTest(t, 4, "a", "b", "c", "a")
				f.SetEnableUseJointConsensus(false)
				f.SetMaxAffinityMergeRegionSize(0)
				for id := uint64(1); id <= 4; id++ {
					zone := f.GetStore(id).GetLabelValue("zone")
					f.SetStoreLabel(id, map[string]string{"zone": zone, "disk": "ssd"})
				}
				rule := f.ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
				rule.LabelConstraints = []placement.LabelConstraint{{Key: "disk", Op: placement.In, Values: []string{"ssd"}}}
				re.NoError(f.ruleManager.SetRule(rule))
				id := balanceTestGroupID("_tidb_pt_100_p1")
				f.group(id, 1, 1, []uint64{1, 2, 3}, "a", "b", "c")
				fixedID := balanceTestGroupID("_tidb_pt_100_p2")
				f.group(fixedID, 3, 1, []uint64{1, 2, 3}, "d", "e")
				f.policy(affinity.BalancePolicyFixed, fixedID)
				snapshot := f.snapshot()
				revision := f.ruleManager.GetRevision()
				res, err := f.planBalanceTargets(t.Context(), snapshot)
				re.NoError(err)
				re.Len(res.targets, 1)
				target := res.targets[0]
				re.Equal(id, target.ID)
				re.Contains(target.VoterStoreIDs, uint64(4))
				var changed []*core.RegionInfo
				beforeAffinityBalanceCommit(t, func() {
					first := f.GetRegion(1)
					if event == "split" {
						mid := []byte(balanceTestKey("am"))
						changed = []*core.RegionInfo{
							first.Clone(core.WithEndKey(mid), core.WithIncVersion()),
							first.Clone(core.WithNewRegionID(4), core.WithStartKey(mid), core.WithIncVersion()),
						}
					} else {
						changed = []*core.RegionInfo{first.Clone(core.WithEndKey(f.GetRegion(2).GetEndKey()), core.WithIncVersion())}
					}
					for _, region := range changed {
						f.PutRegion(region)
					}
					switch gate {
					case "placement":
						f.SetStoreLabel(4, map[string]string{"zone": "a", "disk": "hdd"})
					case "best-location":
						f.SetStoreLabel(4, map[string]string{"zone": "b", "disk": "ssd"})
					}
				})
				written, reason, err := f.commitBalancePlan(t.Context(), snapshot, res, revision)
				re.NoError(err)
				re.Empty(reason)
				re.Equal([]string{id}, written)
				re.Equal(*target, f.affinityManager.GetAffinityGroupState(id).Group)
				re.Equal(revision, f.ruleManager.GetRevision())
				if event == "merge" {
					re.Nil(f.GetRegion(2))
				}
				if gate == "allowed" {
					for _, region := range changed {
						ops := f.Check(region)
						re.Len(ops, 1)
						re.NotEqual(operator.OpKind(0), ops[0].Kind()&operator.OpAffinity)
						operator.ApplyOperator(f.Cluster, ops[0])
						moved := f.GetRegion(region.GetID())
						re.Equal(target.LeaderStoreID, moved.GetLeader().GetStoreId())
						re.True(f.ruleManager.FitRegionWithoutCache(f.Cluster, moved).IsSatisfied())
						re.True(f.isRegionPlacementRuleSatisfiedWithBestLocation(moved, true))
					}
					return
				}
				region := changed[0]
				re.True(f.isRegionPlacementRuleSatisfiedWithBestLocation(region, true))
				virtual := cloneRegionWithReplacePeerStores(region, target.LeaderStoreID, target.VoterStoreIDs...)
				fit := f.ruleManager.FitRegionWithoutCache(f.Cluster, virtual)
				if gate == "placement" {
					re.False(fit.IsSatisfied())
				} else {
					re.True(fit.IsSatisfied())
					re.Equal(bestLocationRejected, f.regionPlacementBestLocation(virtual, fit))
				}
				type firstWin struct {
					expired *affinity.GroupState
					targets []affinity.GroupTarget
					err     error
				}
				firstWins := make(chan firstWin, 1)
				f.affinityManager.SetTargetWriter(balanceExecutionTargetWriter{write: func(targets []affinity.GroupTarget) ([]string, error) {
					expired := f.affinityManager.GetAffinityGroupState(id)
					ids, err := f.affinityManager.UpdateAffinityGroupTargets(targets)
					firstWins <- firstWin{expired: expired, targets: targets, err: err}
					return ids, err
				}})
				re.Empty(f.Check(region))
				var observed firstWin
				testutil.Eventually(re, func() bool {
					select {
					case observed = <-firstWins:
						return true
					default:
						return false
					}
				})
				re.NoError(observed.err)
				re.True(observed.expired.RegularSchedulingAllowed)
				re.False(observed.expired.AffinitySchedulingAllowed)
				re.Len(observed.targets, 1)
				re.Equal(observed.targets[0].Expected, *target)
				re.Equal(uint64(1), observed.targets[0].LeaderStoreID)
				re.Equal([]uint64{1, 2, 3}, observed.targets[0].VoterStoreIDs)
				learned := f.affinityManager.GetAffinityGroupState(id)
				re.True(learned.AffinitySchedulingAllowed)
				re.False(learned.RegularSchedulingAllowed)
				re.Equal(uint64(1), learned.LeaderStoreID)
				re.Equal([]uint64{1, 2, 3}, learned.VoterStoreIDs)
				raw, err := f.GetStorage().LoadAffinityGroup(id)
				re.NoError(err)
				var stored affinity.Group
				re.NoError(json.Unmarshal([]byte(raw), &stored))
				re.Equal(learned.Group, stored)
			})
		}
	}
}
