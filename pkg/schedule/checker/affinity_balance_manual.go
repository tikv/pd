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
	"fmt"
	"slices"
	"time"

	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/storage/endpoint"
)

const (
	manualBalancePlanningTimeout  = 30 * time.Second
	manualBalanceExecutionTimeout = 30 * time.Minute
)

// Manual planning and execution use the same complete Region validation as
// the worker. Only the outer candidate/Region loop checks cancellation.
func (c *AffinityChecker) validateManualBalanceTarget(ctx context.Context, snapshot *affinity.BalanceSnapshot,
	actual *affinity.GroupState, target *affinity.Group) (balanceStatus, uint64, error) {
	if actual == nil || !actual.AffinitySchedulingAllowed || len(snapshot.Ranges[actual.ID].KeyRanges) == 0 {
		return balanceUnready, 0, nil
	}
	if target != nil {
		if _, err := affinity.NewBalanceScorer([]*affinity.Group{target}); err != nil ||
			len(target.VoterStoreIDs) != len(actual.VoterStoreIDs) || target.ID != actual.ID {
			return balanceRejected, 0, nil
		}
		if status := c.balanceTargetExecutionStatus(&actual.Group, target); status != balanceAllowed {
			return status, 0, nil
		}
	}
	scan := newBalanceRegionScan(snapshot.Ranges[actual.ID])
	waiting := c.waitingAffinityRegions()
	var bytes uint64
	for {
		if err := ctx.Err(); err != nil {
			return balanceUnready, 0, err
		}
		region, covered, done := scan.next(c.cluster)
		if !covered {
			return balanceUnready, 0, nil
		}
		if done {
			break
		}
		if status := c.validateBalanceRegion(actual, target, region, waiting); status != balanceAllowed {
			return status, 0, nil
		}
		bytes += approximateBalanceRegionBytes(region)
	}
	var moved uint64
	if target != nil {
		if status := c.balanceTargetExecutionStatus(&actual.Group, target); status != balanceAllowed {
			return status, 0, nil
		}
		for _, id := range target.VoterStoreIDs {
			if !slices.Contains(actual.VoterStoreIDs, id) {
				moved++
			}
		}
	}
	return balanceAllowed, bytes * moved, ctx.Err()
}

func manualPlanTarget(plan affinity.BalancePlan) *affinity.Group {
	return &affinity.Group{ID: plan.GroupID, LeaderStoreID: plan.LeaderStoreID, VoterStoreIDs: slices.Clone(plan.VoterStoreIDs)}
}

func manualPlanGroup(snapshot *affinity.BalanceSnapshot, id string) *affinity.GroupState {
	for _, group := range snapshot.Groups {
		if group.ID == id {
			return group
		}
	}
	return nil
}

func (c *AffinityChecker) refreshManualBalance(ctx context.Context, snapshot *affinity.BalanceSnapshot, fingerprint uint64) (*affinity.BalanceSnapshot, error) {
	current, err := c.affinityManager.RefreshBalanceSnapshot(ctx, snapshot)
	if err != nil {
		return nil, err
	}
	version, exists := c.affinityManager.GetBalanceVersion(current.TableID)
	if !exists || version.Ranges != current.Version.Ranges || version.Stores != current.Version.Stores ||
		version.Rules != current.Version.Rules || version.Labels != current.Version.Labels ||
		!sameBalanceMetadata(snapshot.Version, current.Version) && c.balanceInputFingerprint(current) != fingerprint {
		return nil, affinity.ErrBalanceConflict
	}
	return current, nil
}

func (c *AffinityChecker) rebalanceTable(ctx context.Context, groupID string, apply bool) ([]affinity.BalancePlan, error) {
	if !c.balanceTargetSubmissionAllowed() {
		return nil, affinity.ErrBalanceNotReady
	}
	ctx, cancel := context.WithTimeout(ctx, manualBalanceExecutionTimeout)
	defer cancel()
	planningCtx, cancelPlanning := context.WithTimeout(ctx, manualBalancePlanningTimeout)
	defer cancelPlanning()
	snapshot := c.affinityManager.GetBalanceSnapshot(groupID)
	if snapshot == nil {
		return nil, affinity.ErrBalanceConflict
	}
	if err := c.affinityManager.PrepareBalanceCommit(planningCtx, snapshot); err != nil {
		return nil, err
	}
	fingerprint := c.balanceInputFingerprint(snapshot)
	if !c.affinityManager.BalanceSnapshotCurrent(snapshot) {
		return nil, affinity.ErrBalanceConflict
	}
	plans, err := c.planTableRebalance(planningCtx, snapshot)
	for i := range plans {
		plans[i].Revision = snapshot.Revision()
	}
	if err != nil {
		return plans, err
	}
	// Revalidate the complete table before reporting a preview or submitting
	// the first batch. Later batches revalidate only targets not yet submitted;
	// already-submitted groups are allowed to migrate concurrently.
	snapshot, err = c.refreshManualBalance(planningCtx, snapshot, fingerprint)
	if err != nil {
		return plans, err
	}
	changed := make([]int, 0, len(plans))
	for i := range plans {
		group := manualPlanGroup(snapshot, plans[i].GroupID)
		if group == nil {
			return plans, affinity.ErrBalanceConflict
		}
		if group.BalancePolicy == affinity.BalancePolicyFixed {
			continue
		}
		status, _, err := c.validateManualBalanceTarget(planningCtx, snapshot, group, manualPlanTarget(plans[i]))
		if err != nil {
			return plans, err
		}
		if status != balanceAllowed {
			return plans, affinity.ErrBalanceNotReady
		}
		if plans[i].Changed {
			changed = append(changed, i)
		}
	}
	if err := planningCtx.Err(); err != nil {
		return plans, err
	}
	if !c.affinityManager.BalanceSnapshotCurrent(snapshot) {
		return plans, affinity.ErrBalanceConflict
	}
	if err := c.affinityManager.CheckBalanceDependencies(planningCtx, snapshot); err != nil {
		return plans, err
	}
	if !apply || len(changed) == 0 {
		return plans, nil
	}
	cancelPlanning()
	for _, index := range changed {
		plans[index].Status = "not_submitted"
	}
	submitted := 0
	fail := func(err error, index int) ([]affinity.BalancePlan, error) {
		plans[index].Reason = err.Error()
		if submitted > 0 {
			err = fmt.Errorf("%w: already committed %d groups", err, submitted)
		}
		return plans, err
	}
	for start := 0; start < len(changed); start += endpoint.MaxAffinityTxnBatchOps {
		end := min(start+endpoint.MaxAffinityTxnBatchOps, len(changed))
		snapshot, err = c.refreshManualBalance(ctx, snapshot, fingerprint)
		if err != nil {
			return fail(err, changed[start])
		}
		targets := make([]*affinity.Group, 0, end-start)
		for _, index := range changed[start:end] {
			target := manualPlanTarget(plans[index])
			status, _, err := c.validateManualBalanceTarget(ctx, snapshot, manualPlanGroup(snapshot, target.ID), target)
			if err != nil {
				return fail(err, index)
			}
			if status != balanceAllowed {
				return fail(affinity.ErrBalanceNotReady, index)
			}
			targets = append(targets, target)
		}
		for _, target := range targets {
			if c.balanceTargetExecutionStatus(&manualPlanGroup(snapshot, target.ID).Group, target) != balanceAllowed {
				return fail(affinity.ErrBalanceNotReady, changed[start])
			}
		}
		if !c.balanceTargetSubmissionAllowed() {
			return fail(affinity.ErrBalanceNotReady, changed[start])
		}
		committed, err := c.affinityManager.CommitBalanceTargets(ctx, snapshot, targets)
		if err != nil {
			// A transport error does not prove whether this transaction committed.
			for _, index := range changed[start:end] {
				plans[index].Status = "unknown"
			}
			return fail(err, changed[start])
		}
		if !committed {
			return fail(affinity.ErrBalanceConflict, changed[start])
		}
		for _, index := range changed[start:end] {
			plans[index].Status = "submitted"
		}
		submitted += len(targets)
	}
	// Every target has been submitted before any convergence wait begins.
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		snapshot, err = c.refreshManualBalance(ctx, snapshot, fingerprint)
		if err != nil {
			return fail(err, changed[0])
		}
		complete := true
		for _, index := range changed {
			version, exists := c.affinityManager.GetBalanceVersion(snapshot.TableID)
			if !exists || version.Ranges != snapshot.Version.Ranges || version.Stores != snapshot.Version.Stores {
				return fail(affinity.ErrBalanceConflict, index)
			}
			if version.Rules != snapshot.Version.Rules || version.Labels != snapshot.Version.Labels {
				snapshot, err = c.refreshManualBalance(ctx, snapshot, fingerprint)
				if err != nil {
					return fail(err, index)
				}
			}
			converged, err := c.checkManualBalanceConvergence(ctx, snapshot, &plans[index])
			if err != nil {
				return fail(err, index)
			}
			if converged {
				plans[index].Status = "applied"
			} else {
				plans[index].Status = "submitted"
				complete = false
			}
		}
		if complete {
			if _, err := c.refreshManualBalance(ctx, snapshot, fingerprint); err != nil {
				return fail(err, changed[0])
			}
			return plans, nil
		}
		select {
		case <-ctx.Done():
			return fail(ctx.Err(), changed[0])
		case <-ticker.C:
		}
	}
}

func (c *AffinityChecker) checkManualBalanceConvergence(ctx context.Context, snapshot *affinity.BalanceSnapshot, plan *affinity.BalancePlan) (bool, error) {
	actual := c.affinityManager.GetAffinityGroupState(plan.GroupID)
	expected := manualPlanGroup(snapshot, plan.GroupID)
	if actual == nil || expected == nil || actual.CreateTimestamp != expected.CreateTimestamp || actual.BalancePolicy != expected.BalancePolicy ||
		actual.LeaderStoreID != plan.LeaderStoreID || !slices.Equal(actual.VoterStoreIDs, normalizeManualVoters(plan.VoterStoreIDs)) {
		return false, affinity.ErrBalanceConflict
	}
	status, _, err := c.validateManualBalanceTarget(ctx, snapshot, actual, nil)
	return status == balanceAllowed && err == nil, err
}

func normalizeManualVoters(voters []uint64) []uint64 {
	result := slices.Clone(voters)
	slices.Sort(result)
	return result
}
