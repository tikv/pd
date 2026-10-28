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

package affinity

import (
	"context"
	"encoding/json"
	"slices"

	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
)

// GroupTarget is a leader and voters decided for a group from the group in Expected.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type GroupTarget struct {
	// Expected is the persisted group the target was decided from. The target is
	// written only if the stored group still equals it.
	Expected      Group    `json:"expected"`
	LeaderStoreID uint64   `json:"leader_store_id"`
	VoterStoreIDs []uint64 `json:"voter_store_ids"`
}

// GroupTargetsRequest is the body of the PD API that writes group targets.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type GroupTargetsRequest struct {
	Targets []GroupTarget `json:"targets"`
}

// GroupTargetsResponse lists the groups whose targets were written.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type GroupTargetsResponse struct {
	WrittenGroupIDs []string `json:"written_group_ids"`
}

// TargetWriter writes the targets decided by a Manager that does not own the
// storage, such as the Manager of the scheduling service.
type TargetWriter interface {
	// WriteGroupTargets writes the targets whose groups still equal their
	// expected values and returns the IDs of the written groups. If it fails
	// partway, it returns the IDs of the groups written before the failure with
	// the error.
	WriteGroupTargets(ctx context.Context, targets []GroupTarget) ([]string, error)
}

// SetTargetWriter makes the Manager send the targets it decides to w instead of
// writing its own storage. It must be called before the Manager is used.
func (m *Manager) SetTargetWriter(w TargetWriter) {
	m.targetWriter = w
}

// UpdateAffinityGroupTargets writes the targets whose stored groups still equal
// their expected values and returns the IDs of the written groups. Targets are
// committed in chunks that each fit in one etcd transaction, and a written group
// takes the availability of its target. A target that is already stored, for
// example by an earlier request whose reply was lost, is reported as written. A
// group whose stored value differs is not written; its in-memory group is
// refreshed from storage instead. A target with a store that PD knows to be
// unavailable is skipped, because the scheduling service that decided it may not
// know it yet; an invalid target fails the whole request.
func (m *Manager) UpdateAffinityGroupTargets(targets []GroupTarget) ([]string, error) {
	return m.updateGroupTargetsInChunks(m.ctx, targets)
}

func (m *Manager) updateGroupTargetsInChunks(ctx context.Context, targets []GroupTarget) ([]string, error) {
	if len(targets) == 0 {
		return nil, errs.ErrEmptyRequest.FastGenByArgs("no targets provided")
	}
	adjusted := make([]GroupTarget, 0, len(targets))
	seen := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		if _, ok := seen[target.Expected.ID]; ok {
			return nil, errs.ErrAffinityGroupContent.FastGenByArgs("duplicate group " + target.Expected.ID)
		}
		seen[target.Expected.ID] = struct{}{}
		adjustedTarget, err := m.adjustGroupTarget(target)
		if err != nil {
			return nil, err
		}
		if err := m.checkHasUnavailableStore(adjustedTarget.LeaderStoreID, adjustedTarget.VoterStoreIDs); err != nil {
			log.Info("skip affinity group target with an unavailable store",
				zap.String("group-id", target.Expected.ID), errs.ZapError(err))
			continue
		}
		adjusted = append(adjusted, adjustedTarget)
	}

	written := make([]string, 0, len(adjusted))
	for chunk := range slices.Chunk(adjusted, etcdutil.MaxEtcdTxnOps) {
		if err := ctx.Err(); err != nil {
			return written, err
		}
		ids, err := m.updateGroupTargets(ctx, chunk)
		written = append(written, ids...)
		if err != nil {
			return written, err
		}
	}
	return written, nil
}

// adjustGroupTarget validates a target and sorts its voters.
func (m *Manager) adjustGroupTarget(target GroupTarget) (GroupTarget, error) {
	if target.LeaderStoreID == 0 || len(target.VoterStoreIDs) == 0 {
		return GroupTarget{}, errs.ErrAffinityGroupContent.FastGenByArgs("leader store ID and voter store IDs must be provided")
	}
	if err := m.AdjustGroup(&Group{
		ID:            target.Expected.ID,
		LeaderStoreID: target.LeaderStoreID,
		VoterStoreIDs: target.VoterStoreIDs,
	}); err != nil {
		return GroupTarget{}, err
	}
	target.Expected.VoterStoreIDs = sortedStoreIDs(target.Expected.VoterStoreIDs)
	target.VoterStoreIDs = sortedStoreIDs(target.VoterStoreIDs)
	return target, nil
}

func (m *Manager) updateGroupTargets(ctx context.Context, targets []GroupTarget) ([]string, error) {
	m.metaMutex.Lock()
	defer m.metaMutex.Unlock()
	return m.updateGroupTargetsLocked(ctx, targets)
}

// updateGroupTargetsLocked writes adjusted targets in one transaction. The
// transaction loads every stored group, so it fails if any of them changes
// before the commit. metaMutex must be held.
func (m *Manager) updateGroupTargetsLocked(ctx context.Context, targets []GroupTarget) ([]string, error) {
	// stored is the group loaded for each target, and saved the group that holds
	// its target after the commit, if it is written.
	stored := make([]*Group, len(targets))
	saved := make([]*Group, len(targets))
	err := m.storage.RunInTxn(ctx, func(txn kv.Txn) error {
		for i, target := range targets {
			value, err := m.storage.LoadAffinityGroupInTxn(txn, target.Expected.ID)
			if err != nil {
				return err
			}
			if value == "" {
				continue
			}
			group := &Group{}
			if err := json.Unmarshal([]byte(value), group); err != nil {
				log.Warn("failed to unmarshal stored affinity group, skipping its target",
					zap.String("group-id", target.Expected.ID), zap.Error(err))
				continue
			}
			group.VoterStoreIDs = sortedStoreIDs(group.VoterStoreIDs)
			stored[i] = group
			switch {
			case sameStoredGroup(group, &target.Expected):
				saved[i] = target.result(group)
				if err := m.storage.SaveAffinityGroup(txn, group.ID, saved[i]); err != nil {
					return err
				}
			case sameStoredGroup(group, target.result(&target.Expected)):
				// An earlier request with this target committed.
				saved[i] = group
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	written := make([]string, 0, len(targets))
	for i, target := range targets {
		switch {
		case saved[i] != nil:
			written = append(written, target.Expected.ID)
			m.publishGroupTargetLocked(saved[i])
		case stored[i] != nil:
			m.refreshGroupTargetLocked(stored[i])
		}
	}
	return written, nil
}

// publishGroupTargetLocked makes the in-memory group equal a stored group whose
// target was just written, replacing the runtime group if it was recreated, and
// sets its availability from the store conditions known now. metaMutex must be held.
func (m *Manager) publishGroupTargetLocked(stored *Group) {
	m.Lock()
	defer m.Unlock()
	groupInfo, ok := m.groups[stored.ID]
	if !ok {
		log.Warn("written affinity group target is not in memory", zap.String("group-id", stored.ID))
		return
	}
	m.syncGroupTargetLocked(groupInfo, stored)
	groupInfo = m.groups[stored.ID]
	m.setTargetAvailabilityLocked(groupInfo)
	m.resetCountLocked(groupInfo)
}

// sendGroupTargets sends targets to the target writer and returns the IDs of
// the written groups. A written target is published unless another change has
// reached its group meanwhile. If the writer fails partway, the targets written
// before the failure are published and returned with the error. metaMutex must
// not be held.
func (m *Manager) sendGroupTargets(ctx context.Context, targets []GroupTarget) ([]string, error) {
	// Send the targets without holding metaMutex, so that a slow PD does not block the watcher.
	written, err := m.targetWriter.WriteGroupTargets(ctx, targets)
	if err != nil {
		targetWriteFailureCounter.Inc()
		log.Warn("failed to write affinity group targets",
			zap.Int("target-count", len(targets)), zap.Int("written-count", len(written)), errs.ZapError(err))
	}

	m.metaMutex.Lock()
	defer m.metaMutex.Unlock()
	writtenIDs := make(map[string]struct{}, len(written))
	for _, id := range written {
		writtenIDs[id] = struct{}{}
	}
	for _, target := range targets {
		if _, ok := writtenIDs[target.Expected.ID]; !ok {
			continue
		}
		current := m.GetAffinityGroupState(target.Expected.ID)
		// The watcher may have applied the target already.
		result := target.result(&target.Expected)
		if current == nil || !sameStoredGroup(&current.Group, &target.Expected) && !sameStoredGroup(&current.Group, result) {
			continue
		}
		m.publishGroupTargetLocked(result)
	}
	return written, err
}

// refreshGroupTargetLocked makes the in-memory group match the stored group,
// for example after a commit whose result was unknown. metaMutex must be held.
func (m *Manager) refreshGroupTargetLocked(stored *Group) {
	m.Lock()
	defer m.Unlock()
	groupInfo, ok := m.groups[stored.ID]
	if !ok || sameStoredGroup(&groupInfo.Group, stored) {
		return
	}
	log.Info("refresh affinity group target from storage",
		zap.String("group-id", stored.ID),
		zap.Uint64("leader-store-id", stored.LeaderStoreID),
		zap.Uint64s("voter-store-ids", stored.VoterStoreIDs))
	m.syncGroupTargetLocked(groupInfo, stored)
}

// result returns the stored group after the target is written to it.
func (t *GroupTarget) result(stored *Group) *Group {
	group := *stored
	group.LeaderStoreID = t.LeaderStoreID
	group.VoterStoreIDs = slices.Clone(t.VoterStoreIDs)
	return &group
}

// sameStoredGroup reports whether two persisted groups are the same. Voter order
// does not matter. It is the only comparison of persisted groups, so that a field
// added to Group is compared everywhere.
func sameStoredGroup(a, b *Group) bool {
	return a.ID == b.ID &&
		a.CreateTimestamp == b.CreateTimestamp &&
		sameBalancePolicy(a.BalancePolicy, b.BalancePolicy) &&
		a.LeaderStoreID == b.LeaderStoreID &&
		slices.Equal(sortedStoreIDs(a.VoterStoreIDs), sortedStoreIDs(b.VoterStoreIDs))
}

// sameBalancePolicy compares policies, where an empty policy means auto.
func sameBalancePolicy(a, b BalancePolicy) bool {
	if a == "" {
		a = BalancePolicyAuto
	}
	if b == "" {
		b = BalancePolicyAuto
	}
	return a == b
}

func sortedStoreIDs(ids []uint64) []uint64 {
	if len(ids) == 0 {
		return nil
	}
	sorted := slices.Clone(ids)
	slices.Sort(sorted)
	return sorted
}
