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
	// expected values and returns the IDs of the written groups.
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
// becomes available. A group whose stored value differs is not written; its
// in-memory group is refreshed from storage instead.
func (m *Manager) UpdateAffinityGroupTargets(targets []GroupTarget) ([]string, error) {
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
		target, err := m.adjustGroupTarget(target)
		if err != nil {
			return nil, err
		}
		adjusted = append(adjusted, target)
	}

	var written []string
	for chunk := range slices.Chunk(adjusted, etcdutil.MaxEtcdTxnOps) {
		ids, err := m.updateGroupTargets(chunk)
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
	if err := m.checkHasUnavailableStore(target.LeaderStoreID, target.VoterStoreIDs); err != nil {
		return GroupTarget{}, err
	}
	target.Expected.VoterStoreIDs = sortedStoreIDs(target.Expected.VoterStoreIDs)
	target.VoterStoreIDs = sortedStoreIDs(target.VoterStoreIDs)
	return target, nil
}

func (m *Manager) updateGroupTargets(targets []GroupTarget) ([]string, error) {
	m.metaMutex.Lock()
	defer m.metaMutex.Unlock()
	return m.updateGroupTargetsLocked(targets)
}

// updateGroupTargetsLocked writes adjusted targets in one transaction. The
// transaction loads every stored group, so it fails if any of them changes
// before the commit. metaMutex must be held.
func (m *Manager) updateGroupTargetsLocked(targets []GroupTarget) ([]string, error) {
	stored := make([]*Group, len(targets))
	err := m.storage.RunInTxn(m.ctx, func(txn kv.Txn) error {
		for i, target := range targets {
			stored[i] = nil
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
			if !sameStoredGroup(group, &target.Expected) {
				continue
			}
			if err := m.storage.SaveAffinityGroup(txn, group.ID, target.result(group)); err != nil {
				return err
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
		case stored[i] == nil:
		case sameStoredGroup(stored[i], &target.Expected):
			written = append(written, target.Expected.ID)
			if _, err := m.updateGroupPeers(target.Expected.ID, target.LeaderStoreID, target.VoterStoreIDs); err != nil {
				log.Warn("written affinity group target is not in memory",
					zap.String("group-id", target.Expected.ID), zap.Error(err))
			}
		default:
			m.refreshGroupTargetLocked(stored[i])
		}
	}
	return written, nil
}

// refreshGroupTargetLocked makes the in-memory group match the stored group,
// for example after a commit whose result was unknown. metaMutex must be held.
func (m *Manager) refreshGroupTargetLocked(stored *Group) {
	m.Lock()
	defer m.Unlock()
	groupInfo, ok := m.groups[stored.ID]
	if !ok || (groupInfo.CreateTimestamp == stored.CreateTimestamp &&
		groupInfo.LeaderStoreID == stored.LeaderStoreID &&
		slices.Equal(sortedStoreIDs(groupInfo.VoterStoreIDs), stored.VoterStoreIDs)) {
		return
	}
	log.Info("refresh affinity group target from storage",
		zap.String("group-id", stored.ID),
		zap.Uint64("leader-store-id", stored.LeaderStoreID),
		zap.Uint64s("voter-store-ids", stored.VoterStoreIDs))
	sameInstance := groupInfo.CreateTimestamp == stored.CreateTimestamp
	targetChanged := groupInfo.LeaderStoreID != stored.LeaderStoreID ||
		!slices.Equal(sortedStoreIDs(groupInfo.VoterStoreIDs), stored.VoterStoreIDs)
	m.syncGroupTargetLocked(groupInfo, stored)
	if sameInstance && targetChanged {
		// A write whose result was unknown committed this target. Check it against
		// the store conditions known now, because the availability check applies
		// them again only when they change.
		groupInfo.SetAvailability(calcGroupAvailability(m.unavailableStores, stored.LeaderStoreID, stored.VoterStoreIDs))
	}
}

// result returns the stored group after the target is written to it.
func (t *GroupTarget) result(stored *Group) *Group {
	group := *stored
	group.LeaderStoreID = t.LeaderStoreID
	group.VoterStoreIDs = slices.Clone(t.VoterStoreIDs)
	return &group
}

// sameStoredGroup reports whether two persisted groups are the same. Voter order does not matter.
func sameStoredGroup(a, b *Group) bool {
	return a.ID == b.ID &&
		a.CreateTimestamp == b.CreateTimestamp &&
		a.LeaderStoreID == b.LeaderStoreID &&
		slices.Equal(sortedStoreIDs(a.VoterStoreIDs), sortedStoreIDs(b.VoterStoreIDs))
}

func sortedStoreIDs(ids []uint64) []uint64 {
	if len(ids) == 0 {
		return nil
	}
	sorted := slices.Clone(ids)
	slices.Sort(sorted)
	return sorted
}
