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

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keypath"
)

// GroupTarget is a target decided from the persisted group in Expected.
// NOTE: This type is exported by HTTP API.
type GroupTarget struct {
	Expected      Group    `json:"expected"`
	LeaderStoreID uint64   `json:"leader_store_id"`
	VoterStoreIDs []uint64 `json:"voter_store_ids"`
}

// GroupTargetsRequest is the body of the conditional target API.
type GroupTargetsRequest struct {
	Targets []GroupTarget `json:"targets"`
}

// GroupTargetsResponse lists targets committed or already stored.
type GroupTargetsResponse struct {
	WrittenGroupIDs []string `json:"written_group_ids"`
}

// TargetWriter persists targets in PD. The watcher publishes them locally.
type TargetWriter interface {
	WriteGroupTargets(context.Context, []GroupTarget) ([]string, error)
}

// SetTargetWriter configures an MCS Manager before it is used.
func (m *Manager) SetTargetWriter(w TargetWriter) {
	m.targetWriter = w
}

// UpdateAffinityGroupTargets validates the whole request, then commits each
// target in its own conditional transaction. Earlier commits survive an error.
// Each target releases metaMutex before the next one starts.
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
		if target.LeaderStoreID == 0 || len(target.VoterStoreIDs) == 0 {
			return nil, errs.ErrAffinityGroupContent.FastGenByArgs("leader store ID and voter store IDs must be provided")
		}
		if err := m.AdjustGroup(&Group{ID: target.Expected.ID, LeaderStoreID: target.LeaderStoreID, VoterStoreIDs: target.VoterStoreIDs}); err != nil {
			return nil, err
		}
		target.Expected.VoterStoreIDs = sortedStoreIDs(target.Expected.VoterStoreIDs)
		target.VoterStoreIDs = sortedStoreIDs(target.VoterStoreIDs)
		adjusted = append(adjusted, target)
	}
	written := make([]string, 0, len(adjusted))
	for _, target := range adjusted {
		if err := m.checkHasUnavailableStore(target.LeaderStoreID, target.VoterStoreIDs); err != nil {
			continue
		}
		ok, err := m.updateGroupTarget(target)
		if err != nil {
			return written, err
		}
		if ok {
			written = append(written, target.Expected.ID)
		}
	}
	return written, nil
}

func (m *Manager) updateGroupTarget(target GroupTarget) (bool, error) {
	m.metaMutex.Lock()
	defer m.metaMutex.Unlock()
	var stored *Group
	written := false
	err := m.storage.RunInTxn(m.ctx, func(txn kv.Txn) error {
		value, err := txn.Load(keypath.AffinityGroupPath(target.Expected.ID))
		if err != nil || value == "" {
			return err
		}
		group := &Group{}
		if err := json.Unmarshal([]byte(value), group); err != nil {
			return err
		}
		group.VoterStoreIDs = sortedStoreIDs(group.VoterStoreIDs)
		stored = group
		result := target.result()
		switch {
		case sameStoredGroup(group, result):
			written = true
		case sameStoredGroup(group, &target.Expected):
			if err := m.storage.SaveAffinityGroup(txn, group.ID, result); err != nil {
				return err
			}
			stored, written = result, true
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	if stored != nil {
		m.Lock()
		if group, ok := m.groups[stored.ID]; ok {
			m.syncGroupTargetLocked(group, stored)
			if written {
				group = m.groups[stored.ID]
				m.setTargetAvailabilityLocked(group)
				m.resetCountLocked(group)
			}
		}
		m.Unlock()
	}
	return written, nil
}

func (t *GroupTarget) result() *Group {
	group := t.Expected
	group.LeaderStoreID = t.LeaderStoreID
	group.VoterStoreIDs = slices.Clone(t.VoterStoreIDs)
	return &group
}

// sameStoredGroup compares every persisted field, ignoring voter order. The
// existing second-resolution creation timestamp cannot distinguish same-ID
// recreations within one second.
func sameStoredGroup(a, b *Group) bool {
	return a.ID == b.ID && a.CreateTimestamp == b.CreateTimestamp &&
		a.LeaderStoreID == b.LeaderStoreID &&
		slices.Equal(sortedStoreIDs(a.VoterStoreIDs), sortedStoreIDs(b.VoterStoreIDs))
}

func sortedStoreIDs(ids []uint64) []uint64 {
	if len(ids) == 0 {
		return nil
	}
	ids = slices.Clone(ids)
	slices.Sort(ids)
	return ids
}
