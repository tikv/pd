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

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/kv"
)

// ErrTargetWriterNotReady indicates that no request could be sent yet.
var ErrTargetWriterNotReady = errors.New("affinity target writer is not ready")

// ErrTargetWriterUnsupported indicates that PD does not implement the target endpoint.
var ErrTargetWriterUnsupported = errors.New("affinity target endpoint is unsupported")

// GroupTarget is a target decided from the persisted group in Expected.
// NOTE: This type is exported by HTTP API.
type GroupTarget struct {
	Expected Group `json:"expected"`
	// ExpectedModRevision is the observed etcd key revision. Zero compares only the group contents.
	ExpectedModRevision int64    `json:"expected_mod_revision,omitempty"`
	LeaderStoreID       uint64   `json:"leader_store_id"`
	VoterStoreIDs       []uint64 `json:"voter_store_ids"`
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
// target in its own conditional transaction. Groups currently in range conflict
// cannot save a changed target, but an already stored result is still confirmed.
// Cancellation of ctx or the Manager context stops further work. The returned
// IDs include targets confirmed before cancellation or another error.
func (m *Manager) UpdateAffinityGroupTargets(ctx context.Context, targets []GroupTarget) ([]string, error) {
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
		if target.ExpectedModRevision < 0 {
			return nil, errs.ErrAffinityGroupContent.FastGenByArgs("expected modification revision must be non-negative")
		}
		// Group creation permits an entirely unset target.
		if target.LeaderStoreID == 0 && len(target.VoterStoreIDs) == 0 {
			return nil, errs.ErrAffinityGroupContent.FastGenByArgs("leader store ID and voter store IDs must be provided")
		}
		if err := validateGroupStructure(&Group{ID: target.Expected.ID, LeaderStoreID: target.LeaderStoreID, VoterStoreIDs: target.VoterStoreIDs}); err != nil {
			return nil, err
		}
		target.Expected.VoterStoreIDs = sortedStoreIDs(target.Expected.VoterStoreIDs)
		target.VoterStoreIDs = sortedStoreIDs(target.VoterStoreIDs)
		adjusted = append(adjusted, target)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(m.ctx, cancel)
	defer stop()

	written := make([]string, 0, len(adjusted))
	for _, target := range adjusted {
		// The Manager's AfterFunc runs asynchronously.
		if m.ctx.Err() != nil {
			cancel()
		}
		if err := ctx.Err(); err != nil {
			return written, err
		}
		ok, err := m.updateGroupTarget(ctx, target)
		if err != nil {
			return written, err
		}
		if ok {
			written = append(written, target.Expected.ID)
		}
	}
	return written, nil
}

func (m *Manager) updateGroupTarget(ctx context.Context, target GroupTarget) (bool, error) {
	m.metaMutex.Lock()
	defer m.metaMutex.Unlock()
	failpoint.InjectCall("afterLockGroupTarget")
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := m.ctx.Err(); err != nil {
		return false, err
	}
	m.RLock()
	initialGroup := m.groups[target.Expected.ID]
	var initialVer uint64
	if initialGroup != nil {
		initialVer = initialGroup.AffinityVer
	}
	m.RUnlock()

	var stored *Group
	written := false
	err := m.storage.RunInTxn(ctx, func(txn kv.Txn) error {
		value, revision, err := m.storage.LoadAffinityGroupInTxn(txn, target.Expected.ID)
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
		revisionMatches := target.ExpectedModRevision == 0 || target.ExpectedModRevision == revision
		switch {
		case sameStoredGroup(group, result):
			written = true
		case revisionMatches && sameStoredGroup(group, &target.Expected):
			m.RLock()
			current := m.groups[group.ID]
			conflict := current != nil && current.rangeConflict
			m.RUnlock()
			if conflict {
				return nil
			}
			if err := m.checkHasUnavailableStore(target.LeaderStoreID, target.VoterStoreIDs); err != nil {
				return nil
			}
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
			// A retry can restore a healthy persisted target. Sync handles
			// confirmation of local targets.
			changed := m.syncGroupTargetLocked(group, stored)
			if written && !changed && !group.IsAvailable() && m.storeConditionsKnown &&
				group == initialGroup && group.AffinityVer == initialVer &&
				m.checkHasUnavailableStoreLocked(group.LeaderStoreID, group.VoterStoreIDs) == nil {
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
	group.VoterStoreIDs = t.VoterStoreIDs
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
