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
	"errors"
	"slices"

	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
)

// ErrTargetWriterNotReady indicates that no request could be sent yet.
var ErrTargetWriterNotReady = errors.New("affinity target writer is not ready")

// ErrTargetWriterUnsupported indicates that PD does not implement the target endpoint.
var ErrTargetWriterUnsupported = errors.New("affinity target endpoint is unsupported")

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
	Error           string   `json:"error,omitempty"`
}

// TargetWriter persists targets in PD. The watcher publishes them locally.
type TargetWriter interface {
	WriteGroupTargets(context.Context, []GroupTarget) ([]string, error)
}

// SetTargetWriter configures an MCS Manager before it is used.
func (m *Manager) SetTargetWriter(w TargetWriter) {
	m.targetWriter = w
}

// UpdateAffinityGroupTargets validates the whole request, then conditionally
// commits bounded batches. Earlier batches survive a later error.
func (m *Manager) UpdateAffinityGroupTargets(ctx context.Context, targets []GroupTarget) ([]string, error) {
	return m.updateGroupTargets(ctx, targets)
}

func (m *Manager) updateGroupTargets(ctx context.Context, targets []GroupTarget) ([]string, error) {
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
		if err := validateGroupFormat(&Group{ID: target.Expected.ID, LeaderStoreID: target.LeaderStoreID, VoterStoreIDs: target.VoterStoreIDs}); err != nil {
			return nil, err
		}
		target.Expected.VoterStoreIDs = sortedStoreIDs(target.Expected.VoterStoreIDs)
		target.VoterStoreIDs = sortedStoreIDs(target.VoterStoreIDs)
		adjusted = append(adjusted, target)
	}
	adjusted = slices.DeleteFunc(adjusted, func(target GroupTarget) bool {
		for _, id := range target.VoterStoreIDs {
			if store := m.storeSetInformer.GetStore(id); store == nil || store.IsRemoved() {
				return true
			}
		}
		return m.checkHasUnavailableStore(target.LeaderStoreID, target.VoterStoreIDs) != nil
	})
	written := make([]string, 0, len(adjusted))
	for chunk := range slices.Chunk(adjusted, etcdutil.MaxEtcdTxnOps) {
		if err := ctx.Err(); err != nil {
			return written, err
		}
		m.metaMutex.Lock()
		ids, err := m.updateGroupTargetsLocked(ctx, chunk)
		m.metaMutex.Unlock()
		written = append(written, ids...)
		if err != nil {
			return written, err
		}
		failpoint.InjectCall("afterAffinityTargetWritten")
	}
	return written, nil
}

func (m *Manager) updateGroupTarget(ctx context.Context, target GroupTarget) (bool, error) {
	m.metaMutex.Lock()
	defer m.metaMutex.Unlock()
	return m.updateGroupTargetLocked(ctx, target)
}

// updateGroupTargetLocked requires metaMutex to serialize metadata changes.
func (m *Manager) updateGroupTargetLocked(ctx context.Context, target GroupTarget) (bool, error) {
	written, err := m.updateGroupTargetsLocked(ctx, []GroupTarget{target})
	return len(written) != 0, err
}

// updateGroupTargetsLocked requires metaMutex to serialize metadata changes.
func (m *Manager) updateGroupTargetsLocked(ctx context.Context, targets []GroupTarget) ([]string, error) {
	keys := make([]string, len(targets))
	for i, target := range targets {
		keys[i] = keypath.AffinityGroupPath(target.Expected.ID)
	}
	stored := make([]*Group, len(targets))
	written := make([]string, 0, len(targets))
	accepted := make([]bool, len(targets))
	err := m.storage.RunInTxn(ctx, func(txn kv.Txn) error {
		if err := kv.Prefetch(txn, keys); err != nil {
			return err
		}
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
				return err
			}
			group.VoterStoreIDs = sortedStoreIDs(group.VoterStoreIDs)
			stored[i] = group
			result := target.result()
			switch {
			case sameStoredGroup(group, result):
				accepted[i] = true
			case sameStoredGroup(group, &target.Expected):
				if err := m.storage.SaveAffinityGroup(txn, group.ID, result); err != nil {
					return err
				}
				stored[i], accepted[i] = result, true
			}
			if accepted[i] {
				written = append(written, group.ID)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	m.Lock()
	defer m.Unlock()
	for i, value := range stored {
		if value == nil {
			continue
		}
		if group, ok := m.groups[value.ID]; ok {
			wasUnpersisted := group.targetUnpersisted
			if changed := m.syncGroupTargetLocked(group, value); accepted[i] && !changed && (!wasUnpersisted || !group.IsAvailable()) {
				if m.storeConditionsKnown {
					m.setTargetAvailabilityLocked(group)
				}
				m.resetCountLocked(group)
			}
		}
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
	ids = slices.Clone(ids)
	slices.Sort(ids)
	return ids
}
