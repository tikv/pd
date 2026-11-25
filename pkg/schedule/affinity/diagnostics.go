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
	"bytes"

	"github.com/tikv/pd/pkg/utils/keyutil"
)

// OperatorCount aggregates running operators by their display kind.
type OperatorCount struct {
	Kind  string `json:"kind"`
	Count uint64 `json:"count"`
}

// BlockedReason describes a diagnosed condition that prevents convergence.
type BlockedReason struct {
	Code string `json:"code"`
	Text string `json:"text"`
}

// GroupDiagnostics is a point-in-time diagnostic sample for an affinity group.
// Pointer fields distinguish an unknown value from a known zero. Slice fields
// are nil when coverage is incomplete and non-nil when the value is known.
type GroupDiagnostics struct {
	EffectivePolicy      *string         `json:"effective_policy"`
	ObservedAt           string          `json:"observed_at"`
	ActualLeaderStoreIDs []uint64        `json:"actual_leader_store_ids"`
	ActualVoterStoreIDs  [][]uint64      `json:"actual_voter_store_ids"`
	ApproximateSizeMiB   *uint64         `json:"approximate_size_mib"`
	PendingMoveMiB       *uint64         `json:"pending_move_mib"`
	LeaderSpread         *uint64         `json:"leader_spread"`
	VoterSpread          *uint64         `json:"voter_spread"`
	RunningOperators     []OperatorCount `json:"running_operators"`
	BlockedReasons       []BlockedReason `json:"blocked_reasons"`
}

// GroupSamplingSnapshot is an immutable view of the group metadata used while
// sampling regions. Version and ranges must still match after the scan.
type GroupSamplingSnapshot struct {
	State         *GroupState
	Ranges        GroupKeyRanges
	Version       uint64
	Availability  string
	RangeConflict bool
}

// GetGroupSamplingSnapshot returns group metadata without updating the checker
// cache. The returned ranges and group state do not share mutable slices with
// the manager.
func (m *Manager) GetGroupSamplingSnapshot(id string) *GroupSamplingSnapshot {
	m.RLock()
	groupInfo, ok := m.groups[id]
	if !ok {
		m.RUnlock()
		return nil
	}
	snapshot := &GroupSamplingSnapshot{
		State:         newGroupState(groupInfo),
		Version:       groupInfo.AffinityVer,
		Availability:  groupInfo.GetAvailability().String(),
		RangeConflict: groupInfo.rangeConflict,
	}
	cached, hasCached := m.keyRanges[id]
	labelRule := groupInfo.LabelRule
	m.RUnlock()

	snapshot.Ranges.GroupID = id
	if hasCached {
		snapshot.Ranges.KeyRanges = cloneKeyRanges(cached.KeyRanges)
		return snapshot
	}
	if labelRule != nil {
		ranges, err := extractKeyRangesFromLabelRule(labelRule)
		if err == nil {
			snapshot.Ranges.KeyRanges = cloneKeyRanges(ranges.KeyRanges)
		}
	}
	return snapshot
}

// SameGroupSampling reports whether two snapshots describe the same committed
// group targets and ranges.
func SameGroupSampling(a, b *GroupSamplingSnapshot) bool {
	if a == nil || b == nil || a.Version != b.Version || a.State.ID != b.State.ID || len(a.Ranges.KeyRanges) != len(b.Ranges.KeyRanges) {
		return false
	}
	for i := range a.Ranges.KeyRanges {
		if !bytes.Equal(a.Ranges.KeyRanges[i].StartKey, b.Ranges.KeyRanges[i].StartKey) ||
			!bytes.Equal(a.Ranges.KeyRanges[i].EndKey, b.Ranges.KeyRanges[i].EndKey) {
			return false
		}
	}
	return true
}

func cloneKeyRanges(ranges []keyutil.KeyRange) []keyutil.KeyRange {
	cloned := make([]keyutil.KeyRange, len(ranges))
	for i, r := range ranges {
		cloned[i] = keyutil.KeyRange{StartKey: bytes.Clone(r.StartKey), EndKey: bytes.Clone(r.EndKey)}
	}
	return cloned
}
