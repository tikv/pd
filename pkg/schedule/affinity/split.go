// Copyright 2025 TiKV Project Authors.
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
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/hbstream"
)

// For affinity regions, we use calculation: (maxSize + buffer) * multiplier
// Derived from default values:
//   - Default MaxMergeRegionSize = 54MB
//   - Default region-split-size = 256MB
const (
	affinityRegionSizeBufferMB   = 10
	affinityRegionSizeMultiplier = 4

	// Match the storage-side SplitValidator TTL. Evicted or expired entries fall
	// back to that TTL, as do entries lost on a PD leader or scheduling primary change.
	splitDeniedRegionTTL   = 600 * time.Second
	splitDeniedRegionLimit = 100_000
	// Renew halfway through storage's TTL to tolerate missed heartbeats.
	splitDeniedRenewMargin = splitDeniedRegionTTL / 2
)

// AllowSplit returns true if the region can be auto split
func (m *Manager) AllowSplit(region *core.RegionInfo, reason pdpb.SplitReason) bool {
	allowed := m.isSplitAllowed(region, reason)
	if !allowed {
		regionSplitDenyCount.Inc()
	}
	return allowed
}

// isSplitAllowed also serves heartbeat renewal checks, which must not count a
// new denied split request each time they evaluate the current policy.
func (m *Manager) isSplitAllowed(region *core.RegionInfo, reason pdpb.SplitReason) bool {
	if region == nil {
		// The default behavior is to allow it.
		return true
	}

	if reason == pdpb.SplitReason_ADMIN {
		return true
	}

	if m.conf.GetAffinityScheduleLimit() == 0 {
		return true
	}

	configSize := int64(m.conf.GetMaxAffinityMergeRegionSize())
	if configSize == 0 {
		return true
	}

	group, _ := m.GetRegionAffinityGroupState(region)
	if group != nil && group.LeaderStoreID != 0 && len(group.VoterStoreIDs) != 0 {
		maxSize := (configSize + affinityRegionSizeBufferMB) * affinityRegionSizeMultiplier
		maxKeys := maxSize * config.RegionSizeToKeysRatio
		// Retain split suppression while the group has a target, even if it expires.
		// Size and key-count limits still allow oversized Regions to split.
		if region.GetApproximateSize() < maxSize && region.GetApproximateKeys() < maxKeys {
			return false
		}
	}

	// The default behavior is to allow it.
	return true
}

// CheckSplit checks a split request and, if denied, notifies the Region leader
// and remembers to restore auto-splitting when a later heartbeat allows it.
func (m *Manager) CheckSplit(region *core.RegionInfo, reason pdpb.SplitReason, streams *hbstream.HeartbeatStreams) bool {
	if reason == pdpb.SplitReason_ADMIN {
		return true
	}
	m.splitMutex.Lock()
	defer m.splitMutex.Unlock()
	if m.AllowSplit(region, reason) {
		return true
	}
	if m.sendSplitLocked(region, streams, false) {
		// Tests track a new denial as if its renewal were due.
		failpoint.Inject("shortenSplitDenialTracking", func() {
			m.splitDeniedRegions.Put(region.GetID(), time.Now().Add(splitDeniedRenewMargin/2))
		})
	}
	return false
}

// sendSplitLocked updates best-effort tracking. The caller holds splitMutex.
func (m *Manager) sendSplitLocked(region *core.RegionInfo, streams *hbstream.HeartbeatStreams, enabled bool) bool {
	if region.GetLeader() == nil {
		return false
	}
	streams.SendMsg(region, &hbstream.Operation{ChangeSplit: &pdpb.ChangeSplit{AutoSplitEnabled: enabled}})
	if enabled {
		m.splitDeniedRegions.Remove(region.GetID())
	} else {
		m.splitDeniedRegions.Put(region.GetID(), time.Now().Add(splitDeniedRegionTTL))
	}
	return true
}

// MaybeEnableSplit sends one recovery notification for a previously denied Region.
// While the Region is still denied, it renews ChangeSplit(false) instead when the
// deadline is near. Call it for accepted heartbeats, including those that do not
// change Region metadata.
func (m *Manager) MaybeEnableSplit(region *core.RegionInfo, streams *hbstream.HeartbeatStreams) {
	if region == nil || region.GetLeader() == nil {
		return
	}
	if _, ok := m.splitDeniedRegions.Peek(region.GetID()); !ok {
		return
	}

	// Serialize notification attempts; untracked heartbeats skip the lock.
	m.splitMutex.Lock()
	defer m.splitMutex.Unlock()
	deadline, ok := m.splitDeniedRegions.Peek(region.GetID())
	if !ok {
		return
	}
	remaining := time.Until(deadline.(time.Time))
	if remaining <= 0 {
		m.splitDeniedRegions.Remove(region.GetID())
		return
	}
	enabled := m.isSplitAllowed(region, pdpb.SplitReason_SIZE)
	if enabled || remaining < splitDeniedRenewMargin {
		m.sendSplitLocked(region, streams, enabled)
	}
}
