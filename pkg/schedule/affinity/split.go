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
)

type splitDenial struct {
	expiresAt time.Time
}

// AllowSplit returns true if the region can be auto split
func (m *Manager) AllowSplit(region *core.RegionInfo, reason pdpb.SplitReason) bool {
	allowed := m.isSplitAllowed(region, reason)
	if !allowed {
		regionSplitDenyCount.Inc()
	}
	return allowed
}

// isSplitAllowed checks the policy without counting a denied split request.
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
	if m.AllowSplit(region, reason) {
		return true
	}
	if region.GetLeader() != nil {
		m.splitMutex.Lock()
		defer m.splitMutex.Unlock()
		streams.SendMsg(region, &hbstream.Operation{ChangeSplit: &pdpb.ChangeSplit{AutoSplitEnabled: false}})
		m.splitDeniedRegions.Put(region.GetID(), &splitDenial{expiresAt: time.Now().Add(splitDeniedRegionTTL)})
	}
	return false
}

// MaybeEnableSplit sends one recovery notification for a previously denied Region.
// Call it for accepted heartbeats, including those that do not change Region metadata.
// Heartbeats leave the TiKV validator's TTL alone so it can run a normal split
// check at expiry even if the disabled checker's estimates stay below PD's limits.
func (m *Manager) MaybeEnableSplit(region *core.RegionInfo, streams *hbstream.HeartbeatStreams) {
	if region == nil || region.GetLeader() == nil {
		return
	}
	regionID := region.GetID()
	tracked, ok := m.splitDeniedRegions.Peek(regionID)
	if !ok {
		return
	}
	denial := tracked.(*splitDenial)
	if time.Now().Before(denial.expiresAt) && !m.isSplitAllowed(region, pdpb.SplitReason_SIZE) {
		return
	}

	m.splitMutex.Lock()
	defer m.splitMutex.Unlock()
	// Another heartbeat may have recovered this Region, or a new split
	// request may have been denied while this heartbeat was checking policy.
	if current, ok := m.splitDeniedRegions.Peek(regionID); !ok || current != denial {
		return
	}
	if time.Now().Before(denial.expiresAt) {
		streams.SendMsg(region, &hbstream.Operation{ChangeSplit: &pdpb.ChangeSplit{AutoSplitEnabled: true}})
	}
	m.splitDeniedRegions.Remove(regionID)
}
