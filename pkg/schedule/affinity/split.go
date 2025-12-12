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
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/config"
)

// For affinity regions, we use calculation: (maxSize + buffer) * multiplier
// Derived from default values:
//   - Default MaxMergeRegionSize = 54MB
//   - Default region-split-size = 256MB
const (
	affinityRegionSizeBufferMB   = 10
	affinityRegionSizeMultiplier = 4
)

// AllowSplit returns true if the region can be auto split
func (m *Manager) AllowSplit(region *core.RegionInfo, reason pdpb.SplitReason) bool {
	if region == nil {
		// The default behavior is to allow it.
		return true
	}

	if reason == pdpb.SplitReason_ADMIN {
		return true
	}

	if !m.conf.IsAffinitySchedulingEnabled() {
		return true
	}

	configSize := int64(m.conf.GetMaxAffinityMergeRegionSize())
	if configSize == 0 {
		return true
	}

	group, _ := m.GetRegionAffinityGroupState(region)
	if group != nil && !group.RegularSchedulingAllowed {
		maxSize := (configSize + affinityRegionSizeBufferMB) * affinityRegionSizeMultiplier
		maxKeys := maxSize * config.RegionSizeToKeysRatio
		// Only block splitting when the Region is in the affinity state.
		// But still allow splitting if the Region size is too big.
		if region.GetApproximateSize() < maxSize && region.GetApproximateKeys() < maxKeys {
			regionSplitDenyCount.Inc()
			return false
		}
	}

	// The default behavior is to allow it.
	return true
}
