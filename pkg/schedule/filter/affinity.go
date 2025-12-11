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

package filter

import (
	"strconv"

	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/config"
	sche "github.com/tikv/pd/pkg/schedule/core"
)

// For affinity regions, we use calculation: (maxSize + buffer) * multiplier
// Derived from default values:
//   - Default MaxMergeRegionSize = 54MB
//   - Default region-split-size = 256MB
const (
	affinityRegionSizeBufferMB   = 10
	affinityRegionSizeMultiplier = 4
)

// AllowAutoSplit returns true if the region can be auto split
func AllowAutoSplit(cluster sche.ClusterInformer, region *core.RegionInfo, reason pdpb.SplitReason) bool {
	if region == nil {
		// The default behavior is to allow it.
		return true
	}

	if !cluster.GetCheckerConfig().IsAffinitySchedulingEnabled() {
		return true
	}

	if reason == pdpb.SplitReason_ADMIN {
		return true
	}

	configSize := int64(cluster.GetCheckerConfig().GetMaxAffinityMergeRegionSize())
	if configSize == 0 {
		return true
	}

	if affinityManager := cluster.GetAffinityManager(); affinityManager != nil {
		group, _ := affinityManager.GetRegionAffinityGroupState(region)
		if group != nil && !group.RegularSchedulingEnabled {
			maxSize := (configSize + affinityRegionSizeBufferMB) * affinityRegionSizeMultiplier
			maxKeys := maxSize * config.RegionSizeToKeysRatio
			// Only block splitting when the Region is in the affinity state.
			// But still allow splitting if the Region size is too big.
			if region.GetApproximateSize() < maxSize && region.GetApproximateKeys() < maxKeys {
				sourceID := strconv.FormatUint(region.GetLeader().GetStoreId(), 10)
				filterSourceCounter.WithLabelValues("split-deny-by-affinity", reason.String(), sourceID).Inc()
				return false
			}
		}
	}

	// The default behavior is to allow it.
	return true
}
