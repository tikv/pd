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
	sche "github.com/tikv/pd/pkg/schedule/core"
)

const (
	splitOptionLabel     = "auto_split"
	splitOptionValueDeny = "deny"
)

// AllowAutoSplit returns true if the region can be auto split
func AllowAutoSplit(cluster sche.ClusterInformer, region *core.RegionInfo, reason pdpb.AutoSplitReason) bool {
	if region == nil {
		return false
	}

	switch reason {
	case pdpb.AutoSplitReason_LOAD, pdpb.AutoSplitReason_SIZE:
	default:
		return true
	}

	if cluster.GetRegionLabeler().GetRegionLabel(region, splitOptionLabel) == splitOptionValueDeny {
		sourceID := strconv.FormatUint(region.GetLeader().GetStoreId(), 10)
		filterSourceCounter.WithLabelValues("split_deny_by_label", reason.String(), sourceID).Inc()
		return false
	}

	return true
}
