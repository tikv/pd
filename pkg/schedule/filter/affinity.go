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
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/affinity"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/plan"
)

type affinityFilter struct {
	affinityManager *affinity.Manager
}

// NewAffinityFilter creates a RegionFilter that filters all affinity regions.
func NewAffinityFilter(cluster sche.SharedCluster) RegionFilter {
	return &affinityFilter{
		affinityManager: cluster.GetAffinityManager(),
	}
}

// Select implements the RegionFilter interface.
func (f *affinityFilter) Select(region *core.RegionInfo) *plan.Status {
	if f.affinityManager != nil && f.affinityManager.IsRegionAffinity(region) {
		return statusRegionAffinity
	}
	return statusOK
}
