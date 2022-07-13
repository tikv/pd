// Copyright 2022 TiKV Project Authors.
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
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/placement"
	"github.com/tikv/pd/server/schedule/plan"
)

var (
	statusRegionOK          = plan.NewStatus(plan.StatusOK)
	statusRegionPendingPeer = plan.NewStatus(plan.StatusRegionUnhealthy)
	statusRegionDownPeer    = plan.NewStatus(plan.StatusRegionUnhealthy)
	statusRegionEmpty       = plan.NewStatus(plan.StatusRegionEmpty)
	statusRegionIsolation   = plan.NewStatus(plan.StatusIsolationNotMatch)
	statusRegionRule        = plan.NewStatus(plan.StatusRuleNotMatch)
)

// SelectRegions selects regions that be selected from the list.
func SelectRegions(regions []*core.RegionInfo, filters ...RegionFilter) []*core.RegionInfo {
	return filterRegionsBy(regions, func(r *core.RegionInfo) bool {
		return slice.AllOf(filters, func(i int) bool {
			return filters[i].Select(r).IsOK()
		})
	})
}

func filterRegionsBy(regions []*core.RegionInfo, keepPred func(*core.RegionInfo) bool) (selected []*core.RegionInfo) {
	for _, s := range regions {
		if keepPred(s) {
			selected = append(selected, s)
		}
	}
	return
}

// SelectOneRegion selects one region that be selected from the list.
func SelectOneRegion(regions []*core.RegionInfo, filters ...RegionFilter) *core.RegionInfo {
	for _, r := range regions {
		if slice.AllOf(filters, func(i int) bool { return filters[i].Select(r).IsOK() }) {
			return r
		}
	}
	return nil
}

// RegionFilter is an interface to filter region.
type RegionFilter interface {
	// RegionFilter is used to indicate where the filter will act on.
	Scope() string
	Type() string
	Reason() string
	// Return true if the region can be used to schedule.
	Select(region *core.RegionInfo) plan.Status
}

type regionPengdingFilter struct {
	scope string
}

// NewRegionPengdingFilter creates a RegionFilter that filters all regions with pending peers.
func NewRegionPengdingFilter(scope string) RegionFilter {
	return &regionPengdingFilter{scope: scope}
}

func (f *regionPengdingFilter) Scope() string {
	return f.scope
}

func (f *regionPengdingFilter) Type() string {
	return "Unhealthy"
}

func (f *regionPengdingFilter) Reason() string {
	return "This region has too many pending pees."
}

func (f *regionPengdingFilter) Select(region *core.RegionInfo) plan.Status {
	if len(region.GetPendingPeers()) > 0 {
		return statusRegionPendingPeer
	}
	return statusRegionOK
}

type regionDownFilter struct {
	scope string
}

// NewRegionDownFilter creates a RegionFilter that filters all regions with down peers.
func NewRegionDownFilter(scope string) RegionFilter {
	return &regionDownFilter{scope: scope}
}

func (f *regionDownFilter) Scope() string {
	return f.scope
}

func (f *regionDownFilter) Type() string {
	return "Unhealthy"
}

func (f *regionDownFilter) Reason() string {
	return "This region has too many down pees."
}

func (f *regionDownFilter) Select(region *core.RegionInfo) plan.Status {
	if len(region.GetDownPeers()) > 0 {
		return statusRegionDownPeer
	}
	return statusRegionOK
}

type regionReplicatedFilter struct {
	scope   string
	cluster Cluster
}

// NewRegionReplicatedFilter creates a RegionFilter that filters all unreplicated regions.
func NewRegionReplicatedFilter(scope string, cluster Cluster) RegionFilter {
	return &regionReplicatedFilter{scope: scope, cluster: cluster}
}

func (f *regionReplicatedFilter) Scope() string {
	return f.scope
}

func (f *regionReplicatedFilter) Type() string {
	return "NotReplicated"
}

func (f *regionReplicatedFilter) Reason() string {
	if f.cluster.GetOpts().IsPlacementRulesEnabled() {
		return "This region does not fit placement rule."
	}
	return "This region is not replicated"
}

func (f *regionReplicatedFilter) Select(region *core.RegionInfo) plan.Status {
	if f.cluster.GetOpts().IsPlacementRulesEnabled() {
		if !f.cluster.GetRuleManager().FitRegion(f.cluster, region).IsSatisfied() {
			return statusRegionRule
		}
		return statusRegionOK
	}
	if !(len(region.GetLearners()) == 0 && len(region.GetPeers()) == f.cluster.GetOpts().GetMaxReplicas()) {
		return statusRegionIsolation
	}
	return statusRegionOK
}

type regionEmptyFilter struct {
	scope   string
	cluster Cluster
}

// NewRegionEmptyFilter returns creates a RegionFilter that filters all empty regions.
func NewRegionEmptyFilter(scope string, cluster Cluster) RegionFilter {
	return &regionEmptyFilter{scope: scope, cluster: cluster}
}

func (f *regionEmptyFilter) Scope() string {
	return f.scope
}

func (f *regionEmptyFilter) Type() string {
	return "EmptyRegion"
}

func (f *regionEmptyFilter) Reason() string {
	return "This region is empty"
}

func (f *regionEmptyFilter) Select(region *core.RegionInfo) plan.Status {
	if !isEmptyRegionAllowBalance(f.cluster, region) {
		return statusRegionEmpty
	}
	return statusRegionOK
}

// isEmptyRegionAllowBalance checks if a region is an empty region and can be balanced.
func isEmptyRegionAllowBalance(cluster Cluster, region *core.RegionInfo) bool {
	return region.GetApproximateSize() > core.EmptyRegionApproximateSize || cluster.GetRegionCount() < core.InitClusterRegionThreshold
}

// Cluster provides an overview of a cluster's regions distribution.
type Cluster interface {
	core.StoreSetInformer
	core.StoreSetController
	core.RegionSetInformer
	GetOpts() *config.PersistOptions
	GetRuleManager() *placement.RuleManager
}
