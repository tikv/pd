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
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/core/storelimit"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/schedule/plan"
	"github.com/tikv/pd/pkg/slice"
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
func SelectOneRegion(regions []*core.RegionInfo, collector *plan.Collector, filters ...RegionFilter) *core.RegionInfo {
	for _, r := range regions {
		if len(filters) == 0 || slice.AllOf(filters,
			func(i int) bool {
				status := filters[i].Select(r)
				if !status.IsOK() {
					if collector != nil {
						collector.Collect(plan.SetResource(r), plan.SetStatus(status))
					}
					return false
				}
				return true
			}) {
			return r
		}
	}
	return nil
}

// RegionFilter is an interface to filter region.
type RegionFilter interface {
	// Return plan.Status show whether be filtered
	Select(region *core.RegionInfo) *plan.Status
}

type regionPendingFilter struct {
}

// NewRegionPendingFilter creates a RegionFilter that filters all regions with pending peers.
func NewRegionPendingFilter() RegionFilter {
	return &regionPendingFilter{}
}

// Select implements the RegionFilter interface.
func (*regionPendingFilter) Select(region *core.RegionInfo) *plan.Status {
	if hasPendingPeers(region) {
		return statusRegionPendingPeer
	}
	return statusOK
}

type regionDownFilter struct {
}

// NewRegionDownFilter creates a RegionFilter that filters all regions with down peers.
func NewRegionDownFilter() RegionFilter {
	return &regionDownFilter{}
}

// Select implements the RegionFilter interface.
func (*regionDownFilter) Select(region *core.RegionInfo) *plan.Status {
	if hasDownPeers(region) {
		return statusRegionDownPeer
	}
	return statusOK
}

// RegionReplicatedFilter filters all unreplicated regions.
type RegionReplicatedFilter struct {
	cluster sche.SharedCluster
	fit     *placement.RegionFit
}

// NewRegionReplicatedFilter creates a RegionFilter that filters all unreplicated regions.
func NewRegionReplicatedFilter(cluster sche.SharedCluster) RegionFilter {
	return &RegionReplicatedFilter{cluster: cluster}
}

// GetFit returns the region fit.
func (f *RegionReplicatedFilter) GetFit() *placement.RegionFit {
	return f.fit
}

// Select returns Ok if the given region satisfy the replication.
// it will cache the lasted region fit if the region satisfy the replication.
func (f *RegionReplicatedFilter) Select(region *core.RegionInfo) *plan.Status {
	if f.cluster.GetSharedConfig().IsPlacementRulesEnabled() {
		fit := f.cluster.GetRuleManager().FitRegion(f.cluster, region)
		if !fit.IsSatisfied() {
			return statusRegionNotMatchRule
		}
		f.fit = fit
		return statusOK
	}
	if !isRegionReplicasSatisfied(f.cluster, region) {
		return statusRegionNotReplicated
	}
	return statusOK
}

type regionEmptyFilter struct {
	cluster sche.SharedCluster
}

// NewRegionEmptyFilter returns creates a RegionFilter that filters all empty regions.
func NewRegionEmptyFilter(cluster sche.SharedCluster) RegionFilter {
	return &regionEmptyFilter{cluster: cluster}
}

// Select implements the RegionFilter interface.
func (f *regionEmptyFilter) Select(region *core.RegionInfo) *plan.Status {
	if !isEmptyRegionAllowBalance(f.cluster, region) {
		return statusRegionEmpty
	}
	return statusOK
}

// isEmptyRegionAllowBalance returns true if the region is not empty or the number of regions is too small.
func isEmptyRegionAllowBalance(cluster sche.SharedCluster, region *core.RegionInfo) bool {
	return region.GetApproximateSize() > core.EmptyRegionApproximateSize || cluster.GetTotalRegionCount() < core.InitClusterRegionThreshold
}

type regionWitnessFilter struct {
	storeID uint64
}

// NewRegionWitnessFilter returns creates a RegionFilter that filters regions with witness peer on the specific store.
func NewRegionWitnessFilter(storeID uint64) RegionFilter {
	return &regionWitnessFilter{storeID: storeID}
}

// Select implements the RegionFilter interface.
func (f *regionWitnessFilter) Select(region *core.RegionInfo) *plan.Status {
	if region.GetStoreWitness(f.storeID) != nil {
		return statusRegionWitnessPeer
	}
	return statusOK
}

// SnapshotSenderFilter filer the region who's leader store reaches the limit.
type SnapshotSenderFilter struct {
	senders map[uint64]struct{}
}

// NewSnapshotSendFilter returns creates a RegionFilter that filters regions with witness peer on the specific store.
// level should be set as same with the operator priority level.
func NewSnapshotSendFilter(stores []*core.StoreInfo, level constant.PriorityLevel) RegionFilter {
	senders := make(map[uint64]struct{})
	for _, store := range stores {
		if store.IsAvailable(storelimit.SendSnapshot, level) && !store.IsBusy() {
			senders[store.GetID()] = struct{}{}
		}
	}
	return &SnapshotSenderFilter{senders: senders}
}

// Select returns ok if the region leader in the senders.
func (f *SnapshotSenderFilter) Select(region *core.RegionInfo) *plan.Status {
	leaderStoreID := region.GetLeader().GetStoreId()
	if _, ok := f.senders[leaderStoreID]; ok {
		return statusOK
	}
	return statusRegionLeaderSendSnapshotThrottled
}
