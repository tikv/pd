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

package checker

import (
	"bytes"
	"slices"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/affinity"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

// prepareAffinityPeerMove returns a split or waits instead of adding a voter
// when the reported size or keys exceed TiKV's ordinary split thresholds. SCAN
// asks TiKV to find a half split; each child is checked again on a later patrol.
func (c *AffinityChecker) prepareAffinityPeerMove(region *core.RegionInfo, group *affinity.GroupState) (*operator.Operator, bool) {
	if !slices.ContainsFunc(group.VoterStoreIDs, func(id uint64) bool { return region.GetStorePeer(id) == nil }) {
		return nil, true
	}

	wait := func(reason string) (*operator.Operator, bool) {
		log.Debug("wait before affinity peer move", zap.Uint64("region-id", region.GetID()),
			zap.String("group-id", group.ID), zap.String("reason", reason))
		return nil, false
	}
	if region.GetApproximateSize() <= 0 {
		return wait("unknown-size")
	}
	storeConfig := c.cluster.GetStoreConfig()
	if uint64(region.GetApproximateSize()) > storeConfig.GetRegionSplitSize() ||
		uint64(region.GetApproximateKeys()) > storeConfig.GetRegionSplitKeys() {
		op, err := operator.CreateSplitRegionOperator("affinity-split-region", region,
			operator.OpAffinity|operator.OpSplit, pdpb.CheckPolicy_SCAN, nil)
		if err != nil {
			affinityCheckerCreateOpFailedCounter.Inc()
			log.Debug("create affinity split region operator failed", zap.Uint64("region-id", region.GetID()),
				zap.String("group-id", group.ID), errs.ZapError(err))
		}
		return op, false
	}
	// Merges can leave a positive but stale size. Reuse the merge cooldown;
	// after a restart its cache is gone, so allow the same time for statistics.
	if time.Since(c.startTime) < recentMergeTTL || c.recentMergeCache.Exists(region.GetID()) {
		return wait("recent-merge-or-startup")
	}
	// SplitRegion.IsFinish only observes the original Region's changed boundary.
	// Check the persisted group ranges too, including after a checker restart.
	if !affinityGroupRegionsReady(c.cluster, group, false) {
		return wait("incomplete-region-heartbeats")
	}
	return nil, true
}

// affinityGroupRegionsReady verifies coverage using the RegionTree, not the
// manager's cache, which may not have observed all children yet. During a move,
// siblings may already be adding peers; merging additionally requires every
// Region to be healthy and at the current target.
func affinityGroupRegionsReady(cluster sche.SharedCluster, group *affinity.GroupState, requireTarget bool) bool {
	manager := cluster.GetAffinityManager()
	ranges := manager.GetAffinityGroupKeyRanges(group)
	if len(ranges.KeyRanges) == 0 {
		return false
	}
	keyRanges := keyutil.NewKeyRanges(ranges.KeyRanges)
	slices.SortFunc(keyRanges.Ranges(), func(a, b *keyutil.KeyRange) int { return bytes.Compare(a.StartKey, b.StartKey) })
	regions, err := cluster.BatchScanRegions(keyRanges, core.WithOutputMustContainAllKeyRange())
	if err != nil || len(regions) == 0 {
		return false
	}
	// BatchScanRegions checks bounded gaps. Check the unbounded ends too;
	// an empty key denotes -infinity at the start and +infinity at the end.
	merged := keyRanges.Ranges()
	if len(merged[0].StartKey) == 0 && len(regions[0].GetStartKey()) != 0 ||
		len(merged[len(merged)-1].EndKey) == 0 && len(regions[len(regions)-1].GetEndKey()) != 0 {
		return false
	}
	for _, region := range regions {
		if region.GetLeader() == nil || region.GetApproximateSize() <= 0 {
			return false
		}
		current, isAffinity := manager.GetRegionAffinityGroupState(region)
		if current == nil || current.ID != group.ID || current.CreateTimestamp != group.CreateTimestamp ||
			!current.AffinitySchedulingAllowed || current.LeaderStoreID != group.LeaderStoreID ||
			!slices.Equal(current.VoterStoreIDs, group.VoterStoreIDs) {
			return false
		}
		if requireTarget && (!isAffinity || !filter.IsRegionHealthy(region)) {
			return false
		}
	}
	return true
}
