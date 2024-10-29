// Copyright 2017 TiKV Project Authors.
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
	"context"
	"time"

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/cache"
	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/config"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/utils/logutil"
)

const (
	maxTargetRegionSize   = 500
	maxTargetRegionFactor = 4
)

// When a region has label `merge_option=deny`, skip merging the region.
// If label value is `allow` or other value, it will be treated as `allow`.
const (
	mergeOptionLabel     = "merge_option"
	mergeOptionValueDeny = "deny"
)

var gcInterval = time.Minute

// MergeChecker ensures region to merge with adjacent region when size is small
type MergeChecker struct {
	PauseController
	cluster    sche.CheckerCluster
	conf       config.CheckerConfigProvider
	splitCache *cache.TTLUint64
	startTime  time.Time // it's used to judge whether server recently start.
}

// NewMergeChecker creates a merge checker.
func NewMergeChecker(ctx context.Context, cluster sche.CheckerCluster, conf config.CheckerConfigProvider) *MergeChecker {
	splitCache := cache.NewIDTTL(ctx, gcInterval, conf.GetSplitMergeInterval())
	return &MergeChecker{
		cluster:    cluster,
		conf:       conf,
		splitCache: splitCache,
		startTime:  time.Now(),
	}
}

// GetType return MergeChecker's type
func (*MergeChecker) GetType() types.CheckerSchedulerType {
	return types.MergeChecker
}

// RecordRegionSplit put the recently split region into cache. MergeChecker
// will skip check it for a while.
func (c *MergeChecker) RecordRegionSplit(regionIDs []uint64) {
	for _, regionID := range regionIDs {
		c.splitCache.PutWithTTL(regionID, nil, c.conf.GetSplitMergeInterval())
	}
}

// Check verifies a region's replicas, creating an Operator if need.
func (c *MergeChecker) Check(region *core.RegionInfo) []*operator.Operator {
	mergeCheckerCounter.Inc()

	if c.IsPaused() {
		mergeCheckerPausedCounter.Inc()
		return nil
	}

	// update the split cache.
	// It must be called before the following merge checker logic.
	c.splitCache.UpdateTTL(c.conf.GetSplitMergeInterval())

	expireTime := c.startTime.Add(c.conf.GetSplitMergeInterval())
	if time.Now().Before(expireTime) {
		mergeCheckerRecentlyStartCounter.Inc()
		return nil
	}

	if c.splitCache.Exists(region.GetID()) {
		mergeCheckerRecentlySplitCounter.Inc()
		return nil
	}

	// when pd just started, it will load region meta from region storage,
	if region.GetLeader() == nil {
		mergeCheckerNoLeaderCounter.Inc()
		return nil
	}

	// region is not small enough
	if !region.NeedMerge(int64(c.conf.GetMaxMergeRegionSize()), int64(c.conf.GetMaxMergeRegionKeys())) {
		mergeCheckerNoNeedCounter.Inc()
		return nil
	}

	// skip region has down peers or pending peers
	if !filter.IsRegionHealthy(region) {
		mergeCheckerUnhealthyRegionCounter.Inc()
		return nil
	}

	if !filter.IsRegionReplicated(c.cluster, region) {
		mergeCheckerAbnormalReplicaCounter.Inc()
		return nil
	}

	// skip hot region
	if c.cluster.IsRegionHot(region) {
		mergeCheckerHotRegionCounter.Inc()
		return nil
	}

	prev, next := c.cluster.GetAdjacentRegions(region)

	var target *core.RegionInfo
	if c.checkTarget(region, next) {
		target = next
	}
	if !c.conf.IsOneWayMergeEnabled() && c.checkTarget(region, prev) { // allow a region can be merged by two ways.
		if target == nil || prev.GetApproximateSize() < next.GetApproximateSize() { // pick smaller
			target = prev
		}
	}

	if target == nil {
		mergeCheckerNoTargetCounter.Inc()
		return nil
	}

	regionMaxSize := c.cluster.GetStoreConfig().GetRegionMaxSize()
	maxTargetRegionSizeThreshold := int64(float64(regionMaxSize) * float64(maxTargetRegionFactor))
	if maxTargetRegionSizeThreshold < maxTargetRegionSize {
		maxTargetRegionSizeThreshold = maxTargetRegionSize
	}
	if target.GetApproximateSize() > maxTargetRegionSizeThreshold {
		mergeCheckerTargetTooLargeCounter.Inc()
		return nil
	}
	if err := c.cluster.GetStoreConfig().CheckRegionSize(uint64(target.GetApproximateSize()+region.GetApproximateSize()),
		c.conf.GetMaxMergeRegionSize()); err != nil {
		mergeCheckerSplitSizeAfterMergeCounter.Inc()
		return nil
	}

	if err := c.cluster.GetStoreConfig().CheckRegionKeys(uint64(target.GetApproximateKeys()+region.GetApproximateKeys()),
		c.conf.GetMaxMergeRegionKeys()); err != nil {
		mergeCheckerSplitKeysAfterMergeCounter.Inc()
		return nil
	}

	log.Debug("try to merge region",
		logutil.ZapRedactStringer("from", core.RegionToHexMeta(region.GetMeta())),
		logutil.ZapRedactStringer("to", core.RegionToHexMeta(target.GetMeta())))
	ops, err := operator.CreateMergeRegionOperator("merge-region", c.cluster, region, target, operator.OpMerge)
	if err != nil {
		log.Warn("create merge region operator failed", errs.ZapError(err))
		return nil
	}
	mergeCheckerNewOpCounter.Inc()
	if region.GetApproximateSize() > target.GetApproximateSize() ||
		region.GetApproximateKeys() > target.GetApproximateKeys() {
		mergeCheckerLargerSourceCounter.Inc()
	}
	return ops
}

func (c *MergeChecker) checkTarget(region, adjacent *core.RegionInfo) bool {
	if adjacent == nil {
		mergeCheckerAdjNotExistCounter.Inc()
		return false
	}

	if c.splitCache.Exists(adjacent.GetID()) {
		mergeCheckerAdjRecentlySplitCounter.Inc()
		return false
	}

	if c.cluster.IsRegionHot(adjacent) {
		mergeCheckerAdjRegionHotCounter.Inc()
		return false
	}

	if !AllowMerge(c.cluster, region, adjacent) {
		mergeCheckerAdjDisallowMergeCounter.Inc()
		return false
	}

	if !checkPeerStore(c.cluster, region, adjacent) {
		mergeCheckerAdjAbnormalPeerStoreCounter.Inc()
		return false
	}

	if !filter.IsRegionHealthy(adjacent) {
		mergeCheckerAdjSpecialPeerCounter.Inc()
		return false
	}

	if !filter.IsRegionReplicated(c.cluster, adjacent) {
		mergeCheckerAdjAbnormalReplicaCounter.Inc()
		return false
	}

	return true
}

// AllowMerge returns true if two regions can be merged according to the key type.
func AllowMerge(cluster sche.SharedCluster, region, adjacent *core.RegionInfo) bool {
	var start, end []byte
	if bytes.Equal(region.GetEndKey(), adjacent.GetStartKey()) && len(region.GetEndKey()) != 0 {
		start, end = region.GetStartKey(), adjacent.GetEndKey()
	} else if bytes.Equal(adjacent.GetEndKey(), region.GetStartKey()) && len(adjacent.GetEndKey()) != 0 {
		start, end = adjacent.GetStartKey(), region.GetEndKey()
	} else {
		return false
	}

	// The interface probe is used here to get the rule manager and region
	// labeler because AllowMerge is also used by the random merge scheduler,
	// where it is not easy to get references to concrete objects.
	// We can consider using dependency injection techniques to optimize in
	// the future.

	if cluster.GetSharedConfig().IsPlacementRulesEnabled() {
		cl, ok := cluster.(interface{ GetRuleManager() *placement.RuleManager })
		if !ok || len(cl.GetRuleManager().GetSplitKeys(start, end)) > 0 {
			return false
		}
	}

	if cl, ok := cluster.(interface{ GetRegionLabeler() *labeler.RegionLabeler }); ok {
		l := cl.GetRegionLabeler()
		if len(l.GetSplitKeys(start, end)) > 0 {
			return false
		}
		if l.GetRegionLabel(region, mergeOptionLabel) == mergeOptionValueDeny || l.GetRegionLabel(adjacent, mergeOptionLabel) == mergeOptionValueDeny {
			return false
		}
	}

	policy := cluster.GetSharedConfig().GetKeyType()
	switch policy {
	case constant.Table:
		if cluster.GetSharedConfig().IsCrossTableMergeEnabled() {
			return true
		}
		return isTableIDSame(region, adjacent)
	case constant.Raw:
		return true
	case constant.Txn:
		return true
	default:
		return isTableIDSame(region, adjacent)
	}
}

func isTableIDSame(region, adjacent *core.RegionInfo) bool {
	return codec.Key(region.GetStartKey()).TableID() == codec.Key(adjacent.GetStartKey()).TableID()
}

// Check whether there is a peer of the adjacent region on an offline store,
// while the source region has no peer on it. This is to prevent from bringing
// any other peer into an offline store to slow down the offline process.
func checkPeerStore(cluster sche.SharedCluster, region, adjacent *core.RegionInfo) bool {
	regionStoreIDs := region.GetStoreIDs()
	for _, peer := range adjacent.GetPeers() {
		storeID := peer.GetStoreId()
		store := cluster.GetStore(storeID)
		if store == nil || store.IsRemoving() {
			if _, ok := regionStoreIDs[storeID]; !ok {
				return false
			}
		}
	}
	return true
}
