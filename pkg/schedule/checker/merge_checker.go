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
	keyspaceconstant "github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/schedule/config"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/versioninfo/kerneltype"
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
	maxTargetRegionSizeThreshold := max(int64(float64(regionMaxSize)*float64(maxTargetRegionFactor)), maxTargetRegionSize)
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

// AllowMerge returns true if two regions can be merged according to their rules,
// keyspace boundaries and key type.
func AllowMerge(cluster sche.SharedCluster, region, adjacent *core.RegionInfo) bool {
	var start, middle, end []byte
	if bytes.Equal(region.GetEndKey(), adjacent.GetStartKey()) && len(region.GetEndKey()) != 0 {
		start, middle, end = region.GetStartKey(), region.GetEndKey(), adjacent.GetEndKey()
	} else if bytes.Equal(adjacent.GetEndKey(), region.GetStartKey()) && len(adjacent.GetEndKey()) != 0 {
		start, middle, end = adjacent.GetStartKey(), adjacent.GetEndKey(), region.GetEndKey()
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

	// Reclamation deletes keyspace labels as well as placement rules. In
	// NextGen, each side must contain a complete keyspace when merging across
	// keyspaces. Classic raw/txn keys can contain arbitrary APIv2-like bytes.
	if kerneltype.IsNextGen() && !allowMergeKeyspace(start, middle, end) {
		return false
	}

	policy := cluster.GetSharedConfig().GetKeyType()
	switch policy {
	case constant.Table:
		if cluster.GetSharedConfig().IsCrossTableMergeEnabled() {
			return true
		}
		return isSameTableIdentity(region, adjacent)
	case constant.Raw:
		return true
	case constant.Txn:
		return true
	default:
		return isSameTableIdentity(region, adjacent)
	}
}

// allowMergeKeyspace permits merges within a keyspace, or across keyspaces when
// each side contains at least one complete keyspace. It does not depend on labels
// or keyspace metadata, which can already be removed during reclamation.
func allowMergeKeyspace(start, middle, end []byte) bool {
	for _, mode := range codec.KeyspaceModes() {
		first := codec.EncodeKeyspaceBoundary(mode, 0)
		fence := codec.EncodeKeyspaceBoundary(mode, keyspaceconstant.MaxValidKeyspaceID+1)
		if (len(end) > 0 && bytes.Compare(end, first[:]) <= 0) || bytes.Compare(start, fence[:]) >= 0 {
			continue
		}
		if bytes.Compare(start, first[:]) < 0 || len(end) == 0 || bytes.Compare(end, fence[:]) > 0 {
			return false
		}
		leftID := keyspaceBoundaryAtOrAfter(start, mode)
		left := codec.EncodeKeyspaceBoundary(mode, leftID)
		nextID := leftID
		if bytes.Equal(start, left[:]) {
			nextID++
		}
		next := codec.EncodeKeyspaceBoundary(mode, nextID)
		if bytes.Compare(end, next[:]) <= 0 {
			return true // The union stays within one keyspace.
		}
		rightID := keyspaceBoundaryAtOrAfter(middle, mode)
		if leftID > keyspaceconstant.MaxValidKeyspaceID || rightID > keyspaceconstant.MaxValidKeyspaceID {
			return false
		}
		leftEnd := codec.EncodeKeyspaceBoundary(mode, leftID+1)
		rightEnd := codec.EncodeKeyspaceBoundary(mode, rightID+1)
		return bytes.Compare(middle, leftEnd[:]) >= 0 && bytes.Compare(end, rightEnd[:]) >= 0
	}
	return true
}

// keyspaceBoundaryAtOrAfter finds the first canonical boundary >= key. The caller
// guarantees key lies within the mode's range. Split keys can be short byte
// strings rather than complete memcomparable keys.
func keyspaceBoundaryAtOrAfter(key []byte, mode byte) uint32 {
	if key[0] != mode {
		return keyspaceconstant.MaxValidKeyspaceID + 1
	}
	var id uint32
	for pos := 1; pos < codec.KeyspacePrefixLen; pos++ {
		id <<= 8
		if pos < len(key) {
			id |= uint32(key[pos])
		}
	}
	boundary := codec.EncodeKeyspaceBoundary(mode, id)
	if bytes.Compare(key, boundary[:]) > 0 {
		id++
	}
	return id
}

// isSameTableIdentity reports whether two regions belong to the same logical
// table, i.e. the same table ID within the same keyspace (if any).
func isSameTableIdentity(region, adjacent *core.RegionInfo) bool {
	return codec.Key(region.GetStartKey()).TableIdentity() ==
		codec.Key(adjacent.GetStartKey()).TableIdentity()
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
