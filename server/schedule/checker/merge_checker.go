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
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/logutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/labeler"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/opt"
	"github.com/tikv/pd/server/schedule/placement"
)

const maxTargetRegionSize = 500

// When a region has label `merge_option=deny`, skip merging the region.
// If label value is `allow` or other value, it will be treated as `allow`.
const (
	mergeOptionLabel     = "merge_option"
	mergeOptionValueDeny = "deny"
)

// MergeChecker ensures region to merge with adjacent region when size is small
type MergeChecker struct {
	PauseController
	cluster    opt.Cluster
	opts       *config.PersistOptions
	splitCache *cache.TTLUint64
	startTime  time.Time // it's used to judge whether server recently start.
}

// NewMergeChecker creates a merge checker.
func NewMergeChecker(ctx context.Context, cluster opt.Cluster) *MergeChecker {
	opts := cluster.GetOpts()
	splitCache := cache.NewIDTTL(ctx, time.Minute, opts.GetSplitMergeInterval())
	return &MergeChecker{
		cluster:    cluster,
		opts:       opts,
		splitCache: splitCache,
		startTime:  time.Now(),
	}
}

// GetType return MergeChecker's type
func (m *MergeChecker) GetType() string {
	return "merge-checker"
}

// RecordRegionSplit put the recently split region into cache. MergeChecker
// will skip check it for a while.
func (m *MergeChecker) RecordRegionSplit(regionIDs []uint64) {
	for _, regionID := range regionIDs {
		m.splitCache.PutWithTTL(regionID, nil, m.opts.GetSplitMergeInterval())
	}
}

// Check verifies a region's replicas, creating an Operator if need.
func (m *MergeChecker) Check(region *core.RegionInfo) []*operator.Operator {
	checkerCounter.WithLabelValues("merge_checker", "check").Inc()

	if m.IsPaused() {
		checkerCounter.WithLabelValues("merge_checker", "paused").Inc()
		return nil
	}

	expireTime := m.startTime.Add(m.opts.GetSplitMergeInterval())
	if time.Now().Before(expireTime) {
		checkerCounter.WithLabelValues("merge_checker", "recently-start").Inc()
		return nil
	}

	if m.splitCache.Exists(region.GetID()) {
		checkerCounter.WithLabelValues("merge_checker", "recently-split").Inc()
		return nil
	}

	// when pd just started, it will load region meta from etcd
	// but the size for these loaded region info is 0
	// pd don't know the real size of one region until the first heartbeat of the region
	// thus here when size is 0, just skip.
	if region.GetApproximateSize() == 0 {
		checkerCounter.WithLabelValues("merge_checker", "skip").Inc()
		return nil
	}

	// region is not small enough
	if region.GetApproximateSize() > int64(m.opts.GetMaxMergeRegionSize()) ||
		region.GetApproximateKeys() > int64(m.opts.GetMaxMergeRegionKeys()) {
		checkerCounter.WithLabelValues("merge_checker", "no-need").Inc()
		return nil
	}

	// skip region has down peers or pending peers or learner peers
	if !opt.IsRegionHealthy(m.cluster, region) {
		checkerCounter.WithLabelValues("merge_checker", "special-peer").Inc()
		return nil
	}

	if !opt.IsRegionReplicated(m.cluster, region) {
		checkerCounter.WithLabelValues("merge_checker", "abnormal-replica").Inc()
		return nil
	}

	// skip hot region
	if m.cluster.IsRegionHot(region) {
		checkerCounter.WithLabelValues("merge_checker", "hot-region").Inc()
		return nil
	}

	prev, next := m.cluster.GetAdjacentRegions(region)

	var target *core.RegionInfo
	if m.checkTarget(region, next) {
		target = next
	}
	if !m.opts.IsOneWayMergeEnabled() && m.checkTarget(region, prev) { // allow a region can be merged by two ways.
		if target == nil || prev.GetApproximateSize() < next.GetApproximateSize() { // pick smaller
			target = prev
		}
	}

	if target == nil {
		checkerCounter.WithLabelValues("merge_checker", "no-target").Inc()
		return nil
	}

	if target.GetApproximateSize() > maxTargetRegionSize {
		checkerCounter.WithLabelValues("merge_checker", "target-too-large").Inc()
		return nil
	}

	log.Debug("try to merge region",
		logutil.ZapRedactStringer("from", core.RegionToHexMeta(region.GetMeta())),
		logutil.ZapRedactStringer("to", core.RegionToHexMeta(target.GetMeta())))
	ops, err := operator.CreateMergeRegionOperator("merge-region", m.cluster, region, target, operator.OpMerge)
	if err != nil {
		log.Warn("create merge region operator failed", errs.ZapError(err))
		return nil
	}
	checkerCounter.WithLabelValues("merge_checker", "new-operator").Inc()
	if region.GetApproximateSize() > target.GetApproximateSize() ||
		region.GetApproximateKeys() > target.GetApproximateKeys() {
		checkerCounter.WithLabelValues("merge_checker", "larger-source").Inc()
	}
	return ops
}

func (m *MergeChecker) checkTarget(region, adjacent *core.RegionInfo) bool {
	return adjacent != nil && !m.splitCache.Exists(adjacent.GetID()) && !m.cluster.IsRegionHot(adjacent) &&
		AllowMerge(m.cluster, region, adjacent) && opt.IsRegionHealthy(m.cluster, adjacent) &&
		opt.IsRegionReplicated(m.cluster, adjacent)
}

// AllowMerge returns true if two regions can be merged according to the key type.
func AllowMerge(cluster opt.Cluster, region *core.RegionInfo, adjacent *core.RegionInfo) bool {
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

	if cluster.GetOpts().IsPlacementRulesEnabled() {
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

	policy := cluster.GetOpts().GetKeyType()
	switch policy {
	case core.Table:
		if cluster.GetOpts().IsCrossTableMergeEnabled() {
			return true
		}
		return isTableIDSame(region, adjacent)
	case core.Raw:
		return true
	case core.Txn:
		return true
	default:
		return isTableIDSame(region, adjacent)
	}
}

func isTableIDSame(region *core.RegionInfo, adjacent *core.RegionInfo) bool {
	return codec.Key(region.GetStartKey()).TableID() == codec.Key(adjacent.GetStartKey()).TableID()
}
