// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package checker

import (
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/pd/pkg/cache"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/namespace"
	"github.com/pingcap/pd/server/schedule"
	"github.com/pingcap/pd/server/schedule/operator"
	"go.uber.org/zap"
)

// As region split history is not persisted. We put a special marker into
// splitCache to prevent merging any regions when server is recently started.
const mergeBlockMarker = 0

// MergeChecker ensures region to merge with adjacent region when size is small
type MergeChecker struct {
	cluster    schedule.Cluster
	classifier namespace.Classifier
	splitCache *cache.TTLUint64
}

// NewMergeChecker creates a merge checker.
func NewMergeChecker(cluster schedule.Cluster, classifier namespace.Classifier) *MergeChecker {
	splitCache := cache.NewIDTTL(time.Minute, cluster.GetSplitMergeInterval())
	splitCache.Put(mergeBlockMarker)
	return &MergeChecker{
		cluster:    cluster,
		classifier: classifier,
		splitCache: splitCache,
	}
}

// RecordRegionSplit put the recently splitted region into cache. MergeChecker
// will skip check it for a while.
func (m *MergeChecker) RecordRegionSplit(regionID uint64) {
	m.splitCache.PutWithTTL(regionID, nil, m.cluster.GetSplitMergeInterval())
}

// Check verifies a region's replicas, creating an Operator if need.
func (m *MergeChecker) Check(region *core.RegionInfo) []*operator.Operator {
	if m.splitCache.Exists(mergeBlockMarker) {
		checkerCounter.WithLabelValues("merge_checker", "recently_start").Inc()
		return nil
	}

	if m.splitCache.Exists(region.GetID()) {
		checkerCounter.WithLabelValues("merge_checker", "recently_split").Inc()
		return nil
	}

	checkerCounter.WithLabelValues("merge_checker", "check").Inc()

	// when pd just started, it will load region meta from etcd
	// but the size for these loaded region info is 0
	// pd don't know the real size of one region until the first heartbeat of the region
	// thus here when size is 0, just skip.
	if region.GetApproximateSize() == 0 {
		checkerCounter.WithLabelValues("merge_checker", "skip").Inc()
		return nil
	}

	// region is not small enough
	if region.GetApproximateSize() > int64(m.cluster.GetMaxMergeRegionSize()) ||
		region.GetApproximateKeys() > int64(m.cluster.GetMaxMergeRegionKeys()) {
		checkerCounter.WithLabelValues("merge_checker", "no_need").Inc()
		return nil
	}

	// skip region has down peers or pending peers or learner peers
	if len(region.GetDownPeers()) > 0 || len(region.GetPendingPeers()) > 0 || len(region.GetLearners()) > 0 {
		checkerCounter.WithLabelValues("merge_checker", "special_peer").Inc()
		return nil
	}

	if len(region.GetPeers()) != m.cluster.GetMaxReplicas() {
		checkerCounter.WithLabelValues("merge_checker", "abnormal_replica").Inc()
		return nil
	}

	// skip hot region
	if m.cluster.IsRegionHot(region) {
		checkerCounter.WithLabelValues("merge_checker", "hot_region").Inc()
		return nil
	}

	prev, next := m.cluster.GetAdjacentRegions(region)

	target := m.checkTarget(region, next, nil)
	if !m.cluster.GetEnableOneWayMerge() {
		target = m.checkTarget(region, prev, target)
	}

	if target == nil {
		checkerCounter.WithLabelValues("merge_checker", "no_target").Inc()
		return nil
	}

	log.Debug("try to merge region", zap.Stringer("from", core.RegionToHexMeta(region.GetMeta())), zap.Stringer("to", core.RegionToHexMeta(target.GetMeta())))
	ops, err := operator.CreateMergeRegionOperator("merge-region", m.cluster, region, target, operator.OpMerge)
	if err != nil {
		log.Warn("create merge region operator failed", zap.Error(err))
		return nil
	}
	checkerCounter.WithLabelValues("merge_checker", "new_operator").Inc()
	if region.GetApproximateSize() > target.GetApproximateSize() ||
		region.GetApproximateKeys() > target.GetApproximateKeys() {
		checkerCounter.WithLabelValues("merge_checker", "larger_source").Inc()
	}
	return ops
}

func (m *MergeChecker) checkTarget(region, adjacent, target *core.RegionInfo) *core.RegionInfo {
	// if is not hot region and under same namespace
	if adjacent != nil && !m.cluster.IsRegionHot(adjacent) &&
		m.classifier.AllowMerge(region, adjacent) &&
		len(adjacent.GetDownPeers()) == 0 && len(adjacent.GetPendingPeers()) == 0 && len(adjacent.GetLearners()) == 0 {
		// if both region is not hot, prefer the one with smaller size
		if target == nil || target.GetApproximateSize() > adjacent.GetApproximateSize() {
			// peer count should equal
			if len(adjacent.GetPeers()) == m.cluster.GetMaxReplicas() {
				target = adjacent
			}
		}
	}
	return target
}
