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

package schedule

import (
	"time"

	log "github.com/pingcap/log"
	"github.com/pingcap/pd/server/cache"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/namespace"
	"go.uber.org/zap"
)

// As region split history is not persisted. We put a special marker into
// splitCache to prevent merging any regions when server is recently started.
const mergeBlockMarker = 0

// MergeChecker ensures region to merge with adjacent region when size is small
type MergeChecker struct {
	cluster    Cluster
	classifier namespace.Classifier
	splitCache *cache.TTLUint64
}

// NewMergeChecker creates a merge checker.
func NewMergeChecker(cluster Cluster, classifier namespace.Classifier) *MergeChecker {
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
func (m *MergeChecker) Check(region *core.RegionInfo) []*Operator {
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
	if m.cluster.IsRegionHot(region.GetID()) {
		checkerCounter.WithLabelValues("merge_checker", "hot_region").Inc()
		return nil
	}

	// Always merge left into right.
	_, target := m.cluster.GetAdjacentRegions(region)

	if !m.checkTarget(region, target) {
		checkerCounter.WithLabelValues("merge_checker", "no_target").Inc()
		return nil
	}

	checkerCounter.WithLabelValues("merge_checker", "new_operator").Inc()
	log.Debug("try to merge region", zap.Reflect("from", core.HexRegionMeta(region.GetMeta())), zap.Reflect("to", core.HexRegionMeta(target.GetMeta())))
	ops, err := CreateMergeRegionOperator("merge-region", m.cluster, region, target, OpMerge)
	if err != nil {
		return nil
	}
	return ops
}

func (m *MergeChecker) checkTarget(region, target *core.RegionInfo) bool {
	// if is not hot region and under same namespace
	if target != nil && !m.cluster.IsRegionHot(target.GetID()) &&
		m.classifier.AllowMerge(region, target) &&
		len(target.GetDownPeers()) == 0 && len(target.GetPendingPeers()) == 0 && len(target.GetLearners()) == 0 {
		// peer count should equal
		return len(target.GetPeers()) == m.cluster.GetMaxReplicas()
	}
	return false
}
