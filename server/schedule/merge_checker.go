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
	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/namespace"
	log "github.com/sirupsen/logrus"
)

// MergeChecker ensures region to merge with adjacent region when size is small
type MergeChecker struct {
	cluster    Cluster
	classifier namespace.Classifier
}

// NewMergeChecker creates a merge checker.
func NewMergeChecker(cluster Cluster, classifier namespace.Classifier) *MergeChecker {
	return &MergeChecker{
		cluster:    cluster,
		classifier: classifier,
	}
}

// Check verifies a region's replicas, creating an Operator if need.
func (m *MergeChecker) Check(region *core.RegionInfo) (*Operator, *Operator) {
	checkerCounter.WithLabelValues("merge_checker", "check").Inc()

	// region size is not small enough
	if region.ApproximateSize >= int64(m.cluster.GetMaxMergeRegionSize()) {
		checkerCounter.WithLabelValues("merge_checker", "no_need").Inc()
		return nil, nil
	}

	// skip hot region
	if m.cluster.IsRegionHot(region.GetId()) {
		checkerCounter.WithLabelValues("merge_checker", "hot_region").Inc()
		return nil, nil
	}

	var target *core.RegionInfo
	prev, next := m.cluster.GetAdjacentRegions(region)

	target = m.checkTarget(region, prev, target)
	target = m.checkTarget(region, next, target)

	if target == nil {
		checkerCounter.WithLabelValues("merge_checker", "no_target").Inc()
		return nil, nil
	}

	steps, kind, err := m.matchPeers(region, target)
	if err != nil {
		return nil, nil
	}

	checkerCounter.WithLabelValues("merge_checker", "new_operator").Inc()
	log.Debugf("try to merge region {%v} into region {%v}", region, target)
	op1, op2 := CreateMergeRegionOperator("merge-region", region, target, kind, steps)

	return op1, op2
}

func (m *MergeChecker) checkTarget(region, adjacent, target *core.RegionInfo) *core.RegionInfo {
	peerCount := len(region.Region.GetPeers())

	// if is not hot region and under same namesapce
	if adjacent != nil && !m.cluster.IsRegionHot(adjacent.GetId()) && m.classifier.AllowMerge(region, adjacent) {
		// if both region is not hot, prefer the one with smaller size
		if target == nil || target.ApproximateSize > adjacent.ApproximateSize {
			// peer count should equal
			if peerCount == len(adjacent.Region.GetPeers()) {
				target = adjacent
			}
		}
	}
	return target
}

func (m *MergeChecker) matchPeers(source *core.RegionInfo, target *core.RegionInfo) ([]OperatorStep, OperatorKind, error) {
	storeIDs := make(map[uint64]struct{})
	var steps []OperatorStep
	var kind OperatorKind

	sourcePeers := source.Region.GetPeers()
	targetPeers := target.Region.GetPeers()

	for _, peer := range targetPeers {
		storeIDs[peer.GetStoreId()] = struct{}{}
	}

	// Add missing peers.
	for id := range storeIDs {
		if source.GetStorePeer(id) != nil {
			continue
		}
		peer, err := m.cluster.AllocPeer(id)
		if err != nil {
			log.Debugf("peer alloc failed: %v", err)
			return nil, kind, errors.Trace(err)
		}
		steps = append(steps, AddPeer{ToStore: id, PeerID: peer.Id})
		kind |= OpRegion
	}

	// Check whether to transfer leader or not
	intersection := m.getIntersectionStores(sourcePeers, targetPeers)
	leaderID := source.Leader.GetStoreId()
	isFound := false
	for _, storeID := range intersection {
		if storeID == leaderID {
			isFound = true
			break
		}
	}
	if !isFound {
		steps = append(steps, TransferLeader{FromStore: source.Leader.GetStoreId(), ToStore: target.Leader.GetStoreId()})
		kind |= OpLeader
	}

	// Remove redundant peers.
	for _, peer := range sourcePeers {
		if _, ok := storeIDs[peer.GetStoreId()]; ok {
			continue
		}
		steps = append(steps, RemovePeer{FromStore: peer.GetStoreId()})
		kind |= OpRegion
	}

	return steps, kind, nil
}

func (m *MergeChecker) getIntersectionStores(a []*metapb.Peer, b []*metapb.Peer) []uint64 {
	set := make([]uint64, 0)
	hash := make(map[uint64]struct{})

	for _, peer := range a {
		hash[peer.GetStoreId()] = struct{}{}
	}

	for _, peer := range b {
		if _, found := hash[peer.GetStoreId()]; found {
			set = append(set, peer.GetStoreId())
		}
	}

	return set
}
