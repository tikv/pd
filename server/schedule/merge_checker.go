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
	"bytes"

	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/namespace"
)

// MergeChecker ensures region to merge with adjacent region when size is small
type MergeChecker struct {
	opt        Options
	cluster    Cluster
	classifier namespace.Classifier
	filters    []Filter
}

// NewMergeChecker creates a merge checker.
func NewMergeChecker(opt Options, cluster Cluster, classifier namespace.Classifier) *MergeChecker {
	filters := []Filter{
		NewHealthFilter(opt),
		NewSnapshotCountFilter(opt),
	}

	return &MergeChecker{
		opt:        opt,
		cluster:    cluster,
		classifier: classifier,
		filters:    filters,
	}
}

// Check verifies a region's replicas, creating an Operator if need.
func (m *MergeChecker) Check(region *core.RegionInfo) (*Operator, *Operator) {
	// region size is not small enough
	if region.ApproximateSize > int64(m.opt.GetMaxMergeRegionSize()) {
		return nil, nil
	}

	// skip hot region
	if m.cluster.IsRegionHot(region.GetId()) {
		return nil, nil
	}

	var target *core.RegionInfo
	prev, next := m.cluster.GetAdjacentRegions(region)
	// check key to avoid key range hole
	if prev != nil && bytes.Compare(prev.Region.EndKey, region.Region.StartKey) == 0 {
		prev = nil
	}
	if next != nil && bytes.Compare(region.Region.EndKey, next.Region.StartKey) == 0 {
		next = nil
	}

	namespace := m.classifier.GetRegionNamespace(region)
	// if is not hot region and under same namesapce
	if prev != nil && !m.cluster.IsRegionHot(prev.GetId()) && m.classifier.GetRegionNamespace(prev) == namespace {
		target = prev
	}
	if next != nil && !m.cluster.IsRegionHot(next.GetId()) && m.classifier.GetRegionNamespace(next) == namespace {
		// if both region is not hot, prefer the one with smaller size
		if target == nil || target.ApproximateSize > next.ApproximateSize {
			target = next
		}
	}

	if target == nil {
		return nil, nil
	}

	sourcePeers := region.Region.GetPeers()
	targetPeers := target.Region.GetPeers()

	// when peer count is not equal, don't merge
	if len(sourcePeers) != len(targetPeers) {
		return nil, nil
	}

	steps, err := m.matchPeers(region, target)
	if err != nil {
		return nil, nil
	}
	op1, op2 := CreateMergeRegionOperator("merge-region", region, target, core.RegionKind, steps)
	op1.SetPriorityLevel(core.HighPriority)
	op2.SetPriorityLevel(core.HighPriority)

	return op1, op2
}

func (m *MergeChecker) matchPeers(source *core.RegionInfo, target *core.RegionInfo) ([]OperatorStep, error) {
	storeIDs := make(map[uint64]bool)
	var steps []OperatorStep

	sourcePeers := source.Region.GetPeers()
	targetPeers := target.Region.GetPeers()

	for _, peer := range targetPeers {
		storeIDs[peer.GetStoreId()] = true
	}

	// Add missing peers.
	for id := range storeIDs {
		if source.GetStorePeer(id) != nil {
			continue
		}
		peer, err := m.cluster.AllocPeer(id)
		if err != nil {
			return nil, errors.Trace(err)
		}
		steps = append(steps, AddPeer{ToStore: id, PeerID: peer.Id})
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
	if isFound == false {
		steps = append(steps, TransferLeader{FromStore: source.Leader.GetStoreId(), ToStore: target.Leader.GetStoreId()})
	}

	// Remove redundant peers.
	for _, peer := range sourcePeers {
		if _, ok := storeIDs[peer.GetStoreId()]; ok {
			continue
		}
		steps = append(steps, RemovePeer{FromStore: peer.GetStoreId()})
	}

	return steps, nil
}

func (m *MergeChecker) getIntersectionStores(a []*metapb.Peer, b []*metapb.Peer) []uint64 {
	set := make([]uint64, 0)
	hash := make(map[uint64]bool)

	for _, peer := range a {
		hash[peer.GetStoreId()] = true
	}

	for _, peer := range b {
		if _, found := hash[peer.GetStoreId()]; found {
			set = append(set, peer.GetStoreId())
			delete(hash, peer.GetStoreId())
		}
	}

	return set
}
