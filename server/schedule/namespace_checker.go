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
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/namespace"
)

// NamespaceChecker ensures region to go to the right place.
type NamespaceChecker struct {
	opt        Options
	cluster    Cluster
	filters    []Filter
	classifier namespace.Classifier
}

// NewNamespaceChecker creates a namespace checker.
func NewNamespaceChecker(opt Options, cluster Cluster) *NamespaceChecker {
	filters := []Filter{
		NewHealthFilter(opt),
		NewSnapshotCountFilter(opt),
	}

	return &NamespaceChecker{
		opt:     opt,
		cluster: cluster,
		filters: filters,
		//TODO change it to classifier for namespace
		classifier: namespace.DefaultClassifer,
	}
}

// Check verifies a region's namespace, creating an Operator if need.
func (n *NamespaceChecker) Check(region *core.RegionInfo) *Operator {
	for _, peer := range region.GetPeers() {
		newPeer := n.SelectBestPeerToRelocate(region, n.filters...)
		if newPeer == nil {
			return nil
		}
		return CreateMovePeerOperator("makeNamespaceRelocation", region, core.RegionKind, peer.GetStoreId(), newPeer.GetStoreId(), newPeer.GetId())
	}

	return nil
}

// SelectBestPeerToRelocate return a new peer that to be used to move a region
func (n *NamespaceChecker) SelectBestPeerToRelocate(region *core.RegionInfo, filters ...Filter) *metapb.Peer {
	storeID := n.SelectBestStoreToRelocate(region, filters...)
	if storeID == 0 {
		return nil
	}
	newPeer, err := n.cluster.AllocPeer(storeID)
	if err != nil {
		return nil
	}
	return newPeer
}

// SelectBestStoreToRelocate returns the store to relocate
func (n *NamespaceChecker) SelectBestStoreToRelocate(region *core.RegionInfo, filters ...Filter) uint64 {
	newFilters := []Filter{
		NewStateFilter(n.opt),
		NewStorageThresholdFilter(n.opt),
		NewExcludedFilter(nil, region.GetStoreIds()),
	}
	filters = append(filters, newFilters...)

	//FIXME we need to compare whether all the peer's store id of the region has already been in the namespace's store id list
	ns := n.classifier.GetRegionNamespace(region)
	selector := NewNamespaceSelector(ns, n.filters...)
	target := selector.SelectTarget(n.cluster.GetStores(), filters...)
	if target == nil {
		return 0
	}
	return target.GetId()
}
