// Copyright 2018 PingCAP, Inc.
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
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/namespace"
)

// SlaveLabel slave label.
const SlaveLabel = "slave"

// SlaveChecker ensures region has the best replicas.
type SlaveChecker struct {
	cluster    Cluster
	classifier namespace.Classifier
	filters    []Filter
}

// NewSlaveChecker creates a slave checker.
func NewSlaveChecker(cluster Cluster, classifier namespace.Classifier) *SlaveChecker {
	filters := []Filter{
		StoreStateFilter{MoveRegion: true},
	}

	return &SlaveChecker{
		cluster:    cluster,
		classifier: classifier,
		filters:    filters,
	}
}

// Check verifies a region's replicas, creating an Operator if need.
func (r *SlaveChecker) Check(region *core.RegionInfo) *Operator {
	checkerCounter.WithLabelValues("slave_checker", "check").Inc()
	if len(region.GetPeers()) != r.cluster.GetMaxReplicas() || len(region.GetPendingPeers()) != 0 || len(region.GetDownPeers()) != 0 {
		checkerCounter.WithLabelValues("slave_checker", "skip").Inc()
		return nil
	}

	ids := r.cluster.GetSlaveStoreIDs()
	stores := make([]*core.StoreInfo, 0, len(ids))
	for _, id := range ids {
		stores = append(stores, r.cluster.GetStore(id))
	}
	if len(stores) == 0 {
		checkerCounter.WithLabelValues("slave_checker", "no_slave_store").Inc()
		return nil
	}
	switch len(region.GetSlavePeers()) {
	case 0:
		store := r.selectBestSlavesStore(region, stores)
		if store == nil {
			return nil
		}
		var steps []OperatorStep
		if r.cluster.IsRaftLearnerEnabled() {
			peer, err := r.cluster.AllocPeer(store.GetId())
			if err != nil {
				return nil
			}
			steps = []OperatorStep{AddLearner{ToStore: peer.GetStoreId(), PeerID: peer.GetId()}}
		} else {
			return nil
		}
		checkerCounter.WithLabelValues("slave_checker", "new_operator").Inc()
		return NewOperator("addSlaveLearner", region.GetID(), region.GetRegionEpoch(), OpReplica|OpRegion, steps...)
	case 1:
		// TODO: deal with the down slave peers
		return nil
	case 2:
		// TODO: remove the extra slave.
	default:
	}
	return nil
}

func (r *SlaveChecker) selectBestSlavesStore(region *core.RegionInfo, stores []*core.StoreInfo) *core.StoreInfo {
	var filters []Filter
	filters = append(filters, r.filters...)
	if r.classifier != nil {
		filters = append(filters, NewNamespaceFilter(r.classifier, r.classifier.GetRegionNamespace(region)))
	}
	regionStores := r.cluster.GetRegionStores(region)
	selector := NewReplicaSelector(regionStores, r.cluster.GetLocationLabels())
	return selector.SelectTarget(r.cluster, stores, filters...)
}
