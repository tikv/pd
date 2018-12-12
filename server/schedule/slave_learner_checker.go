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
	"math/rand"

	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/namespace"
	log "github.com/sirupsen/logrus"
)

// SlaveLabel slave label.
const SlaveLabel = "slave"

// SlaveChecker ensures region has the best replicas.
type SlaveChecker struct {
	cluster      Cluster
	classifier   namespace.Classifier
	filters      []Filter
	opController *OperatorController
}

// NewSlaveChecker creates a slave checker.
func NewSlaveChecker(cluster Cluster, classifier namespace.Classifier, opController *OperatorController) *SlaveChecker {
	filters := []Filter{
		StoreStateFilter{MoveRegion: true},
	}

	return &SlaveChecker{
		cluster:      cluster,
		classifier:   classifier,
		filters:      filters,
		opController: opController,
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
	slaveCount := len(region.GetSlavePeers())
	switch {
	case slaveCount == 0:
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
	case slaveCount == 1:
		op := r.checkDownSlavePeer(region)
		if op != nil {
			return op
		}
		op = r.checkOfflinePeer(region)
		if op != nil {
			return op
		}
		return r.selectBestReplacementStore(region)
	case slaveCount > 1:
		log.Fatalf("[region %d] not expect slave peers: %+v", region.GetID(), region)
	default:
	}
	return nil
}

func (r SlaveChecker) selectBestReplacementStore(region *core.RegionInfo) *Operator {
	if len(region.GetSlaveDownPeers()) != 0 || len(region.GetSlavePendingPeers()) != 0 {
		checkerCounter.WithLabelValues("slave_learner_checker", "unhealth_slave").Inc()
		return nil
	}
	slaves := region.GetSlavePeers()
	if len(slaves) != 1 {
		checkerCounter.WithLabelValues("slave_learner_checker", "extra_slave").Inc()
		return nil
	}
	slave := slaves[0]
	source := r.cluster.GetStore(slave.GetStoreId())

	// random pick region
	if rand.Intn(source.RegionCount)%2 == 0 {
		return nil
	}

	// init filters
	var filters []Filter
	filters = append(filters, r.filters...)
	if r.classifier != nil {
		filters = append(filters, NewNamespaceFilter(r.classifier, r.classifier.GetRegionNamespace(region)))
	}
	excludeStores := region.GetStoreIds()
	excludeStores[slave.GetStoreId()] = struct{}{}
	filters = append(filters, NewExcludedFilter(nil, excludeStores))

	// get slave stores
	var slaveStores []*core.StoreInfo
	for _, storeID := range r.cluster.GetSlaveStoreIDs() {
		slaveStores = append(slaveStores, r.cluster.GetStore(storeID))
	}

	// select a replacement
	selector := NewReplicaSelector(r.cluster.GetRegionStores(region), r.cluster.GetLocationLabels(), r.filters...)
	target := selector.SelectTarget(r.cluster, slaveStores, filters...)
	if target == nil {
		checkerCounter.WithLabelValues("slave_learner_checker", "all_right").Inc()
		return nil
	}

	opInfluence := r.opController.GetOpInfluence(r.cluster)
	if !r.shouldBalance(source, target, region, opInfluence) {
		checkerCounter.WithLabelValues("slave_learner_checker", "all_right").Inc()
		return nil
	}

	newPeer, err := r.cluster.AllocPeer(target.GetId())
	if err != nil {
		checkerCounter.WithLabelValues("slave_learner_checker", "no_peer").Inc()
		return nil
	}
	steps := []OperatorStep{
		RemovePeer{FromStore: source.GetId()},
		AddLearner{ToStore: target.GetId(), PeerID: newPeer.GetId()},
	}
	return NewOperator("replaceSlavePeer", region.GetID(), region.GetRegionEpoch(), OpReplica|OpRegion, steps...)
}

func (r *SlaveChecker) shouldBalance(source, target *core.StoreInfo, region *core.RegionInfo, opInfluence OpInfluence) bool {
	// The reason we use max(regionSize, averageRegionSize) to check is:
	// 1. prevent moving small regions between stores with close scores, leading to unnecessary balance.
	// 2. prevent moving huge regions, leading to over balance.
	regionSize := region.GetApproximateSize()
	if regionSize < r.cluster.GetAverageRegionSize() {
		regionSize = r.cluster.GetAverageRegionSize()
	}

	regionSize = int64(float64(regionSize) * r.cluster.GetTolerantSizeRatio())
	sourceDelta := opInfluence.GetStoreInfluence(source.GetId()).RegionSize - regionSize
	targetDelta := opInfluence.GetStoreInfluence(target.GetId()).RegionSize + regionSize

	// Make sure after move, source score is still greater than target score.
	return source.RegionScore(r.cluster.GetHighSpaceRatio(), r.cluster.GetLowSpaceRatio(), sourceDelta) >
		target.RegionScore(r.cluster.GetHighSpaceRatio(), r.cluster.GetLowSpaceRatio(), targetDelta)
}

func (r *SlaveChecker) checkDownSlavePeer(region *core.RegionInfo) *Operator {
	if !r.cluster.IsRemoveDownReplicaEnabled() {
		return nil
	}

	for _, stats := range region.GetSlaveDownPeers() {
		peer := stats.GetPeer()
		if peer == nil {
			continue
		}
		store := r.cluster.GetStore(peer.GetStoreId())
		if store == nil {
			log.Infof("lost the store %d, maybe you are recovering the PD cluster.", peer.GetStoreId())
			return nil
		}
		if store.DownTime() < r.cluster.GetMaxStoreDownTime() {
			continue
		}
		if stats.GetDownSeconds() < uint64(r.cluster.GetMaxStoreDownTime().Seconds()) {
			continue
		}
		return CreateRemovePeerOperator("removeDownSlavePeer", r.cluster, OpReplica, region, peer.GetStoreId())
	}
	return nil
}

func (r *SlaveChecker) checkOfflinePeer(region *core.RegionInfo) *Operator {
	if !r.cluster.IsReplaceOfflineReplicaEnabled() {
		return nil
	}

	for _, peer := range region.GetSlavePeers() {
		store := r.cluster.GetStore(peer.GetStoreId())
		if store == nil {
			log.Infof("lost the store %d, maybe you are recovering the PD cluster.", peer.GetStoreId())
			return nil
		}
		if store.IsUp() {
			continue
		}
		return CreateRemovePeerOperator("removeOfflineSlavePeer", r.cluster, OpReplica, region, peer.GetStoreId())
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
