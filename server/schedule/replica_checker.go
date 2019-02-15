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
	"fmt"

	"github.com/pingcap/kvproto/pkg/metapb"
	log "github.com/pingcap/log"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/namespace"
	"go.uber.org/zap"
)

// ReplicaChecker ensures region has the best replicas.
// Including the following:
// Replica number management.
// Unhealth replica management, mainly used for disaster recovery of TiKV.
// Location management, mainly used for corss data center deployment.
type ReplicaChecker struct {
	cluster    Cluster
	classifier namespace.Classifier
	filters    []Filter
}

// NewReplicaChecker creates a replica checker.
func NewReplicaChecker(cluster Cluster, classifier namespace.Classifier) *ReplicaChecker {
	filters := []Filter{
		NewHealthFilter(),
		NewSnapshotCountFilter(),
	}

	return &ReplicaChecker{
		cluster:    cluster,
		classifier: classifier,
		filters:    filters,
	}
}

// Check verifies a region's replicas, creating an Operator if need.
func (r *ReplicaChecker) Check(region *core.RegionInfo) *Operator {
	checkerCounter.WithLabelValues("replica_checker", "check").Inc()
	if op := r.checkDownPeer(region); op != nil {
		checkerCounter.WithLabelValues("replica_checker", "new_operator").Inc()
		op.SetPriorityLevel(core.HighPriority)
		return op
	}
	if op := r.checkOfflinePeer(region); op != nil {
		checkerCounter.WithLabelValues("replica_checker", "new_operator").Inc()
		op.SetPriorityLevel(core.HighPriority)
		return op
	}

	if len(region.GetPeers()) < r.cluster.GetMaxReplicas() && r.cluster.IsMakeUpReplicaEnabled() {
		log.Debug("region has fewer than max replicas", zap.Uint64("region-id", region.GetID()), zap.Int("peers", len(region.GetPeers())))
		newPeer, _ := r.selectBestPeerToAddReplica(region, NewStorageThresholdFilter())
		if newPeer == nil {
			checkerCounter.WithLabelValues("replica_checker", "no_target_store").Inc()
			return nil
		}
		var steps []OperatorStep
		if r.cluster.IsRaftLearnerEnabled() {
			steps = []OperatorStep{
				AddLearner{ToStore: newPeer.GetStoreId(), PeerID: newPeer.GetId()},
				PromoteLearner{ToStore: newPeer.GetStoreId(), PeerID: newPeer.GetId()},
			}
		} else {
			steps = []OperatorStep{
				AddPeer{ToStore: newPeer.GetStoreId(), PeerID: newPeer.GetId()},
			}
		}
		checkerCounter.WithLabelValues("replica_checker", "new_operator").Inc()
		return NewOperator("makeUpReplica", region.GetID(), region.GetRegionEpoch(), OpReplica|OpRegion, steps...)
	}

	// when add learner peer, the number of peer will exceed max replicas for a while,
	// just comparing the the number of voters to avoid too many cancel add operator log.
	if len(region.GetVoters()) > r.cluster.GetMaxReplicas() && r.cluster.IsRemoveExtraReplicaEnabled() {
		log.Debug("region has more than max replicas", zap.Uint64("region-id", region.GetID()), zap.Int("peers", len(region.GetPeers())))
		oldPeer, _ := r.selectWorstPeer(region)
		if oldPeer == nil {
			checkerCounter.WithLabelValues("replica_checker", "no_worst_peer").Inc()
			return nil
		}
		checkerCounter.WithLabelValues("replica_checker", "new_operator").Inc()
		return CreateRemovePeerOperator("removeExtraReplica", r.cluster, OpReplica, region, oldPeer.GetStoreId())
	}

	return r.checkBestReplacement(region)
}

// SelectBestReplacementStore returns a store id that to be used to replace the old peer and distinct score.
func (r *ReplicaChecker) SelectBestReplacementStore(region *core.RegionInfo, oldPeer *metapb.Peer, filters ...Filter) (uint64, float64) {
	filters = append(filters, NewExcludedFilter(nil, region.GetStoreIds()))
	newRegion := region.Clone(core.WithRemoveStorePeer(oldPeer.GetStoreId()))
	return r.selectBestStoreToAddReplica(newRegion, filters...)
}

// selectBestPeerToAddReplica returns a new peer that to be used to add a replica and distinct score.
func (r *ReplicaChecker) selectBestPeerToAddReplica(region *core.RegionInfo, filters ...Filter) (*metapb.Peer, float64) {
	storeID, score := r.selectBestStoreToAddReplica(region, filters...)
	if storeID == 0 {
		log.Debug("no best store to add replica", zap.Uint64("region-id", region.GetID()))
		return nil, 0
	}
	newPeer, err := r.cluster.AllocPeer(storeID)
	if err != nil {
		return nil, 0
	}
	return newPeer, score
}

// selectBestStoreToAddReplica returns the store to add a replica.
func (r *ReplicaChecker) selectBestStoreToAddReplica(region *core.RegionInfo, filters ...Filter) (uint64, float64) {
	// Add some must have filters.
	newFilters := []Filter{
		NewStateFilter(),
		NewPendingPeerCountFilter(),
		NewExcludedFilter(nil, region.GetStoreIds()),
	}
	filters = append(filters, r.filters...)
	filters = append(filters, newFilters...)
	if r.classifier != nil {
		filters = append(filters, NewNamespaceFilter(r.classifier, r.classifier.GetRegionNamespace(region)))
	}
	regionStores := r.cluster.GetRegionStores(region)
	selector := NewReplicaSelector(regionStores, r.cluster.GetLocationLabels(), r.filters...)
	target := selector.SelectTarget(r.cluster, r.cluster.GetStores(), filters...)
	if target == nil {
		return 0, 0
	}
	return target.GetID(), DistinctScore(r.cluster.GetLocationLabels(), regionStores, target)
}

// selectWorstPeer returns the worst peer in the region.
func (r *ReplicaChecker) selectWorstPeer(region *core.RegionInfo) (*metapb.Peer, float64) {
	regionStores := r.cluster.GetRegionStores(region)
	selector := NewReplicaSelector(regionStores, r.cluster.GetLocationLabels(), r.filters...)
	worstStore := selector.SelectSource(r.cluster, regionStores)
	if worstStore == nil {
		log.Debug("no worst store", zap.Uint64("region-id", region.GetID()))
		return nil, 0
	}
	return region.GetStorePeer(worstStore.GetID()), DistinctScore(r.cluster.GetLocationLabels(), regionStores, worstStore)
}

func (r *ReplicaChecker) checkDownPeer(region *core.RegionInfo) *Operator {
	if !r.cluster.IsRemoveDownReplicaEnabled() {
		return nil
	}

	for _, stats := range region.GetDownPeers() {
		peer := stats.GetPeer()
		if peer == nil {
			continue
		}
		store := r.cluster.GetStore(peer.GetStoreId())
		if store == nil {
			log.Info("lost the store, maybe you are recovering the PD cluster", zap.Uint64("store-id", peer.GetStoreId()))
			return nil
		}
		if store.DownTime() < r.cluster.GetMaxStoreDownTime() {
			continue
		}
		if stats.GetDownSeconds() < uint64(r.cluster.GetMaxStoreDownTime().Seconds()) {
			continue
		}

		return r.fixPeer(region, peer, "Down")
	}
	return nil
}

func (r *ReplicaChecker) checkOfflinePeer(region *core.RegionInfo) *Operator {
	if !r.cluster.IsReplaceOfflineReplicaEnabled() {
		return nil
	}

	// just skip learner
	if len(region.GetLearners()) != 0 {
		return nil
	}

	for _, peer := range region.GetPeers() {
		store := r.cluster.GetStore(peer.GetStoreId())
		if store == nil {
			log.Info("lost the store, maybe you are recovering the PD cluster", zap.Uint64("store-id", peer.GetStoreId()))
			return nil
		}
		if store.IsUp() {
			continue
		}

		return r.fixPeer(region, peer, "Offline")
	}

	return nil
}

func (r *ReplicaChecker) checkBestReplacement(region *core.RegionInfo) *Operator {
	if !r.cluster.IsLocationReplacementEnabled() {
		return nil
	}

	oldPeer, oldScore := r.selectWorstPeer(region)
	if oldPeer == nil {
		checkerCounter.WithLabelValues("replica_checker", "all_right").Inc()
		return nil
	}
	storeID, newScore := r.SelectBestReplacementStore(region, oldPeer, NewStorageThresholdFilter())
	if storeID == 0 {
		checkerCounter.WithLabelValues("replica_checker", "no_replacement_store").Inc()
		return nil
	}
	// Make sure the new peer is better than the old peer.
	if newScore <= oldScore {
		log.Debug("no better peer", zap.Uint64("region-id", region.GetID()), zap.Float64("new-score", newScore), zap.Float64("old-score", oldScore))
		checkerCounter.WithLabelValues("replica_checker", "not_better").Inc()
		return nil
	}
	newPeer, err := r.cluster.AllocPeer(storeID)
	if err != nil {
		return nil
	}
	checkerCounter.WithLabelValues("replica_checker", "new_operator").Inc()
	return CreateMovePeerOperator("moveToBetterLocation", r.cluster, region, OpReplica, oldPeer.GetStoreId(), newPeer.GetStoreId(), newPeer.GetId())
}

func (r *ReplicaChecker) fixPeer(region *core.RegionInfo, peer *metapb.Peer, status string) *Operator {
	removeExtra := fmt.Sprintf("removeExtra%sReplica", status)
	// Check the number of replicas first.
	if len(region.GetPeers()) > r.cluster.GetMaxReplicas() {
		return CreateRemovePeerOperator(removeExtra, r.cluster, OpReplica, region, peer.GetStoreId())
	}

	removePending := fmt.Sprintf("removePending%sReplica", status)
	// Consider we have 3 peers (A, B, C), we set the store that contains C to
	// offline/down while C is pending. If we generate an operator that adds a replica
	// D then removes C, D will not be successfully added util C is normal again.
	// So it's better to remove C directly.
	if region.GetPendingPeer(peer.GetId()) != nil {
		return CreateRemovePeerOperator(removePending, r.cluster, OpReplica, region, peer.GetStoreId())
	}

	storeID, _ := r.SelectBestReplacementStore(region, peer, NewStorageThresholdFilter())
	if storeID == 0 {
		log.Debug("no best store to add replica", zap.Uint64("region-id", region.GetID()))
		return nil
	}
	newPeer, err := r.cluster.AllocPeer(storeID)
	if err != nil {
		return nil
	}

	replace := fmt.Sprintf("replace%sReplica", status)
	return CreateMovePeerOperator(replace, r.cluster, region, OpReplica, peer.GetStoreId(), newPeer.GetStoreId(), newPeer.GetId())
}
