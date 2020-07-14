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
	"fmt"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/pd/v4/server/core"
	"github.com/pingcap/pd/v4/server/schedule/filter"
	"github.com/pingcap/pd/v4/server/schedule/operator"
	"github.com/pingcap/pd/v4/server/schedule/opt"
	"github.com/pingcap/pd/v4/server/schedule/selector"
	"go.uber.org/zap"
)

const (
	replicaCheckerName = "replica-checker"
)

const (
	offlineStatus = "offline"
	downStatus    = "down"
)

// ReplicaChecker ensures region has the best replicas.
// Including the following:
// Replica number management.
// Unhealthy replica management, mainly used for disaster recovery of TiKV.
// Location management, mainly used for cross data center deployment.
type ReplicaChecker struct {
	cluster opt.Cluster
}

// NewReplicaChecker creates a replica checker.
func NewReplicaChecker(cluster opt.Cluster) *ReplicaChecker {
	return &ReplicaChecker{
		cluster: cluster,
	}
}

// Check verifies a region's replicas, creating an operator.Operator if need.
func (r *ReplicaChecker) Check(region *core.RegionInfo) *operator.Operator {
	checkerCounter.WithLabelValues("replica_checker", "check").Inc()
	if op := r.checkDownPeer(region); op != nil {
		checkerCounter.WithLabelValues("replica_checker", "new-operator").Inc()
		op.SetPriorityLevel(core.HighPriority)
		return op
	}
	if op := r.checkOfflinePeer(region); op != nil {
		checkerCounter.WithLabelValues("replica_checker", "new-operator").Inc()
		op.SetPriorityLevel(core.HighPriority)
		return op
	}
	if op := r.checkMakeUpReplica(region); op != nil {
		checkerCounter.WithLabelValues("replica_checker", "new-operator").Inc()
		return op
	}
	if op := r.checkRemoveExtraReplica(region); op != nil {
		checkerCounter.WithLabelValues("replica_checker", "new-operator").Inc()
		return op
	}
	if op := r.checkLocationReplacement(region); op != nil {
		checkerCounter.WithLabelValues("replica_checker", "new-operator").Inc()
		return op
	}
	return nil
}

// selectStoreToAdd returns the store to add a replica to a region.
// `extraFilters` is used to set up more filters based on the context that
// calling this method.
//
// For example, to select a target store to replace a region's peer, we can
// first create a tmp region instance that removed the peer, then call
// `selectStoreToAdd` to decide the target store. Meanwhile, we need to provide
// more constraints to ensure that the newly selected node cannot be the same
// as the original one, and the isolation level cannot be reduced after
// replacement.
func (r *ReplicaChecker) selectStoreToAdd(region *core.RegionInfo, extraFilters ...filter.Filter) uint64 {
	// The selection process uses a two-stage fashion. The first stage
	// ignores the temporary state of the stores and selects the stores
	// with the highest score according to the location label. The second
	// stage considers all temporary states and capacity factors to select
	// the most suitable target.
	//
	// The reason for it is to prevent the non-optimal replica placement due
	// to the short-term state, resulting in redundant scheduling.

	filters := []filter.Filter{
		filter.NewExcludedFilter(replicaCheckerName, nil, region.GetStoreIds()),
		filter.NewStorageThresholdFilter(replicaCheckerName),
		filter.NewSpecialUseFilter(replicaCheckerName),
		filter.StoreStateFilter{ActionScope: replicaCheckerName, MoveRegion: true, AllowTemporaryStates: true},
	}
	if len(extraFilters) > 0 {
		filters = append(filters, extraFilters...)
	}

	regionStores := r.cluster.GetRegionStores(region)
	isolationComparer := selector.IsolationComparer(r.cluster.GetLocationLabels(), regionStores)
	strictStateFilter := filter.StoreStateFilter{ActionScope: replicaCheckerName, MoveRegion: true}
	target := selector.NewCandidates(r.cluster.GetStores()).
		FilterTarget(r.cluster, filters...).
		Sort(isolationComparer).Reverse().Top(isolationComparer). // greater isolation score is better
		Sort(selector.RegionScoreComparer(r.cluster)).            // less region score is better
		FilterTarget(r.cluster, strictStateFilter).PickFirst()    // the filter does not ignore temp states
	if target == nil {
		return 0
	}
	return target.GetID()
}

// selectStoreToReplace returns a store to replace oldStore. The location
// placement after scheduling should be not worse than original.
func (r *ReplicaChecker) selectStoreToReplace(region *core.RegionInfo, oldStore uint64) uint64 {
	filters := []filter.Filter{
		filter.NewExcludedFilter(replicaCheckerName, nil, region.GetStoreIds()),
		filter.NewLocationSafeguard(replicaCheckerName, r.cluster.GetLocationLabels(), r.cluster.GetRegionStores(region), r.cluster.GetStore(oldStore)),
	}
	newRegion := region.Clone(core.WithRemoveStorePeer(oldStore))
	return r.selectStoreToAdd(newRegion, filters...)
}

// selectStoreToImprove returns a store to replace oldStore. The location
// placement after scheduling should be better than original.
func (r *ReplicaChecker) selectStoreToImprove(region *core.RegionInfo, oldStore uint64) uint64 {
	filters := []filter.Filter{
		filter.NewExcludedFilter(replicaCheckerName, nil, region.GetStoreIds()),
		filter.NewLocationImprover(replicaCheckerName, r.cluster.GetLocationLabels(), r.cluster.GetRegionStores(region), r.cluster.GetStore(oldStore)),
	}
	newRegion := region.Clone(core.WithRemoveStorePeer(oldStore))
	return r.selectStoreToAdd(newRegion, filters...)
}

// selectStoreToRemove returns the best option to remove from the region.
func (r *ReplicaChecker) selectStoreToRemove(region *core.RegionInfo) uint64 {
	regionStores := r.cluster.GetRegionStores(region)
	isolationComparer := selector.IsolationComparer(r.cluster.GetLocationLabels(), regionStores)
	source := selector.NewCandidates(regionStores).
		FilterSource(r.cluster, filter.StoreStateFilter{ActionScope: replicaCheckerName, MoveRegion: true}).
		Sort(isolationComparer).Top(isolationComparer).
		Sort(selector.RegionScoreComparer(r.cluster)).Reverse().
		PickFirst()
	if source == nil {
		log.Debug("no removable store", zap.Uint64("region-id", region.GetID()))
		return 0
	}
	return source.GetID()
}

func (r *ReplicaChecker) checkDownPeer(region *core.RegionInfo) *operator.Operator {
	if !r.cluster.IsRemoveDownReplicaEnabled() {
		return nil
	}

	for _, stats := range region.GetDownPeers() {
		peer := stats.GetPeer()
		if peer == nil {
			continue
		}
		storeID := peer.GetStoreId()
		store := r.cluster.GetStore(storeID)
		if store == nil {
			log.Warn("lost the store, maybe you are recovering the PD cluster", zap.Uint64("store-id", storeID))
			return nil
		}
		if store.DownTime() < r.cluster.GetMaxStoreDownTime() {
			continue
		}
		if stats.GetDownSeconds() < uint64(r.cluster.GetMaxStoreDownTime().Seconds()) {
			continue
		}

		return r.fixPeer(region, storeID, downStatus)
	}
	return nil
}

func (r *ReplicaChecker) checkOfflinePeer(region *core.RegionInfo) *operator.Operator {
	if !r.cluster.IsReplaceOfflineReplicaEnabled() {
		return nil
	}

	// just skip learner
	if len(region.GetLearners()) != 0 {
		return nil
	}

	for _, peer := range region.GetPeers() {
		storeID := peer.GetStoreId()
		store := r.cluster.GetStore(storeID)
		if store == nil {
			log.Warn("lost the store, maybe you are recovering the PD cluster", zap.Uint64("store-id", storeID))
			return nil
		}
		if store.IsUp() {
			continue
		}

		return r.fixPeer(region, storeID, offlineStatus)
	}

	return nil
}

func (r *ReplicaChecker) checkMakeUpReplica(region *core.RegionInfo) *operator.Operator {
	if !r.cluster.IsMakeUpReplicaEnabled() {
		return nil
	}
	if len(region.GetPeers()) >= r.cluster.GetMaxReplicas() {
		return nil
	}
	log.Debug("region has fewer than max replicas", zap.Uint64("region-id", region.GetID()), zap.Int("peers", len(region.GetPeers())))
	target := r.selectStoreToAdd(region)
	if target == 0 {
		log.Debug("no store to add replica", zap.Uint64("region-id", region.GetID()))
		checkerCounter.WithLabelValues("replica_checker", "no-target-store").Inc()
		return nil
	}
	newPeer := &metapb.Peer{StoreId: target}
	op, err := operator.CreateAddPeerOperator("make-up-replica", r.cluster, region, newPeer, operator.OpReplica)
	if err != nil {
		log.Debug("create make-up-replica operator fail", zap.Error(err))
		return nil
	}
	return op
}

func (r *ReplicaChecker) checkRemoveExtraReplica(region *core.RegionInfo) *operator.Operator {
	if !r.cluster.IsRemoveExtraReplicaEnabled() {
		return nil
	}
	// when add learner peer, the number of peer will exceed max replicas for a while,
	// just comparing the the number of voters to avoid too many cancel add operator log.
	if len(region.GetVoters()) <= r.cluster.GetMaxReplicas() {
		return nil
	}
	log.Debug("region has more than max replicas", zap.Uint64("region-id", region.GetID()), zap.Int("peers", len(region.GetPeers())))
	old := r.selectStoreToRemove(region)
	if old == 0 {
		checkerCounter.WithLabelValues("replica_checker", "no-worst-peer").Inc()
		return nil
	}
	op, err := operator.CreateRemovePeerOperator("remove-extra-replica", r.cluster, operator.OpReplica, region, old)
	if err != nil {
		checkerCounter.WithLabelValues("replica_checker", "create-operator-fail").Inc()
		return nil
	}
	return op
}

func (r *ReplicaChecker) checkLocationReplacement(region *core.RegionInfo) *operator.Operator {
	if !r.cluster.IsLocationReplacementEnabled() {
		return nil
	}

	oldStore := r.selectStoreToRemove(region)
	if oldStore == 0 {
		checkerCounter.WithLabelValues("replica_checker", "all-right").Inc()
		return nil
	}
	newStore := r.selectStoreToImprove(region, oldStore)
	if newStore == 0 {
		log.Debug("no better peer", zap.Uint64("region-id", region.GetID()))
		checkerCounter.WithLabelValues("replica_checker", "not-better").Inc()
		return nil
	}

	newPeer := &metapb.Peer{StoreId: newStore}
	op, err := operator.CreateMovePeerOperator("move-to-better-location", r.cluster, region, operator.OpReplica, oldStore, newPeer)
	if err != nil {
		checkerCounter.WithLabelValues("replica_checker", "create-operator-fail").Inc()
		return nil
	}
	return op
}

func (r *ReplicaChecker) fixPeer(region *core.RegionInfo, storeID uint64, status string) *operator.Operator {
	// Check the number of replicas first.
	if len(region.GetPeers()) > r.cluster.GetMaxReplicas() {
		removeExtra := fmt.Sprintf("remove-extra-%s-replica", status)
		op, err := operator.CreateRemovePeerOperator(removeExtra, r.cluster, operator.OpReplica, region, storeID)
		if err != nil {
			reason := fmt.Sprintf("%s-fail", removeExtra)
			checkerCounter.WithLabelValues("replica_checker", reason).Inc()
			return nil
		}
		return op
	}

	target := r.selectStoreToReplace(region, storeID)
	if target == 0 {
		reason := fmt.Sprintf("no-store-%s", status)
		checkerCounter.WithLabelValues("replica_checker", reason).Inc()
		log.Debug("no best store to add replica", zap.Uint64("region-id", region.GetID()))
		return nil
	}
	newPeer := &metapb.Peer{StoreId: target}
	replace := fmt.Sprintf("replace-%s-replica", status)
	op, err := operator.CreateMovePeerOperator(replace, r.cluster, region, operator.OpReplica, storeID, newPeer)
	if err != nil {
		reason := fmt.Sprintf("%s-fail", replace)
		checkerCounter.WithLabelValues("replica_checker", reason).Inc()
		return nil
	}
	return op
}
