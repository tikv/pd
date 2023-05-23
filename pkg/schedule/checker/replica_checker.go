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
	"fmt"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/cache"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/config"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/operator"
	"go.uber.org/zap"
)

const (
	replicaCheckerName = "replica-checker"
	replicaChecker     = "replica_checker"
	offlineStatus      = "offline"
	downStatus         = "down"
)

var (
	// WithLabelValues is a heavy operation, define variable to avoid call it every time.
	replicaCheckerCounter                         = checkerCounter.WithLabelValues(replicaChecker, "check")
	replicaCheckerPausedCounter                   = checkerCounter.WithLabelValues(replicaChecker, "paused")
	replicaCheckerNewOpCounter                    = checkerCounter.WithLabelValues(replicaChecker, "new-operator")
	replicaCheckerNoTargetStoreCounter            = checkerCounter.WithLabelValues(replicaChecker, "no-target-store")
	replicaCheckerNoWorstPeerCounter              = checkerCounter.WithLabelValues(replicaChecker, "no-worst-peer")
	replicaCheckerCreateOpFailedCounter           = checkerCounter.WithLabelValues(replicaChecker, "create-operator-failed")
	replicaCheckerAllRightCounter                 = checkerCounter.WithLabelValues(replicaChecker, "all-right")
	replicaCheckerNotBetterCounter                = checkerCounter.WithLabelValues(replicaChecker, "not-better")
	replicaCheckerRemoveExtraOfflineFailedCounter = checkerCounter.WithLabelValues(replicaChecker, "remove-extra-offline-replica-failed")
	replicaCheckerRemoveExtraDownFailedCounter    = checkerCounter.WithLabelValues(replicaChecker, "remove-extra-down-replica-failed")
	replicaCheckerNoStoreOfflineCounter           = checkerCounter.WithLabelValues(replicaChecker, "no-store-offline")
	replicaCheckerNoStoreDownCounter              = checkerCounter.WithLabelValues(replicaChecker, "no-store-down")
	replicaCheckerReplaceOfflineFailedCounter     = checkerCounter.WithLabelValues(replicaChecker, "replace-offline-replica-failed")
	replicaCheckerReplaceDownFailedCounter        = checkerCounter.WithLabelValues(replicaChecker, "replace-down-replica-failed")
)

// ReplicaChecker ensures region has the best replicas.
// Including the following:
// Replica number management.
// Unhealthy replica management, mainly used for disaster recovery of TiKV.
// Location management, mainly used for cross data center deployment.
type ReplicaChecker struct {
	PauseController
	cluster           sche.ClusterInformer
	conf              config.Config
	regionWaitingList cache.Cache
}

// NewReplicaChecker creates a replica checker.
func NewReplicaChecker(cluster sche.ClusterInformer, conf config.Config, regionWaitingList cache.Cache) *ReplicaChecker {
	return &ReplicaChecker{
		cluster:           cluster,
		conf:              conf,
		regionWaitingList: regionWaitingList,
	}
}

// GetType return ReplicaChecker's type
func (r *ReplicaChecker) GetType() string {
	return replicaCheckerName
}

// Check verifies a region's replicas, creating an operator.Operator if need.
func (r *ReplicaChecker) Check(region *core.RegionInfo) *operator.Operator {
	replicaCheckerCounter.Inc()
	if r.IsPaused() {
		replicaCheckerPausedCounter.Inc()
		return nil
	}
	if op := r.checkDownPeer(region); op != nil {
		replicaCheckerNewOpCounter.Inc()
		op.SetPriorityLevel(constant.High)
		return op
	}
	if op := r.checkOfflinePeer(region); op != nil {
		replicaCheckerNewOpCounter.Inc()
		op.SetPriorityLevel(constant.High)
		return op
	}
	if op := r.checkMakeUpReplica(region); op != nil {
		replicaCheckerNewOpCounter.Inc()
		op.SetPriorityLevel(constant.High)
		return op
	}
	if op := r.checkRemoveExtraReplica(region); op != nil {
		replicaCheckerNewOpCounter.Inc()
		return op
	}
	if op := r.checkLocationReplacement(region); op != nil {
		replicaCheckerNewOpCounter.Inc()
		return op
	}
	return nil
}

func (r *ReplicaChecker) checkDownPeer(region *core.RegionInfo) *operator.Operator {
	if !r.conf.IsRemoveDownReplicaEnabled() {
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
		// Only consider the state of the Store, not `stats.DownSeconds`.
		if store.DownTime() < r.conf.GetMaxStoreDownTime() {
			continue
		}
		return r.fixPeer(region, storeID, downStatus)
	}
	return nil
}

func (r *ReplicaChecker) checkOfflinePeer(region *core.RegionInfo) *operator.Operator {
	if !r.conf.IsReplaceOfflineReplicaEnabled() {
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
	if !r.conf.IsMakeUpReplicaEnabled() {
		return nil
	}
	if len(region.GetPeers()) >= r.conf.GetMaxReplicas() {
		return nil
	}
	log.Debug("region has fewer than max replicas", zap.Uint64("region-id", region.GetID()), zap.Int("peers", len(region.GetPeers())))
	regionStores := r.cluster.GetRegionStores(region)
	target, filterByTempState := r.strategy(region).SelectStoreToAdd(regionStores)
	if target == 0 {
		log.Debug("no store to add replica", zap.Uint64("region-id", region.GetID()))
		replicaCheckerNoTargetStoreCounter.Inc()
		if filterByTempState {
			r.regionWaitingList.Put(region.GetID(), nil)
		}
		return nil
	}
	newPeer := &metapb.Peer{StoreId: target}
	op, err := operator.CreateAddPeerOperator("make-up-replica", r.cluster, region, newPeer, operator.OpReplica)
	if err != nil {
		log.Debug("create make-up-replica operator fail", errs.ZapError(err))
		return nil
	}
	return op
}

func (r *ReplicaChecker) checkRemoveExtraReplica(region *core.RegionInfo) *operator.Operator {
	if !r.conf.IsRemoveExtraReplicaEnabled() {
		return nil
	}
	// when add learner peer, the number of peer will exceed max replicas for a while,
	// just comparing the the number of voters to avoid too many cancel add operator log.
	if len(region.GetVoters()) <= r.conf.GetMaxReplicas() {
		return nil
	}
	log.Debug("region has more than max replicas", zap.Uint64("region-id", region.GetID()), zap.Int("peers", len(region.GetPeers())))
	regionStores := r.cluster.GetRegionStores(region)
	old := r.strategy(region).SelectStoreToRemove(regionStores)
	if old == 0 {
		replicaCheckerNoWorstPeerCounter.Inc()
		r.regionWaitingList.Put(region.GetID(), nil)
		return nil
	}
	op, err := operator.CreateRemovePeerOperator("remove-extra-replica", r.cluster, operator.OpReplica, region, old)
	if err != nil {
		replicaCheckerCreateOpFailedCounter.Inc()
		return nil
	}
	return op
}

func (r *ReplicaChecker) checkLocationReplacement(region *core.RegionInfo) *operator.Operator {
	if !r.conf.IsLocationReplacementEnabled() {
		return nil
	}

	strategy := r.strategy(region)
	regionStores := r.cluster.GetRegionStores(region)
	oldStore := strategy.SelectStoreToRemove(regionStores)
	if oldStore == 0 {
		replicaCheckerAllRightCounter.Inc()
		return nil
	}
	newStore, _ := strategy.SelectStoreToImprove(regionStores, oldStore)
	if newStore == 0 {
		log.Debug("no better peer", zap.Uint64("region-id", region.GetID()))
		replicaCheckerNotBetterCounter.Inc()
		return nil
	}

	newPeer := &metapb.Peer{StoreId: newStore}
	op, err := operator.CreateMovePeerOperator("move-to-better-location", r.cluster, region, operator.OpReplica, oldStore, newPeer)
	if err != nil {
		replicaCheckerCreateOpFailedCounter.Inc()
		return nil
	}
	return op
}

func (r *ReplicaChecker) fixPeer(region *core.RegionInfo, storeID uint64, status string) *operator.Operator {
	// Check the number of replicas first.
	if len(region.GetVoters()) > r.conf.GetMaxReplicas() {
		removeExtra := fmt.Sprintf("remove-extra-%s-replica", status)
		op, err := operator.CreateRemovePeerOperator(removeExtra, r.cluster, operator.OpReplica, region, storeID)
		if err != nil {
			if status == offlineStatus {
				replicaCheckerRemoveExtraOfflineFailedCounter.Inc()
			} else if status == downStatus {
				replicaCheckerRemoveExtraDownFailedCounter.Inc()
			}
			return nil
		}
		return op
	}

	regionStores := r.cluster.GetRegionStores(region)
	target, filterByTempState := r.strategy(region).SelectStoreToFix(regionStores, storeID)
	if target == 0 {
		if status == offlineStatus {
			replicaCheckerNoStoreOfflineCounter.Inc()
		} else if status == downStatus {
			replicaCheckerNoStoreDownCounter.Inc()
		}
		log.Debug("no best store to add replica", zap.Uint64("region-id", region.GetID()))
		if filterByTempState {
			r.regionWaitingList.Put(region.GetID(), nil)
		}
		return nil
	}
	newPeer := &metapb.Peer{StoreId: target}
	replace := fmt.Sprintf("replace-%s-replica", status)
	op, err := operator.CreateMovePeerOperator(replace, r.cluster, region, operator.OpReplica, storeID, newPeer)
	if err != nil {
		if status == offlineStatus {
			replicaCheckerReplaceOfflineFailedCounter.Inc()
		} else if status == downStatus {
			replicaCheckerReplaceDownFailedCounter.Inc()
		}
		return nil
	}
	return op
}

func (r *ReplicaChecker) strategy(region *core.RegionInfo) *ReplicaStrategy {
	return &ReplicaStrategy{
		checkerName:    replicaCheckerName,
		cluster:        r.cluster,
		locationLabels: r.conf.GetLocationLabels(),
		isolationLevel: r.conf.GetIsolationLevel(),
		region:         region,
	}
}
