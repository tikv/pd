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
	"math/rand"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/cache"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/config"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/types"
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
	PauseController
	cluster                 sche.CheckerCluster
	conf                    config.CheckerConfigProvider
	pendingProcessedRegions *cache.TTLUint64
	r                       *rand.Rand
}

// NewReplicaChecker creates a replica checker.
func NewReplicaChecker(cluster sche.CheckerCluster, conf config.CheckerConfigProvider, pendingProcessedRegions *cache.TTLUint64) *ReplicaChecker {
	return &ReplicaChecker{
		cluster:                 cluster,
		conf:                    conf,
		pendingProcessedRegions: pendingProcessedRegions,
		r:                       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Name return ReplicaChecker's name.
func (*ReplicaChecker) Name() string {
	return types.ReplicaChecker.String()
}

// GetType return ReplicaChecker's type.
func (*ReplicaChecker) GetType() types.CheckerSchedulerType {
	return types.ReplicaChecker
}

// Check verifies a region's replicas, creating an operator.Operator if need.
func (c *ReplicaChecker) Check(region *core.RegionInfo) *operator.Operator {
	replicaCheckerCounter.Inc()
	if c.IsPaused() {
		replicaCheckerPausedCounter.Inc()
		return nil
	}
	if op := c.checkDownPeer(region); op != nil {
		replicaCheckerNewOpCounter.Inc()
		op.SetPriorityLevel(constant.High)
		return op
	}
	if op := c.checkOfflinePeer(region); op != nil {
		replicaCheckerNewOpCounter.Inc()
		op.SetPriorityLevel(constant.High)
		return op
	}
	if op := c.checkMakeUpReplica(region); op != nil {
		replicaCheckerNewOpCounter.Inc()
		op.SetPriorityLevel(constant.High)
		return op
	}
	if op := c.checkRemoveExtraReplica(region); op != nil {
		replicaCheckerNewOpCounter.Inc()
		return op
	}
	if op := c.checkLocationReplacement(region); op != nil {
		replicaCheckerNewOpCounter.Inc()
		return op
	}
	return nil
}

func (c *ReplicaChecker) checkDownPeer(region *core.RegionInfo) *operator.Operator {
	if !c.conf.IsRemoveDownReplicaEnabled() {
		return nil
	}

	for _, stats := range region.GetDownPeers() {
		peer := stats.GetPeer()
		if peer == nil {
			continue
		}
		storeID := peer.GetStoreId()
		store := c.cluster.GetStore(storeID)
		if store == nil {
			log.Warn("lost the store, maybe you are recovering the PD cluster", zap.Uint64("store-id", storeID))
			return nil
		}
		// Only consider the state of the Store, not `stats.DownSeconds`.
		if store.DownTime() < c.conf.GetMaxStoreDownTime() {
			continue
		}
		return c.fixPeer(region, storeID, downStatus)
	}
	return nil
}

func (c *ReplicaChecker) checkOfflinePeer(region *core.RegionInfo) *operator.Operator {
	if !c.conf.IsReplaceOfflineReplicaEnabled() {
		return nil
	}

	// just skip learner
	if len(region.GetLearners()) != 0 {
		return nil
	}

	for _, peer := range region.GetPeers() {
		storeID := peer.GetStoreId()
		store := c.cluster.GetStore(storeID)
		if store == nil {
			log.Warn("lost the store, maybe you are recovering the PD cluster", zap.Uint64("store-id", storeID))
			return nil
		}
		if store.IsUp() {
			continue
		}

		return c.fixPeer(region, storeID, offlineStatus)
	}

	return nil
}

func (c *ReplicaChecker) checkMakeUpReplica(region *core.RegionInfo) *operator.Operator {
	if !c.conf.IsMakeUpReplicaEnabled() {
		return nil
	}
	if len(region.GetPeers()) >= c.conf.GetMaxReplicas() {
		return nil
	}
	log.Debug("region has fewer than max replicas", zap.Uint64("region-id", region.GetID()), zap.Int("peers", len(region.GetPeers())))
	regionStores := c.cluster.GetRegionStores(region)
	target, filterByTempState := c.strategy(c.r, region).SelectStoreToAdd(regionStores)
	if target == 0 {
		log.Debug("no store to add replica", zap.Uint64("region-id", region.GetID()))
		replicaCheckerNoTargetStoreCounter.Inc()
		if filterByTempState {
			c.pendingProcessedRegions.Put(region.GetID(), nil)
		}
		return nil
	}
	newPeer := &metapb.Peer{StoreId: target}
	op, err := operator.CreateAddPeerOperator("make-up-replica", c.cluster, region, newPeer, operator.OpReplica)
	if err != nil {
		log.Debug("create make-up-replica operator fail", errs.ZapError(err))
		return nil
	}
	return op
}

func (c *ReplicaChecker) checkRemoveExtraReplica(region *core.RegionInfo) *operator.Operator {
	if !c.conf.IsRemoveExtraReplicaEnabled() {
		return nil
	}
	// when add learner peer, the number of peer will exceed max replicas for a while,
	// just comparing the the number of voters to avoid too many cancel add operator log.
	if len(region.GetVoters()) <= c.conf.GetMaxReplicas() {
		return nil
	}
	log.Debug("region has more than max replicas", zap.Uint64("region-id", region.GetID()), zap.Int("peers", len(region.GetPeers())))
	regionStores := c.cluster.GetRegionStores(region)
	old := c.strategy(c.r, region).SelectStoreToRemove(regionStores)
	if old == 0 {
		replicaCheckerNoWorstPeerCounter.Inc()
		c.pendingProcessedRegions.Put(region.GetID(), nil)
		return nil
	}
	op, err := operator.CreateRemovePeerOperator("remove-extra-replica", c.cluster, operator.OpReplica, region, old)
	if err != nil {
		replicaCheckerCreateOpFailedCounter.Inc()
		return nil
	}
	return op
}

func (c *ReplicaChecker) checkLocationReplacement(region *core.RegionInfo) *operator.Operator {
	if !c.conf.IsLocationReplacementEnabled() {
		return nil
	}

	strategy := c.strategy(c.r, region)
	regionStores := c.cluster.GetRegionStores(region)
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
	op, err := operator.CreateMovePeerOperator("move-to-better-location", c.cluster, region, operator.OpReplica, oldStore, newPeer)
	if err != nil {
		replicaCheckerCreateOpFailedCounter.Inc()
		return nil
	}
	return op
}

func (c *ReplicaChecker) fixPeer(region *core.RegionInfo, storeID uint64, status string) *operator.Operator {
	// Check the number of replicas first.
	if len(region.GetVoters()) > c.conf.GetMaxReplicas() {
		removeExtra := fmt.Sprintf("remove-extra-%s-replica", status)
		op, err := operator.CreateRemovePeerOperator(removeExtra, c.cluster, operator.OpReplica, region, storeID)
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

	regionStores := c.cluster.GetRegionStores(region)
	target, filterByTempState := c.strategy(c.r, region).SelectStoreToFix(regionStores, storeID)
	if target == 0 {
		if status == offlineStatus {
			replicaCheckerNoStoreOfflineCounter.Inc()
		} else if status == downStatus {
			replicaCheckerNoStoreDownCounter.Inc()
		}
		log.Debug("no best store to add replica", zap.Uint64("region-id", region.GetID()))
		if filterByTempState {
			c.pendingProcessedRegions.Put(region.GetID(), nil)
		}
		return nil
	}
	newPeer := &metapb.Peer{StoreId: target}
	replace := fmt.Sprintf("replace-%s-replica", status)
	op, err := operator.CreateMovePeerOperator(replace, c.cluster, region, operator.OpReplica, storeID, newPeer)
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

func (c *ReplicaChecker) strategy(r *rand.Rand, region *core.RegionInfo) *ReplicaStrategy {
	return &ReplicaStrategy{
		checkerName:    c.Name(),
		cluster:        c.cluster,
		locationLabels: c.conf.GetLocationLabels(),
		isolationLevel: c.conf.GetIsolationLevel(),
		region:         region,
		r:              r,
	}
}
