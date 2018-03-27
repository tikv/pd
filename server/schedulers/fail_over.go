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

package schedulers

import (
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
	log "github.com/sirupsen/logrus"
)

func init() {
	schedule.RegisterScheduler("fail-over", func(limiter *schedule.Limiter, args []string) (schedule.Scheduler, error) {
		return newFailOverScheduler(limiter), nil
	})
}

type failOverScheduler struct {
	*baseScheduler
	leaderSelector schedule.Selector
}

func newFailOverScheduler(limiter *schedule.Limiter) schedule.Scheduler {
	filters := []schedule.Filter{
		schedule.NewStateFilter(),
		schedule.NewHealthFilter(),
		schedule.NewSnapshotCountFilter(),
		schedule.NewStorageThresholdFilter(),
		schedule.NewPendingPeerCountFilter(),
	}
	base := newBaseScheduler(limiter)
	return &failOverScheduler{
		baseScheduler:  base,
		leaderSelector: schedule.NewBalanceSelector(core.RegionKind, filters),
	}
}

func (f *failOverScheduler) GetName() string {
	return "fail-over-scheduler"
}

func (f *failOverScheduler) GetType() string {
	return "fail-over"
}

func (f *failOverScheduler) IsScheduleAllowed(cluster schedule.Cluster) bool {
	return f.limiter.OperatorCount(schedule.OpReplica) < cluster.GetReplicaScheduleLimit()
}

func (f *failOverScheduler) Schedule(cluster schedule.Cluster, opInfluence schedule.OpInfluence) []*schedule.Operator {
	stores := cluster.GetStores()
	for _, store := range stores {
		if store.DownTime() > cluster.GetMaxStoreDownTime() {
			op := f.handleDownStore(cluster, store)
			if op != nil {
				op.SetPriorityLevel(core.HighPriority)
				return []*schedule.Operator{op}
			}
		}
		if store.IsOffline() {
			op := f.handleOfflineStore(cluster, store)
			if op != nil {
				op.SetPriorityLevel(core.HighPriority)
				return []*schedule.Operator{op}
			}
		}
		if store.IsLowSpace() {
			op := f.handleLowSpaceStore(cluster, store)
			if op != nil {
				op.SetPriorityLevel(core.HighPriority)
				return []*schedule.Operator{op}
			}
		}
	}
	return nil
}

func (f *failOverScheduler) handleDownStore(cluster schedule.Cluster, store *core.StoreInfo) *schedule.Operator {
	region := cluster.RandLeaderRegion(store.GetId())
	if region != nil {
		log.Warnf("[region %d] Down Store [%v] have leader region, Maybe have network problem.", region.GetId(), store)
		schedulerCounter.WithLabelValues(f.GetName(), "down_leader").Inc()
		return nil
	}
	region = cluster.RandFollowerRegion(store.GetId())
	if region == nil {
		return nil
	}
	if region.HealthPeerCount() < cluster.GetMaxReplicas()/2+1 {
		log.Errorf("[region %d] region unhealth: %s", region.GetId(), region)
		schedulerCounter.WithLabelValues(f.GetName(), "unhealth").Inc()
		return nil
	}

	peer := region.GetStorePeer(store.GetId())
	if peer == nil {
		return nil
	}
	downPeer := region.GetDownPeer(peer.GetId())
	if downPeer == nil {
		log.Warnf("[region %d] peer %v not down in down store", region.GetId(), peer)
	}
	return schedule.CreateRemovePeerOperator("removeDownReplica", cluster, schedule.OpReplica, region, peer.GetStoreId())
}

func (f *failOverScheduler) handleOfflineStore(cluster schedule.Cluster, store *core.StoreInfo) *schedule.Operator {
	op := f.transferOutHotRegion(cluster, store)
	if op != nil {
		return op
	}
	region := cluster.RandLeaderRegion(store.GetId())
	if region == nil {
		region = cluster.RandFollowerRegion(store.GetId())
	}
	if region == nil {
		schedulerCounter.WithLabelValues(f.GetName(), "no_offine_region").Inc()
		return nil
	}
	if region.HealthPeerCount() < cluster.GetMaxReplicas()/2+1 {
		log.Errorf("[region %d] region unhealth: %v", region.GetId(), region)
		schedulerCounter.WithLabelValues(f.GetName(), "unhealth").Inc()
		return nil
	}
	peer := region.GetStorePeer(store.GetId())
	if peer == nil {
		return nil
	}
	// Check the number of replicas first.
	if len(region.Peers) > cluster.GetMaxReplicas() && region.HealthPeerCount() > cluster.GetMaxReplicas()/2+1 {
		return schedule.CreateRemovePeerOperator("removeExtraOfflineReplica", cluster, schedule.OpReplica, region, peer.GetStoreId())
	}
	// Consider we have 3 peers (A, B, C), we set the store that contains C to
	// offline while C is pending. If we generate an operator that adds a replica
	// D then removes C, D will not be successfully added util C is normal again.
	// So it's better to remove C directly.
	if region.GetPendingPeer(peer.GetId()) != nil {
		return schedule.CreateRemovePeerOperator("removePendingOfflineReplica", cluster, schedule.OpReplica, region, peer.GetStoreId())
	}
	if region.GetDownPeer(peer.GetId()) != nil {
		return schedule.CreateRemovePeerOperator("removeDownOfflineReplica", cluster, schedule.OpReplica, region, peer.GetStoreId())
	}
	checker := schedule.NewReplicaChecker(cluster, nil)
	newPeer := checker.SelectBestReplacedPeerToAddReplica(region, peer)
	if newPeer == nil {
		log.Debugf("[region %d] no best peer to add replica", region.GetId())
		schedulerCounter.WithLabelValues(f.GetName(), "no_peer").Inc()
		return nil
	}
	return schedule.CreateMovePeerOperator("makeUpOfflineReplica", cluster, region, schedule.OpReplica, peer.GetStoreId(), newPeer.GetStoreId(), newPeer.GetId())
}

func (f *failOverScheduler) handleLowSpaceStore(cluster schedule.Cluster, store *core.StoreInfo) *schedule.Operator {
	return f.transferOutHotRegion(cluster, store)
}

func (f *failOverScheduler) transferOutHotRegion(cluster schedule.Cluster, store *core.StoreInfo) *schedule.Operator {
	region := cluster.RandHotRegionFromStore(store.GetId(), schedule.ReadFlow)
	if region != nil && region.Leader.GetStoreId() == store.GetId() {
		target := f.leaderSelector.SelectTarget(cluster, cluster.GetFollowerStores(region))
		if target != nil {
			step := schedule.TransferLeader{FromStore: region.Leader.GetStoreId(), ToStore: target.GetId()}
			return schedule.NewOperator("transferOutHotRead", region.GetId(), schedule.OpBalance|schedule.OpLeader|schedule.OpReplica, step)
		}
		schedulerCounter.WithLabelValues(f.GetName(), "no_target_hot_read").Inc()
	}
	region = cluster.RandHotRegionFromStore(store.GetId(), schedule.WriteFlow)
	if region != nil {
		peer := region.GetStorePeer(store.GetId())
		if region.GetDownPeer(peer.GetId()) != nil || region.GetPendingPeer(peer.GetId()) != nil {
			return schedule.CreateRemovePeerOperator("removeUnhealthHotPeer", cluster, schedule.OpBalance|schedule.OpReplica, region, store.GetId())
		}
		checker := schedule.NewReplicaChecker(cluster, nil)
		newPeer := checker.SelectBestReplacedPeerToAddReplica(region, peer)
		if newPeer == nil {
			schedulerCounter.WithLabelValues(f.GetName(), "no_target_hot_write").Inc()
			return nil
		}
		return schedule.CreateMovePeerOperator("transferOutHotWrite", cluster, region, schedule.OpReplica, peer.GetStoreId(), newPeer.GetStoreId(), newPeer.GetId())

	}
	return nil
}
