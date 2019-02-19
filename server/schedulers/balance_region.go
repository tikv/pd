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

package schedulers

import (
	"fmt"
	"strconv"

	"github.com/pingcap/kvproto/pkg/metapb"
	log "github.com/pingcap/log"
	"github.com/pingcap/pd/server/cache"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
	"go.uber.org/zap"
)

func init() {
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, args []string) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

// balanceRegionRetryLimit is the limit to retry schedule for selected store.
const balanceRegionRetryLimit = 10

type balanceRegionScheduler struct {
	*baseScheduler
	selector     *schedule.BalanceSelector
	taintStores  *cache.TTLUint64
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController) schedule.Scheduler {
	taintStores := newTaintCache()
	filters := []schedule.Filter{
		schedule.StoreStateFilter{MoveRegion: true},
		schedule.NewCacheFilter(taintStores),
	}
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		selector:      schedule.NewBalanceSelector(core.RegionKind, filters),
		taintStores:   taintStores,
		opController:  opController,
	}
	return s
}

func (s *balanceRegionScheduler) GetName() string {
	return "balance-region-scheduler"
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster schedule.Cluster) bool {
	return s.opController.OperatorCount(schedule.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster schedule.Cluster) []*schedule.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()

	stores := cluster.GetStores()

	// source is the store with highest region score in the list that can be selected as balance source.
	source := s.selector.SelectSource(cluster, stores)
	if source == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no_store").Inc()
		// Unlike the balanceLeaderScheduler, we don't need to clear the taintCache
		// here. Because normally region score won't change rapidly, and the region
		// balance requires lower sensitivity compare to leader balance.
		return nil
	}

	log.Debug("store has the max region score", zap.String("scheduler", s.GetName()), zap.Uint64("store-id", source.GetID()))
	sourceLabel := strconv.FormatUint(source.GetID(), 10)
	balanceRegionCounter.WithLabelValues("source_store", sourceLabel).Inc()

	opInfluence := s.opController.GetOpInfluence(cluster)
	var hasPotentialTarget bool
	for i := 0; i < balanceRegionRetryLimit; i++ {
		// Priority the region that has a follower in the source store.
		region := cluster.RandFollowerRegion(source.GetID(), core.HealthRegion())
		if region == nil {
			// Then the region has the leader in the source store
			region = cluster.RandLeaderRegion(source.GetID(), core.HealthRegion())
		}
		if region == nil {
			schedulerCounter.WithLabelValues(s.GetName(), "no_region").Inc()
			continue
		}
		log.Debug("select region", zap.String("scheduler", s.GetName()), zap.Uint64("region-id", region.GetID()))

		// We don't schedule region with abnormal number of replicas.
		if len(region.GetPeers()) != cluster.GetMaxReplicas() {
			log.Debug("region has abnormal replica count", zap.String("scheduler", s.GetName()), zap.Uint64("region-id", region.GetID()))
			schedulerCounter.WithLabelValues(s.GetName(), "abnormal_replica").Inc()
			continue
		}

		// Skip hot regions.
		if cluster.IsRegionHot(region.GetID()) {
			log.Debug("region is hot", zap.String("scheduler", s.GetName()), zap.Uint64("region-id", region.GetID()))
			schedulerCounter.WithLabelValues(s.GetName(), "region_hot").Inc()
			continue
		}

		if !s.hasPotentialTarget(cluster, region, source, opInfluence) {
			continue
		}
		hasPotentialTarget = true

		oldPeer := region.GetStorePeer(source.GetID())
		if op := s.transferPeer(cluster, region, oldPeer, opInfluence); op != nil {
			schedulerCounter.WithLabelValues(s.GetName(), "new_operator").Inc()
			return []*schedule.Operator{op}
		}
	}

	if !hasPotentialTarget {
		// If no potential target store can be found for the selected store, ignore it for a while.
		log.Debug("no operator created for selected store", zap.String("scheduler", s.GetName()), zap.Uint64("store-id", source.GetID()))
		balanceRegionCounter.WithLabelValues("add_taint", sourceLabel).Inc()
		s.taintStores.Put(source.GetID())
	}

	return nil
}

// transferPeer selects the best store to create a new peer to replace the old peer.
func (s *balanceRegionScheduler) transferPeer(cluster schedule.Cluster, region *core.RegionInfo, oldPeer *metapb.Peer, opInfluence schedule.OpInfluence) *schedule.Operator {
	// scoreGuard guarantees that the distinct score will not decrease.
	stores := cluster.GetRegionStores(region)
	source := cluster.GetStore(oldPeer.GetStoreId())
	scoreGuard := schedule.NewDistinctScoreFilter(cluster.GetLocationLabels(), stores, source)

	checker := schedule.NewReplicaChecker(cluster, nil)
	storeID, _ := checker.SelectBestReplacementStore(region, oldPeer, scoreGuard)
	if storeID == 0 {
		schedulerCounter.WithLabelValues(s.GetName(), "no_replacement").Inc()
		return nil
	}

	target := cluster.GetStore(storeID)
	log.Debug("", zap.Uint64("region-id", region.GetID()), zap.Uint64("source-store", source.GetID()), zap.Uint64("target-store", target.GetID()))

	if !shouldBalance(cluster, source, target, region, core.RegionKind, opInfluence) {
		log.Debug("skip balance region",
			zap.String("scheduler", s.GetName()), zap.Uint64("region-id", region.GetID()), zap.Uint64("source-store", source.GetID()), zap.Uint64("target-store", target.GetID()),
			zap.Int64("source-size", source.GetRegionSize()), zap.Float64("source-score", source.RegionScore(cluster.GetHighSpaceRatio(), cluster.GetLowSpaceRatio(), 0)),
			zap.Int64("source-influence", opInfluence.GetStoreInfluence(source.GetID()).ResourceSize(core.RegionKind)),
			zap.Int64("target-size", target.GetRegionSize()), zap.Float64("target-score", target.RegionScore(cluster.GetHighSpaceRatio(), cluster.GetLowSpaceRatio(), 0)),
			zap.Int64("target-influence", opInfluence.GetStoreInfluence(target.GetID()).ResourceSize(core.RegionKind)),
			zap.Int64("average-region-size", cluster.GetAverageRegionSize()))
		schedulerCounter.WithLabelValues(s.GetName(), "skip").Inc()
		return nil
	}

	newPeer, err := cluster.AllocPeer(storeID)
	if err != nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no_peer").Inc()
		return nil
	}
	balanceRegionCounter.WithLabelValues("move_peer", fmt.Sprintf("store%d-out", source.GetID())).Inc()
	balanceRegionCounter.WithLabelValues("move_peer", fmt.Sprintf("store%d-in", target.GetID())).Inc()
	return schedule.CreateMovePeerOperator("balance-region", cluster, region, schedule.OpBalance, oldPeer.GetStoreId(), newPeer.GetStoreId(), newPeer.GetId())
}

// hasPotentialTarget is used to determine whether the specified sourceStore
// cannot find a matching targetStore in the long term.
// The main factor for judgment includes StoreState, DistinctScore, and
// ResourceScore, while excludes factors such as ServerBusy, too many snapshot,
// which may recover soon.
func (s *balanceRegionScheduler) hasPotentialTarget(cluster schedule.Cluster, region *core.RegionInfo, source *core.StoreInfo, opInfluence schedule.OpInfluence) bool {
	filters := []schedule.Filter{
		schedule.NewExcludedFilter(nil, region.GetStoreIds()),
		schedule.NewDistinctScoreFilter(cluster.GetLocationLabels(), cluster.GetRegionStores(region), source),
	}

	for _, store := range cluster.GetStores() {
		if schedule.FilterTarget(cluster, store, filters) {
			continue
		}
		if !store.IsUp() || store.DownTime() > cluster.GetMaxStoreDownTime() {
			continue
		}
		if !shouldBalance(cluster, source, store, region, core.RegionKind, opInfluence) {
			continue
		}
		return true
	}
	return false
}
