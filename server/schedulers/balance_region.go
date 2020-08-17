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
	"strconv"

	"github.com/pingcap/kvproto/pkg/metapb"
	log "github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/pd/server/cache"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule"
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
	name         string
	selector     *schedule.BalanceSelector
	taintStores  *cache.TTLUint64
	opController *schedule.OperatorController
	counter      *prometheus.CounterVec
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	taintStores := newTaintCache()
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
		counter:       balanceRegionCounter,
		taintStores:   taintStores,
	}
	for _, opt := range opts {
		opt(s)
	}
	filters := []schedule.Filter{
		schedule.StoreStateFilter{ActionScope: s.GetName(), MoveRegion: true},
		schedule.NewCacheFilter(s.GetName(), taintStores),
	}
	kind := core.NewScheduleKind(core.RegionKind, core.BySize)
	s.selector = schedule.NewBalanceSelector(kind, filters)
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

// WithBalanceRegionCounter sets the counter for the scheduler.
func WithBalanceRegionCounter(counter *prometheus.CounterVec) BalanceRegionCreateOption {
	return func(s *balanceRegionScheduler) {
		s.counter = counter
	}
}

// WithBalanceRegionName sets the name for the scheduler.
func WithBalanceRegionName(name string) BalanceRegionCreateOption {
	return func(s *balanceRegionScheduler) {
		s.name = name
	}
}

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
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

	sourceID := source.GetID()
	log.Debug("store has the max region score", zap.String("scheduler", s.GetName()), zap.Uint64("store-id", sourceID))
	sourceAddress := source.GetAddress()
	sourceLabel := strconv.FormatUint(sourceID, 10)
	s.counter.WithLabelValues("source_store", sourceAddress, sourceLabel).Inc()

	opInfluence := s.opController.GetOpInfluence(cluster)
	var hasPotentialTarget bool
	for i := 0; i < balanceRegionRetryLimit; i++ {
		// Priority picks the region that has a pending peer.
		// Pending region may means the disk is overload, remove the pending region firstly.
		region := cluster.RandPendingRegion(sourceID, core.HealthRegionAllowPending())
		if region == nil {
			// Then picks the region that has a follower in the source store.
			region = cluster.RandFollowerRegion(sourceID, core.HealthRegion())
		}
		if region == nil {
			// Last, picks the region has the leader in the source store.
			region = cluster.RandLeaderRegion(sourceID, core.HealthRegion())
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

		oldPeer := region.GetStorePeer(sourceID)
		if op := s.transferPeer(cluster, region, oldPeer, opInfluence); op != nil {
			schedulerCounter.WithLabelValues(s.GetName(), "new_operator").Inc()
			return []*schedule.Operator{op}
		}
	}

	if !hasPotentialTarget {
		// If no potential target store can be found for the selected store, ignore it for a while.
		log.Debug("no operator created for selected store", zap.String("scheduler", s.GetName()), zap.Uint64("store-id", sourceID))
		balanceRegionCounter.WithLabelValues("add_taint", sourceAddress, sourceLabel).Inc()
		s.taintStores.Put(sourceID)
	}

	return nil
}

// transferPeer selects the best store to create a new peer to replace the old peer.
func (s *balanceRegionScheduler) transferPeer(cluster schedule.Cluster, region *core.RegionInfo, oldPeer *metapb.Peer, opInfluence schedule.OpInfluence) *schedule.Operator {
	// scoreGuard guarantees that the distinct score will not decrease.
	stores := cluster.GetRegionStores(region)
	source := cluster.GetStore(oldPeer.GetStoreId())
	scoreGuard := schedule.NewDistinctScoreFilter(s.GetName(), cluster.GetLocationLabels(), stores, source)

	checker := schedule.NewReplicaChecker(cluster, nil, s.GetName())
	storeID, _ := checker.SelectBestReplacementStore(region, oldPeer, scoreGuard)
	if storeID == 0 {
		schedulerCounter.WithLabelValues(s.GetName(), "no_replacement").Inc()
		return nil
	}

	target := cluster.GetStore(storeID)
	regionID := region.GetID()
	sourceID := source.GetID()
	targetID := target.GetID()
	log.Debug("", zap.Uint64("region-id", regionID), zap.Uint64("source-store", sourceID), zap.Uint64("target-store", targetID))

	kind := core.NewScheduleKind(core.RegionKind, core.BySize)
	if !shouldBalance(cluster, source, target, region, kind, opInfluence, s.GetName()) {
		schedulerCounter.WithLabelValues(s.GetName(), "skip").Inc()
		return nil
	}

	newPeer, err := cluster.AllocPeer(storeID)
	if err != nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no_peer").Inc()
		return nil
	}
	op, err := schedule.CreateMovePeerOperator("balance-region", cluster, region, schedule.OpBalance, oldPeer.GetStoreId(), newPeer.GetStoreId(), newPeer.GetId())
	if err != nil {
		schedulerCounter.WithLabelValues(s.GetName(), "create_operator_fail").Inc()
		return nil
	}
	sourceLabel := strconv.FormatUint(sourceID, 10)
	targetLabel := strconv.FormatUint(targetID, 10)
	s.counter.WithLabelValues("move-peer", source.GetAddress()+"-out", sourceLabel).Inc()
	s.counter.WithLabelValues("move-peer", target.GetAddress()+"-in", targetLabel).Inc()
	balanceDirectionCounter.WithLabelValues(s.GetName(), sourceLabel, targetLabel).Inc()
	return op
}

// hasPotentialTarget is used to determine whether the specified sourceStore
// cannot find a matching targetStore in the long term.
// The main factor for judgment includes StoreState, DistinctScore, and
// ResourceScore, while excludes factors such as ServerBusy, too many snapshot,
// which may recover soon.
func (s *balanceRegionScheduler) hasPotentialTarget(cluster schedule.Cluster, region *core.RegionInfo, source *core.StoreInfo, opInfluence schedule.OpInfluence) bool {
	filters := []schedule.Filter{
		schedule.NewExcludedFilter(s.GetName(), nil, region.GetStoreIds()),
		schedule.NewDistinctScoreFilter(s.GetName(), cluster.GetLocationLabels(), cluster.GetRegionStores(region), source),
	}

	for _, store := range cluster.GetStores() {
		if schedule.FilterTarget(cluster, store, filters) {
			continue
		}
		if !store.IsUp() || store.DownTime() > cluster.GetMaxStoreDownTime() {
			continue
		}
		kind := core.NewScheduleKind(core.RegionKind, core.BySize)
		if !shouldBalance(cluster, source, store, region, kind, opInfluence, s.GetName()) {
			continue
		}
		return true
	}
	return false
}
