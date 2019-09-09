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
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/pd/server/checker"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
	"github.com/pingcap/pd/server/schedule/filter"
	"github.com/pingcap/pd/server/schedule/operator"
	"github.com/pingcap/pd/server/schedule/selector"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

func init() {
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, args []string) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	hitsStoreTTL            = 5 * time.Minute
	// The scheduler selects the same source or source-target for a long time
	// and do not create an operator will trigger the hit filter. the
	// calculation of this time is as follows:
	// ScheduleIntervalFactor default is 1.3 , and MinScheduleInterval is 10ms,
	// the total time spend  t = a1 * (1-pow(q,n)) / (1 - q), where a1 = 10,
	// q = 1.3, and n = 30, so t = 87299ms ≈ 87s.
	hitsStoreCountThreshold = 30 * balanceRegionRetryLimit
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	selector     *selector.BalanceSelector
	opController *schedule.OperatorController
	hitsCounter  *hitsStoreBuilder
	counter      *prometheus.CounterVec
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	filters := []filter.Filter{
		filter.StoreStateFilter{MoveRegion: true},
	}
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		selector:      selector.NewBalanceSelector(core.RegionKind, filters),
		opController:  opController,
		hitsCounter:   newHitsStoreBuilder(hitsStoreTTL, hitsStoreCountThreshold),
		counter:       balanceRegionCounter,
	}
	for _, opt := range opts {
		opt(s)
	}
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
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster schedule.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster schedule.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	stores := cluster.GetStores()

	// source is the store with highest region score in the list that can be selected as balance source.
	f := s.hitsCounter.buildSourceFilter(cluster)
	source := s.selector.SelectSource(cluster, stores, f)
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
			s.hitsCounter.put(source, nil)
			continue
		}
		log.Debug("select region", zap.String("scheduler", s.GetName()), zap.Uint64("region-id", region.GetID()))

		// We don't schedule region with abnormal number of replicas.
		if len(region.GetPeers()) != cluster.GetMaxReplicas() {
			log.Debug("region has abnormal replica count", zap.String("scheduler", s.GetName()), zap.Uint64("region-id", region.GetID()))
			schedulerCounter.WithLabelValues(s.GetName(), "abnormal_replica").Inc()
			s.hitsCounter.put(source, nil)
			continue
		}

		// Skip hot regions.
		if cluster.IsRegionHot(region) {
			log.Debug("region is hot", zap.String("scheduler", s.GetName()), zap.Uint64("region-id", region.GetID()))
			schedulerCounter.WithLabelValues(s.GetName(), "region_hot").Inc()
			s.hitsCounter.put(source, nil)
			continue
		}

		oldPeer := region.GetStorePeer(sourceID)
		if op := s.transferPeer(cluster, region, oldPeer); op != nil {
			schedulerCounter.WithLabelValues(s.GetName(), "new_operator").Inc()
			return []*operator.Operator{op}
		}
	}
	return nil
}

// transferPeer selects the best store to create a new peer to replace the old peer.
func (s *balanceRegionScheduler) transferPeer(cluster schedule.Cluster, region *core.RegionInfo, oldPeer *metapb.Peer) *operator.Operator {
	// scoreGuard guarantees that the distinct score will not decrease.
	stores := cluster.GetRegionStores(region)
	sourceStoreID := oldPeer.GetStoreId()
	source := cluster.GetStore(sourceStoreID)
	if source == nil {
		log.Error("failed to get the source store", zap.Uint64("store-id", sourceStoreID))
	}
	scoreGuard := filter.NewDistinctScoreFilter(cluster.GetLocationLabels(), stores, source)
	hitsFilter := s.hitsCounter.buildTargetFilter(cluster, source)
	checker := checker.NewReplicaChecker(cluster, nil)
	storeID, _ := checker.SelectBestReplacementStore(region, oldPeer, scoreGuard, hitsFilter)
	if storeID == 0 {
		schedulerCounter.WithLabelValues(s.GetName(), "no_replacement").Inc()
		s.hitsCounter.put(source, nil)
		return nil
	}

	target := cluster.GetStore(storeID)
	if target == nil {
		log.Error("failed to get the target store", zap.Uint64("store-id", storeID))
	}
	regionID := region.GetID()
	sourceID := source.GetID()
	targetID := target.GetID()
	log.Debug("", zap.Uint64("region-id", regionID), zap.Uint64("source-store", sourceID), zap.Uint64("target-store", targetID))

	opInfluence := s.opController.GetOpInfluence(cluster)
	if !shouldBalance(cluster, source, target, region, core.RegionKind, opInfluence) {
		log.Debug("skip balance region",
			zap.String("scheduler", s.GetName()), zap.Uint64("region-id", regionID), zap.Uint64("source-store", sourceID), zap.Uint64("target-store", targetID),
			zap.Int64("source-size", source.GetRegionSize()), zap.Float64("source-score", source.RegionScore(cluster.GetHighSpaceRatio(), cluster.GetLowSpaceRatio(), 0)),
			zap.Int64("source-influence", opInfluence.GetStoreInfluence(sourceID).ResourceSize(core.RegionKind)),
			zap.Int64("target-size", target.GetRegionSize()), zap.Float64("target-score", target.RegionScore(cluster.GetHighSpaceRatio(), cluster.GetLowSpaceRatio(), 0)),
			zap.Int64("target-influence", opInfluence.GetStoreInfluence(targetID).ResourceSize(core.RegionKind)),
			zap.Int64("average-region-size", cluster.GetAverageRegionSize()))
		schedulerCounter.WithLabelValues(s.GetName(), "skip").Inc()
		s.hitsCounter.put(source, target)
		return nil
	}

	newPeer, err := cluster.AllocPeer(storeID)
	if err != nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no_peer").Inc()
		return nil
	}
	op, err := operator.CreateMovePeerOperator("balance-region", cluster, region, operator.OpBalance, oldPeer.GetStoreId(), newPeer.GetStoreId(), newPeer.GetId())
	if err != nil {
		schedulerCounter.WithLabelValues(s.GetName(), "create_operator_fail").Inc()
		return nil
	}
	s.hitsCounter.remove(source, target)
	s.hitsCounter.remove(source, nil)
	sourceLabel := strconv.FormatUint(sourceID, 10)
	targetLabel := strconv.FormatUint(targetID, 10)
	s.counter.WithLabelValues("move_peer", source.GetAddress()+"-out", sourceLabel).Inc()
	s.counter.WithLabelValues("move_peer", target.GetAddress()+"-in", targetLabel).Inc()
	s.counter.WithLabelValues("direction", "from_to", sourceLabel+"-"+targetLabel).Inc()
	return op
}

type record struct {
	lastTime time.Time
	count    int
}
type hitsStoreBuilder struct {
	hits      map[string]*record
	ttl       time.Duration
	threshold int
}

func newHitsStoreBuilder(ttl time.Duration, threshold int) *hitsStoreBuilder {
	return &hitsStoreBuilder{
		hits:      make(map[string]*record),
		ttl:       ttl,
		threshold: threshold,
	}
}

func (h *hitsStoreBuilder) getKey(source, target *core.StoreInfo) string {
	if source == nil {
		return ""
	}
	key := fmt.Sprintf("s%d", source.GetID())
	if target != nil {
		key = fmt.Sprintf("%s->t%d", key, target.GetID())
	}
	return key
}

func (h *hitsStoreBuilder) filter(source, target *core.StoreInfo) bool {
	key := h.getKey(source, target)
	if key == "" {
		return false
	}
	if item, ok := h.hits[key]; ok {
		if time.Since(item.lastTime) > h.ttl {
			delete(h.hits, key)
		}
		if time.Since(item.lastTime) <= h.ttl && item.count >= h.threshold {
			log.Debug("skip the the store", zap.String("scheduler", balanceRegionName), zap.String("filter-key", key))
			return true
		}
	}
	return false
}

func (h *hitsStoreBuilder) remove(source, target *core.StoreInfo) {
	key := h.getKey(source, target)
	if _, ok := h.hits[key]; ok && key != "" {
		delete(h.hits, key)
	}
}

func (h *hitsStoreBuilder) put(source, target *core.StoreInfo) {
	key := h.getKey(source, target)
	if key == "" {
		return
	}
	if item, ok := h.hits[key]; ok {
		if time.Since(item.lastTime) >= h.ttl {
			item.count = 0
		} else {
			item.count++
		}
		item.lastTime = time.Now()
	} else {
		item := &record{lastTime: time.Now()}
		h.hits[key] = item
	}
}

func (h *hitsStoreBuilder) buildSourceFilter(cluster schedule.Cluster) filter.Filter {
	f := filter.NewBlacklistStoreFilter(filter.BlacklistSource)
	for _, source := range cluster.GetStores() {
		if h.filter(source, nil) {
			f.Add(source.GetID())
		}
	}
	return f
}

func (h *hitsStoreBuilder) buildTargetFilter(cluster schedule.Cluster, source *core.StoreInfo) filter.Filter {
	f := filter.NewBlacklistStoreFilter(filter.BlacklistTarget)
	for _, target := range cluster.GetStores() {
		if h.filter(source, target) {
			f.Add(target.GetID())
		}
	}
	return f
}
