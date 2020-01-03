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
	"sort"
	"strconv"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
	"github.com/pingcap/pd/server/schedule/checker"
	"github.com/pingcap/pd/server/schedule/filter"
	"github.com/pingcap/pd/server/schedule/operator"
	"github.com/pingcap/pd/server/schedule/opt"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

func init() {
	schedule.RegisterSliceDecoderBuilder(BalanceRegionType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			conf, ok := v.(*balanceRegionSchedulerConfig)
			if !ok {
				return ErrScheduleConfigNotExist
			}
			ranges, err := getKeyRanges(args)
			if err != nil {
				return errors.WithStack(err)
			}
			conf.Ranges = ranges
			conf.Name = BalanceRegionName
			return nil
		}
	})
	schedule.RegisterScheduler(BalanceRegionType, func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &balanceRegionSchedulerConfig{}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newBalanceRegionScheduler(opController, conf), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	// BalanceRegionName is balance region scheduler name.
	BalanceRegionName = "balance-region-scheduler"
	// BalanceRegionType is balance region scheduler type.
	BalanceRegionType = "balance-region"
	hitsStoreTTL      = 5 * time.Minute
	// The scheduler selects the same source or source-target for a long time
	// and do not create an operator will trigger the hit filter. the
	// calculation of this time is as follows:
	// ScheduleIntervalFactor default is 1.3 , and MinScheduleInterval is 10ms,
	// the total time spend  t = a1 * (1-pow(q,n)) / (1 - q), where a1 = 10,
	// q = 1.3, and n = 30, so t = 87299ms ≈ 87s.
	hitsStoreCountThreshold = 30 * balanceRegionRetryLimit
)

type balanceRegionSchedulerConfig struct {
	Name   string          `json:"name"`
	Ranges []core.KeyRange `json:"ranges"`
}

type balanceRegionScheduler struct {
	*BaseScheduler
	conf         *balanceRegionSchedulerConfig
	opController *schedule.OperatorController
	filters      []filter.Filter
	hitsCounter  *hitsStoreBuilder
	counter      *prometheus.CounterVec
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, conf *balanceRegionSchedulerConfig, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := NewBaseScheduler(opController)
	scheduler := &balanceRegionScheduler{
		BaseScheduler: base,
		conf:          conf,
		opController:  opController,
		hitsCounter:   newHitsStoreBuilder(hitsStoreTTL, hitsStoreCountThreshold),
		counter:       balanceRegionCounter,
	}
	for _, setOption := range opts {
		setOption(scheduler)
	}
	scheduler.filters = []filter.Filter{filter.StoreStateFilter{ActionScope: scheduler.GetName(), MoveRegion: true}}
	return scheduler
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
		s.conf.Name = name
	}
}

func (s *balanceRegionScheduler) GetName() string {
	return s.conf.Name
}

func (s *balanceRegionScheduler) GetType() string {
	return BalanceRegionType
}

func (s *balanceRegionScheduler) EncodeConfig() ([]byte, error) {
	return schedule.EncodeConfig(s.conf)
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	stores := cluster.GetStores()
	filters := append(s.filters, s.hitsCounter.buildSourceFilter(s.GetName(), cluster))
	stores = filter.SelectSourceStores(stores, filters, cluster)
	sort.Slice(stores, func(i, j int) bool {
		return stores[i].RegionScore(cluster.GetHighSpaceRatio(), cluster.GetLowSpaceRatio(), 0) > stores[j].RegionScore(cluster.GetHighSpaceRatio(), cluster.GetLowSpaceRatio(), 0)
	})
	for _, source := range stores {
		sourceID := source.GetID()

		for i := 0; i < balanceRegionRetryLimit; i++ {
			// Priority pick the region that has a pending peer.
			// Pending region may means the disk is overload, remove the pending region firstly.
			region := cluster.RandPendingRegion(sourceID, s.conf.Ranges, opt.HealthAllowPending(cluster), opt.ReplicatedRegion(cluster))
			if region == nil {
				// Then pick the region that has a follower in the source store.
				region = cluster.RandFollowerRegion(sourceID, s.conf.Ranges, opt.HealthRegion(cluster), opt.ReplicatedRegion(cluster))
			}
			if region == nil {
				// Then pick the region has the leader in the source store.
				region = cluster.RandLeaderRegion(sourceID, s.conf.Ranges, opt.HealthRegion(cluster), opt.ReplicatedRegion(cluster))
			}
			if region == nil {
				// Finally pick learner.
				region = cluster.RandLearnerRegion(sourceID, s.conf.Ranges, opt.HealthRegion(cluster), opt.ReplicatedRegion(cluster))
			}
			if region == nil {
				s.hitsCounter.put(source, nil)
				schedulerCounter.WithLabelValues(s.GetName(), "no-region").Inc()
				continue
			}
			log.Debug("select region", zap.String("scheduler", s.GetName()), zap.Uint64("region-id", region.GetID()))

			// Skip hot regions.
			if cluster.IsRegionHot(region) {
				log.Debug("region is hot", zap.String("scheduler", s.GetName()), zap.Uint64("region-id", region.GetID()))
				schedulerCounter.WithLabelValues(s.GetName(), "region-hot").Inc()
				s.hitsCounter.put(source, nil)
				continue
			}

			oldPeer := region.GetStorePeer(sourceID)
			if op := s.transferPeer(cluster, region, oldPeer); op != nil {
				op.Counters = append(op.Counters, schedulerCounter.WithLabelValues(s.GetName(), "new-operator"))
				return []*operator.Operator{op}
			}
		}
	}
	return nil
}

// transferPeer selects the best store to create a new peer to replace the old peer.
func (s *balanceRegionScheduler) transferPeer(cluster opt.Cluster, region *core.RegionInfo, oldPeer *metapb.Peer) *operator.Operator {
	// scoreGuard guarantees that the distinct score will not decrease.
	stores := cluster.GetRegionStores(region)
	sourceStoreID := oldPeer.GetStoreId()
	source := cluster.GetStore(sourceStoreID)
	if source == nil {
		log.Error("failed to get the source store", zap.Uint64("store-id", sourceStoreID))
		s.hitsCounter.put(source, nil)
		return nil
	}
	exclude := make(map[uint64]struct{})
	excludeFilter := filter.NewExcludedFilter(s.GetName(), nil, exclude)
	hitsFilter := s.hitsCounter.buildTargetFilter(s.GetName(), cluster, source)
	for {
		var target *core.StoreInfo
		if cluster.IsPlacementRulesEnabled() {
			scoreGuard := filter.NewRuleFitFilter(s.GetName(), cluster, region, sourceStoreID)
			fit := cluster.FitRegion(region)
			rf := fit.GetRuleFit(oldPeer.GetId())
			if rf == nil {
				schedulerCounter.WithLabelValues(s.GetName(), "skip-orphan-peer").Inc()
				return nil
			}
			target = checker.SelectStoreToReplacePeerByRule(s.GetName(), cluster, region, fit, rf, oldPeer, scoreGuard, excludeFilter, hitsFilter)
		} else {
			scoreGuard := filter.NewDistinctScoreFilter(s.GetName(), cluster.GetLocationLabels(), stores, source)
			replicaChecker := checker.NewReplicaChecker(cluster, s.GetName())
			storeID, _ := replicaChecker.SelectBestReplacementStore(region, oldPeer, scoreGuard, excludeFilter, hitsFilter)
			if storeID != 0 {
				target = cluster.GetStore(storeID)
			}
		}
		if target == nil {
			schedulerCounter.WithLabelValues(s.GetName(), "no-replacement").Inc()
			return nil
		}
		exclude[target.GetID()] = struct{}{} // exclude next round.

		regionID := region.GetID()
		sourceID := source.GetID()
		targetID := target.GetID()
		log.Debug("", zap.Uint64("region-id", regionID), zap.Uint64("source-store", sourceID), zap.Uint64("target-store", targetID))

		opInfluence := s.opController.GetOpInfluence(cluster)
		kind := core.NewScheduleKind(core.RegionKind, core.BySize)
		if !shouldBalance(cluster, source, target, region, kind, opInfluence, s.GetName()) {
			schedulerCounter.WithLabelValues(s.GetName(), "skip").Inc()
			s.hitsCounter.put(source, target)
			continue
		}

		newPeer := &metapb.Peer{StoreId: target.GetID(), IsLearner: oldPeer.IsLearner}
		op, err := operator.CreateMovePeerOperator("balance-region", cluster, region, operator.OpBalance, oldPeer.GetStoreId(), newPeer)
		if err != nil {
			schedulerCounter.WithLabelValues(s.GetName(), "create-operator-fail").Inc()
			return nil
		}
		s.hitsCounter.remove(source, target)
		s.hitsCounter.remove(source, nil)
		sourceLabel := strconv.FormatUint(sourceID, 10)
		targetLabel := strconv.FormatUint(targetID, 10)
		op.Counters = append(op.Counters,
			s.counter.WithLabelValues("move-peer", source.GetAddress()+"-out", sourceLabel),
			s.counter.WithLabelValues("move-peer", target.GetAddress()+"-in", targetLabel),
			balanceDirectionCounter.WithLabelValues(s.GetName(), sourceLabel, targetLabel),
		)
		return op
	}
}

const hitLimitFactor = 3

type record struct {
	lastTime time.Time
	count    int
}
type hitsStoreBuilder struct {
	hits       map[string]*record
	ttl        map[uint64]time.Duration
	defaultTTL time.Duration
	threshold  int
}

func newHitsStoreBuilder(defaultTTL time.Duration, threshold int) *hitsStoreBuilder {
	return &hitsStoreBuilder{
		hits:       make(map[string]*record),
		ttl:        make(map[uint64]time.Duration),
		defaultTTL: defaultTTL,
		threshold:  threshold,
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
	ttl := h.getTTL(source, target)
	if key == "" {
		return false
	}
	if item, ok := h.hits[key]; ok {
		if time.Since(item.lastTime) > ttl {
			delete(h.hits, key)
		}
		if time.Since(item.lastTime) <= ttl && item.count >= h.threshold {
			log.Debug("skip the the store", zap.String("scheduler", BalanceRegionName), zap.String("filter-key", key))
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
	ttl := h.getTTL(source, target)
	if key == "" {
		return
	}
	if item, ok := h.hits[key]; ok {
		if time.Since(item.lastTime) >= ttl {
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

func (h *hitsStoreBuilder) buildSourceFilter(scope string, cluster opt.Cluster) filter.Filter {
	f := filter.NewBlacklistStoreFilter(scope, filter.BlacklistSource)
	h.updateTTL(cluster.GetAllStoresLimit())
	for _, source := range cluster.GetStores() {
		if h.filter(source, nil) {
			f.Add(source.GetID())
		}
	}
	return f
}

func (h *hitsStoreBuilder) buildTargetFilter(scope string, cluster opt.Cluster, source *core.StoreInfo) filter.Filter {
	f := filter.NewBlacklistStoreFilter(scope, filter.BlacklistTarget)
	h.updateTTL(cluster.GetAllStoresLimit())
	for _, target := range cluster.GetStores() {
		if h.filter(source, target) {
			f.Add(target.GetID())
		}
	}
	return f
}

func (h *hitsStoreBuilder) getTTL(source, target *core.StoreInfo) time.Duration {
	sourceTTL := h.readTTL(source)
	targetTTL := h.readTTL(target)
	if sourceTTL < targetTTL {
		return sourceTTL
	}
	return targetTTL
}

func (h *hitsStoreBuilder) readTTL(store *core.StoreInfo) time.Duration {
	ttl := h.defaultTTL
	if store != nil {
		if sourceTTL, ok := h.ttl[store.GetID()]; ok && sourceTTL < ttl {
			ttl = sourceTTL
		}
	}
	return ttl
}

func (h *hitsStoreBuilder) updateTTL(limits map[uint64]float64) {
	for storeID, limit := range limits {
		h.ttl[storeID] = time.Second * time.Duration(limit*limit*limit*4/10000)
		if h.ttl[storeID] > h.defaultTTL {
			h.ttl[storeID] = h.defaultTTL
		}
	}
}
