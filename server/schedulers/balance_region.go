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
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"sort"
	"strconv"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/filter"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/opt"
	"go.uber.org/zap"
)

func init() {
	schedule.RegisterSliceDecoderBuilder(BalanceRegionType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			conf, ok := v.(*balanceRegionSchedulerConfig)
			if !ok {
				return errs.ErrScheduleConfigNotExist.FastGenByArgs()
			}
			ranges, err := getKeyRanges(args)
			if err != nil {
				return err
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
		counter:       balanceRegionCounter,
	}
	for _, setOption := range opts {
		setOption(scheduler)
	}
	scheduler.filters = []filter.Filter{
		filter.StoreStateFilter{ActionScope: scheduler.GetName(), MoveRegion: true},
		filter.NewSpecialUseFilter(scheduler.GetName()),
	}
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
	return s.opController.OperatorCount(operator.OpRegion)-s.opController.OperatorCount(operator.OpMerge) < cluster.GetOpts().GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	stores := cluster.GetStores()
	opts := cluster.GetOpts()
	stores = filter.SelectSourceStores(stores, s.filters, opts)
	opInfluence := s.opController.GetOpInfluence(cluster)
	kind := core.NewScheduleKind(core.RegionKind, core.BySize)
	sort.Slice(stores, func(i, j int) bool {
		iOp := opInfluence.GetStoreInfluence(stores[i].GetID()).ResourceProperty(kind)
		jOp := opInfluence.GetStoreInfluence(stores[j].GetID()).ResourceProperty(kind)
		return stores[i].RegionScore(opts.GetHighSpaceRatio(), opts.GetLowSpaceRatio(), iOp) >
			stores[j].RegionScore(opts.GetHighSpaceRatio(), opts.GetLowSpaceRatio(), jOp)
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
				schedulerCounter.WithLabelValues(s.GetName(), "no-region").Inc()
				continue
			}
			log.Debug("select region", zap.String("scheduler", s.GetName()), zap.Uint64("region-id", region.GetID()))

			// Skip hot regions.
			if cluster.IsRegionHot(region) {
				log.Debug("region is hot", zap.String("scheduler", s.GetName()), zap.Uint64("region-id", region.GetID()))
				schedulerCounter.WithLabelValues(s.GetName(), "region-hot").Inc()
				continue
			}
			// Check region whether have leader
			if region.GetLeader() == nil {
				log.Warn("region have no leader", zap.String("scheduler", s.GetName()), zap.Uint64("region-id", region.GetID()))
				schedulerCounter.WithLabelValues(s.GetName(), "no-leader").Inc()
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
	sourceStoreID := oldPeer.GetStoreId()
	source := cluster.GetStore(sourceStoreID)
	if source == nil {
		log.Error("failed to get the source store", zap.Uint64("store-id", sourceStoreID), errs.ZapError(errs.ErrGetSourceStore))
		return nil
	}

	filters := []filter.Filter{
		filter.NewExcludedFilter(s.GetName(), nil, region.GetStoreIds()),
		filter.NewPlacementSafeguard(s.GetName(), cluster, region, source),
		filter.NewSpecialUseFilter(s.GetName()),
		filter.StoreStateFilter{ActionScope: s.GetName(), MoveRegion: true},
	}

	candidates := filter.NewCandidates(cluster.GetStores()).
		FilterTarget(cluster.GetOpts(), filters...).
		Sort(filter.RegionScoreComparer(cluster.GetOpts()))

	for _, target := range candidates.Stores {
		regionID := region.GetID()
		sourceID := source.GetID()
		targetID := target.GetID()
		log.Debug("", zap.Uint64("region-id", regionID), zap.Uint64("source-store", sourceID), zap.Uint64("target-store", targetID))

		opInfluence := s.opController.GetOpInfluence(cluster)
		kind := core.NewScheduleKind(core.RegionKind, core.BySize)
		shouldBalance, sourceScore, targetScore := shouldBalance(cluster, source, target, region, kind, opInfluence, s.GetName())
		if !shouldBalance {
			schedulerCounter.WithLabelValues(s.GetName(), "skip").Inc()
			continue
		}

		newPeer := &metapb.Peer{StoreId: target.GetID(), Role: oldPeer.Role}
		op, err := operator.CreateMovePeerOperator(BalanceRegionType, cluster, region, operator.OpRegion, oldPeer.GetStoreId(), newPeer)
		if err != nil {
			schedulerCounter.WithLabelValues(s.GetName(), "create-operator-fail").Inc()
			return nil
		}
		sourceLabel := strconv.FormatUint(sourceID, 10)
		targetLabel := strconv.FormatUint(targetID, 10)
		op.Counters = append(op.Counters,
			balanceDirectionCounter.WithLabelValues(s.GetName(), sourceLabel, targetLabel),
		)
		op.FinishedCounters = append(op.FinishedCounters,
			s.counter.WithLabelValues("move-peer", sourceLabel+"-out"),
			s.counter.WithLabelValues("move-peer", targetLabel+"-in"),
		)
		op.AdditionalInfos["sourceScore"] = strconv.FormatFloat(sourceScore, 'f', 2, 64)
		op.AdditionalInfos["targetScore"] = strconv.FormatFloat(targetScore, 'f', 2, 64)
		return op
	}

	schedulerCounter.WithLabelValues(s.GetName(), "no-replacement").Inc()
	return nil
}
