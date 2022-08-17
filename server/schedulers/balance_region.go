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
	"github.com/tikv/pd/server/schedule/plan"
	"github.com/tikv/pd/server/storage/endpoint"
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
	schedule.RegisterScheduler(BalanceRegionType, func(opController *schedule.OperatorController, storage endpoint.ConfigStorage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
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
	*retryQuota
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
		retryQuota:    newRetryQuota(balanceRegionRetryLimit, defaultMinRetryLimit, defaultRetryQuotaAttenuation),
		conf:          conf,
		opController:  opController,
		counter:       balanceRegionCounter,
	}
	for _, setOption := range opts {
		setOption(scheduler)
	}
	scheduler.filters = []filter.Filter{
		&filter.StoreStateFilter{ActionScope: scheduler.GetName(), MoveRegion: true},
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

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster schedule.Cluster) bool {
	allowed := s.opController.OperatorCount(operator.OpRegion) < cluster.GetOpts().GetRegionScheduleLimit()
	if !allowed {
		operator.OperatorLimitCounter.WithLabelValues(s.GetType(), operator.OpRegion.String()).Inc()
	}
	return allowed
}

func (s *balanceRegionScheduler) Schedule(cluster schedule.Cluster, dryRun bool) ([]*operator.Operator, []plan.Plan) {
	basePlan := NewBalanceSchedulerPlan()
	var collector *plan.Collector
	var planFunc func() []plan.Plan
	if dryRun {
		collector = plan.NewCollector(basePlan)
		planFunc = collector.GetPlans
	} else {
		planFunc = func() []plan.Plan {
			return nil
		}
	}
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	stores := cluster.GetStores()
	opts := cluster.GetOpts()
	stores = filter.SelectSourceStores(stores, s.filters, opts, collector)
	opInfluence := s.opController.GetOpInfluence(cluster)
	s.OpController.GetFastOpInfluence(cluster, opInfluence)
	kind := core.NewScheduleKind(core.RegionKind, core.BySize)
	solver := newSolver(basePlan, kind, cluster, opInfluence)

	sort.Slice(stores, func(i, j int) bool {
		iOp := solver.GetOpInfluence(stores[i].GetID())
		jOp := solver.GetOpInfluence(stores[j].GetID())
		return stores[i].RegionScore(opts.GetRegionScoreFormulaVersion(), opts.GetHighSpaceRatio(), opts.GetLowSpaceRatio(), iOp) >
			stores[j].RegionScore(opts.GetRegionScoreFormulaVersion(), opts.GetHighSpaceRatio(), opts.GetLowSpaceRatio(), jOp)
	})

	pendingFilter := filter.NewRegionPendingFilter()
	downFilter := filter.NewRegionDownFilter()
	replicaFilter := filter.NewRegionReplicatedFilter(cluster)
	baseRegionFilters := []filter.RegionFilter{downFilter, replicaFilter}
	switch cluster.(type) {
	case *schedule.RangeCluster:
		// allow empty region to be scheduled in range cluster
	default:
		baseRegionFilters = append(baseRegionFilters, filter.NewRegionEmptyFilter(cluster))
	}

	basePlan.step++
	for _, solver.source = range stores {
		retryLimit := s.retryQuota.GetLimit(solver.source)
		for i := 0; i < retryLimit; i++ {
			schedulerCounter.WithLabelValues(s.GetName(), "total").Inc()
			// Priority pick the region that has a pending peer.
			// Pending region may means the disk is overload, remove the pending region firstly.
			solver.region = filter.SelectOneRegion(cluster.RandPendingRegions(solver.SourceStoreID(), s.conf.Ranges), collector,
				baseRegionFilters...)
			if solver.region == nil {
				// Then pick the region that has a follower in the source store.
				solver.region = filter.SelectOneRegion(cluster.RandFollowerRegions(solver.SourceStoreID(), s.conf.Ranges), collector,
					append(baseRegionFilters, pendingFilter)...)
			}
			if solver.region == nil {
				// Then pick the region has the leader in the source store.
				solver.region = filter.SelectOneRegion(cluster.RandLeaderRegions(solver.SourceStoreID(), s.conf.Ranges), collector,
					append(baseRegionFilters, pendingFilter)...)
			}
			if solver.region == nil {
				// Finally pick learner.
				solver.region = filter.SelectOneRegion(cluster.RandLearnerRegions(solver.SourceStoreID(), s.conf.Ranges), collector,
					append(baseRegionFilters, pendingFilter)...)
			}
			if solver.region == nil {
				schedulerCounter.WithLabelValues(s.GetName(), "no-region").Inc()
				continue
			}
			log.Debug("select region", zap.String("scheduler", s.GetName()), zap.Uint64("region-id", solver.region.GetID()))
			// Skip hot regions.
			if cluster.IsRegionHot(solver.region) {
				log.Debug("region is hot", zap.String("scheduler", s.GetName()), zap.Uint64("region-id", solver.region.GetID()))
				schedulerCounter.WithLabelValues(s.GetName(), "region-hot").Inc()
				continue
			}
			// Check region whether have leader
			if solver.region.GetLeader() == nil {
				log.Warn("region have no leader", zap.String("scheduler", s.GetName()), zap.Uint64("region-id", solver.region.GetID()))
				schedulerCounter.WithLabelValues(s.GetName(), "no-leader").Inc()
				continue
			}
			basePlan.step++
			if op := s.transferPeer(solver, collector); op != nil {
				s.retryQuota.ResetLimit(solver.source)
				op.Counters = append(op.Counters, schedulerCounter.WithLabelValues(s.GetName(), "new-operator"))
				return []*operator.Operator{op}, planFunc()
			}
			basePlan.step--
		}
		s.retryQuota.Attenuate(solver.source)
	}
	s.retryQuota.GC(stores)
	return nil, planFunc()
}

// transferPeer selects the best store to create a new peer to replace the old peer.
func (s *balanceRegionScheduler) transferPeer(solver *solver, collector *plan.Collector) *operator.Operator {
	filters := []filter.Filter{
		filter.NewExcludedFilter(s.GetName(), nil, solver.region.GetStoreIDs()),
		filter.NewPlacementSafeguard(s.GetName(), solver.GetOpts(), solver.GetBasicCluster(), solver.GetRuleManager(), solver.region, solver.source),
		filter.NewRegionScoreFilter(s.GetName(), solver.source, solver.GetOpts()),
		filter.NewSpecialUseFilter(s.GetName()),
		&filter.StoreStateFilter{ActionScope: s.GetName(), MoveRegion: true},
	}

	candidates := filter.NewCandidates(solver.GetStores()).
		FilterTarget(solver.GetOpts(), collector, filters...).
		Sort(filter.RegionScoreComparer(solver.GetOpts()))

	if len(candidates.Stores) != 0 {
		solver.step++
	}
	for _, solver.target = range candidates.Stores {
		regionID := solver.region.GetID()
		sourceID := solver.source.GetID()
		targetID := solver.target.GetID()
		log.Debug("", zap.Uint64("region-id", regionID), zap.Uint64("source-store", sourceID), zap.Uint64("target-store", targetID))

		if !solver.shouldBalance(s.GetName()) {
			schedulerCounter.WithLabelValues(s.GetName(), "skip").Inc()
			if collector != nil {
				collector.Collect(plan.SetStatus(plan.NewStatus(plan.StatusStoreScoreDisallowed)))
			}
			continue
		}

		oldPeer := solver.region.GetStorePeer(sourceID)
		newPeer := &metapb.Peer{StoreId: solver.target.GetID(), Role: oldPeer.Role}
		solver.step++
		op, err := operator.CreateMovePeerOperator(BalanceRegionType, solver, solver.region, operator.OpRegion, oldPeer.GetStoreId(), newPeer)
		if err != nil {
			schedulerCounter.WithLabelValues(s.GetName(), "create-operator-fail").Inc()
			if collector != nil {
				collector.Collect(plan.SetStatus(plan.NewStatus(plan.StatusCreateOperatorFailed)))
			}
			return nil
		}
		if collector != nil {
			collector.Collect()
		}
		solver.step--
		sourceLabel := strconv.FormatUint(sourceID, 10)
		targetLabel := strconv.FormatUint(targetID, 10)
		op.FinishedCounters = append(op.FinishedCounters,
			balanceDirectionCounter.WithLabelValues(s.GetName(), sourceLabel, targetLabel),
			s.counter.WithLabelValues("move-peer", sourceLabel+"-out"),
			s.counter.WithLabelValues("move-peer", targetLabel+"-in"),
		)
		op.AdditionalInfos["sourceScore"] = strconv.FormatFloat(solver.sourceScore, 'f', 2, 64)
		op.AdditionalInfos["targetScore"] = strconv.FormatFloat(solver.targetScore, 'f', 2, 64)
		return op
	}

	schedulerCounter.WithLabelValues(s.GetName(), "no-replacement").Inc()
	if len(candidates.Stores) != 0 {
		solver.step--
	}
	return nil
}
