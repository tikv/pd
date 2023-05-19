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
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/schedule"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/plan"
	"go.uber.org/zap"
)

const (
	// BalanceRegionName is balance region scheduler name.
	BalanceRegionName = "balance-region-scheduler"
	// BalanceRegionType is balance region scheduler type.
	BalanceRegionType = "balance-region"
)

var (
	// WithLabelValues is a heavy operation, define variable to avoid call it every time.
	balanceRegionScheduleCounter      = balanceRegionCounter.WithLabelValues(BalanceRegionName, "schedule")
	balanceRegionNoRegionCounter      = balanceRegionCounter.WithLabelValues(BalanceRegionName, "no-region")
	balanceRegionHotCounter           = balanceRegionCounter.WithLabelValues(BalanceRegionName, "region-hot")
	balanceRegionNoLeaderCounter      = balanceRegionCounter.WithLabelValues(BalanceRegionName, "no-leader")
	balanceRegionNewOpCounter         = balanceRegionCounter.WithLabelValues(BalanceRegionName, "new-operator")
	balanceRegionSkipCounter          = balanceRegionCounter.WithLabelValues(BalanceRegionName, "skip")
	balanceRegionCreateOpFailCounter  = balanceRegionCounter.WithLabelValues(BalanceRegionName, "create-operator-fail")
	balanceRegionNoReplacementCounter = balanceRegionCounter.WithLabelValues(BalanceRegionName, "no-replacement")
)

type balanceRegionSchedulerConfig struct {
	Name   string          `json:"name"`
	Ranges []core.KeyRange `json:"ranges"`
}

type balanceRegionScheduler struct {
	*BaseScheduler
	*retryQuota
	conf          *balanceRegionSchedulerConfig
	opController  *operator.Controller
	filters       []filter.Filter
	counter       *prometheus.CounterVec
	filterCounter *filter.Counter
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *operator.Controller, conf *balanceRegionSchedulerConfig, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := NewBaseScheduler(opController)
	scheduler := &balanceRegionScheduler{
		BaseScheduler: base,
		retryQuota:    newRetryQuota(),
		conf:          conf,
		opController:  opController,
		counter:       balanceRegionCounter,
		filterCounter: filter.NewCounter(filter.BalanceRegion.String()),
	}
	for _, setOption := range opts {
		setOption(scheduler)
	}
	scheduler.filters = []filter.Filter{
		&filter.StoreStateFilter{ActionScope: scheduler.GetName(), MoveRegion: true, OperatorLevel: constant.Medium},
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

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster sche.ClusterInformer) bool {
	allowed := s.opController.OperatorCount(operator.OpRegion) < cluster.GetOpts().GetRegionScheduleLimit()
	if !allowed {
		operator.OperatorLimitCounter.WithLabelValues(s.GetType(), operator.OpRegion.String()).Inc()
	}
	return allowed
}

func (s *balanceRegionScheduler) Schedule(cluster sche.ClusterInformer, dryRun bool) ([]*operator.Operator, []plan.Plan) {
	basePlan := NewBalanceSchedulerPlan()
	var collector *plan.Collector
	if dryRun {
		collector = plan.NewCollector(basePlan)
	}
	balanceRegionScheduleCounter.Inc()
	stores := cluster.GetStores()
	opts := cluster.GetOpts()
	snapshotFilter := filter.NewSnapshotSendFilter(stores, constant.Medium)
	faultTargets := filter.SelectUnavailableTargetStores(stores, s.filters, opts, collector, s.filterCounter)
	sourceStores := filter.SelectSourceStores(stores, s.filters, opts, collector, s.filterCounter)
	opInfluence := s.opController.GetOpInfluence(cluster)
	s.OpController.GetFastOpInfluence(cluster, opInfluence)
	kind := constant.NewScheduleKind(constant.RegionKind, constant.BySize)
	solver := newSolver(basePlan, kind, cluster, opInfluence)

	sort.Slice(sourceStores, func(i, j int) bool {
		iOp := solver.GetOpInfluence(sourceStores[i].GetID())
		jOp := solver.GetOpInfluence(sourceStores[j].GetID())
		return sourceStores[i].RegionScore(opts.GetRegionScoreFormulaVersion(), opts.GetHighSpaceRatio(), opts.GetLowSpaceRatio(), iOp) >
			sourceStores[j].RegionScore(opts.GetRegionScoreFormulaVersion(), opts.GetHighSpaceRatio(), opts.GetLowSpaceRatio(), jOp)
	})

	pendingFilter := filter.NewRegionPendingFilter()
	downFilter := filter.NewRegionDownFilter()
	replicaFilter := filter.NewRegionReplicatedFilter(cluster)
	baseRegionFilters := []filter.RegionFilter{downFilter, replicaFilter, snapshotFilter}
	switch cluster.(type) {
	case *schedule.RangeCluster:
		// allow empty region to be scheduled in range cluster
	default:
		baseRegionFilters = append(baseRegionFilters, filter.NewRegionEmptyFilter(cluster))
	}

	if collector != nil && len(sourceStores) > 0 {
		collector.Collect(plan.SetResource(sourceStores[0]), plan.SetStatus(plan.NewStatus(plan.StatusStoreScoreDisallowed)))
	}

	solver.step++
	var sourceIndex int

	// sourcesStore is sorted by region score desc, so we pick the first store as source store.
	for sourceIndex, solver.source = range sourceStores {
		retryLimit := s.retryQuota.GetLimit(solver.source)
		solver.sourceScore = solver.sourceStoreScore(s.GetName())
		if sourceIndex == len(sourceStores)-1 {
			break
		}
		for i := 0; i < retryLimit; i++ {
			// Priority pick the region that has a pending peer.
			// Pending region may mean the disk is overload, remove the pending region firstly.
			solver.region = filter.SelectOneRegion(cluster.RandPendingRegions(solver.SourceStoreID(), s.conf.Ranges), collector,
				append(baseRegionFilters, filter.NewRegionWitnessFilter(solver.SourceStoreID()))...)
			if solver.region == nil {
				// Then pick the region that has a follower in the source store.
				solver.region = filter.SelectOneRegion(cluster.RandFollowerRegions(solver.SourceStoreID(), s.conf.Ranges), collector,
					append(baseRegionFilters, filter.NewRegionWitnessFilter(solver.SourceStoreID()), pendingFilter)...)
			}
			if solver.region == nil {
				// Then pick the region has the leader in the source store.
				solver.region = filter.SelectOneRegion(cluster.RandLeaderRegions(solver.SourceStoreID(), s.conf.Ranges), collector,
					append(baseRegionFilters, filter.NewRegionWitnessFilter(solver.SourceStoreID()), pendingFilter)...)
			}
			if solver.region == nil {
				// Finally, pick learner.
				solver.region = filter.SelectOneRegion(cluster.RandLearnerRegions(solver.SourceStoreID(), s.conf.Ranges), collector,
					append(baseRegionFilters, filter.NewRegionWitnessFilter(solver.SourceStoreID()), pendingFilter)...)
			}
			if solver.region == nil {
				balanceRegionNoRegionCounter.Inc()
				continue
			}
			log.Debug("select region", zap.String("scheduler", s.GetName()), zap.Uint64("region-id", solver.region.GetID()))
			// Skip hot regions.
			if cluster.IsRegionHot(solver.region) {
				log.Debug("region is hot", zap.String("scheduler", s.GetName()), zap.Uint64("region-id", solver.region.GetID()))
				if collector != nil {
					collector.Collect(plan.SetResource(solver.region), plan.SetStatus(plan.NewStatus(plan.StatusRegionHot)))
				}
				balanceRegionHotCounter.Inc()
				continue
			}
			// Check region leader
			if solver.region.GetLeader() == nil {
				log.Warn("region have no leader", zap.String("scheduler", s.GetName()), zap.Uint64("region-id", solver.region.GetID()))
				if collector != nil {
					collector.Collect(plan.SetResource(solver.region), plan.SetStatus(plan.NewStatus(plan.StatusRegionNoLeader)))
				}
				balanceRegionNoLeaderCounter.Inc()
				continue
			}
			solver.step++
			// the replica filter will cache the last region fit and the select one will only pict the first one region that
			// satisfy all the filters, so the region fit must belong the scheduled region.
			solver.fit = replicaFilter.(*filter.RegionReplicatedFilter).GetFit()
			if op := s.transferPeer(solver, collector, sourceStores[sourceIndex+1:], faultTargets); op != nil {
				s.retryQuota.ResetLimit(solver.source)
				op.Counters = append(op.Counters, balanceRegionNewOpCounter)
				return []*operator.Operator{op}, collector.GetPlans()
			}
			solver.step--
		}
		s.retryQuota.Attenuate(solver.source)
	}
	s.filterCounter.Flush()
	s.retryQuota.GC(stores)
	return nil, collector.GetPlans()
}

// transferPeer selects the best store to create a new peer to replace the old peer.
func (s *balanceRegionScheduler) transferPeer(solver *solver, collector *plan.Collector, dstStores []*core.StoreInfo, faultStores []*core.StoreInfo) *operator.Operator {
	excludeTargets := solver.region.GetStoreIDs()
	for _, store := range faultStores {
		excludeTargets[store.GetID()] = struct{}{}
	}
	// the order of the filters should be sorted by the cost of the cpu overhead.
	// the more expensive the filter is, the later it should be placed.
	filters := []filter.Filter{
		filter.NewExcludedFilter(s.GetName(), nil, excludeTargets),
		filter.NewPlacementSafeguard(s.GetName(), solver.GetOpts(), solver.GetBasicCluster(), solver.GetRuleManager(),
			solver.region, solver.source, solver.fit),
	}
	candidates := filter.NewCandidates(dstStores).FilterTarget(solver.GetOpts(), collector, s.filterCounter, filters...)
	if len(candidates.Stores) != 0 {
		solver.step++
	}

	// candidates are sorted by region score desc, so we pick the last store as target store.
	for i := range candidates.Stores {
		solver.target = candidates.Stores[len(candidates.Stores)-i-1]
		solver.targetScore = solver.targetStoreScore(s.GetName())
		regionID := solver.region.GetID()
		sourceID := solver.source.GetID()
		targetID := solver.target.GetID()
		log.Debug("candidate store", zap.Uint64("region-id", regionID), zap.Uint64("source-store", sourceID), zap.Uint64("target-store", targetID))

		if !solver.shouldBalance(s.GetName()) {
			balanceRegionSkipCounter.Inc()
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
			balanceRegionCreateOpFailCounter.Inc()
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
			// TODO: pre-allocate gauge metrics
			s.counter.WithLabelValues("move-peer", sourceLabel+"-out"),
			s.counter.WithLabelValues("move-peer", targetLabel+"-in"),
		)
		op.AdditionalInfos["sourceScore"] = strconv.FormatFloat(solver.sourceScore, 'f', 2, 64)
		op.AdditionalInfos["targetScore"] = strconv.FormatFloat(solver.targetScore, 'f', 2, 64)
		return op
	}

	balanceRegionNoReplacementCounter.Inc()
	if len(candidates.Stores) != 0 {
		solver.step--
	}
	return nil
}
