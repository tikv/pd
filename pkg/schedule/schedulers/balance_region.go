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
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/plan"
	"github.com/tikv/pd/pkg/schedule/types"
	"go.uber.org/zap"
)

type balanceRegionSchedulerConfig struct {
	baseDefaultSchedulerConfig

	Ranges []core.KeyRange `json:"ranges"`
	// TODO: When we prepare to use Ranges, we will need to implement the ReloadConfig function for this scheduler.
}

type balanceRegionScheduler struct {
	*BaseScheduler
	*retryQuota
	name          string
	conf          *balanceRegionSchedulerConfig
	filters       []filter.Filter
	filterCounter *filter.Counter
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *operator.Controller, conf *balanceRegionSchedulerConfig, opts ...BalanceRegionCreateOption) Scheduler {
	scheduler := &balanceRegionScheduler{
		BaseScheduler: NewBaseScheduler(opController, types.BalanceRegionScheduler, conf),
		retryQuota:    newRetryQuota(),
		name:          types.BalanceRegionScheduler.String(),
		conf:          conf,
	}
	for _, setOption := range opts {
		setOption(scheduler)
	}
	scheduler.filters = []filter.Filter{
		&filter.StoreStateFilter{ActionScope: scheduler.GetName(), MoveRegion: true, OperatorLevel: constant.Medium},
		filter.NewSpecialUseFilter(scheduler.GetName()),
	}
	scheduler.filterCounter = filter.NewCounter(scheduler.GetName())
	return scheduler
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

// WithBalanceRegionName sets the name for the scheduler.
func WithBalanceRegionName(name string) BalanceRegionCreateOption {
	return func(s *balanceRegionScheduler) {
		s.name = name
	}
}

// EncodeConfig implements the Scheduler interface.
func (s *balanceRegionScheduler) EncodeConfig() ([]byte, error) {
	return EncodeConfig(s.conf)
}

// IsScheduleAllowed implements the Scheduler interface.
func (s *balanceRegionScheduler) IsScheduleAllowed(cluster sche.SchedulerCluster) bool {
	allowed := s.OpController.OperatorCount(operator.OpRegion) < cluster.GetSchedulerConfig().GetRegionScheduleLimit()
	if !allowed {
		operator.IncOperatorLimitCounter(s.GetType(), operator.OpRegion)
	}
	return allowed
}

// Schedule implements the Scheduler interface.
func (s *balanceRegionScheduler) Schedule(cluster sche.SchedulerCluster, dryRun bool) ([]*operator.Operator, []plan.Plan) {
	basePlan := plan.NewBalanceSchedulerPlan()
	defer s.filterCounter.Flush()
	var collector *plan.Collector
	if dryRun {
		collector = plan.NewCollector(basePlan)
	}
	balanceRegionScheduleCounter.Inc()
	stores := cluster.GetStores()
	conf := cluster.GetSchedulerConfig()
	snapshotFilter := filter.NewSnapshotSendFilter(stores, constant.Medium)
	faultTargets := filter.SelectUnavailableTargetStores(stores, s.filters, conf, collector, s.filterCounter)
	sourceStores := filter.SelectSourceStores(stores, s.filters, conf, collector, s.filterCounter)
	opInfluence := s.OpController.GetOpInfluence(cluster.GetBasicCluster())
	s.OpController.GetFastOpInfluence(cluster.GetBasicCluster(), opInfluence)
	kind := constant.NewScheduleKind(constant.RegionKind, constant.BySize)
	solver := newSolver(basePlan, kind, cluster, opInfluence)

	sort.Slice(sourceStores, func(i, j int) bool {
		iOp := solver.getOpInfluence(sourceStores[i].GetID())
		jOp := solver.getOpInfluence(sourceStores[j].GetID())
		return sourceStores[i].RegionScore(conf.GetRegionScoreFormulaVersion(), conf.GetHighSpaceRatio(), conf.GetLowSpaceRatio(), iOp) >
			sourceStores[j].RegionScore(conf.GetRegionScoreFormulaVersion(), conf.GetHighSpaceRatio(), conf.GetLowSpaceRatio(), jOp)
	})

	pendingFilter := filter.NewRegionPendingFilter()
	downFilter := filter.NewRegionDownFilter()
	replicaFilter := filter.NewRegionReplicatedFilter(cluster)
	baseRegionFilters := []filter.RegionFilter{downFilter, replicaFilter, snapshotFilter}
	switch cluster.(type) {
	case *rangeCluster:
		// allow empty region to be scheduled in range cluster
	default:
		baseRegionFilters = append(baseRegionFilters, filter.NewRegionEmptyFilter(cluster))
	}

	if collector != nil && len(sourceStores) > 0 {
		collector.Collect(plan.SetResource(sourceStores[0]), plan.SetStatus(plan.NewStatus(plan.StatusStoreScoreDisallowed)))
	}

	solver.Step++
	var sourceIndex int

	// sourcesStore is sorted by region score desc, so we pick the first store as source store.
	for sourceIndex, solver.Source = range sourceStores {
		retryLimit := s.retryQuota.getLimit(solver.Source)
		solver.sourceScore = solver.sourceStoreScore(s.GetName())
		if sourceIndex == len(sourceStores)-1 {
			break
		}
		for range retryLimit {
			// Priority pick the region that has a pending peer.
			// Pending region may mean the disk is overload, remove the pending region firstly.
			solver.Region = filter.SelectOneRegion(cluster.RandPendingRegions(solver.sourceStoreID(), s.conf.Ranges), collector,
				append(baseRegionFilters, filter.NewRegionWitnessFilter(solver.sourceStoreID()))...)
			if solver.Region == nil {
				// Then pick the region that has a follower in the source store.
				solver.Region = filter.SelectOneRegion(cluster.RandFollowerRegions(solver.sourceStoreID(), s.conf.Ranges), collector,
					append(baseRegionFilters, filter.NewRegionWitnessFilter(solver.sourceStoreID()), pendingFilter)...)
			}
			if solver.Region == nil {
				// Then pick the region has the leader in the source store.
				solver.Region = filter.SelectOneRegion(cluster.RandLeaderRegions(solver.sourceStoreID(), s.conf.Ranges), collector,
					append(baseRegionFilters, filter.NewRegionWitnessFilter(solver.sourceStoreID()), pendingFilter)...)
			}
			if solver.Region == nil {
				// Finally, pick learner.
				solver.Region = filter.SelectOneRegion(cluster.RandLearnerRegions(solver.sourceStoreID(), s.conf.Ranges), collector,
					append(baseRegionFilters, filter.NewRegionWitnessFilter(solver.sourceStoreID()), pendingFilter)...)
			}
			if solver.Region == nil {
				balanceRegionNoRegionCounter.Inc()
				continue
			}
			log.Debug("select region", zap.String("scheduler", s.GetName()), zap.Uint64("region-id", solver.Region.GetID()))
			// Skip hot regions.
			if cluster.IsRegionHot(solver.Region) {
				log.Debug("region is hot", zap.String("scheduler", s.GetName()), zap.Uint64("region-id", solver.Region.GetID()))
				if collector != nil {
					collector.Collect(plan.SetResource(solver.Region), plan.SetStatus(plan.NewStatus(plan.StatusRegionHot)))
				}
				balanceRegionHotCounter.Inc()
				continue
			}
			// Check region leader
			if solver.Region.GetLeader() == nil {
				log.Warn("region have no leader", zap.String("scheduler", s.GetName()), zap.Uint64("region-id", solver.Region.GetID()))
				if collector != nil {
					collector.Collect(plan.SetResource(solver.Region), plan.SetStatus(plan.NewStatus(plan.StatusRegionNoLeader)))
				}
				balanceRegionNoLeaderCounter.Inc()
				continue
			}
			solver.Step++
			// the replica filter will cache the last region fit and the select one will only pict the first one region that
			// satisfy all the filters, so the region fit must belong the scheduled region.
			solver.fit = replicaFilter.(*filter.RegionReplicatedFilter).GetFit()
			if op := s.transferPeer(solver, collector, sourceStores[sourceIndex+1:], faultTargets); op != nil {
				s.retryQuota.resetLimit(solver.Source)
				op.Counters = append(op.Counters, balanceRegionNewOpCounter)
				return []*operator.Operator{op}, collector.GetPlans()
			}
			solver.Step--
		}
		s.retryQuota.attenuate(solver.Source)
	}
	s.retryQuota.gc(stores)
	return nil, collector.GetPlans()
}

// transferPeer selects the best store to create a new peer to replace the old peer.
func (s *balanceRegionScheduler) transferPeer(solver *solver, collector *plan.Collector, dstStores []*core.StoreInfo, faultStores []*core.StoreInfo) *operator.Operator {
	excludeTargets := solver.Region.GetStoreIDs()
	for _, store := range faultStores {
		excludeTargets[store.GetID()] = struct{}{}
	}
	// the order of the filters should be sorted by the cost of the cpu overhead.
	// the more expensive the filter is, the later it should be placed.
	conf := solver.GetSchedulerConfig()
	filters := []filter.Filter{
		filter.NewExcludedFilter(s.GetName(), nil, excludeTargets),
		filter.NewPlacementSafeguard(s.GetName(), conf, solver.GetBasicCluster(), solver.GetRuleManager(),
			solver.Region, solver.Source, solver.fit),
	}
	candidates := filter.NewCandidates(s.R, dstStores).FilterTarget(conf, collector, s.filterCounter, filters...)
	if len(candidates.Stores) != 0 {
		solver.Step++
	}

	// candidates are sorted by region score desc, so we pick the last store as target store.
	for i := range candidates.Stores {
		solver.Target = candidates.Stores[len(candidates.Stores)-i-1]
		solver.targetScore = solver.targetStoreScore(s.GetName())
		regionID := solver.Region.GetID()
		sourceID := solver.Source.GetID()
		targetID := solver.Target.GetID()
		log.Debug("candidate store", zap.Uint64("region-id", regionID), zap.Uint64("source-store", sourceID), zap.Uint64("target-store", targetID))

		if !solver.shouldBalance(s.GetName()) {
			balanceRegionSkipCounter.Inc()
			if collector != nil {
				collector.Collect(plan.SetStatus(plan.NewStatus(plan.StatusStoreScoreDisallowed)))
			}
			continue
		}

		oldPeer := solver.Region.GetStorePeer(sourceID)
		newPeer := &metapb.Peer{StoreId: solver.Target.GetID(), Role: oldPeer.Role}
		solver.Step++
		op, err := operator.CreateMovePeerOperator(s.GetName(), solver, solver.Region, operator.OpRegion, oldPeer.GetStoreId(), newPeer)
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
		solver.Step--
		sourceLabel := strconv.FormatUint(sourceID, 10)
		targetLabel := strconv.FormatUint(targetID, 10)
		op.FinishedCounters = append(op.FinishedCounters,
			balanceDirectionCounter.WithLabelValues(s.GetName(), sourceLabel, targetLabel),
		)
		op.SetAdditionalInfo("sourceScore", strconv.FormatFloat(solver.sourceScore, 'f', 2, 64))
		op.SetAdditionalInfo("targetScore", strconv.FormatFloat(solver.targetScore, 'f', 2, 64))
		return op
	}

	balanceRegionNoReplacementCounter.Inc()
	if len(candidates.Stores) != 0 {
		solver.Step--
	}
	return nil
}
