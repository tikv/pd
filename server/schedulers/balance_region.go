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
	"github.com/tikv/pd/server/schedule/diagnosis"
	"github.com/tikv/pd/server/schedule/filter"
	"github.com/tikv/pd/server/schedule/operator"
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
	*diagnosis.DiagnosisController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, conf *balanceRegionSchedulerConfig, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := NewBaseScheduler(opController)
	scheduler := &balanceRegionScheduler{
		BaseScheduler:       base,
		retryQuota:          newRetryQuota(balanceRegionRetryLimit, defaultMinRetryLimit, defaultRetryQuotaAttenuation),
		conf:                conf,
		opController:        opController,
		counter:             balanceRegionCounter,
		DiagnosisController: diagnosis.NewDiagnosisController(opController.Ctx()),
	}
	for _, setOption := range opts {
		setOption(scheduler)
	}
	scheduler.filters = []filter.Filter{
		&filter.LongTermStateFilter{ActionScope: scheduler.GetName(), MoveRegion: true},
		&filter.TemporaryStateFilter{ActionScope: scheduler.GetName(), MoveRegion: true},
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

func (s *balanceRegionScheduler) Schedule(cluster schedule.Cluster) []*operator.Operator {
	s.DiagnosisController.InitSchedule()
	// **step = 0
	defer s.DiagnosisController.CleanUpSchedule(true)
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	opts := cluster.GetOpts()

	// source init
	stores := cluster.GetStores()

	// source filter
	stores = filter.SelectSourceStoresWithDiagnosis(stores, s.filters, opts, s.DiagnosisController)

	opInfluence := s.opController.GetOpInfluence(cluster)
	s.OpController.GetFastOpInfluence(cluster, opInfluence)
	kind := core.NewScheduleKind(core.RegionKind, core.BySize)
	plan := newBalancePlan(kind, cluster, opInfluence)
	sort.Slice(stores, func(i, j int) bool {
		iOp := plan.GetOpInfluence(stores[i].GetID())
		jOp := plan.GetOpInfluence(stores[j].GetID())
		return stores[i].RegionScore(opts.GetRegionScoreFormulaVersion(), opts.GetHighSpaceRatio(), opts.GetLowSpaceRatio(), iOp) >
			stores[j].RegionScore(opts.GetRegionScoreFormulaVersion(), opts.GetHighSpaceRatio(), opts.GetLowSpaceRatio(), jOp)
	})

	pendingFilter := filter.NewRegionPengdingFilter(s.GetName())
	downFilter := filter.NewRegionDownFilter(s.GetName())
	replicaFilter := filter.NewRegionReplicatedFilter(s.GetName(), cluster)
	hotFilter := filter.NewRegionHotFilter(s.GetName(), cluster)
	leaderFilter := filter.NewRegionNoLeaderFilter(s.GetName())
	var allowBalanceEmptyRegion filter.RegionFilter
	switch cluster.(type) {
	case *schedule.RangeCluster:
		// allow empty region to be scheduled in range cluster
		allowBalanceEmptyRegion = filter.NewRegionAlwaysAllowFilter()
	default:
		allowBalanceEmptyRegion = filter.NewRegionEmptyFilter(s.GetName(), cluster)
	}

	s.DiagnosisController.NextStep()
	// ** step = 1
	for _, plan.source = range stores {
		s.DiagnosisController.SetObject(plan.SourceStoreID())
		retryLimit := s.retryQuota.GetLimit(plan.source)
		for i := 0; i < retryLimit; i++ {
			schedulerCounter.WithLabelValues(s.GetName(), "total").Inc()
			// Priority pick the region that has a pending peer.
			// Pending region may means the disk is overload, remove the pending region firstly.
			plan.region = filter.SelectOneRegionWithDiagnosis(cluster.RandPendingRegions(plan.SourceStoreID(), s.conf.Ranges), s.DiagnosisController,
				downFilter, replicaFilter, allowBalanceEmptyRegion, hotFilter, leaderFilter)
			if plan.region == nil {
				// Then pick the region that has a follower in the source store.
				plan.region = filter.SelectOneRegionWithDiagnosis(cluster.RandFollowerRegions(plan.SourceStoreID(), s.conf.Ranges), s.DiagnosisController,
					pendingFilter, downFilter, replicaFilter, allowBalanceEmptyRegion, hotFilter, leaderFilter)
			}
			if plan.region == nil {
				// Then pick the region has the leader in the source store.
				plan.region = filter.SelectOneRegionWithDiagnosis(cluster.RandLeaderRegions(plan.SourceStoreID(), s.conf.Ranges), s.DiagnosisController,
					pendingFilter, downFilter, replicaFilter, allowBalanceEmptyRegion, hotFilter, leaderFilter)
			}
			if plan.region == nil {
				// Finally pick learner.
				plan.region = filter.SelectOneRegionWithDiagnosis(cluster.RandLearnerRegions(plan.SourceStoreID(), s.conf.Ranges), s.DiagnosisController,
					pendingFilter, downFilter, replicaFilter, allowBalanceEmptyRegion, hotFilter, leaderFilter)
			}
			if plan.region == nil {
				schedulerCounter.WithLabelValues(s.GetName(), "no-region").Inc()
				continue
			}
			log.Debug("select region", zap.String("scheduler", s.GetName()), zap.Uint64("region-id", plan.region.GetID()))

			s.DiagnosisController.NextStep()
			// ** step = 2
			s.DiagnosisController.SetObject(plan.region.GetID())
			if op := s.transferPeer(plan); op != nil {
				s.retryQuota.ResetLimit(plan.source)
				op.Counters = append(op.Counters, schedulerCounter.WithLabelValues(s.GetName(), "new-operator"))
				return []*operator.Operator{op}
			}
			// ** step = 1
			s.DiagnosisController.LastStep()
		}
		s.retryQuota.Attenuate(plan.source)
		s.DiagnosisController.CleanUpSchedule(false)
	}
	s.retryQuota.GC(stores)
	return nil
}

// transferPeer selects the best store to create a new peer to replace the old peer.
func (s *balanceRegionScheduler) transferPeer(plan *balancePlan) *operator.Operator {
	filters := []filter.Filter{
		filter.NewExcludedFilter(s.GetName(), nil, plan.region.GetStoreIds()),
		filter.NewPlacementSafeguard(s.GetName(), plan.GetOpts(), plan.GetBasicCluster(), plan.GetRuleManager(), plan.region, plan.source),
		filter.NewRegionScoreFilter(s.GetName(), plan.source, plan.GetOpts()),
		filter.NewSpecialUseFilter(s.GetName()),
		&filter.LongTermStateFilter{ActionScope: s.GetName(), MoveRegion: true},
		&filter.TemporaryStateFilter{ActionScope: s.GetName(), MoveRegion: true},
	}

	candidates := filter.NewCandidates(plan.GetStores()).
		FilterTargetWithDiagnosis(plan.GetOpts(), s.DiagnosisController, filters...).
		Sort(filter.RegionScoreComparer(plan.GetOpts()))

	s.DiagnosisController.NextStep()
	// **step = 3
	for _, plan.target = range candidates.Stores {
		regionID := plan.region.GetID()
		sourceID := plan.source.GetID()
		targetID := plan.target.GetID()
		s.DiagnosisController.SetObject(targetID)
		log.Debug("", zap.Uint64("region-id", regionID), zap.Uint64("source-store", sourceID), zap.Uint64("target-store", targetID))

		if !plan.shouldBalance(s.GetName()) {
			s.DiagnosisController.Diagnose(targetID, "should-not-balance")
			schedulerCounter.WithLabelValues(s.GetName(), "skip").Inc()
			continue
		}

		oldPeer := plan.region.GetStorePeer(sourceID)
		newPeer := &metapb.Peer{StoreId: plan.target.GetID(), Role: oldPeer.Role}
		op, err := operator.CreateMovePeerOperator(BalanceRegionType, plan, plan.region, operator.OpRegion, oldPeer.GetStoreId(), newPeer)
		if err != nil {
			schedulerCounter.WithLabelValues(s.GetName(), "create-operator-fail").Inc()
			return nil
		}
		sourceLabel := strconv.FormatUint(sourceID, 10)
		targetLabel := strconv.FormatUint(targetID, 10)
		op.FinishedCounters = append(op.FinishedCounters,
			balanceDirectionCounter.WithLabelValues(s.GetName(), sourceLabel, targetLabel),
			s.counter.WithLabelValues("move-peer", sourceLabel+"-out"),
			s.counter.WithLabelValues("move-peer", targetLabel+"-in"),
		)
		op.AdditionalInfos["sourceScore"] = strconv.FormatFloat(plan.sourceScore, 'f', 2, 64)
		op.AdditionalInfos["targetScore"] = strconv.FormatFloat(plan.targetScore, 'f', 2, 64)
		return op
	}

	schedulerCounter.WithLabelValues(s.GetName(), "no-replacement").Inc()
	s.DiagnosisController.LastStep()
	// **step=2
	return nil
}
