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

	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/filter"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/storage/endpoint"
	"go.uber.org/zap"
)

const (
	// BalanceLeaderName is balance leader scheduler name.
	BalanceLeaderName = "balance-leader-scheduler"
	// BalanceLeaderType is balance leader scheduler type.
	BalanceLeaderType = "balance-leader"
	// balanceLeaderRetryLimit is the limit to retry schedule for selected source store and target store.
	balanceLeaderRetryLimit = 10
)

func init() {
	schedule.RegisterSliceDecoderBuilder(BalanceLeaderType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			conf, ok := v.(*balanceLeaderSchedulerConfig)
			if !ok {
				return errs.ErrScheduleConfigNotExist.FastGenByArgs()
			}
			ranges, err := getKeyRanges(args)
			if err != nil {
				return err
			}
			conf.Ranges = ranges
			conf.Name = BalanceLeaderName
			conf.Batch = 5
			return nil
		}
	})

	schedule.RegisterScheduler(BalanceLeaderType, func(opController *schedule.OperatorController, storage endpoint.ConfigStorage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &balanceLeaderSchedulerConfig{}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newBalanceLeaderScheduler(opController, conf), nil
	})
}

type balanceLeaderSchedulerConfig struct {
	Name   string          `json:"name"`
	Ranges []core.KeyRange `json:"ranges"`
	Batch  int             `json:"batch"`
}

type balanceLeaderScheduler struct {
	*BaseScheduler
	*retryQuota
	conf         *balanceLeaderSchedulerConfig
	opController *schedule.OperatorController
	filters      []filter.Filter
	counter      *prometheus.CounterVec
}

// newBalanceLeaderScheduler creates a scheduler that tends to keep leaders on
// each store balanced.
func newBalanceLeaderScheduler(opController *schedule.OperatorController, conf *balanceLeaderSchedulerConfig, options ...BalanceLeaderCreateOption) schedule.Scheduler {
	base := NewBaseScheduler(opController)

	s := &balanceLeaderScheduler{
		BaseScheduler: base,
		retryQuota:    newRetryQuota(balanceLeaderRetryLimit, defaultMinRetryLimit, defaultRetryQuotaAttenuation),
		conf:          conf,
		opController:  opController,
		counter:       balanceLeaderCounter,
	}
	for _, option := range options {
		option(s)
	}
	s.filters = []filter.Filter{
		&filter.StoreStateFilter{ActionScope: s.GetName(), TransferLeader: true},
		filter.NewSpecialUseFilter(s.GetName()),
	}
	return s
}

// BalanceLeaderCreateOption is used to create a scheduler with an option.
type BalanceLeaderCreateOption func(s *balanceLeaderScheduler)

// WithBalanceLeaderCounter sets the counter for the scheduler.
func WithBalanceLeaderCounter(counter *prometheus.CounterVec) BalanceLeaderCreateOption {
	return func(s *balanceLeaderScheduler) {
		s.counter = counter
	}
}

// WithBalanceLeaderName sets the name for the scheduler.
func WithBalanceLeaderName(name string) BalanceLeaderCreateOption {
	return func(s *balanceLeaderScheduler) {
		s.conf.Name = name
	}
}

func (l *balanceLeaderScheduler) GetName() string {
	return l.conf.Name
}

func (l *balanceLeaderScheduler) GetType() string {
	return BalanceLeaderType
}

func (l *balanceLeaderScheduler) EncodeConfig() ([]byte, error) {
	return schedule.EncodeConfig(l.conf)
}

func (l *balanceLeaderScheduler) IsScheduleAllowed(cluster schedule.Cluster) bool {
	allowed := l.opController.OperatorCount(operator.OpLeader) < cluster.GetOpts().GetLeaderScheduleLimit()
	if !allowed {
		operator.OperatorLimitCounter.WithLabelValues(l.GetType(), operator.OpLeader.String()).Inc()
	}
	return allowed
}

type batchController struct {
	cluster     schedule.Cluster
	sources     []*core.StoreInfo
	targets     []*core.StoreInfo
	sourceIndex int
	targetIndex int
	plan        *balancePlan
	usedRegions map[uint64]struct{}
}

func (bc *batchController) initSort() {
	sort.Slice(bc.sources, func(i, j int) bool {
		iOp := bc.plan.GetOpInfluence(bc.sources[i].GetID())
		jOp := bc.plan.GetOpInfluence(bc.sources[j].GetID())
		return bc.sources[i].LeaderScore(bc.plan.kind.Policy, iOp) >
			bc.sources[j].LeaderScore(bc.plan.kind.Policy, jOp)
	})
	sort.Slice(bc.targets, func(i, j int) bool {
		iOp := bc.plan.GetOpInfluence(bc.targets[i].GetID())
		jOp := bc.plan.GetOpInfluence(bc.targets[j].GetID())
		return bc.targets[i].LeaderScore(bc.plan.kind.Policy, iOp) <
			bc.targets[j].LeaderScore(bc.plan.kind.Policy, jOp)
	})
}

// hasOptionalStore returns whether there is remaining and optional stores
func (bc *batchController) hasOptionalStore() bool {
	return bc.sourceIndex < len(bc.sources) || bc.targetIndex < len(bc.targets)
}

// hasOptionalSourceStore returns whether there is remaining and optional source stores
func (bc *batchController) hasOptionalSourceStore() bool {
	return bc.sourceIndex < len(bc.sources)
}

// hasOptionalTargetStore returns whether there is remaining and optional target stores
func (bc *batchController) hasOptionalTargetStore() bool {
	return bc.targetIndex < len(bc.targets)
}

// getCurrentSourceStore returns processing source store
func (bc *batchController) getCurrentSourceStore() *core.StoreInfo {
	return bc.sources[bc.sourceIndex]
}

// getCurrentTargetStore returns processing target store
func (bc *batchController) getCurrentTargetStore() *core.StoreInfo {
	return bc.targets[bc.targetIndex]
}

// isRepeatedOperator is used to check whether the new operator is repeated
func (bc *batchController) isRepeatedOperator(op *operator.Operator) bool {
	// Currently only check whether region is used
	_, ok := bc.usedRegions[op.RegionID()]
	return ok
}

// nextSourceStore is used to change processing source store
func (bc *batchController) nextSourceStore() {
	bc.sourceIndex++
}

// nextTargeStore is used to change processing target store
func (bc *batchController) nextTargetStore() {
	bc.targetIndex++
}

// getProcessedSourceStores returns process source stores slice
func (bc *batchController) getProcessedSourceStores() []*core.StoreInfo {
	return bc.sources[:bc.sourceIndex]
}

// getProcessedTargetStores returns processed target stores slice
func (bc *batchController) getProcessedTargetStores() []*core.StoreInfo {
	return bc.targets[:bc.targetIndex]
}

// makeInfluence is used to make influence using new operator
func (bc *batchController) makeInfluence(op *operator.Operator) {
	bc.usedRegions[op.RegionID()] = struct{}{}
	schedule.AddOpInfluence(op, bc.plan.opInfluence, bc.cluster)
	resortStores(bc.sources, bc.sourceIndex, func(i, j int) bool {
		iOp := bc.plan.GetOpInfluence(bc.sources[i].GetID())
		jOp := bc.plan.GetOpInfluence(bc.sources[j].GetID())
		return bc.sources[i].LeaderScore(bc.plan.kind.Policy, iOp) <=
			bc.sources[j].LeaderScore(bc.plan.kind.Policy, jOp)
	})
	resortStores(bc.targets, bc.targetIndex, func(i, j int) bool {
		iOp := bc.plan.GetOpInfluence(bc.targets[i].GetID())
		jOp := bc.plan.GetOpInfluence(bc.targets[j].GetID())
		return bc.targets[i].LeaderScore(bc.plan.kind.Policy, iOp) <=
			bc.targets[j].LeaderScore(bc.plan.kind.Policy, jOp)
	})
}

func (l *balanceLeaderScheduler) Schedule(cluster schedule.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(l.GetName(), "schedule").Inc()

	leaderSchedulePolicy := cluster.GetOpts().GetLeaderSchedulePolicy()
	opInfluence := l.opController.GetOpInfluence(cluster)
	kind := core.NewScheduleKind(core.LeaderKind, leaderSchedulePolicy)
	plan := newBalancePlan(kind, cluster, opInfluence)

	stores := cluster.GetStores()
	bc := &batchController{
		cluster:     cluster,
		sources:     filter.SelectSourceStores(stores, l.filters, cluster.GetOpts()),
		targets:     filter.SelectTargetStores(stores, l.filters, cluster.GetOpts()),
		plan:        plan,
		usedRegions: make(map[uint64]struct{}),
	}
	bc.initSort()
	result := make([]*operator.Operator, 0, l.conf.Batch)
	// sourceIndex and targetIndex represent which store is currently processed
	for bc.hasOptionalStore() {
		if bc.hasOptionalSourceStore() {
			used := false
			plan.source, plan.target = bc.getCurrentSourceStore(), nil
			retryLimit := l.retryQuota.GetLimit(plan.source)
			log.Debug("store leader score", zap.String("scheduler", l.GetName()), zap.Uint64("source-store", plan.SourceStoreID()))
			l.counter.WithLabelValues("high-score", plan.SourceMetricLabel()).Inc()
			for j := 0; j < retryLimit; j++ {
				schedulerCounter.WithLabelValues(l.GetName(), "total").Inc()
				if op := l.transferLeaderOut(plan); op != nil {
					if bc.isRepeatedOperator(op) {
						continue
					}
					l.retryQuota.ResetLimit(plan.source)
					op.Counters = append(op.Counters, l.counter.WithLabelValues("transfer-out", plan.SourceMetricLabel()))
					result = append(result, op)
					if len(result) >= l.conf.Batch {
						return result
					}
					// The follow is used to make influence
					used = true
					bc.makeInfluence(op)
					break
				}
			}
			if !used {
				// if current index store can't create operator, try to process on next store
				bc.nextSourceStore()
				l.Attenuate(plan.source)
				log.Debug("no operator created for selected stores", zap.String("scheduler", l.GetName()), zap.Uint64("source", plan.SourceStoreID()))
			}
		}
		if bc.hasOptionalTargetStore() {
			used := false
			plan.source, plan.target = nil, bc.getCurrentTargetStore()
			retryLimit := l.retryQuota.GetLimit(plan.target)
			log.Debug("store leader score", zap.String("scheduler", l.GetName()), zap.Uint64("target-store", plan.TargetStoreID()))
			l.counter.WithLabelValues("low-score", plan.TargetMetricLabel()).Inc()
			for j := 0; j < retryLimit; j++ {
				schedulerCounter.WithLabelValues(l.GetName(), "total").Inc()
				if op := l.transferLeaderIn(plan); op != nil {
					if bc.isRepeatedOperator(op) {
						continue
					}
					l.retryQuota.ResetLimit(plan.target)
					op.Counters = append(op.Counters, l.counter.WithLabelValues("transfer-in", plan.TargetMetricLabel()))
					result = append(result, op)
					if len(result) >= l.conf.Batch {
						return result
					}
					// The follow is used to make influence
					used = true
					bc.makeInfluence(op)
					break
				}
			}
			if !used {
				// if current index store can't create operator, try to process on next store
				bc.nextTargetStore()
				l.Attenuate(plan.target)
				log.Debug("no operator created for selected stores", zap.String("scheduler", l.GetName()), zap.Uint64("target", plan.TargetStoreID()))
			}
		}
	}
	l.retryQuota.GC(append(bc.getProcessedSourceStores(), bc.getProcessedTargetStores()...))
	return result
}

// resortStores is used to sort stores again after creating an operator.
// It will repeatedly swap the specific store and next store if they are in wrong order.
// In general, it has very few swaps. In the worst case, the time complexity is O(n).
func resortStores(stores []*core.StoreInfo, pos int, less func(i, j int) bool) {
	swapper := func(i, j int) { stores[i], stores[j] = stores[j], stores[i] }
	for ; pos+1 < len(stores) && less(pos, pos+1); pos++ {
		swapper(pos, pos+1)
	}
}

// transferLeaderOut transfers leader from the source store.
// It randomly selects a health region from the source store, then picks
// the best follower peer and transfers the leader.
func (l *balanceLeaderScheduler) transferLeaderOut(plan *balancePlan) *operator.Operator {
	plan.region = plan.RandLeaderRegion(plan.SourceStoreID(), l.conf.Ranges, schedule.IsRegionHealthy)
	if plan.region == nil {
		log.Debug("store has no leader", zap.String("scheduler", l.GetName()), zap.Uint64("store-id", plan.SourceStoreID()))
		schedulerCounter.WithLabelValues(l.GetName(), "no-leader-region").Inc()
		return nil
	}
	targets := plan.GetFollowerStores(plan.region)
	finalFilters := l.filters
	opts := plan.GetOpts()
	if leaderFilter := filter.NewPlacementLeaderSafeguard(l.GetName(), opts, plan.GetBasicCluster(), plan.GetRuleManager(), plan.region, plan.source); leaderFilter != nil {
		finalFilters = append(l.filters, leaderFilter)
	}
	targets = filter.SelectTargetStores(targets, finalFilters, opts)
	leaderSchedulePolicy := opts.GetLeaderSchedulePolicy()
	sort.Slice(targets, func(i, j int) bool {
		iOp := plan.GetOpInfluence(targets[i].GetID())
		jOp := plan.GetOpInfluence(targets[j].GetID())
		return targets[i].LeaderScore(leaderSchedulePolicy, iOp) < targets[j].LeaderScore(leaderSchedulePolicy, jOp)
	})
	for _, plan.target = range targets {
		if op := l.createOperator(plan); op != nil {
			return op
		}
	}
	log.Debug("region has no target store", zap.String("scheduler", l.GetName()), zap.Uint64("region-id", plan.region.GetID()))
	schedulerCounter.WithLabelValues(l.GetName(), "no-target-store").Inc()
	return nil
}

// transferLeaderIn transfers leader to the target store.
// It randomly selects a health region from the target store, then picks
// the worst follower peer and transfers the leader.
func (l *balanceLeaderScheduler) transferLeaderIn(plan *balancePlan) *operator.Operator {
	plan.region = plan.RandFollowerRegion(plan.TargetStoreID(), l.conf.Ranges, schedule.IsRegionHealthy)
	if plan.region == nil {
		log.Debug("store has no follower", zap.String("scheduler", l.GetName()), zap.Uint64("store-id", plan.TargetStoreID()))
		schedulerCounter.WithLabelValues(l.GetName(), "no-follower-region").Inc()
		return nil
	}
	leaderStoreID := plan.region.GetLeader().GetStoreId()
	plan.source = plan.GetStore(leaderStoreID)
	if plan.source == nil {
		log.Debug("region has no leader or leader store cannot be found",
			zap.String("scheduler", l.GetName()),
			zap.Uint64("region-id", plan.region.GetID()),
			zap.Uint64("store-id", leaderStoreID),
		)
		schedulerCounter.WithLabelValues(l.GetName(), "no-leader").Inc()
		return nil
	}
	finalFilters := l.filters
	opts := plan.GetOpts()
	if leaderFilter := filter.NewPlacementLeaderSafeguard(l.GetName(), opts, plan.GetBasicCluster(), plan.GetRuleManager(), plan.region, plan.source); leaderFilter != nil {
		finalFilters = append(l.filters, leaderFilter)
	}
	target := filter.NewCandidates([]*core.StoreInfo{plan.target}).
		FilterTarget(opts, finalFilters...).
		PickFirst()
	if target == nil {
		log.Debug("region has no target store", zap.String("scheduler", l.GetName()), zap.Uint64("region-id", plan.region.GetID()))
		schedulerCounter.WithLabelValues(l.GetName(), "no-target-store").Inc()
		return nil
	}
	return l.createOperator(plan)
}

// createOperator creates the operator according to the source and target store.
// If the region is hot or the difference between the two stores is tolerable, then
// no new operator need to be created, otherwise create an operator that transfers
// the leader from the source store to the target store for the region.
func (l *balanceLeaderScheduler) createOperator(plan *balancePlan) *operator.Operator {
	if plan.IsRegionHot(plan.region) {
		log.Debug("region is hot region, ignore it", zap.String("scheduler", l.GetName()), zap.Uint64("region-id", plan.region.GetID()))
		schedulerCounter.WithLabelValues(l.GetName(), "region-hot").Inc()
		return nil
	}

	if !plan.shouldBalance(l.GetName()) {
		schedulerCounter.WithLabelValues(l.GetName(), "skip").Inc()
		return nil
	}

	op, err := operator.CreateTransferLeaderOperator(BalanceLeaderType, plan, plan.region, plan.region.GetLeader().GetStoreId(), plan.TargetStoreID(), []uint64{}, operator.OpLeader)
	if err != nil {
		log.Debug("fail to create balance leader operator", errs.ZapError(err))
		return nil
	}
	op.Counters = append(op.Counters,
		schedulerCounter.WithLabelValues(l.GetName(), "new-operator"),
	)
	op.FinishedCounters = append(op.FinishedCounters,
		balanceDirectionCounter.WithLabelValues(l.GetName(), plan.SourceMetricLabel(), plan.TargetMetricLabel()),
		l.counter.WithLabelValues("move-leader", plan.SourceMetricLabel()+"-out"),
		l.counter.WithLabelValues("move-leader", plan.TargetMetricLabel()+"-in"),
	)
	op.AdditionalInfos["sourceScore"] = strconv.FormatFloat(plan.sourceScore, 'f', 2, 64)
	op.AdditionalInfos["targetScore"] = strconv.FormatFloat(plan.targetScore, 'f', 2, 64)
	return op
}
