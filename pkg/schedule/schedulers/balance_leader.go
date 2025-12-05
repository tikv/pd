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
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"sort"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/unrolled/render"
	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/errs"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/plan"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/utils/keyutil"
	"github.com/tikv/pd/pkg/utils/reflectutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
)

const (
	// BalanceLeaderBatchSize is the default number of operators to transfer leaders by one scheduling.
	// Default value is 4 which is subjected by scheduler-max-waiting-operator and leader-schedule-limit
	// If you want to increase balance speed more, please increase above-mentioned param.
	BalanceLeaderBatchSize = 4
	// MaxBalanceLeaderBatchSize is maximum of balance leader batch size
	MaxBalanceLeaderBatchSize = 10

	transferIn  = "transfer-in"
	transferOut = "transfer-out"
)

type balanceLeaderSchedulerParam struct {
	Ranges []keyutil.KeyRange `json:"ranges"`
	// Batch is used to generate multiple operators by one scheduling
	Batch int `json:"batch"`
}

type balanceLeaderSchedulerConfig struct {
	baseDefaultSchedulerConfig
	balanceLeaderSchedulerParam
}

func (conf *balanceLeaderSchedulerConfig) update(data []byte) (int, any) {
	conf.Lock()
	defer conf.Unlock()

	param := &conf.balanceLeaderSchedulerParam
	oldConfig, _ := json.Marshal(param)

	if err := json.Unmarshal(data, param); err != nil {
		return http.StatusInternalServerError, err.Error()
	}
	newConfig, _ := json.Marshal(param)
	if !bytes.Equal(oldConfig, newConfig) {
		if !conf.validateLocked() {
			if err := json.Unmarshal(oldConfig, param); err != nil {
				return http.StatusInternalServerError, err.Error()
			}
			return http.StatusBadRequest, "invalid batch size which should be an integer between 1 and 10"
		}
		conf.balanceLeaderSchedulerParam = *param
		if err := conf.save(); err != nil {
			log.Warn("failed to save balance-leader-scheduler config", errs.ZapError(err))
		}
		log.Info("balance-leader-scheduler config is updated", zap.ByteString("old", oldConfig), zap.ByteString("new", newConfig))
		return http.StatusOK, "Config is updated."
	}
	m := make(map[string]any)
	if err := json.Unmarshal(data, &m); err != nil {
		return http.StatusInternalServerError, err.Error()
	}
	ok := reflectutil.FindSameFieldByJSON(param, m)
	if ok {
		return http.StatusOK, "Config is the same with origin, so do nothing."
	}
	return http.StatusBadRequest, "Config item is not found."
}

func (conf *balanceLeaderSchedulerParam) validateLocked() bool {
	return conf.Batch >= 1 && conf.Batch <= 10
}

func (conf *balanceLeaderSchedulerConfig) clone() *balanceLeaderSchedulerParam {
	conf.RLock()
	defer conf.RUnlock()
	ranges := make([]keyutil.KeyRange, len(conf.Ranges))
	copy(ranges, conf.Ranges)
	return &balanceLeaderSchedulerParam{
		Ranges: ranges,
		Batch:  conf.Batch,
	}
}

func (conf *balanceLeaderSchedulerConfig) getBatch() int {
	conf.RLock()
	defer conf.RUnlock()
	return conf.Batch
}

func (conf *balanceLeaderSchedulerConfig) getRanges() []keyutil.KeyRange {
	conf.RLock()
	defer conf.RUnlock()
	ranges := make([]keyutil.KeyRange, len(conf.Ranges))
	copy(ranges, conf.Ranges)
	return ranges
}

type balanceLeaderHandler struct {
	rd     *render.Render
	config *balanceLeaderSchedulerConfig
}

func newBalanceLeaderHandler(conf *balanceLeaderSchedulerConfig) http.Handler {
	handler := &balanceLeaderHandler{
		config: conf,
		rd:     render.New(render.Options{IndentJSON: true}),
	}
	router := mux.NewRouter()
	router.HandleFunc("/config", handler.updateConfig).Methods(http.MethodPost)
	router.HandleFunc("/list", handler.listConfig).Methods(http.MethodGet)
	return router
}

func (handler *balanceLeaderHandler) updateConfig(w http.ResponseWriter, r *http.Request) {
	data, _ := io.ReadAll(r.Body)
	r.Body.Close()
	httpCode, v := handler.config.update(data)
	handler.rd.JSON(w, httpCode, v)
}

func (handler *balanceLeaderHandler) listConfig(w http.ResponseWriter, _ *http.Request) {
	conf := handler.config.clone()
	handler.rd.JSON(w, http.StatusOK, conf)
}

type balanceLeaderScheduler struct {
	*BaseScheduler
	*retryQuota
	conf          *balanceLeaderSchedulerConfig
	handler       http.Handler
	filters       []filter.Filter
	filterCounter *filter.Counter
}

// newBalanceLeaderScheduler creates a scheduler that tends to keep leaders on
// each store balanced.
func newBalanceLeaderScheduler(opController *operator.Controller, conf *balanceLeaderSchedulerConfig, options ...BalanceLeaderCreateOption) Scheduler {
	s := &balanceLeaderScheduler{
		BaseScheduler: NewBaseScheduler(opController, types.BalanceLeaderScheduler, conf),
		retryQuota:    newRetryQuota(),
		conf:          conf,
		handler:       newBalanceLeaderHandler(conf),
	}
	for _, option := range options {
		option(s)
	}
	s.filters = []filter.Filter{
		&filter.StoreStateFilter{ActionScope: s.GetName(), TransferLeader: true, OperatorLevel: constant.High},
		filter.NewSpecialUseFilter(s.GetName()),
	}
	s.filterCounter = filter.NewCounter(s.GetName())
	return s
}

// ServeHTTP implements the http.Handler interface.
func (s *balanceLeaderScheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

// BalanceLeaderCreateOption is used to create a scheduler with an option.
type BalanceLeaderCreateOption func(s *balanceLeaderScheduler)

// WithBalanceLeaderName sets the name for the scheduler.
func WithBalanceLeaderName(name string) BalanceLeaderCreateOption {
	return func(s *balanceLeaderScheduler) {
		s.name = name
	}
}

// EncodeConfig implements the Scheduler interface.
func (s *balanceLeaderScheduler) EncodeConfig() ([]byte, error) {
	s.conf.RLock()
	defer s.conf.RUnlock()
	return EncodeConfig(s.conf)
}

// ReloadConfig implements the Scheduler interface.
func (s *balanceLeaderScheduler) ReloadConfig() error {
	s.conf.Lock()
	defer s.conf.Unlock()

	newCfg := &balanceLeaderSchedulerConfig{}
	if err := s.conf.load(newCfg); err != nil {
		return err
	}
	s.conf.Ranges = newCfg.Ranges
	s.conf.Batch = newCfg.Batch
	return nil
}

// IsScheduleAllowed implements the Scheduler interface.
func (s *balanceLeaderScheduler) IsScheduleAllowed(cluster sche.SchedulerCluster) bool {
	allowed := s.OpController.OperatorCount(operator.OpLeader) < cluster.GetSchedulerConfig().GetLeaderScheduleLimit()
	if !allowed {
		operator.IncOperatorLimitCounter(s.GetType(), operator.OpLeader)
	}
	return allowed
}

// candidateStores for balance_leader, order by `getStore` `asc`
type candidateStores struct {
	stores   []*core.StoreInfo
	getScore func(*core.StoreInfo) float64
	index    int
	asc      bool
}

func newCandidateStores(stores []*core.StoreInfo, asc bool, getScore func(*core.StoreInfo) float64) *candidateStores {
	cs := &candidateStores{stores: stores, getScore: getScore, asc: asc}
	sort.Slice(cs.stores, cs.sortFunc())
	return cs
}

func (cs *candidateStores) sortFunc() (less func(int, int) bool) {
	less = func(i, j int) bool {
		scorei := cs.getScore(cs.stores[i])
		scorej := cs.getScore(cs.stores[j])
		return cs.less(cs.stores[i].GetID(), scorei, cs.stores[j].GetID(), scorej)
	}
	return less
}

func (cs *candidateStores) less(iID uint64, scorei float64, jID uint64, scorej float64) bool {
	if typeutil.Float64Equal(scorei, scorej) {
		// when the stores share the same score, returns the one with the bigger ID,
		// Since we assume that the bigger storeID, the newer store(which would be scheduled as soon as possible).
		return iID > jID
	}
	if cs.asc {
		return scorei < scorej
	}
	return scorei > scorej
}

// hasStore returns returns true when there are leftover stores.
func (cs *candidateStores) hasStore() bool {
	return cs.index < len(cs.stores)
}

func (cs *candidateStores) getStore() *core.StoreInfo {
	return cs.stores[cs.index]
}

func (cs *candidateStores) next() {
	cs.index++
}

func (cs *candidateStores) binarySearch(store *core.StoreInfo) (index int) {
	score := cs.getScore(store)
	searchFunc := func(i int) bool {
		curScore := cs.getScore(cs.stores[i])
		return !cs.less(cs.stores[i].GetID(), curScore, store.GetID(), score)
	}
	return sort.Search(len(cs.stores)-1, searchFunc)
}

// return the slice of index for the searched stores.
func (cs *candidateStores) binarySearchStores(stores ...*core.StoreInfo) (offsets []int) {
	if !cs.hasStore() {
		return
	}
	for _, store := range stores {
		index := cs.binarySearch(store)
		offsets = append(offsets, index)
	}
	return offsets
}

// resortStoreWithPos is used to sort stores again after creating an operator.
// It will repeatedly swap the specific store and next store if they are in wrong order.
// In general, it has very few swaps. In the worst case, the time complexity is O(n).
func (cs *candidateStores) resortStoreWithPos(pos int) {
	swapper := func(i, j int) { cs.stores[i], cs.stores[j] = cs.stores[j], cs.stores[i] }
	score := cs.getScore(cs.stores[pos])
	storeID := cs.stores[pos].GetID()
	for ; pos+1 < len(cs.stores); pos++ {
		curScore := cs.getScore(cs.stores[pos+1])
		if cs.less(storeID, score, cs.stores[pos+1].GetID(), curScore) {
			break
		}
		swapper(pos, pos+1)
	}
	for ; pos > 1; pos-- {
		curScore := cs.getScore(cs.stores[pos-1])
		if !cs.less(storeID, score, cs.stores[pos-1].GetID(), curScore) {
			break
		}
		swapper(pos, pos-1)
	}
}

// Schedule implements the Scheduler interface.
func (s *balanceLeaderScheduler) Schedule(cluster sche.SchedulerCluster, dryRun bool) ([]*operator.Operator, []plan.Plan) {
	basePlan := plan.NewBalanceSchedulerPlan()
	var collector *plan.Collector
	if dryRun {
		collector = plan.NewCollector(basePlan)
	}
	defer s.filterCounter.Flush()
	batch := s.conf.getBatch()
	balanceLeaderScheduleCounter.Inc()

	leaderSchedulePolicy := cluster.GetSchedulerConfig().GetLeaderSchedulePolicy()
	opInfluence := s.OpController.GetOpInfluence(cluster.GetBasicCluster())
	kind := constant.NewScheduleKind(constant.LeaderKind, leaderSchedulePolicy)
	solver := newSolver(basePlan, kind, cluster, opInfluence)

	stores := cluster.GetStores()
	scoreFunc := func(store *core.StoreInfo) float64 {
		return store.LeaderScore(solver.kind.Policy, solver.getOpInfluence(store.GetID()))
	}
	sourceCandidate := newCandidateStores(filter.SelectSourceStores(stores, s.filters, cluster.GetSchedulerConfig(), collector, s.filterCounter), false, scoreFunc)
	targetCandidate := newCandidateStores(filter.SelectTargetStores(stores, s.filters, cluster.GetSchedulerConfig(), nil, s.filterCounter), true, scoreFunc)
	usedRegions := make(map[uint64]struct{})

	result := make([]*operator.Operator, 0, batch)
	for sourceCandidate.hasStore() || targetCandidate.hasStore() {
		// first choose source
		if sourceCandidate.hasStore() {
			op := createTransferLeaderOperator(sourceCandidate, transferOut, s, solver, usedRegions, collector)
			if op != nil {
				result = append(result, op)
				if len(result) >= batch {
					return result, collector.GetPlans()
				}
				makeInfluence(op, solver, usedRegions, sourceCandidate, targetCandidate)
			}
		}
		// next choose target
		if targetCandidate.hasStore() {
			op := createTransferLeaderOperator(targetCandidate, transferIn, s, solver, usedRegions, nil)
			if op != nil {
				result = append(result, op)
				if len(result) >= batch {
					return result, collector.GetPlans()
				}
				makeInfluence(op, solver, usedRegions, sourceCandidate, targetCandidate)
			}
		}
	}
	s.gc(append(sourceCandidate.stores, targetCandidate.stores...))
	return result, collector.GetPlans()
}

func createTransferLeaderOperator(cs *candidateStores, dir string, s *balanceLeaderScheduler,
	ssolver *solver, usedRegions map[uint64]struct{}, collector *plan.Collector) *operator.Operator {
	store := cs.getStore()
	ssolver.Step++
	defer func() { ssolver.Step-- }()
	retryLimit := s.getLimit(store)
	var creator func(*solver, *plan.Collector) *operator.Operator
	switch dir {
	case transferOut:
		ssolver.Source, ssolver.Target = store, nil
		creator = s.transferLeaderOut
	case transferIn:
		ssolver.Source, ssolver.Target = nil, store
		creator = s.transferLeaderIn
	}
	var op *operator.Operator
	for range retryLimit {
		if op = creator(ssolver, collector); op != nil {
			if _, ok := usedRegions[op.RegionID()]; !ok {
				break
			}
			op = nil
		}
	}
	if op != nil {
		s.resetLimit(store)
	} else {
		s.attenuate(store)
		log.Debug("no operator created for selected stores", zap.String("scheduler", s.GetName()), zap.Uint64(dir, store.GetID()))
		cs.next()
	}
	return op
}

func makeInfluence(op *operator.Operator, plan *solver, usedRegions map[uint64]struct{}, candidates ...*candidateStores) {
	usedRegions[op.RegionID()] = struct{}{}
	candidateUpdateStores := make([][]int, len(candidates))
	for id, candidate := range candidates {
		storesIDs := candidate.binarySearchStores(plan.Source, plan.Target)
		candidateUpdateStores[id] = storesIDs
	}
	operator.AddOpInfluence(op, plan.opInfluence, plan.GetBasicCluster())
	for id, candidate := range candidates {
		for _, pos := range candidateUpdateStores[id] {
			candidate.resortStoreWithPos(pos)
		}
	}
}

// transferLeaderOut transfers leader from the source store.
// It randomly selects a health region from the source store, then picks
// the best follower peer and transfers the leader.
func (s *balanceLeaderScheduler) transferLeaderOut(solver *solver, collector *plan.Collector) *operator.Operator {
	rs := s.conf.getRanges()
	if s.GetName() == types.BalanceLeaderScheduler.String() {
		km := solver.GetKeyRangeManager()
		if !km.IsEmpty() {
			// todo: check all key ranges not only the first
			rs = km.GetNonOverlappingKeyRanges(&rs[0])
		}
	}
	solver.Region = filter.SelectOneRegion(solver.RandLeaderRegions(solver.sourceStoreID(), rs),
		collector, filter.NewRegionPendingFilter(), filter.NewRegionDownFilter(), filter.NewAffinityFilter(solver.SchedulerCluster))
	if solver.Region == nil {
		log.Debug("store has no leader", zap.String("scheduler", s.GetName()), zap.Uint64("store-id", solver.sourceStoreID()))
		balanceLeaderNoLeaderRegionCounter.Inc()
		return nil
	}
	if solver.IsRegionHot(solver.Region) {
		log.Debug("region is hot region, ignore it", zap.String("scheduler", s.GetName()), zap.Uint64("region-id", solver.Region.GetID()))
		if collector != nil {
			collector.Collect(plan.SetResource(solver.Region), plan.SetStatus(plan.NewStatus(plan.StatusRegionHot)))
		}
		balanceLeaderRegionHotCounter.Inc()
		return nil
	}
	solver.Step++
	defer func() { solver.Step-- }()
	targets := solver.GetFollowerStores(solver.Region)
	finalFilters := s.filters
	conf := solver.GetSchedulerConfig()
	if leaderFilter := filter.NewPlacementLeaderSafeguard(s.GetName(), conf, solver.GetBasicCluster(), solver.GetRuleManager(), solver.Region, solver.Source, false /*allowMoveLeader*/); leaderFilter != nil {
		finalFilters = append(s.filters, leaderFilter)
	}
	targets = filter.SelectTargetStores(targets, finalFilters, conf, collector, s.filterCounter)
	leaderSchedulePolicy := conf.GetLeaderSchedulePolicy()
	sort.Slice(targets, func(i, j int) bool {
		iOp := solver.getOpInfluence(targets[i].GetID())
		jOp := solver.getOpInfluence(targets[j].GetID())
		return targets[i].LeaderScore(leaderSchedulePolicy, iOp) < targets[j].LeaderScore(leaderSchedulePolicy, jOp)
	})
	for _, solver.Target = range targets {
		if op := s.createOperator(solver, collector); op != nil {
			return op
		}
	}
	log.Debug("region has no target store", zap.String("scheduler", s.GetName()), zap.Uint64("region-id", solver.Region.GetID()))
	balanceLeaderNoTargetStoreCounter.Inc()
	return nil
}

// transferLeaderIn transfers leader to the target store.
// It randomly selects a health region from the target store, then picks
// the worst follower peer and transfers the leader.
func (s *balanceLeaderScheduler) transferLeaderIn(solver *solver, collector *plan.Collector) *operator.Operator {
	rs := s.conf.getRanges()
	if s.GetName() == types.BalanceLeaderScheduler.String() {
		km := solver.GetKeyRangeManager()
		if !km.IsEmpty() {
			rs = km.GetNonOverlappingKeyRanges(&rs[0])
		}
	}
	solver.Region = filter.SelectOneRegion(solver.RandFollowerRegions(solver.targetStoreID(), rs),
		nil, filter.NewRegionPendingFilter(), filter.NewRegionDownFilter(), filter.NewAffinityFilter(solver.SchedulerCluster))
	if solver.Region == nil {
		log.Debug("store has no follower", zap.String("scheduler", s.GetName()), zap.Uint64("store-id", solver.targetStoreID()))
		balanceLeaderNoFollowerRegionCounter.Inc()
		return nil
	}
	if solver.IsRegionHot(solver.Region) {
		log.Debug("region is hot region, ignore it", zap.String("scheduler", s.GetName()), zap.Uint64("region-id", solver.Region.GetID()))
		balanceLeaderRegionHotCounter.Inc()
		return nil
	}
	leaderStoreID := solver.Region.GetLeader().GetStoreId()
	solver.Source = solver.GetStore(leaderStoreID)
	if solver.Source == nil {
		log.Debug("region has no leader or leader store cannot be found",
			zap.String("scheduler", s.GetName()),
			zap.Uint64("region-id", solver.Region.GetID()),
			zap.Uint64("store-id", leaderStoreID),
		)
		balanceLeaderNoLeaderRegionCounter.Inc()
		return nil
	}
	// Check if the source store is available as a source.
	conf := solver.GetSchedulerConfig()
	if filter.NewCandidates(s.R, []*core.StoreInfo{solver.Source}).
		FilterSource(conf, nil, s.filterCounter, s.filters...).Len() == 0 {
		log.Debug("store cannot be used as source", zap.String("scheduler", s.GetName()), zap.Uint64("store-id", solver.Source.GetID()))
		balanceLeaderNoSourceStoreCounter.Inc()
		return nil
	}

	// Check if the target store is available as a target.
	finalFilters := s.filters
	if leaderFilter := filter.NewPlacementLeaderSafeguard(s.GetName(), conf, solver.GetBasicCluster(), solver.GetRuleManager(), solver.Region, solver.Source, false /*allowMoveLeader*/); leaderFilter != nil {
		finalFilters = append(s.filters, leaderFilter)
	}
	target := filter.NewCandidates(s.R, []*core.StoreInfo{solver.Target}).
		FilterTarget(conf, nil, s.filterCounter, finalFilters...).
		PickFirst()
	if target == nil {
		log.Debug("region has no target store", zap.String("scheduler", s.GetName()), zap.Uint64("region-id", solver.Region.GetID()))
		balanceLeaderNoTargetStoreCounter.Inc()
		return nil
	}
	return s.createOperator(solver, collector)
}

// createOperator creates the operator according to the source and target store.
// If the region is hot or the difference between the two stores is tolerable, then
// no new operator need to be created, otherwise create an operator that transfers
// the leader from the source store to the target store for the region.
func (s *balanceLeaderScheduler) createOperator(solver *solver, collector *plan.Collector) *operator.Operator {
	solver.Step++
	defer func() { solver.Step-- }()
	solver.sourceScore, solver.targetScore = solver.sourceStoreScore(s.GetName()), solver.targetStoreScore(s.GetName())
	if !solver.shouldBalance(s.GetName()) {
		balanceLeaderSkipCounter.Inc()
		if collector != nil {
			collector.Collect(plan.SetStatus(plan.NewStatus(plan.StatusStoreScoreDisallowed)))
		}
		return nil
	}
	solver.Step++
	defer func() { solver.Step-- }()
	op, err := operator.CreateTransferLeaderOperator(s.GetName(), solver, solver.Region, solver.targetStoreID(), []uint64{}, operator.OpLeader)
	if err != nil {
		log.Debug("fail to create balance leader operator", errs.ZapError(err))
		if collector != nil {
			collector.Collect(plan.SetStatus(plan.NewStatus(plan.StatusCreateOperatorFailed)))
		}
		return nil
	}
	op.Counters = append(op.Counters,
		balanceLeaderNewOpCounter,
	)
	op.FinishedCounters = append(op.FinishedCounters,
		balanceDirectionCounter.WithLabelValues(s.GetName(), solver.sourceMetricLabel(), "out"),
		balanceDirectionCounter.WithLabelValues(s.GetName(), solver.targetMetricLabel(), "in"),
	)
	op.SetAdditionalInfo("sourceScore", strconv.FormatFloat(solver.sourceScore, 'f', 2, 64))
	op.SetAdditionalInfo("targetScore", strconv.FormatFloat(solver.targetScore, 'f', 2, 64))
	return op
}
