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
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/reflectutil"
	"github.com/tikv/pd/pkg/syncutil"
	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/filter"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/plan"
	"github.com/tikv/pd/server/storage/endpoint"
	"github.com/unrolled/render"
	"go.uber.org/zap"
)

const (
	// BalanceLeaderName is balance leader scheduler name.
	BalanceLeaderName = "balance-leader-scheduler"
	// BalanceLeaderType is balance leader scheduler type.
	BalanceLeaderType = "balance-leader"
	// balanceLeaderRetryLimit is the limit to retry schedule for selected source store and target store.
	balanceLeaderRetryLimit = 10
	// BalanceLeaderBatchSize is the default number of operators to transfer leaders by one scheduling.
	// Default value is 4 which is subjected by scheduler-max-waiting-operator and leader-schedule-limit
	// If you want to increase balance speed more, please increase above-mentioned param.
	BalanceLeaderBatchSize = 4
	// MaxBalanceLeaderBatchSize is maximum of balance leader batch size
	MaxBalanceLeaderBatchSize = 10

	transferIn  = "transfer-in"
	transferOut = "transfer-out"
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
			conf.Batch = BalanceLeaderBatchSize
			return nil
		}
	})

	schedule.RegisterScheduler(BalanceLeaderType, func(opController *schedule.OperatorController, storage endpoint.ConfigStorage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &balanceLeaderSchedulerConfig{storage: storage}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		if conf.Batch == 0 {
			conf.Batch = BalanceLeaderBatchSize
		}
		return newBalanceLeaderScheduler(opController, conf), nil
	})
}

type balanceLeaderSchedulerConfig struct {
	mu      syncutil.RWMutex
	storage endpoint.ConfigStorage
	Ranges  []core.KeyRange `json:"ranges"`
	// Batch is used to generate multiple operators by one scheduling
	Batch int `json:"batch"`
}

func (conf *balanceLeaderSchedulerConfig) Update(data []byte) (int, interface{}) {
	conf.mu.Lock()
	defer conf.mu.Unlock()

	oldc, _ := json.Marshal(conf)

	if err := json.Unmarshal(data, conf); err != nil {
		return http.StatusInternalServerError, err.Error()
	}
	newc, _ := json.Marshal(conf)
	if !bytes.Equal(oldc, newc) {
		if !conf.validate() {
			json.Unmarshal(oldc, conf)
			return http.StatusBadRequest, "invalid batch size which should be an integer between 1 and 10"
		}
		conf.persistLocked()
		return http.StatusOK, "success"
	}
	m := make(map[string]interface{})
	if err := json.Unmarshal(data, &m); err != nil {
		return http.StatusInternalServerError, err.Error()
	}
	ok := reflectutil.FindSameFieldByJSON(conf, m)
	if ok {
		return http.StatusOK, "no changed"
	}
	return http.StatusBadRequest, "config item not found"
}

func (conf *balanceLeaderSchedulerConfig) validate() bool {
	return conf.Batch >= 1 && conf.Batch <= 10
}

func (conf *balanceLeaderSchedulerConfig) Clone() *balanceLeaderSchedulerConfig {
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	ranges := make([]core.KeyRange, len(conf.Ranges))
	copy(ranges, conf.Ranges)
	return &balanceLeaderSchedulerConfig{
		Ranges: ranges,
		Batch:  conf.Batch,
	}
}

func (conf *balanceLeaderSchedulerConfig) persistLocked() error {
	data, err := schedule.EncodeConfig(conf)
	if err != nil {
		return err
	}
	return conf.storage.SaveScheduleConfig(BalanceLeaderName, data)
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
	router.HandleFunc("/config", handler.UpdateConfig).Methods(http.MethodPost)
	router.HandleFunc("/list", handler.ListConfig).Methods(http.MethodGet)
	return router
}

func (handler *balanceLeaderHandler) UpdateConfig(w http.ResponseWriter, r *http.Request) {
	data, _ := io.ReadAll(r.Body)
	r.Body.Close()
	httpCode, v := handler.config.Update(data)
	handler.rd.JSON(w, httpCode, v)
}

func (handler *balanceLeaderHandler) ListConfig(w http.ResponseWriter, r *http.Request) {
	conf := handler.config.Clone()
	handler.rd.JSON(w, http.StatusOK, conf)
}

type balanceLeaderScheduler struct {
	*BaseScheduler
	*retryQuota
	name         string
	conf         *balanceLeaderSchedulerConfig
	handler      http.Handler
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
		name:          BalanceLeaderName,
		conf:          conf,
		handler:       newBalanceLeaderHandler(conf),
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

func (l *balanceLeaderScheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	l.handler.ServeHTTP(w, r)
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
		s.name = name
	}
}

func (l *balanceLeaderScheduler) GetName() string {
	return l.name
}

func (l *balanceLeaderScheduler) GetType() string {
	return BalanceLeaderType
}

func (l *balanceLeaderScheduler) EncodeConfig() ([]byte, error) {
	l.conf.mu.RLock()
	defer l.conf.mu.RUnlock()
	return schedule.EncodeConfig(l.conf)
}

func (l *balanceLeaderScheduler) IsScheduleAllowed(cluster schedule.Cluster) bool {
	allowed := l.opController.OperatorCount(operator.OpLeader) < cluster.GetOpts().GetLeaderScheduleLimit()
	if !allowed {
		operator.OperatorLimitCounter.WithLabelValues(l.GetType(), operator.OpLeader.String()).Inc()
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

func (l *balanceLeaderScheduler) Schedule(cluster schedule.Cluster, dryRun bool) ([]*operator.Operator, []plan.Plan) {
	l.conf.mu.RLock()
	defer l.conf.mu.RUnlock()
	basePlan := NewBalanceSchedulerPlan()
	var collector *plan.Collector
	if dryRun {
		collector = plan.NewCollector(basePlan)
	}
	batch := l.conf.Batch
	schedulerCounter.WithLabelValues(l.GetName(), "schedule").Inc()

	leaderSchedulePolicy := cluster.GetOpts().GetLeaderSchedulePolicy()
	opInfluence := l.opController.GetOpInfluence(cluster)
	kind := core.NewScheduleKind(core.LeaderKind, leaderSchedulePolicy)
	solver := newSolver(basePlan, kind, cluster, opInfluence)

	stores := cluster.GetStores()
	scoreFunc := func(store *core.StoreInfo) float64 {
		return store.LeaderScore(solver.kind.Policy, solver.GetOpInfluence(store.GetID()))
	}
	sourceCandidate := newCandidateStores(filter.SelectSourceStores(stores, l.filters, cluster.GetOpts(), collector), false, scoreFunc)
	targetCandidate := newCandidateStores(filter.SelectTargetStores(stores, l.filters, cluster.GetOpts(), nil), true, scoreFunc)
	usedRegions := make(map[uint64]struct{})

	result := make([]*operator.Operator, 0, batch)
	for sourceCandidate.hasStore() || targetCandidate.hasStore() {
		// first choose source
		if sourceCandidate.hasStore() {
			op := createTransferLeaderOperator(sourceCandidate, transferOut, l, solver, usedRegions, collector)
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
			op := createTransferLeaderOperator(targetCandidate, transferIn, l, solver, usedRegions, nil)
			if op != nil {
				result = append(result, op)
				if len(result) >= batch {
					return result, collector.GetPlans()
				}
				makeInfluence(op, solver, usedRegions, sourceCandidate, targetCandidate)
			}
		}
	}
	l.retryQuota.GC(append(sourceCandidate.stores, targetCandidate.stores...))
	return result, collector.GetPlans()
}

func createTransferLeaderOperator(cs *candidateStores, dir string, l *balanceLeaderScheduler,
	ssolver *solver, usedRegions map[uint64]struct{}, collector *plan.Collector) *operator.Operator {
	store := cs.getStore()
	ssolver.step++
	defer func() { ssolver.step-- }()
	retryLimit := l.retryQuota.GetLimit(store)
	var creator func(*solver, *plan.Collector) *operator.Operator
	switch dir {
	case transferOut:
		ssolver.source, ssolver.target = store, nil
		creator = l.transferLeaderOut
	case transferIn:
		ssolver.source, ssolver.target = nil, store
		creator = l.transferLeaderIn
	}
	var op *operator.Operator
	for i := 0; i < retryLimit; i++ {
		schedulerCounter.WithLabelValues(l.GetName(), "total").Inc()
		if op = creator(ssolver, collector); op != nil {
			if _, ok := usedRegions[op.RegionID()]; !ok {
				break
			}
			op = nil
		}
	}
	if op != nil {
		l.retryQuota.ResetLimit(store)
	} else {
		l.Attenuate(store)
		log.Debug("no operator created for selected stores", zap.String("scheduler", l.GetName()), zap.Uint64(dir, store.GetID()))
		cs.next()
	}
	return op
}

func makeInfluence(op *operator.Operator, plan *solver, usedRegions map[uint64]struct{}, candidates ...*candidateStores) {
	usedRegions[op.RegionID()] = struct{}{}
	candidateUpdateStores := make([][]int, len(candidates))
	for id, candidate := range candidates {
		storesIDs := candidate.binarySearchStores(plan.source, plan.target)
		candidateUpdateStores[id] = storesIDs
	}
	schedule.AddOpInfluence(op, plan.opInfluence, plan.Cluster)
	for id, candidate := range candidates {
		for _, pos := range candidateUpdateStores[id] {
			candidate.resortStoreWithPos(pos)
		}
	}
}

// transferLeaderOut transfers leader from the source store.
// It randomly selects a health region from the source store, then picks
// the best follower peer and transfers the leader.
func (l *balanceLeaderScheduler) transferLeaderOut(solver *solver, collector *plan.Collector) *operator.Operator {
	solver.region = filter.SelectOneRegion(solver.RandLeaderRegions(solver.SourceStoreID(), l.conf.Ranges),
		collector, filter.NewRegionPendingFilter(), filter.NewRegionDownFilter())
	if solver.region == nil {
		log.Debug("store has no leader", zap.String("scheduler", l.GetName()), zap.Uint64("store-id", solver.SourceStoreID()))
		schedulerCounter.WithLabelValues(l.GetName(), "no-leader-region").Inc()
		return nil
	}
	if solver.IsRegionHot(solver.region) {
		log.Debug("region is hot region, ignore it", zap.String("scheduler", l.GetName()), zap.Uint64("region-id", solver.region.GetID()))
		collector.Collect(plan.SetResource(solver.region), plan.SetStatus(plan.NewStatus(plan.StatusRegionHot)))
		schedulerCounter.WithLabelValues(l.GetName(), "region-hot").Inc()
		return nil
	}
	solver.step++
	defer func() { solver.step-- }()
	targets := solver.GetFollowerStores(solver.region)
	finalFilters := l.filters
	opts := solver.GetOpts()
	if leaderFilter := filter.NewPlacementLeaderSafeguard(l.GetName(), opts, solver.GetBasicCluster(), solver.GetRuleManager(), solver.region, solver.source); leaderFilter != nil {
		finalFilters = append(l.filters, leaderFilter)
	}
	targets = filter.SelectTargetStores(targets, finalFilters, opts, nil)
	leaderSchedulePolicy := opts.GetLeaderSchedulePolicy()
	sort.Slice(targets, func(i, j int) bool {
		iOp := solver.GetOpInfluence(targets[i].GetID())
		jOp := solver.GetOpInfluence(targets[j].GetID())
		return targets[i].LeaderScore(leaderSchedulePolicy, iOp) < targets[j].LeaderScore(leaderSchedulePolicy, jOp)
	})
	for _, solver.target = range targets {
		if op := l.createOperator(solver, collector); op != nil {
			return op
		}
	}
	log.Debug("region has no target store", zap.String("scheduler", l.GetName()), zap.Uint64("region-id", solver.region.GetID()))
	schedulerCounter.WithLabelValues(l.GetName(), "no-target-store").Inc()
	return nil
}

// transferLeaderIn transfers leader to the target store.
// It randomly selects a health region from the target store, then picks
// the worst follower peer and transfers the leader.
func (l *balanceLeaderScheduler) transferLeaderIn(solver *solver, collector *plan.Collector) *operator.Operator {
	solver.region = filter.SelectOneRegion(solver.RandFollowerRegions(solver.TargetStoreID(), l.conf.Ranges),
		nil, filter.NewRegionPendingFilter(), filter.NewRegionDownFilter())
	if solver.region == nil {
		log.Debug("store has no follower", zap.String("scheduler", l.GetName()), zap.Uint64("store-id", solver.TargetStoreID()))
		schedulerCounter.WithLabelValues(l.GetName(), "no-follower-region").Inc()
		return nil
	}
	if solver.IsRegionHot(solver.region) {
		log.Debug("region is hot region, ignore it", zap.String("scheduler", l.GetName()), zap.Uint64("region-id", solver.region.GetID()))
		schedulerCounter.WithLabelValues(l.GetName(), "region-hot").Inc()
		return nil
	}
	leaderStoreID := solver.region.GetLeader().GetStoreId()
	solver.source = solver.GetStore(leaderStoreID)
	if solver.source == nil {
		log.Debug("region has no leader or leader store cannot be found",
			zap.String("scheduler", l.GetName()),
			zap.Uint64("region-id", solver.region.GetID()),
			zap.Uint64("store-id", leaderStoreID),
		)
		schedulerCounter.WithLabelValues(l.GetName(), "no-leader").Inc()
		return nil
	}
	finalFilters := l.filters
	opts := solver.GetOpts()
	if leaderFilter := filter.NewPlacementLeaderSafeguard(l.GetName(), opts, solver.GetBasicCluster(), solver.GetRuleManager(), solver.region, solver.source); leaderFilter != nil {
		finalFilters = append(l.filters, leaderFilter)
	}
	target := filter.NewCandidates([]*core.StoreInfo{solver.target}).
		FilterTarget(opts, nil, finalFilters...).
		PickFirst()
	if target == nil {
		log.Debug("region has no target store", zap.String("scheduler", l.GetName()), zap.Uint64("region-id", solver.region.GetID()))
		schedulerCounter.WithLabelValues(l.GetName(), "no-target-store").Inc()
		return nil
	}
	return l.createOperator(solver, collector)
}

// createOperator creates the operator according to the source and target store.
// If the region is hot or the difference between the two stores is tolerable, then
// no new operator need to be created, otherwise create an operator that transfers
// the leader from the source store to the target store for the region.
func (l *balanceLeaderScheduler) createOperator(solver *solver, collector *plan.Collector) *operator.Operator {
	solver.step++
	defer func() { solver.step-- }()
	if !solver.shouldBalance(l.GetName()) {
		schedulerCounter.WithLabelValues(l.GetName(), "skip").Inc()
		collector.Collect(plan.SetStatus(plan.NewStatus(plan.StatusStoreScoreDisallowed)))
		return nil
	}
	solver.step++
	defer func() { solver.step-- }()
	op, err := operator.CreateTransferLeaderOperator(BalanceLeaderType, solver, solver.region, solver.region.GetLeader().GetStoreId(), solver.TargetStoreID(), []uint64{}, operator.OpLeader)
	if err != nil {
		log.Debug("fail to create balance leader operator", errs.ZapError(err))
		collector.Collect(plan.SetStatus(plan.NewStatus(plan.StatusCreateOperatorFailed)))
		return nil
	}
	op.Counters = append(op.Counters,
		schedulerCounter.WithLabelValues(l.GetName(), "new-operator"),
	)
	op.FinishedCounters = append(op.FinishedCounters,
		balanceDirectionCounter.WithLabelValues(l.GetName(), solver.SourceMetricLabel(), solver.TargetMetricLabel()),
		l.counter.WithLabelValues("move-leader", solver.SourceMetricLabel()+"-out"),
		l.counter.WithLabelValues("move-leader", solver.TargetMetricLabel()+"-in"),
	)
	op.AdditionalInfos["sourceScore"] = strconv.FormatFloat(solver.sourceScore, 'f', 2, 64)
	op.AdditionalInfos["targetScore"] = strconv.FormatFloat(solver.targetScore, 'f', 2, 64)
	return op
}
