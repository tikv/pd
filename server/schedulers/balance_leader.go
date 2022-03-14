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
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/gorilla/mux"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/filter"
	"github.com/tikv/pd/server/schedule/operator"
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
	// BalanceLeaderBatchSize is the default number of operators to transfer leaders by one scheduling
	BalanceLeaderBatchSize = 5

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
			conf.Name = BalanceLeaderName
			conf.Batch = BalanceLeaderBatchSize
			return nil
		}
	})

	schedule.RegisterScheduler(BalanceLeaderType, func(opController *schedule.OperatorController, storage endpoint.ConfigStorage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &balanceLeaderSchedulerConfig{storage: storage}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newBalanceLeaderScheduler(opController, conf), nil
	})
}

type balanceLeaderSchedulerConfig struct {
	mu      sync.RWMutex
	storage endpoint.ConfigStorage
	Name    string          `json:"name"`
	Ranges  []core.KeyRange `json:"ranges"`
	Batch   int             `json:"batch"`
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
		conf.persistLocked()
		return http.StatusOK, "success"
	}
	m := make(map[string]interface{})
	if err := json.Unmarshal(data, &m); err != nil {
		return http.StatusInternalServerError, err.Error()
	}

	t := reflect.TypeOf(conf).Elem()
	for i := 0; i < t.NumField(); i++ {
		jsonTag := t.Field(i).Tag.Get("json")
		if i := strings.Index(jsonTag, ","); i != -1 { // trim 'foobar,string' to 'foobar'
			jsonTag = jsonTag[:i]
		}
		if _, ok := m[jsonTag]; ok {
			return http.StatusOK, "no changed"
		}
	}
	return http.StatusBadRequest, "config item not found"
}

func (conf *balanceLeaderSchedulerConfig) Clone() *balanceLeaderSchedulerConfig {
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	return &balanceLeaderSchedulerConfig{
		Ranges: conf.Ranges,
		Batch:  conf.Batch,
	}
}

func (conf *balanceLeaderSchedulerConfig) Persist() error {
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	data, err := schedule.EncodeConfig(conf)
	if err != nil {
		return err
	}
	return conf.storage.SaveScheduleConfig(conf.Name, data)
}

func (conf *balanceLeaderSchedulerConfig) persistLocked() error {
	data, err := schedule.EncodeConfig(conf)
	if err != nil {
		return err
	}
	return conf.storage.SaveScheduleConfig(conf.Name, data)
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
	router.HandleFunc("/config", handler.UpdateConfig).Methods("POST")
	router.HandleFunc("/list", handler.ListConfig).Methods("GET")
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

type candidateStores struct {
	stores        []*core.StoreInfo
	storeIndexMap map[uint64]int
	index         int
	compareOption func([]*core.StoreInfo) func(int, int) bool
}

func newCandidateStores(stores []*core.StoreInfo, compareOption func([]*core.StoreInfo) func(int, int) bool) *candidateStores {
	cs := &candidateStores{stores: stores, compareOption: compareOption}
	cs.storeIndexMap = map[uint64]int{}
	cs.initSort()
	return cs
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

func (cs *candidateStores) initSort() {
	sort.Slice(cs.stores, cs.compareOption(cs.stores))
	for i := 0; i < len(cs.stores); i++ {
		cs.storeIndexMap[cs.stores[i].GetID()] = i
	}
}

func (cs *candidateStores) reSort(stores ...*core.StoreInfo) {
	if !cs.hasStore() {
		return
	}
	for _, store := range stores {
		index, ok := cs.storeIndexMap[store.GetID()]
		if !ok {
			continue
		}
		resortStores(cs.stores, cs.storeIndexMap, index, cs.compareOption(cs.stores))
	}
}

func (l *balanceLeaderScheduler) Schedule(cluster schedule.Cluster) []*operator.Operator {
	l.conf.mu.RLock()
	defer l.conf.mu.RUnlock()
	schedulerCounter.WithLabelValues(l.GetName(), "schedule").Inc()

	leaderSchedulePolicy := cluster.GetOpts().GetLeaderSchedulePolicy()
	opInfluence := l.opController.GetOpInfluence(cluster)
	kind := core.NewScheduleKind(core.LeaderKind, leaderSchedulePolicy)
	plan := newBalancePlan(kind, cluster, opInfluence)

	stores := cluster.GetStores()
	greaterOption := func(stores []*core.StoreInfo) func(int, int) bool {
		return func(i, j int) bool {
			iOp := plan.GetOpInfluence(stores[i].GetID())
			jOp := plan.GetOpInfluence(stores[j].GetID())
			return stores[i].LeaderScore(plan.kind.Policy, iOp) >
				stores[j].LeaderScore(plan.kind.Policy, jOp)
		}
	}
	lessOption := func(stores []*core.StoreInfo) func(int, int) bool {
		return func(i, j int) bool {
			iOp := plan.GetOpInfluence(stores[i].GetID())
			jOp := plan.GetOpInfluence(stores[j].GetID())
			return stores[i].LeaderScore(plan.kind.Policy, iOp) <
				stores[j].LeaderScore(plan.kind.Policy, jOp)
		}
	}
	sourceCandidate := newCandidateStores(filter.SelectSourceStores(stores, l.filters, cluster.GetOpts()), greaterOption)
	targetCandidate := newCandidateStores(filter.SelectTargetStores(stores, l.filters, cluster.GetOpts()), lessOption)
	usedRegions := make(map[uint64]struct{})

	result := make([]*operator.Operator, 0, l.conf.Batch)
	for sourceCandidate.hasStore() || targetCandidate.hasStore() {
		// first choose source
		if sourceCandidate.hasStore() {
			op := createTransferLeaderOperator(sourceCandidate, transferOut, l, plan, usedRegions)
			if op != nil {
				result = append(result, op)
				if len(result) >= l.conf.Batch {
					return result
				}
				makeInfluence(op, plan, usedRegions, sourceCandidate, targetCandidate)
			}
		}
		// next choose target
		if targetCandidate.hasStore() {
			op := createTransferLeaderOperator(targetCandidate, transferIn, l, plan, usedRegions)
			if op != nil {
				result = append(result, op)
				if len(result) >= l.conf.Batch {
					return result
				}
				makeInfluence(op, plan, usedRegions, sourceCandidate, targetCandidate)
			}
		}
	}
	l.retryQuota.GC(append(sourceCandidate.stores, targetCandidate.stores...))
	return result
}

func createTransferLeaderOperator(cs *candidateStores, dir string, l *balanceLeaderScheduler,
	plan *balancePlan, usedRegions map[uint64]struct{}) *operator.Operator {
	store := cs.getStore()
	retryLimit := l.retryQuota.GetLimit(store)
	var creator func(*balancePlan) *operator.Operator
	switch dir {
	case transferOut:
		plan.source, plan.target = store, nil
		l.counter.WithLabelValues("high-score", plan.SourceMetricLabel()).Inc()
		creator = l.transferLeaderOut
	case transferIn:
		plan.source, plan.target = nil, store
		l.counter.WithLabelValues("low-score", plan.TargetMetricLabel()).Inc()
		creator = l.transferLeaderIn
	}
	var op *operator.Operator
	for i := 0; i < retryLimit; i++ {
		schedulerCounter.WithLabelValues(l.GetName(), "total").Inc()
		if op = creator(plan); op != nil {
			if _, ok := usedRegions[op.RegionID()]; !ok {
				break
			}
			op = nil
		}
	}
	if op != nil {
		l.retryQuota.ResetLimit(store)
		op.Counters = append(op.Counters, l.counter.WithLabelValues(dir, plan.SourceMetricLabel()))
	} else {
		l.Attenuate(store)
		log.Debug("no operator created for selected stores", zap.String("scheduler", l.GetName()), zap.Uint64(dir, store.GetID()))
		cs.next()
	}
	return op
}

func makeInfluence(op *operator.Operator, plan *balancePlan, usedRegions map[uint64]struct{}, candidates ...*candidateStores) {
	usedRegions[op.RegionID()] = struct{}{}
	schedule.AddOpInfluence(op, plan.opInfluence, plan.Cluster)
	for _, candidate := range candidates {
		candidate.reSort(plan.source, plan.target)
	}
}

// resortStores is used to sort stores again after creating an operator.
// It will repeatedly swap the specific store and next store if they are in wrong order.
// In general, it has very few swaps. In the worst case, the time complexity is O(n).
func resortStores(stores []*core.StoreInfo, index map[uint64]int, pos int, less func(i, j int) bool) {
	swapper := func(i, j int) { stores[i], stores[j] = stores[j], stores[i] }
	for ; pos+1 < len(stores) && !less(pos, pos+1); pos++ {
		swapper(pos, pos+1)
		index[stores[pos].GetID()] = pos
	}
	for ; pos > 1 && less(pos, pos-1); pos-- {
		swapper(pos, pos-1)
		index[stores[pos].GetID()] = pos
	}
	index[stores[pos].GetID()] = pos
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
