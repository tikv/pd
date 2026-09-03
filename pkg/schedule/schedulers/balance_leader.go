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
	"math"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/unrolled/render"
	"go.uber.org/zap"
	"golang.org/x/time/rate"

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
	MaxBalanceLeaderBatchSize = 100

	transferIn  = "transfer-in"
	transferOut = "transfer-out"

	// inboundLeaderTransferBurst is intentionally fixed at one. The limiter is
	// test-only and is meant to pace leader return instead of allowing a batch to
	// arrive at a target store at once.
	inboundLeaderTransferBurst = 1
)

var (
	invalidBalanceLeaderBatchSizeMsg         = "invalid batch size which should be an integer between 1 and " + strconv.Itoa(MaxBalanceLeaderBatchSize)
	invalidInboundLeaderTransferRateLimitMsg = "invalid inbound leader transfer rate limit: store ID and leaders per second must be positive, and burst must be 1"
)

type balanceLeaderSchedulerParam struct {
	Ranges []keyutil.KeyRange `json:"ranges"`
	// Batch is used to generate multiple operators by one scheduling
	Batch int `json:"batch"`
	// InboundLeaderTransferRateLimits limits balance-leader operators by their
	// target store. The rate unit is leaders per second and the burst is fixed at
	// one leader. An absent store ID means unlimited.
	InboundLeaderTransferRateLimits map[uint64]inboundLeaderTransferRateLimit `json:"inbound-leader-transfer-rate-limits,omitempty"`
}

type inboundLeaderTransferRateLimit struct {
	LeadersPerSecond float64 `json:"leaders-per-second"`
	Burst            int     `json:"burst"`
}

type balanceLeaderSchedulerConfig struct {
	baseDefaultSchedulerConfig
	balanceLeaderSchedulerParam

	// inboundLeaderTransferLimiters is runtime-only state derived lazily from
	// InboundLeaderTransferRateLimits. It is protected by the embedded mutex.
	inboundLeaderTransferLimiters map[uint64]*rate.Limiter
	now                           func() time.Time
}

func (conf *balanceLeaderSchedulerConfig) update(data []byte) (int, any) {
	conf.Lock()
	defer conf.Unlock()

	oldParam := conf.cloneLocked()
	newParam := conf.cloneLocked()
	oldConfig, _ := json.Marshal(oldParam)

	if err := json.Unmarshal(data, newParam); err != nil {
		return http.StatusInternalServerError, err.Error()
	}
	newConfig, _ := json.Marshal(newParam)
	if !bytes.Equal(oldConfig, newConfig) {
		if errMsg := newParam.validateLocked(); errMsg != "" {
			return http.StatusBadRequest, errMsg
		}
		conf.balanceLeaderSchedulerParam = *newParam
		if err := conf.save(); err != nil {
			log.Warn("failed to save balance-leader-scheduler config", errs.ZapError(err))
			conf.balanceLeaderSchedulerParam = *oldParam
			return http.StatusInternalServerError, err.Error()
		}
		log.Info("balance-leader-scheduler config is updated", zap.ByteString("old", oldConfig), zap.ByteString("new", newConfig))
		return http.StatusOK, "Config is updated."
	}
	m := make(map[string]any)
	if err := json.Unmarshal(data, &m); err != nil {
		return http.StatusInternalServerError, err.Error()
	}
	ok := reflectutil.FindSameFieldByJSON(newParam, m)
	if ok {
		return http.StatusOK, "Config is the same with origin, so do nothing."
	}
	return http.StatusBadRequest, "Config item is not found."
}

func (conf *balanceLeaderSchedulerParam) validateLocked() string {
	if conf.Batch < 1 || conf.Batch > MaxBalanceLeaderBatchSize {
		return invalidBalanceLeaderBatchSizeMsg
	}
	for storeID, limit := range conf.InboundLeaderTransferRateLimits {
		if storeID == 0 || limit.LeadersPerSecond <= 0 || math.IsNaN(limit.LeadersPerSecond) || math.IsInf(limit.LeadersPerSecond, 0) || limit.Burst != inboundLeaderTransferBurst {
			return invalidInboundLeaderTransferRateLimitMsg
		}
	}
	return ""
}

func (conf *balanceLeaderSchedulerConfig) clone() *balanceLeaderSchedulerParam {
	conf.RLock()
	defer conf.RUnlock()
	return conf.cloneLocked()
}

func (conf *balanceLeaderSchedulerConfig) cloneLocked() *balanceLeaderSchedulerParam {
	ranges := make([]keyutil.KeyRange, len(conf.Ranges))
	copy(ranges, conf.Ranges)
	rateLimits := make(map[uint64]inboundLeaderTransferRateLimit, len(conf.InboundLeaderTransferRateLimits))
	for storeID, limit := range conf.InboundLeaderTransferRateLimits {
		rateLimits[storeID] = limit
	}
	return &balanceLeaderSchedulerParam{
		Ranges:                          ranges,
		Batch:                           conf.Batch,
		InboundLeaderTransferRateLimits: rateLimits,
	}
}

func (conf *balanceLeaderSchedulerConfig) currentTimeLocked() time.Time {
	if conf.now != nil {
		return conf.now()
	}
	return time.Now()
}

func (conf *balanceLeaderSchedulerConfig) getInboundLeaderTransferLimiterLocked(storeID uint64) (*rate.Limiter, bool) {
	limit, ok := conf.InboundLeaderTransferRateLimits[storeID]
	if !ok {
		delete(conf.inboundLeaderTransferLimiters, storeID)
		return nil, false
	}
	if conf.inboundLeaderTransferLimiters == nil {
		conf.inboundLeaderTransferLimiters = make(map[uint64]*rate.Limiter)
	}
	limiter, ok := conf.inboundLeaderTransferLimiters[storeID]
	now := conf.currentTimeLocked()
	if !ok {
		limiter = rate.NewLimiter(rate.Limit(limit.LeadersPerSecond), inboundLeaderTransferBurst)
		conf.inboundLeaderTransferLimiters[storeID] = limiter
	} else if limiter.Limit() != rate.Limit(limit.LeadersPerSecond) {
		limiter.SetLimitAt(now, rate.Limit(limit.LeadersPerSecond))
	}
	return limiter, true
}

func (conf *balanceLeaderSchedulerConfig) isInboundLeaderTransferAllowed(storeID uint64) bool {
	conf.Lock()
	defer conf.Unlock()
	limiter, limited := conf.getInboundLeaderTransferLimiterLocked(storeID)
	return !limited || limiter.TokensAt(conf.currentTimeLocked()) >= 1
}

func (conf *balanceLeaderSchedulerConfig) takeInboundLeaderTransfer(storeID uint64) bool {
	conf.Lock()
	defer conf.Unlock()
	limiter, limited := conf.getInboundLeaderTransferLimiterLocked(storeID)
	return !limited || limiter.AllowN(conf.currentTimeLocked(), 1)
}

func (conf *balanceLeaderSchedulerConfig) setInboundLeaderTransferRate(storeID uint64, leadersPerSecond float64) (int, any) {
	if storeID == 0 || leadersPerSecond <= 0 || math.IsNaN(leadersPerSecond) || math.IsInf(leadersPerSecond, 0) {
		return http.StatusBadRequest, "store ID and leaders per second must be positive"
	}
	conf.Lock()
	defer conf.Unlock()
	hadRateLimits := conf.InboundLeaderTransferRateLimits != nil
	if conf.InboundLeaderTransferRateLimits == nil {
		conf.InboundLeaderTransferRateLimits = make(map[uint64]inboundLeaderTransferRateLimit)
	}
	newLimit := inboundLeaderTransferRateLimit{
		LeadersPerSecond: leadersPerSecond,
		Burst:            inboundLeaderTransferBurst,
	}
	if conf.InboundLeaderTransferRateLimits[storeID] == newLimit {
		return http.StatusOK, "Config is the same with origin, so do nothing."
	}
	oldLimit, hadLimit := conf.InboundLeaderTransferRateLimits[storeID]
	conf.InboundLeaderTransferRateLimits[storeID] = newLimit
	if err := conf.save(); err != nil {
		log.Warn("failed to save balance-leader-scheduler config", errs.ZapError(err))
		if hadLimit {
			conf.InboundLeaderTransferRateLimits[storeID] = oldLimit
		} else if hadRateLimits {
			delete(conf.InboundLeaderTransferRateLimits, storeID)
		} else {
			conf.InboundLeaderTransferRateLimits = nil
		}
		return http.StatusInternalServerError, err.Error()
	}
	if limiter, ok := conf.inboundLeaderTransferLimiters[storeID]; ok {
		limiter.SetLimitAt(conf.currentTimeLocked(), rate.Limit(leadersPerSecond))
	}
	return http.StatusOK, "Config is updated."
}

func (conf *balanceLeaderSchedulerConfig) deleteInboundLeaderTransferRate(storeID uint64) (int, any) {
	if storeID == 0 {
		return http.StatusBadRequest, "store ID must be positive"
	}
	conf.Lock()
	defer conf.Unlock()
	if _, ok := conf.InboundLeaderTransferRateLimits[storeID]; !ok {
		return http.StatusOK, "Config item does not exist, so do nothing."
	}
	oldLimit := conf.InboundLeaderTransferRateLimits[storeID]
	delete(conf.InboundLeaderTransferRateLimits, storeID)
	if err := conf.save(); err != nil {
		log.Warn("failed to save balance-leader-scheduler config", errs.ZapError(err))
		conf.InboundLeaderTransferRateLimits[storeID] = oldLimit
		return http.StatusInternalServerError, err.Error()
	}
	delete(conf.inboundLeaderTransferLimiters, storeID)
	return http.StatusOK, "Config is updated."
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
	router.HandleFunc("/config/inbound-leader-transfer-rate", handler.setInboundLeaderTransferRate).Methods(http.MethodPost)
	router.HandleFunc("/config/inbound-leader-transfer-rate/{storeID}", handler.deleteInboundLeaderTransferRate).Methods(http.MethodDelete)
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

func (handler *balanceLeaderHandler) setInboundLeaderTransferRate(w http.ResponseWriter, r *http.Request) {
	var input struct {
		StoreID          uint64  `json:"store-id"`
		LeadersPerSecond float64 `json:"leaders-per-second"`
	}
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		handler.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	httpCode, v := handler.config.setInboundLeaderTransferRate(input.StoreID, input.LeadersPerSecond)
	handler.rd.JSON(w, httpCode, v)
}

func (handler *balanceLeaderHandler) deleteInboundLeaderTransferRate(w http.ResponseWriter, r *http.Request) {
	storeID, err := strconv.ParseUint(mux.Vars(r)["storeID"], 10, 64)
	if err != nil {
		handler.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	httpCode, v := handler.config.deleteInboundLeaderTransferRate(storeID)
	handler.rd.JSON(w, httpCode, v)
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
	s.conf.InboundLeaderTransferRateLimits = newCfg.InboundLeaderTransferRateLimits
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
func (s *balanceLeaderScheduler) Schedule(cluster sche.SchedulerCluster, collectDiagnostics bool) ([]*operator.Operator, []plan.Plan) {
	return s.scheduleWithInboundLeaderTransferLimit(cluster, collectDiagnostics, false)
}

func (s *balanceLeaderScheduler) diagnoseDryRun(cluster sche.SchedulerCluster) ([]*operator.Operator, []plan.Plan) {
	return s.scheduleWithInboundLeaderTransferLimit(cluster, true, true)
}

func (s *balanceLeaderScheduler) scheduleWithInboundLeaderTransferLimit(
	cluster sche.SchedulerCluster,
	collectDiagnostics bool,
	bypassInboundLeaderTransferRateLimit bool,
) ([]*operator.Operator, []plan.Plan) {
	basePlan := plan.NewBalanceSchedulerPlan()
	var collector *plan.Collector
	if collectDiagnostics {
		collector = plan.NewCollector(basePlan)
	}
	defer s.filterCounter.Flush(cluster)
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
			op := createTransferLeaderOperator(sourceCandidate, transferOut, s, solver, usedRegions, collector, bypassInboundLeaderTransferRateLimit)
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
			op := createTransferLeaderOperator(targetCandidate, transferIn, s, solver, usedRegions, nil, bypassInboundLeaderTransferRateLimit)
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
	ssolver *solver, usedRegions map[uint64]struct{}, collector *plan.Collector, bypassInboundLeaderTransferRateLimit bool) *operator.Operator {
	store := cs.getStore()
	if dir == transferIn && !bypassInboundLeaderTransferRateLimit && !s.conf.isInboundLeaderTransferAllowed(store.GetID()) {
		balanceLeaderCounterWithEvent("inbound-target-rate-limited").Inc()
		cs.next()
		return nil
	}
	ssolver.Step++
	defer func() { ssolver.Step-- }()
	retryLimit := s.getLimit(store)
	var creator func(*solver, *plan.Collector, bool) *operator.Operator
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
		if op = creator(ssolver, collector, bypassInboundLeaderTransferRateLimit); op != nil {
			if _, ok := usedRegions[op.RegionID()]; ok {
				op = nil
				continue
			}
			if bypassInboundLeaderTransferRateLimit || s.conf.takeInboundLeaderTransfer(ssolver.targetStoreID()) {
				break
			}
			balanceLeaderCounterWithEvent("inbound-target-rate-limited").Inc()
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
func (s *balanceLeaderScheduler) transferLeaderOut(solver *solver, collector *plan.Collector, bypassInboundLeaderTransferRateLimit bool) *operator.Operator {
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
		if op := s.createOperator(solver, collector, bypassInboundLeaderTransferRateLimit); op != nil {
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
func (s *balanceLeaderScheduler) transferLeaderIn(solver *solver, collector *plan.Collector, bypassInboundLeaderTransferRateLimit bool) *operator.Operator {
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
	if filter.NewCandidates([]*core.StoreInfo{solver.Source}).
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
	target := filter.NewCandidates([]*core.StoreInfo{solver.Target}).
		FilterTarget(conf, nil, s.filterCounter, finalFilters...).
		PickFirst()
	if target == nil {
		log.Debug("region has no target store", zap.String("scheduler", s.GetName()), zap.Uint64("region-id", solver.Region.GetID()))
		balanceLeaderNoTargetStoreCounter.Inc()
		return nil
	}
	return s.createOperator(solver, collector, bypassInboundLeaderTransferRateLimit)
}

// createOperator creates the operator according to the source and target store.
// If the region is hot or the difference between the two stores is tolerable, then
// no new operator need to be created, otherwise create an operator that transfers
// the leader from the source store to the target store for the region.
func (s *balanceLeaderScheduler) createOperator(solver *solver, collector *plan.Collector, bypassInboundLeaderTransferRateLimit bool) *operator.Operator {
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
	if !bypassInboundLeaderTransferRateLimit && !s.conf.isInboundLeaderTransferAllowed(solver.targetStoreID()) {
		balanceLeaderCounterWithEvent("inbound-target-rate-limited").Inc()
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
