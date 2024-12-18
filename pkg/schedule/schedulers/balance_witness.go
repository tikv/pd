// Copyright 2022 TiKV Project Authors.
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
	"github.com/prometheus/client_golang/prometheus"
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
	"github.com/tikv/pd/pkg/utils/reflectutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

const (
	// balanceWitnessBatchSize is the default number of operators to transfer witnesses by one scheduling.
	// Default value is 4 which is subjected by scheduler-max-waiting-operator and witness-schedule-limit
	// If you want to increase balance speed more, please increase above-mentioned param.
	balanceWitnessBatchSize = 4
	// MaxBalanceWitnessBatchSize is maximum of balance witness batch size
	MaxBalanceWitnessBatchSize = 10
)

type balanceWitnessSchedulerConfig struct {
	syncutil.RWMutex
	schedulerConfig

	Ranges []core.KeyRange `json:"ranges"`
	// Batch is used to generate multiple operators by one scheduling
	Batch int `json:"batch"`
}

func (conf *balanceWitnessSchedulerConfig) update(data []byte) (int, any) {
	conf.Lock()
	defer conf.Unlock()

	oldc, _ := json.Marshal(conf)

	if err := json.Unmarshal(data, conf); err != nil {
		return http.StatusInternalServerError, err.Error()
	}
	newc, _ := json.Marshal(conf)
	if !bytes.Equal(oldc, newc) {
		if !conf.validateLocked() {
			if err := json.Unmarshal(oldc, conf); err != nil {
				return http.StatusInternalServerError, err.Error()
			}
			return http.StatusBadRequest, "invalid batch size which should be an integer between 1 and 10"
		}
		if err := conf.save(); err != nil {
			log.Warn("failed to persist config", zap.Error(err))
		}
		log.Info("balance-witness-scheduler config is updated", zap.ByteString("old", oldc), zap.ByteString("new", newc))
		return http.StatusOK, "Config is updated."
	}
	m := make(map[string]any)
	if err := json.Unmarshal(data, &m); err != nil {
		return http.StatusInternalServerError, err.Error()
	}
	ok := reflectutil.FindSameFieldByJSON(conf, m)
	if ok {
		return http.StatusOK, "Config is the same with origin, so do nothing."
	}
	return http.StatusBadRequest, "Config item is not found."
}

func (conf *balanceWitnessSchedulerConfig) validateLocked() bool {
	return conf.Batch >= 1 && conf.Batch <= 10
}

func (conf *balanceWitnessSchedulerConfig) clone() *balanceWitnessSchedulerConfig {
	conf.RLock()
	defer conf.RUnlock()
	ranges := make([]core.KeyRange, len(conf.Ranges))
	copy(ranges, conf.Ranges)
	return &balanceWitnessSchedulerConfig{
		Ranges: ranges,
		Batch:  conf.Batch,
	}
}

func (conf *balanceWitnessSchedulerConfig) getBatch() int {
	conf.RLock()
	defer conf.RUnlock()
	return conf.Batch
}

func (conf *balanceWitnessSchedulerConfig) getRanges() []core.KeyRange {
	conf.RLock()
	defer conf.RUnlock()
	ranges := make([]core.KeyRange, len(conf.Ranges))
	copy(ranges, conf.Ranges)
	return ranges
}

type balanceWitnessHandler struct {
	rd     *render.Render
	config *balanceWitnessSchedulerConfig
}

func newBalanceWitnessHandler(conf *balanceWitnessSchedulerConfig) http.Handler {
	handler := &balanceWitnessHandler{
		config: conf,
		rd:     render.New(render.Options{IndentJSON: true}),
	}
	router := mux.NewRouter()
	router.HandleFunc("/config", handler.updateConfig).Methods(http.MethodPost)
	router.HandleFunc("/list", handler.listConfig).Methods(http.MethodGet)
	return router
}

func (handler *balanceWitnessHandler) updateConfig(w http.ResponseWriter, r *http.Request) {
	data, _ := io.ReadAll(r.Body)
	r.Body.Close()
	httpCode, v := handler.config.update(data)
	handler.rd.JSON(w, httpCode, v)
}

func (handler *balanceWitnessHandler) listConfig(w http.ResponseWriter, _ *http.Request) {
	conf := handler.config.clone()
	handler.rd.JSON(w, http.StatusOK, conf)
}

type balanceWitnessScheduler struct {
	*BaseScheduler
	*retryQuota
	conf          *balanceWitnessSchedulerConfig
	handler       http.Handler
	filters       []filter.Filter
	counter       *prometheus.CounterVec
	filterCounter *filter.Counter
}

// newBalanceWitnessScheduler creates a scheduler that tends to keep witnesses on
// each store balanced.
func newBalanceWitnessScheduler(opController *operator.Controller, conf *balanceWitnessSchedulerConfig, options ...BalanceWitnessCreateOption) Scheduler {
	s := &balanceWitnessScheduler{
		BaseScheduler: NewBaseScheduler(opController, types.BalanceWitnessScheduler, conf),
		retryQuota:    newRetryQuota(),
		conf:          conf,
		handler:       newBalanceWitnessHandler(conf),
		counter:       balanceWitnessCounter,
		filterCounter: filter.NewCounter(types.BalanceWitnessScheduler.String()),
	}
	for _, option := range options {
		option(s)
	}
	s.filters = []filter.Filter{
		&filter.StoreStateFilter{ActionScope: s.GetName(), MoveRegion: true, OperatorLevel: constant.Medium},
		filter.NewSpecialUseFilter(s.GetName()),
	}
	return s
}

// ServeHTTP implements the http.Handler interface.
func (s *balanceWitnessScheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

// BalanceWitnessCreateOption is used to create a scheduler with an option.
type BalanceWitnessCreateOption func(s *balanceWitnessScheduler)

// WithBalanceWitnessCounter sets the counter for the scheduler.
func WithBalanceWitnessCounter(counter *prometheus.CounterVec) BalanceWitnessCreateOption {
	return func(s *balanceWitnessScheduler) {
		s.counter = counter
	}
}

// EncodeConfig implements the Scheduler interface.
func (s *balanceWitnessScheduler) EncodeConfig() ([]byte, error) {
	s.conf.RLock()
	defer s.conf.RUnlock()
	return EncodeConfig(s.conf)
}

// ReloadConfig implements the Scheduler interface.
func (s *balanceWitnessScheduler) ReloadConfig() error {
	s.conf.Lock()
	defer s.conf.Unlock()

	newCfg := &balanceWitnessSchedulerConfig{}
	if err := s.conf.load(newCfg); err != nil {
		return err
	}
	s.conf.Ranges = newCfg.Ranges
	s.conf.Batch = newCfg.Batch
	return nil
}

// IsScheduleAllowed implements the Scheduler interface.
func (s *balanceWitnessScheduler) IsScheduleAllowed(cluster sche.SchedulerCluster) bool {
	allowed := s.OpController.OperatorCount(operator.OpWitness) < cluster.GetSchedulerConfig().GetWitnessScheduleLimit()
	if !allowed {
		operator.IncOperatorLimitCounter(s.GetType(), operator.OpWitness)
	}
	return allowed
}

// Schedule implements the Scheduler interface.
func (s *balanceWitnessScheduler) Schedule(cluster sche.SchedulerCluster, dryRun bool) ([]*operator.Operator, []plan.Plan) {
	basePlan := plan.NewBalanceSchedulerPlan()
	var collector *plan.Collector
	if dryRun {
		collector = plan.NewCollector(basePlan)
	}
	batch := s.conf.getBatch()
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()

	opInfluence := s.OpController.GetOpInfluence(cluster.GetBasicCluster())
	kind := constant.NewScheduleKind(constant.WitnessKind, constant.ByCount)
	solver := newSolver(basePlan, kind, cluster, opInfluence)

	stores := cluster.GetStores()
	scoreFunc := func(store *core.StoreInfo) float64 {
		return store.WitnessScore(solver.getOpInfluence(store.GetID()))
	}
	sourceCandidate := newCandidateStores(filter.SelectSourceStores(stores, s.filters, cluster.GetSchedulerConfig(), collector, s.filterCounter), false, scoreFunc)
	usedRegions := make(map[uint64]struct{})

	result := make([]*operator.Operator, 0, batch)
	if sourceCandidate.hasStore() {
		op := createTransferWitnessOperator(sourceCandidate, s, solver, usedRegions, collector)
		if op != nil {
			result = append(result, op)
			if len(result) >= batch {
				return result, collector.GetPlans()
			}
			makeInfluence(op, solver, usedRegions, sourceCandidate)
		}
	}
	s.retryQuota.gc(sourceCandidate.stores)
	return result, collector.GetPlans()
}

func createTransferWitnessOperator(cs *candidateStores, s *balanceWitnessScheduler,
	ssolver *solver, usedRegions map[uint64]struct{}, collector *plan.Collector) *operator.Operator {
	store := cs.getStore()
	ssolver.Step++
	defer func() { ssolver.Step-- }()
	retryLimit := s.retryQuota.getLimit(store)
	ssolver.Source, ssolver.Target = store, nil
	var op *operator.Operator
	for range retryLimit {
		schedulerCounter.WithLabelValues(s.GetName(), "total").Inc()
		if op = s.transferWitnessOut(ssolver, collector); op != nil {
			if _, ok := usedRegions[op.RegionID()]; !ok {
				break
			}
			op = nil
		}
	}
	if op != nil {
		s.retryQuota.resetLimit(store)
	} else {
		s.attenuate(store)
		log.Debug("no operator created for selected stores", zap.String("scheduler", s.GetName()), zap.Uint64("transfer-out", store.GetID()))
		cs.next()
	}
	return op
}

// transferWitnessOut transfers witness from the source store.
// It randomly selects a health region from the source store, then picks
// the best follower peer and transfers the witness.
func (s *balanceWitnessScheduler) transferWitnessOut(solver *solver, collector *plan.Collector) *operator.Operator {
	solver.Region = filter.SelectOneRegion(solver.RandWitnessRegions(solver.sourceStoreID(), s.conf.getRanges()),
		collector, filter.NewRegionPendingFilter(), filter.NewRegionDownFilter())
	if solver.Region == nil {
		log.Debug("store has no witness", zap.String("scheduler", s.GetName()), zap.Uint64("store-id", solver.sourceStoreID()))
		schedulerCounter.WithLabelValues(s.GetName(), "no-witness-region").Inc()
		return nil
	}
	solver.Step++
	defer func() { solver.Step-- }()
	targets := solver.GetNonWitnessVoterStores(solver.Region)
	finalFilters := s.filters
	conf := solver.GetSchedulerConfig()
	if witnessFilter := filter.NewPlacementWitnessSafeguard(s.GetName(), conf, solver.GetBasicCluster(), solver.GetRuleManager(), solver.Region, solver.Source, solver.fit); witnessFilter != nil {
		finalFilters = append(s.filters, witnessFilter)
	}
	targets = filter.SelectTargetStores(targets, finalFilters, conf, collector, s.filterCounter)
	sort.Slice(targets, func(i, j int) bool {
		iOp := solver.getOpInfluence(targets[i].GetID())
		jOp := solver.getOpInfluence(targets[j].GetID())
		return targets[i].WitnessScore(iOp) < targets[j].WitnessScore(jOp)
	})
	for _, solver.Target = range targets {
		if op := s.createOperator(solver, collector); op != nil {
			return op
		}
	}
	log.Debug("region has no target store", zap.String("scheduler", s.GetName()), zap.Uint64("region-id", solver.Region.GetID()))
	schedulerCounter.WithLabelValues(s.GetName(), "no-target-store").Inc()
	return nil
}

// createOperator creates the operator according to the source and target store.
// If the region is hot or the difference between the two stores is tolerable, then
// no new operator need to be created, otherwise create an operator that transfers
// the witness from the source store to the target store for the region.
func (s *balanceWitnessScheduler) createOperator(solver *solver, collector *plan.Collector) *operator.Operator {
	solver.Step++
	defer func() { solver.Step-- }()
	solver.sourceScore, solver.targetScore = solver.sourceStoreScore(s.GetName()), solver.targetStoreScore(s.GetName())
	if !solver.shouldBalance(s.GetName()) {
		schedulerCounter.WithLabelValues(s.GetName(), "skip").Inc()
		if collector != nil {
			collector.Collect(plan.SetStatus(plan.NewStatus(plan.StatusStoreScoreDisallowed)))
		}
		return nil
	}
	solver.Step++
	defer func() { solver.Step-- }()
	op, err := operator.CreateMoveWitnessOperator(s.GetName(), solver, solver.Region, solver.sourceStoreID(), solver.targetStoreID())
	if err != nil {
		log.Debug("fail to create balance witness operator", errs.ZapError(err))
		return nil
	}
	op.Counters = append(op.Counters,
		schedulerCounter.WithLabelValues(s.GetName(), "new-operator"),
	)
	op.FinishedCounters = append(op.FinishedCounters,
		balanceDirectionCounter.WithLabelValues(s.GetName(), solver.sourceMetricLabel(), solver.targetMetricLabel()),
		s.counter.WithLabelValues("move-witness", solver.sourceMetricLabel()+"-out"),
		s.counter.WithLabelValues("move-witness", solver.targetMetricLabel()+"-in"),
	)
	op.SetAdditionalInfo("sourceScore", strconv.FormatFloat(solver.sourceScore, 'f', 2, 64))
	op.SetAdditionalInfo("targetScore", strconv.FormatFloat(solver.targetScore, 'f', 2, 64))
	return op
}
