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
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/errs"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/plan"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/reflectutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/unrolled/render"
	"go.uber.org/zap"
)

const (
	// BalanceWitnessName is balance witness scheduler name.
	BalanceWitnessName = "balance-witness-scheduler"
	// BalanceWitnessType is balance witness scheduler type.
	BalanceWitnessType = "balance-witness"
	// balanceWitnessBatchSize is the default number of operators to transfer witnesses by one scheduling.
	// Default value is 4 which is subjected by scheduler-max-waiting-operator and witness-schedule-limit
	// If you want to increase balance speed more, please increase above-mentioned param.
	balanceWitnessBatchSize = 4
	// MaxBalanceWitnessBatchSize is maximum of balance witness batch size
	MaxBalanceWitnessBatchSize = 10
)

type balanceWitnessSchedulerConfig struct {
	syncutil.RWMutex
	storage endpoint.ConfigStorage
	Ranges  []core.KeyRange `json:"ranges"`
	// Batch is used to generate multiple operators by one scheduling
	Batch int `json:"batch"`
}

func (conf *balanceWitnessSchedulerConfig) Update(data []byte) (int, interface{}) {
	conf.Lock()
	defer conf.Unlock()

	oldc, _ := json.Marshal(conf)

	if err := json.Unmarshal(data, conf); err != nil {
		return http.StatusInternalServerError, err.Error()
	}
	newc, _ := json.Marshal(conf)
	if !bytes.Equal(oldc, newc) {
		if !conf.validateLocked() {
			json.Unmarshal(oldc, conf)
			return http.StatusBadRequest, "invalid batch size which should be an integer between 1 and 10"
		}
		conf.persistLocked()
		log.Info("balance-witness-scheduler config is updated", zap.ByteString("old", oldc), zap.ByteString("new", newc))
		return http.StatusOK, "Config is updated."
	}
	m := make(map[string]interface{})
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

func (conf *balanceWitnessSchedulerConfig) Clone() *balanceWitnessSchedulerConfig {
	conf.RLock()
	defer conf.RUnlock()
	ranges := make([]core.KeyRange, len(conf.Ranges))
	copy(ranges, conf.Ranges)
	return &balanceWitnessSchedulerConfig{
		Ranges: ranges,
		Batch:  conf.Batch,
	}
}

func (conf *balanceWitnessSchedulerConfig) persistLocked() error {
	data, err := EncodeConfig(conf)
	if err != nil {
		return err
	}
	return conf.storage.SaveSchedulerConfig(BalanceWitnessName, data)
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
	router.HandleFunc("/config", handler.UpdateConfig).Methods(http.MethodPost)
	router.HandleFunc("/list", handler.ListConfig).Methods(http.MethodGet)
	return router
}

func (handler *balanceWitnessHandler) UpdateConfig(w http.ResponseWriter, r *http.Request) {
	data, _ := io.ReadAll(r.Body)
	r.Body.Close()
	httpCode, v := handler.config.Update(data)
	handler.rd.JSON(w, httpCode, v)
}

func (handler *balanceWitnessHandler) ListConfig(w http.ResponseWriter, r *http.Request) {
	conf := handler.config.Clone()
	handler.rd.JSON(w, http.StatusOK, conf)
}

type balanceWitnessScheduler struct {
	*BaseScheduler
	*retryQuota
	name          string
	conf          *balanceWitnessSchedulerConfig
	handler       http.Handler
	filters       []filter.Filter
	counter       *prometheus.CounterVec
	filterCounter *filter.Counter
}

// newBalanceWitnessScheduler creates a scheduler that tends to keep witnesses on
// each store balanced.
func newBalanceWitnessScheduler(opController *operator.Controller, conf *balanceWitnessSchedulerConfig, options ...BalanceWitnessCreateOption) Scheduler {
	base := NewBaseScheduler(opController)
	s := &balanceWitnessScheduler{
		BaseScheduler: base,
		retryQuota:    newRetryQuota(),
		name:          BalanceWitnessName,
		conf:          conf,
		handler:       newBalanceWitnessHandler(conf),
		counter:       balanceWitnessCounter,
		filterCounter: filter.NewCounter(filter.BalanceWitness.String()),
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

func (b *balanceWitnessScheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b.handler.ServeHTTP(w, r)
}

// BalanceWitnessCreateOption is used to create a scheduler with an option.
type BalanceWitnessCreateOption func(s *balanceWitnessScheduler)

// WithBalanceWitnessCounter sets the counter for the scheduler.
func WithBalanceWitnessCounter(counter *prometheus.CounterVec) BalanceWitnessCreateOption {
	return func(s *balanceWitnessScheduler) {
		s.counter = counter
	}
}

// WithBalanceWitnessName sets the name for the scheduler.
func WithBalanceWitnessName(name string) BalanceWitnessCreateOption {
	return func(s *balanceWitnessScheduler) {
		s.name = name
	}
}

func (b *balanceWitnessScheduler) GetName() string {
	return b.name
}

func (b *balanceWitnessScheduler) GetType() string {
	return BalanceWitnessType
}

func (b *balanceWitnessScheduler) EncodeConfig() ([]byte, error) {
	b.conf.RLock()
	defer b.conf.RUnlock()
	return EncodeConfig(b.conf)
}

func (b *balanceWitnessScheduler) ReloadConfig() error {
	b.conf.Lock()
	defer b.conf.Unlock()
	cfgData, err := b.conf.storage.LoadSchedulerConfig(b.GetName())
	if err != nil {
		return err
	}
	if len(cfgData) == 0 {
		return nil
	}
	newCfg := &balanceWitnessSchedulerConfig{}
	if err = DecodeConfig([]byte(cfgData), newCfg); err != nil {
		return err
	}
	b.conf.Ranges = newCfg.Ranges
	b.conf.Batch = newCfg.Batch
	return nil
}

func (b *balanceWitnessScheduler) IsScheduleAllowed(cluster sche.SchedulerCluster) bool {
	allowed := b.OpController.OperatorCount(operator.OpWitness) < cluster.GetSchedulerConfig().GetWitnessScheduleLimit()
	if !allowed {
		operator.OperatorLimitCounter.WithLabelValues(b.GetType(), operator.OpWitness.String()).Inc()
	}
	return allowed
}

func (b *balanceWitnessScheduler) Schedule(cluster sche.SchedulerCluster, dryRun bool) ([]*operator.Operator, []plan.Plan) {
	basePlan := plan.NewBalanceSchedulerPlan()
	var collector *plan.Collector
	if dryRun {
		collector = plan.NewCollector(basePlan)
	}
	batch := b.conf.getBatch()
	schedulerCounter.WithLabelValues(b.GetName(), "schedule").Inc()

	opInfluence := b.OpController.GetOpInfluence(cluster.GetBasicCluster())
	kind := constant.NewScheduleKind(constant.WitnessKind, constant.ByCount)
	solver := newSolver(basePlan, kind, cluster, opInfluence)

	stores := cluster.GetStores()
	scoreFunc := func(store *core.StoreInfo) float64 {
		return store.WitnessScore(solver.GetOpInfluence(store.GetID()))
	}
	sourceCandidate := newCandidateStores(filter.SelectSourceStores(stores, b.filters, cluster.GetSchedulerConfig(), collector, b.filterCounter), false, scoreFunc)
	usedRegions := make(map[uint64]struct{})

	result := make([]*operator.Operator, 0, batch)
	if sourceCandidate.hasStore() {
		op := createTransferWitnessOperator(sourceCandidate, b, solver, usedRegions, collector)
		if op != nil {
			result = append(result, op)
			if len(result) >= batch {
				return result, collector.GetPlans()
			}
			makeInfluence(op, solver, usedRegions, sourceCandidate)
		}
	}
	b.retryQuota.GC(sourceCandidate.stores)
	return result, collector.GetPlans()
}

func createTransferWitnessOperator(cs *candidateStores, b *balanceWitnessScheduler,
	ssolver *solver, usedRegions map[uint64]struct{}, collector *plan.Collector) *operator.Operator {
	store := cs.getStore()
	ssolver.Step++
	defer func() { ssolver.Step-- }()
	retryLimit := b.retryQuota.GetLimit(store)
	ssolver.Source, ssolver.Target = store, nil
	var op *operator.Operator
	for i := 0; i < retryLimit; i++ {
		schedulerCounter.WithLabelValues(b.GetName(), "total").Inc()
		if op = b.transferWitnessOut(ssolver, collector); op != nil {
			if _, ok := usedRegions[op.RegionID()]; !ok {
				break
			}
			op = nil
		}
	}
	if op != nil {
		b.retryQuota.ResetLimit(store)
	} else {
		b.Attenuate(store)
		log.Debug("no operator created for selected stores", zap.String("scheduler", b.GetName()), zap.Uint64("transfer-out", store.GetID()))
		cs.next()
	}
	return op
}

// transferWitnessOut transfers witness from the source store.
// It randomly selects a health region from the source store, then picks
// the best follower peer and transfers the witness.
func (b *balanceWitnessScheduler) transferWitnessOut(solver *solver, collector *plan.Collector) *operator.Operator {
	solver.Region = filter.SelectOneRegion(solver.RandWitnessRegions(solver.SourceStoreID(), b.conf.getRanges()),
		collector, filter.NewRegionPendingFilter(), filter.NewRegionDownFilter())
	if solver.Region == nil {
		log.Debug("store has no witness", zap.String("scheduler", b.GetName()), zap.Uint64("store-id", solver.SourceStoreID()))
		schedulerCounter.WithLabelValues(b.GetName(), "no-witness-region").Inc()
		return nil
	}
	solver.Step++
	defer func() { solver.Step-- }()
	targets := solver.GetNonWitnessVoterStores(solver.Region)
	finalFilters := b.filters
	conf := solver.GetSchedulerConfig()
	if witnessFilter := filter.NewPlacementWitnessSafeguard(b.GetName(), conf, solver.GetBasicCluster(), solver.GetRuleManager(), solver.Region, solver.Source, solver.fit); witnessFilter != nil {
		finalFilters = append(b.filters, witnessFilter)
	}
	targets = filter.SelectTargetStores(targets, finalFilters, conf, collector, b.filterCounter)
	sort.Slice(targets, func(i, j int) bool {
		iOp := solver.GetOpInfluence(targets[i].GetID())
		jOp := solver.GetOpInfluence(targets[j].GetID())
		return targets[i].WitnessScore(iOp) < targets[j].WitnessScore(jOp)
	})
	for _, solver.Target = range targets {
		if op := b.createOperator(solver, collector); op != nil {
			return op
		}
	}
	log.Debug("region has no target store", zap.String("scheduler", b.GetName()), zap.Uint64("region-id", solver.Region.GetID()))
	schedulerCounter.WithLabelValues(b.GetName(), "no-target-store").Inc()
	return nil
}

// createOperator creates the operator according to the source and target store.
// If the region is hot or the difference between the two stores is tolerable, then
// no new operator need to be created, otherwise create an operator that transfers
// the witness from the source store to the target store for the region.
func (b *balanceWitnessScheduler) createOperator(solver *solver, collector *plan.Collector) *operator.Operator {
	solver.Step++
	defer func() { solver.Step-- }()
	solver.sourceScore, solver.targetScore = solver.sourceStoreScore(b.GetName()), solver.targetStoreScore(b.GetName())
	if !solver.shouldBalance(b.GetName()) {
		schedulerCounter.WithLabelValues(b.GetName(), "skip").Inc()
		if collector != nil {
			collector.Collect(plan.SetStatus(plan.NewStatus(plan.StatusStoreScoreDisallowed)))
		}
		return nil
	}
	solver.Step++
	defer func() { solver.Step-- }()
	op, err := operator.CreateMoveWitnessOperator(BalanceWitnessType, solver, solver.Region, solver.SourceStoreID(), solver.TargetStoreID())
	if err != nil {
		log.Debug("fail to create balance witness operator", errs.ZapError(err))
		return nil
	}
	op.Counters = append(op.Counters,
		schedulerCounter.WithLabelValues(b.GetName(), "new-operator"),
	)
	op.FinishedCounters = append(op.FinishedCounters,
		balanceDirectionCounter.WithLabelValues(b.GetName(), solver.SourceMetricLabel(), solver.TargetMetricLabel()),
		b.counter.WithLabelValues("move-witness", solver.SourceMetricLabel()+"-out"),
		b.counter.WithLabelValues("move-witness", solver.TargetMetricLabel()+"-in"),
	)
	op.AdditionalInfos["sourceScore"] = strconv.FormatFloat(solver.sourceScore, 'f', 2, 64)
	op.AdditionalInfos["targetScore"] = strconv.FormatFloat(solver.targetScore, 'f', 2, 64)
	return op
}
