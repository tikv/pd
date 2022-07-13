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
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/reflectutil"
	"github.com/tikv/pd/pkg/syncutil"
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
	// BalanceWitnessName is balance witness scheduler name.
	BalanceWitnessName = "balance-witness-scheduler"
	// BalanceWitnessType is balance witness scheduler type.
	BalanceWitnessType = "balance-witness"
	// BalanceWitnessRetryLimit is the limit to retry schedule for selected source store and target store.
	BalanceWitnessRetryLimit = 10
	// BalanceWitnessBatchSize is the default number of operators to transfer witnesss by one scheduling.
	// Default value is 4 which is subjected by scheduler-max-waiting-operator and witness-schedule-limit
	// If you want to increase balance speed more, please increase above-mentioned param.
	BalanceWitnessBatchSize = 4
	// MaxBalanceWitnessBatchSize is maximum of balance witness batch size
	MaxBalanceWitnessBatchSize = 10
)

func init() {
	schedule.RegisterSliceDecoderBuilder(BalanceWitnessType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			conf, ok := v.(*BalanceWitnessSchedulerConfig)
			if !ok {
				return errs.ErrScheduleConfigNotExist.FastGenByArgs()
			}
			ranges, err := getKeyRanges(args)
			if err != nil {
				return err
			}
			conf.Ranges = ranges
			conf.Batch = BalanceWitnessBatchSize
			return nil
		}
	})

	schedule.RegisterScheduler(BalanceWitnessType, func(opController *schedule.OperatorController, storage endpoint.ConfigStorage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &BalanceWitnessSchedulerConfig{storage: storage}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		if conf.Batch == 0 {
			conf.Batch = BalanceWitnessBatchSize
		}
		return newBalanceWitnessScheduler(opController, conf), nil
	})
}

type BalanceWitnessSchedulerConfig struct {
	mu      syncutil.RWMutex
	storage endpoint.ConfigStorage
	Ranges  []core.KeyRange `json:"ranges"`
	// Batch is used to generate multiple operators by one scheduling
	Batch int `json:"batch"`
}

func (conf *BalanceWitnessSchedulerConfig) Update(data []byte) (int, interface{}) {
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

func (conf *BalanceWitnessSchedulerConfig) validate() bool {
	return conf.Batch >= 1 && conf.Batch <= 10
}

func (conf *BalanceWitnessSchedulerConfig) Clone() *BalanceWitnessSchedulerConfig {
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	ranges := make([]core.KeyRange, len(conf.Ranges))
	copy(ranges, conf.Ranges)
	return &BalanceWitnessSchedulerConfig{
		Ranges: ranges,
		Batch:  conf.Batch,
	}
}

func (conf *BalanceWitnessSchedulerConfig) persistLocked() error {
	data, err := schedule.EncodeConfig(conf)
	if err != nil {
		return err
	}
	return conf.storage.SaveScheduleConfig(BalanceWitnessName, data)
}

type BalanceWitnessHandler struct {
	rd     *render.Render
	config *BalanceWitnessSchedulerConfig
}

func newBalanceWitnessHandler(conf *BalanceWitnessSchedulerConfig) http.Handler {
	handler := &BalanceWitnessHandler{
		config: conf,
		rd:     render.New(render.Options{IndentJSON: true}),
	}
	router := mux.NewRouter()
	router.HandleFunc("/config", handler.UpdateConfig).Methods(http.MethodPost)
	router.HandleFunc("/list", handler.ListConfig).Methods(http.MethodGet)
	return router
}

func (handler *BalanceWitnessHandler) UpdateConfig(w http.ResponseWriter, r *http.Request) {
	data, _ := io.ReadAll(r.Body)
	r.Body.Close()
	httpCode, v := handler.config.Update(data)
	handler.rd.JSON(w, httpCode, v)
}

func (handler *BalanceWitnessHandler) ListConfig(w http.ResponseWriter, r *http.Request) {
	conf := handler.config.Clone()
	handler.rd.JSON(w, http.StatusOK, conf)
}

type BalanceWitnessScheduler struct {
	*BaseScheduler
	*retryQuota
	name         string
	conf         *BalanceWitnessSchedulerConfig
	handler      http.Handler
	opController *schedule.OperatorController
	filters      []filter.Filter
	counter      *prometheus.CounterVec
}

// newBalanceWitnessScheduler creates a scheduler that tends to keep witnesss on
// each store balanced.
func newBalanceWitnessScheduler(opController *schedule.OperatorController, conf *BalanceWitnessSchedulerConfig, options ...BalanceWitnessCreateOption) schedule.Scheduler {
	base := NewBaseScheduler(opController)
	s := &BalanceWitnessScheduler{
		BaseScheduler: base,
		retryQuota:    newRetryQuota(BalanceWitnessRetryLimit, defaultMinRetryLimit, defaultRetryQuotaAttenuation),
		name:          BalanceWitnessName,
		conf:          conf,
		handler:       newBalanceWitnessHandler(conf),
		opController:  opController,
		counter:       balanceWitnessCounter,
	}
	for _, option := range options {
		option(s)
	}
	s.filters = []filter.Filter{
		&filter.StoreStateFilter{ActionScope: s.GetName(), MoveRegion: true},
		filter.NewSpecialUseFilter(s.GetName()),
	}
	return s
}

func (l *BalanceWitnessScheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	l.handler.ServeHTTP(w, r)
}

// BalanceWitnessCreateOption is used to create a scheduler with an option.
type BalanceWitnessCreateOption func(s *BalanceWitnessScheduler)

// WithBalanceWitnessCounter sets the counter for the scheduler.
func WithBalanceWitnessCounter(counter *prometheus.CounterVec) BalanceWitnessCreateOption {
	return func(s *BalanceWitnessScheduler) {
		s.counter = counter
	}
}

// WithBalanceWitnessName sets the name for the scheduler.
func WithBalanceWitnessName(name string) BalanceWitnessCreateOption {
	return func(s *BalanceWitnessScheduler) {
		s.name = name
	}
}

func (l *BalanceWitnessScheduler) GetName() string {
	return l.name
}

func (l *BalanceWitnessScheduler) GetType() string {
	return BalanceWitnessType
}

func (l *BalanceWitnessScheduler) EncodeConfig() ([]byte, error) {
	l.conf.mu.RLock()
	defer l.conf.mu.RUnlock()
	return schedule.EncodeConfig(l.conf)
}

func (l *BalanceWitnessScheduler) IsScheduleAllowed(cluster schedule.Cluster) bool {
	allowed := l.opController.OperatorCount(operator.OpWitness) < cluster.GetOpts().GetRegionScheduleLimit() // TODO: add witness schedule limit
	if !allowed {
		operator.OperatorLimitCounter.WithLabelValues(l.GetType(), operator.OpWitness.String()).Inc()
	}
	return allowed
}

func (l *BalanceWitnessScheduler) Schedule(cluster schedule.Cluster, dryRun bool) ([]*operator.Operator, []plan.Plan) {
	l.conf.mu.RLock()
	defer l.conf.mu.RUnlock()
	batch := l.conf.Batch
	schedulerCounter.WithLabelValues(l.GetName(), "schedule").Inc()

	opInfluence := l.opController.GetOpInfluence(cluster)
	kind := core.NewScheduleKind(core.WitnessKind, core.ByCount)
	plan := newBalancePlan(kind, cluster, opInfluence)

	stores := cluster.GetStores()
	scoreFunc := func(store *core.StoreInfo) float64 {
		return store.WitnessScore(plan.GetOpInfluence(store.GetID()))
	}
	sourceCandidate := newCandidateStores(filter.SelectSourceStores(stores, l.filters, cluster.GetOpts()), false, scoreFunc)
	usedRegions := make(map[uint64]struct{})

	result := make([]*operator.Operator, 0, batch)
	if sourceCandidate.hasStore() {
		op := createTransferWitnessOperator(sourceCandidate, l, plan, usedRegions)
		if op != nil {
			result = append(result, op)
			if len(result) >= batch {
				return result, nil
			}
			makeInfluence(op, plan, usedRegions, sourceCandidate)
		}
	}
	l.retryQuota.GC(sourceCandidate.stores)
	return result, nil
}

func createTransferWitnessOperator(cs *candidateStores, l *BalanceWitnessScheduler,
	plan *balancePlan, usedRegions map[uint64]struct{}) *operator.Operator {
	store := cs.getStore()
	retryLimit := l.retryQuota.GetLimit(store)
	var creator func(*balancePlan) *operator.Operator

	plan.source, plan.target = store, nil
	creator = l.transferWitnessOut

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
		op.Counters = append(op.Counters, l.counter.WithLabelValues("transfer-out", plan.SourceMetricLabel()))
	} else {
		l.Attenuate(store)
		log.Debug("no operator created for selected stores", zap.String("scheduler", l.GetName()), zap.Uint64("transfer-out", store.GetID()))
		cs.next()
	}
	return op
}

// transferWitnessOut transfers witness from the source store.
// It randomly selects a health region from the source store, then picks
// the best follower peer and transfers the witness.
func (l *BalanceWitnessScheduler) transferWitnessOut(plan *balancePlan) *operator.Operator {
	plan.region = plan.RandWitnessRegion(plan.SourceStoreID(), l.conf.Ranges, schedule.IsRegionHealthy)
	if plan.region == nil {
		log.Debug("store has no witness", zap.String("scheduler", l.GetName()), zap.Uint64("store-id", plan.SourceStoreID()))
		schedulerCounter.WithLabelValues(l.GetName(), "no-witness-region").Inc()
		return nil
	}
	targets := plan.GetNonWitnessVoterStores(plan.region)
	finalFilters := l.filters
	opts := plan.GetOpts()
	if witnessFilter := filter.NewPlacementWitnessSafeguard(l.GetName(), opts, plan.GetBasicCluster(), plan.GetRuleManager(), plan.region, plan.source); witnessFilter != nil {
		finalFilters = append(l.filters, witnessFilter)
	}
	targets = filter.SelectTargetStores(targets, finalFilters, opts)
	sort.Slice(targets, func(i, j int) bool {
		iOp := plan.GetOpInfluence(targets[i].GetID())
		jOp := plan.GetOpInfluence(targets[j].GetID())
		return targets[i].WitnessScore(iOp) < targets[j].WitnessScore(jOp)
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

// createOperator creates the operator according to the source and target store.
// If the region is hot or the difference between the two stores is tolerable, then
// no new operator need to be created, otherwise create an operator that transfers
// the witness from the source store to the target store for the region.
func (l *BalanceWitnessScheduler) createOperator(plan *balancePlan) *operator.Operator {
	if !plan.shouldBalance(l.GetName()) {
		schedulerCounter.WithLabelValues(l.GetName(), "skip").Inc()
		return nil
	}

	op, err := operator.CreateMoveWitnessOperator(BalanceWitnessType, plan, plan.region, plan.SourceStoreID(), plan.TargetStoreID(), operator.OpWitness)
	if err != nil {
		log.Debug("fail to create balance witness operator", errs.ZapError(err))
		return nil
	}
	op.Counters = append(op.Counters,
		schedulerCounter.WithLabelValues(l.GetName(), "new-operator"),
	)
	op.FinishedCounters = append(op.FinishedCounters,
		balanceDirectionCounter.WithLabelValues(l.GetName(), plan.SourceMetricLabel(), plan.TargetMetricLabel()),
		l.counter.WithLabelValues("move-witness", plan.SourceMetricLabel()+"-out"),
		l.counter.WithLabelValues("move-witness", plan.TargetMetricLabel()+"-in"),
	)
	op.AdditionalInfos["sourceScore"] = strconv.FormatFloat(plan.sourceScore, 'f', 2, 64)
	op.AdditionalInfos["targetScore"] = strconv.FormatFloat(plan.targetScore, 'f', 2, 64)
	return op
}
