// Copyright 2023 TiKV Project Authors.
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
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/unrolled/render"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/plan"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

const (
	alterEpsilon               = 1e-9
	minReCheckDurationGap      = 120 // default gap for re-check the slow node, unit: s
	defaultRecoveryDurationGap = 600 // default gap for recovery, unit: s.
)

type slowCandidate struct {
	storeID   uint64
	captureTS time.Time
	recoverTS time.Time
}

type evictSlowTrendSchedulerParam struct {
	// Duration gap for recovering the candidate, unit: s.
	RecoveryDurationGap uint64 `json:"recovery-duration"`
	// Only evict one store for now
	EvictedStores []uint64 `json:"evict-by-trend-stores"`
}

type evictSlowTrendSchedulerConfig struct {
	syncutil.RWMutex
	schedulerConfig
	evictSlowTrendSchedulerParam

	cluster *core.BasicCluster
	// Candidate for eviction in current tick.
	evictCandidate slowCandidate
	// Last chosen candidate for eviction.
	lastEvictCandidate slowCandidate
}

func initEvictSlowTrendSchedulerConfig() *evictSlowTrendSchedulerConfig {
	return &evictSlowTrendSchedulerConfig{
		schedulerConfig:    &baseSchedulerConfig{},
		evictCandidate:     slowCandidate{},
		lastEvictCandidate: slowCandidate{},
		evictSlowTrendSchedulerParam: evictSlowTrendSchedulerParam{
			RecoveryDurationGap: defaultRecoveryDurationGap,
			EvictedStores:       make([]uint64, 0),
		},
	}
}

func (conf *evictSlowTrendSchedulerConfig) clone() *evictSlowTrendSchedulerParam {
	conf.RLock()
	defer conf.RUnlock()
	return &evictSlowTrendSchedulerParam{
		RecoveryDurationGap: conf.RecoveryDurationGap,
	}
}

func (conf *evictSlowTrendSchedulerConfig) getStores() []uint64 {
	conf.RLock()
	defer conf.RUnlock()
	return conf.EvictedStores
}

func (conf *evictSlowTrendSchedulerConfig) getKeyRangesByID(id uint64) []core.KeyRange {
	if conf.evictedStore() != id {
		return nil
	}
	return []core.KeyRange{core.NewKeyRange("", "")}
}

func (*evictSlowTrendSchedulerConfig) getBatch() int {
	return EvictLeaderBatchSize
}

func (conf *evictSlowTrendSchedulerConfig) hasEvictedStores() bool {
	conf.RLock()
	defer conf.RUnlock()
	return len(conf.EvictedStores) > 0
}

func (conf *evictSlowTrendSchedulerConfig) evictedStore() uint64 {
	if !conf.hasEvictedStores() {
		return 0
	}
	conf.RLock()
	defer conf.RUnlock()
	// If a candidate passes all checks and proved to be slow, it will be
	// recorded in `conf.EvictStores`, and `conf.lastEvictCandidate` will record
	// the captured timestamp of this store.
	return conf.EvictedStores[0]
}

func (conf *evictSlowTrendSchedulerConfig) candidate() uint64 {
	conf.RLock()
	defer conf.RUnlock()
	return conf.evictCandidate.storeID
}

func (conf *evictSlowTrendSchedulerConfig) captureTS() time.Time {
	conf.RLock()
	defer conf.RUnlock()
	return conf.evictCandidate.captureTS
}

func (conf *evictSlowTrendSchedulerConfig) candidateCapturedSecs() uint64 {
	conf.RLock()
	defer conf.RUnlock()
	return DurationSinceAsSecs(conf.evictCandidate.captureTS)
}

func (conf *evictSlowTrendSchedulerConfig) lastCapturedCandidate() *slowCandidate {
	conf.RLock()
	defer conf.RUnlock()
	return &conf.lastEvictCandidate
}

func (conf *evictSlowTrendSchedulerConfig) lastCandidateCapturedSecs() uint64 {
	return DurationSinceAsSecs(conf.lastEvictCandidate.captureTS)
}

// readyForRecovery checks whether the last captured candidate is ready for recovery.
func (conf *evictSlowTrendSchedulerConfig) readyForRecovery() bool {
	conf.RLock()
	defer conf.RUnlock()
	recoveryDurationGap := conf.RecoveryDurationGap
	failpoint.Inject("transientRecoveryGap", func() {
		recoveryDurationGap = 0
	})
	return conf.lastCandidateCapturedSecs() >= recoveryDurationGap
}

func (conf *evictSlowTrendSchedulerConfig) captureCandidate(id uint64) {
	conf.Lock()
	defer conf.Unlock()
	conf.evictCandidate = slowCandidate{
		storeID:   id,
		captureTS: time.Now(),
		recoverTS: time.Now(),
	}
	if conf.lastEvictCandidate == (slowCandidate{}) {
		conf.lastEvictCandidate = conf.evictCandidate
	}
}

func (conf *evictSlowTrendSchedulerConfig) popCandidate(updLast bool) uint64 {
	conf.Lock()
	defer conf.Unlock()
	id := conf.evictCandidate.storeID
	if updLast {
		conf.lastEvictCandidate = conf.evictCandidate
	}
	conf.evictCandidate = slowCandidate{}
	return id
}

func (conf *evictSlowTrendSchedulerConfig) markCandidateRecovered() {
	conf.Lock()
	defer conf.Unlock()
	if conf.lastEvictCandidate != (slowCandidate{}) {
		conf.lastEvictCandidate.recoverTS = time.Now()
	}
}

func (conf *evictSlowTrendSchedulerConfig) setStoreAndPersist(id uint64) error {
	conf.Lock()
	defer conf.Unlock()
	conf.EvictedStores = []uint64{id}
	return conf.save()
}

func (conf *evictSlowTrendSchedulerConfig) clearAndPersist(cluster sche.SchedulerCluster) (oldID uint64, err error) {
	oldID = conf.evictedStore()
	if oldID == 0 {
		return
	}
	address := "?"
	store := cluster.GetStore(oldID)
	if store != nil {
		address = store.GetAddress()
	}
	storeSlowTrendEvictedStatusGauge.WithLabelValues(address, strconv.FormatUint(oldID, 10)).Set(0)
	conf.Lock()
	defer conf.Unlock()
	conf.EvictedStores = []uint64{}
	return oldID, conf.save()
}

type evictSlowTrendHandler struct {
	rd     *render.Render
	config *evictSlowTrendSchedulerConfig
}

func newEvictSlowTrendHandler(config *evictSlowTrendSchedulerConfig) http.Handler {
	h := &evictSlowTrendHandler{
		config: config,
		rd:     render.New(render.Options{IndentJSON: true}),
	}
	router := mux.NewRouter()
	router.HandleFunc("/config", h.updateConfig).Methods(http.MethodPost)
	router.HandleFunc("/list", h.listConfig).Methods(http.MethodGet)
	return router
}

func (handler *evictSlowTrendHandler) updateConfig(w http.ResponseWriter, r *http.Request) {
	var input map[string]any
	if err := apiutil.ReadJSONRespondError(handler.rd, w, r.Body, &input); err != nil {
		return
	}
	recoveryDurationGapFloat, ok := input["recovery-duration"].(float64)
	if !ok {
		handler.rd.JSON(w, http.StatusInternalServerError, errors.New("invalid argument for 'recovery-duration'").Error())
		return
	}
	handler.config.Lock()
	defer handler.config.Unlock()
	prevRecoveryDurationGap := handler.config.RecoveryDurationGap
	recoveryDurationGap := uint64(recoveryDurationGapFloat)
	handler.config.RecoveryDurationGap = recoveryDurationGap
	if err := handler.config.save(); err != nil {
		handler.rd.JSON(w, http.StatusInternalServerError, err.Error())
		handler.config.RecoveryDurationGap = prevRecoveryDurationGap
		return
	}
	log.Info("evict-slow-trend-scheduler update 'recovery-duration' - unit: s", zap.Uint64("prev", prevRecoveryDurationGap), zap.Uint64("cur", recoveryDurationGap))
	handler.rd.JSON(w, http.StatusOK, "Config updated.")
}

func (handler *evictSlowTrendHandler) listConfig(w http.ResponseWriter, _ *http.Request) {
	conf := handler.config.clone()
	handler.rd.JSON(w, http.StatusOK, conf)
}

type evictSlowTrendScheduler struct {
	*BaseScheduler
	conf    *evictSlowTrendSchedulerConfig
	handler http.Handler
}

// GetNextInterval implements the Scheduler interface.
func (s *evictSlowTrendScheduler) GetNextInterval(time.Duration) time.Duration {
	var growthType intervalGrowthType
	// If it already found a slow node as candidate, the next interval should be shorter
	// to make the next scheduling as soon as possible. This adjustment will decrease the
	// response time, as heartbeats from other nodes will be received and updated more quickly.
	if s.conf.hasEvictedStores() {
		growthType = zeroGrowth
	} else {
		growthType = exponentialGrowth
	}
	return intervalGrow(s.GetMinInterval(), MaxScheduleInterval, growthType)
}

// ServeHTTP implements the http.Handler interface.
func (s *evictSlowTrendScheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

// EncodeConfig implements the Scheduler interface.
func (s *evictSlowTrendScheduler) EncodeConfig() ([]byte, error) {
	return EncodeConfig(s.conf)
}

// ReloadConfig implements the Scheduler interface.
func (s *evictSlowTrendScheduler) ReloadConfig() error {
	s.conf.Lock()
	defer s.conf.Unlock()
	newCfg := &evictSlowTrendSchedulerConfig{}
	if err := s.conf.load(newCfg); err != nil {
		return err
	}

	old := make(map[uint64]struct{})
	for _, id := range s.conf.EvictedStores {
		old[id] = struct{}{}
	}
	new := make(map[uint64]struct{})
	for _, id := range newCfg.EvictedStores {
		new[id] = struct{}{}
	}
	pauseAndResumeLeaderTransfer(s.conf.cluster, constant.In, old, new)
	s.conf.RecoveryDurationGap = newCfg.RecoveryDurationGap
	s.conf.EvictedStores = newCfg.EvictedStores
	return nil
}

// PrepareConfig implements the Scheduler interface.
func (s *evictSlowTrendScheduler) PrepareConfig(cluster sche.SchedulerCluster) error {
	evictedStoreID := s.conf.evictedStore()
	if evictedStoreID == 0 {
		return nil
	}
	return cluster.SlowTrendEvicted(evictedStoreID)
}

// CleanConfig implements the Scheduler interface.
func (s *evictSlowTrendScheduler) CleanConfig(cluster sche.SchedulerCluster) {
	s.cleanupEvictLeader(cluster)
}

func (s *evictSlowTrendScheduler) prepareEvictLeader(cluster sche.SchedulerCluster, storeID uint64) error {
	err := s.conf.setStoreAndPersist(storeID)
	if err != nil {
		log.Info("evict-slow-trend-scheduler persist config failed", zap.Uint64("store-id", storeID))
		return err
	}
	return cluster.SlowTrendEvicted(storeID)
}

func (s *evictSlowTrendScheduler) cleanupEvictLeader(cluster sche.SchedulerCluster) {
	evictedStoreID, err := s.conf.clearAndPersist(cluster)
	if err != nil {
		log.Info("evict-slow-trend-scheduler persist config failed", zap.Uint64("store-id", evictedStoreID))
	}
	if evictedStoreID != 0 {
		// Assertion: evictStoreID == s.conf.LastEvictCandidate.storeID
		s.conf.markCandidateRecovered()
		cluster.SlowTrendRecovered(evictedStoreID)
	}
}

func (s *evictSlowTrendScheduler) scheduleEvictLeader(cluster sche.SchedulerCluster) []*operator.Operator {
	store := cluster.GetStore(s.conf.evictedStore())
	if store == nil {
		return nil
	}
	storeSlowTrendEvictedStatusGauge.WithLabelValues(store.GetAddress(), strconv.FormatUint(store.GetID(), 10)).Set(1)
	return scheduleEvictLeaderBatch(s.R, s.GetName(), cluster, s.conf)
}

// IsScheduleAllowed implements the Scheduler interface.
func (s *evictSlowTrendScheduler) IsScheduleAllowed(cluster sche.SchedulerCluster) bool {
	if s.conf.evictedStore() == 0 {
		return true
	}
	allowed := s.OpController.OperatorCount(operator.OpLeader) < cluster.GetSchedulerConfig().GetLeaderScheduleLimit()
	if !allowed {
		operator.IncOperatorLimitCounter(s.GetType(), operator.OpLeader)
	}
	return allowed
}

// Schedule implements the Scheduler interface.
func (s *evictSlowTrendScheduler) Schedule(cluster sche.SchedulerCluster, _ bool) ([]*operator.Operator, []plan.Plan) {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()

	var ops []*operator.Operator

	if s.conf.evictedStore() != 0 {
		store := cluster.GetStore(s.conf.evictedStore())
		if store == nil || store.IsRemoved() {
			// Previous slow store had been removed, remove the scheduler and check
			// slow node next time.
			log.Info("store evicted by slow trend has been removed", zap.Uint64("store-id", store.GetID()))
			storeSlowTrendActionStatusGauge.WithLabelValues("evict", "stop_removed").Inc()
		} else if checkStoreCanRecover(cluster, store) && s.conf.readyForRecovery() {
			log.Info("store evicted by slow trend has been recovered", zap.Uint64("store-id", store.GetID()))
			storeSlowTrendActionStatusGauge.WithLabelValues("evict", "stop_recovered").Inc()
		} else {
			storeSlowTrendActionStatusGauge.WithLabelValues("evict", "continue").Inc()
			return s.scheduleEvictLeader(cluster), nil
		}
		s.cleanupEvictLeader(cluster)
		return ops, nil
	}

	candFreshCaptured := false
	if s.conf.candidate() == 0 {
		candidate := chooseEvictCandidate(cluster, s.conf.lastCapturedCandidate())
		if candidate != nil {
			storeSlowTrendActionStatusGauge.WithLabelValues("candidate", "captured").Inc()
			s.conf.captureCandidate(candidate.GetID())
			candFreshCaptured = true
		}
	} else {
		storeSlowTrendActionStatusGauge.WithLabelValues("candidate", "continue").Inc()
	}

	slowStoreID := s.conf.candidate()
	if slowStoreID == 0 {
		storeSlowTrendActionStatusGauge.WithLabelValues("candidate", "none").Inc()
		return ops, nil
	}

	slowStore := cluster.GetStore(slowStoreID)
	if !candFreshCaptured && checkStoreFasterThanOthers(cluster, slowStore) {
		s.conf.popCandidate(false)
		log.Info("slow store candidate by trend has been cancel", zap.Uint64("store-id", slowStoreID))
		storeSlowTrendActionStatusGauge.WithLabelValues("candidate", "canceled_too_faster").Inc()
		return ops, nil
	}
	if slowStoreRecordTS := s.conf.captureTS(); !checkStoresAreUpdated(cluster, slowStoreID, slowStoreRecordTS) {
		log.Info("slow store candidate waiting for other stores to update heartbeats", zap.Uint64("store-id", slowStoreID))
		storeSlowTrendActionStatusGauge.WithLabelValues("candidate", "wait").Inc()
		return ops, nil
	}

	candCapturedSecs := s.conf.candidateCapturedSecs()
	log.Info("detected slow store by trend, start to evict leaders",
		zap.Uint64("store-id", slowStoreID),
		zap.Uint64("candidate-captured-secs", candCapturedSecs))
	storeSlowTrendMiscGauge.WithLabelValues("candidate", "captured_secs").Set(float64(candCapturedSecs))
	if err := s.prepareEvictLeader(cluster, s.conf.popCandidate(true)); err != nil {
		log.Info("prepare for evicting leader by slow trend failed", zap.Error(err), zap.Uint64("store-id", slowStoreID))
		storeSlowTrendActionStatusGauge.WithLabelValues("evict", "prepare_err").Inc()
		return ops, nil
	}
	storeSlowTrendActionStatusGauge.WithLabelValues("evict", "start").Inc()
	return s.scheduleEvictLeader(cluster), nil
}

func newEvictSlowTrendScheduler(opController *operator.Controller, conf *evictSlowTrendSchedulerConfig) Scheduler {
	handler := newEvictSlowTrendHandler(conf)
	sche := &evictSlowTrendScheduler{
		BaseScheduler: NewBaseScheduler(opController, types.EvictSlowTrendScheduler, conf),
		conf:          conf,
		handler:       handler,
	}
	return sche
}

func chooseEvictCandidate(cluster sche.SchedulerCluster, lastEvictCandidate *slowCandidate) (slowStore *core.StoreInfo) {
	isRaftKV2 := cluster.GetStoreConfig().IsRaftKV2()
	failpoint.Inject("mockRaftKV2", func() {
		isRaftKV2 = true
	})
	stores := cluster.GetStores()
	if len(stores) < 3 {
		storeSlowTrendActionStatusGauge.WithLabelValues("candidate", "none_too_few").Inc()
		return
	}

	var candidates []*core.StoreInfo
	var affectedStoreCount int
	for _, store := range stores {
		if store.IsRemoved() {
			continue
		}
		if !(store.IsPreparing() || store.IsServing()) {
			continue
		}
		if slowTrend := store.GetSlowTrend(); slowTrend != nil {
			if slowTrend.ResultRate < -alterEpsilon {
				affectedStoreCount += 1
			}
			// For the cases of disk io jitters.
			// Normally, if there exists jitters on disk io or network io, the slow store must have a descending
			// trend on QPS and ascending trend on duration. So, the slowTrend must match the following pattern.
			if slowTrend.CauseRate > alterEpsilon && slowTrend.ResultRate < -alterEpsilon {
				candidates = append(candidates, store)
				storeSlowTrendActionStatusGauge.WithLabelValues("candidate", "add").Inc()
				log.Info("evict-slow-trend-scheduler pre-captured candidate",
					zap.Uint64("store-id", store.GetID()),
					zap.Float64("cause-rate", slowTrend.CauseRate),
					zap.Float64("result-rate", slowTrend.ResultRate),
					zap.Float64("cause-value", slowTrend.CauseValue),
					zap.Float64("result-value", slowTrend.ResultValue))
			} else if isRaftKV2 && slowTrend.CauseRate > alterEpsilon {
				// Meanwhile, if the store was previously experiencing slowness in the `Duration` dimension, it should
				// re-check whether this node is still encountering network I/O-related jitters. And If this node matches
				// the last identified candidate, it indicates that the node is still being affected by delays in network I/O,
				// and consequently, it should be re-designated as slow once more.
				// Prerequisite: `raft-kv2` engine has the ability to percept the slow trend on network io jitters.
				// TODO: maybe make it compatible to `raft-kv` later.
				if lastEvictCandidate != nil && lastEvictCandidate.storeID == store.GetID() && DurationSinceAsSecs(lastEvictCandidate.recoverTS) <= minReCheckDurationGap {
					candidates = append(candidates, store)
					storeSlowTrendActionStatusGauge.WithLabelValues("candidate", "add").Inc()
					log.Info("evict-slow-trend-scheduler pre-captured candidate in raft-kv2 cluster",
						zap.Uint64("store-id", store.GetID()),
						zap.Float64("cause-rate", slowTrend.CauseRate),
						zap.Float64("result-rate", slowTrend.ResultRate),
						zap.Float64("cause-value", slowTrend.CauseValue),
						zap.Float64("result-value", slowTrend.ResultValue))
				}
			}
		}
	}
	if len(candidates) == 0 {
		storeSlowTrendActionStatusGauge.WithLabelValues("candidate", "none_no_fit").Inc()
		return
	}
	// TODO: Calculate to judge if one store is way slower than the others
	if len(candidates) > 1 {
		storeSlowTrendActionStatusGauge.WithLabelValues("candidate", "none_too_many").Inc()
		return
	}

	store := candidates[0]

	affectedStoreThreshold := int(float64(len(stores)) * cluster.GetSchedulerConfig().GetSlowStoreEvictingAffectedStoreRatioThreshold())
	if affectedStoreCount < affectedStoreThreshold {
		log.Info("evict-slow-trend-scheduler failed to confirm candidate: it only affect a few stores", zap.Uint64("store-id", store.GetID()))
		storeSlowTrendActionStatusGauge.WithLabelValues("candidate", "none_affect_a_few").Inc()
		return
	}

	if !checkStoreSlowerThanOthers(cluster, store) {
		log.Info("evict-slow-trend-scheduler failed to confirm candidate: it's not slower than others", zap.Uint64("store-id", store.GetID()))
		storeSlowTrendActionStatusGauge.WithLabelValues("candidate", "none_not_slower").Inc()
		return
	}

	storeSlowTrendActionStatusGauge.WithLabelValues("candidate", "add").Inc()
	log.Info("evict-slow-trend-scheduler captured candidate", zap.Uint64("store-id", store.GetID()))
	return store
}

func checkStoresAreUpdated(cluster sche.SchedulerCluster, slowStoreID uint64, slowStoreRecordTS time.Time) bool {
	stores := cluster.GetStores()
	if len(stores) <= 1 {
		return false
	}
	expected := (len(stores) + 1) / 2
	updatedStores := 0
	for _, store := range stores {
		if store.IsRemoved() {
			updatedStores += 1
			continue
		}
		if !(store.IsPreparing() || store.IsServing()) {
			updatedStores += 1
			continue
		}
		if store.GetID() == slowStoreID {
			updatedStores += 1
			continue
		}
		if slowStoreRecordTS.Compare(store.GetLastHeartbeatTS()) <= 0 {
			updatedStores += 1
		}
	}
	storeSlowTrendMiscGauge.WithLabelValues("store", "check_updated_count").Set(float64(updatedStores))
	storeSlowTrendMiscGauge.WithLabelValues("store", "check_updated_expected").Set(float64(expected))
	return updatedStores >= expected
}

func checkStoreSlowerThanOthers(cluster sche.SchedulerCluster, target *core.StoreInfo) bool {
	stores := cluster.GetStores()
	expected := (len(stores)*2 + 1) / 3
	targetSlowTrend := target.GetSlowTrend()
	if targetSlowTrend == nil {
		storeSlowTrendActionStatusGauge.WithLabelValues("candidate", "check_slower_no_data").Inc()
		return false
	}
	slowerThanStoresNum := 0
	for _, store := range stores {
		if store.IsRemoved() {
			continue
		}
		if !(store.IsPreparing() || store.IsServing()) {
			continue
		}
		if store.GetID() == target.GetID() {
			continue
		}
		slowTrend := store.GetSlowTrend()
		// Use `SlowTrend.ResultValue` at first, but not good, `CauseValue` is better
		// Greater `CauseValue` means slower
		if slowTrend != nil && (targetSlowTrend.CauseValue-slowTrend.CauseValue) > alterEpsilon && slowTrend.CauseValue > alterEpsilon {
			slowerThanStoresNum += 1
		}
	}
	storeSlowTrendMiscGauge.WithLabelValues("store", "check_slower_count").Set(float64(slowerThanStoresNum))
	storeSlowTrendMiscGauge.WithLabelValues("store", "check_slower_expected").Set(float64(expected))
	return slowerThanStoresNum >= expected
}

func checkStoreCanRecover(cluster sche.SchedulerCluster, target *core.StoreInfo) bool {
	/*
		//
		// This might not be necessary,
		//   and it also have tiny chances to cause `stuck in evicted`
		//   status when this store restarted,
		//   the `become fast` might be ignore on tikv side
		//   because of the detecting windows are not fully filled yet.
		// Hence, we disabled this event capturing by now but keep the code here for further checking.
		//

		// Wait for the evicted store's `become fast` event
		slowTrend := target.GetSlowTrend()
		if slowTrend == nil || slowTrend.CauseRate >= 0 && slowTrend.ResultRate <= 0 {
			storeSlowTrendActionStatusGauge.WithLabelValues("recover.reject:no-event").Inc()
			return false
		} else {
			storeSlowTrendActionStatusGauge.WithLabelValues("recover.judging:got-event").Inc()
		}
	*/
	return checkStoreFasterThanOthers(cluster, target)
}

func checkStoreFasterThanOthers(cluster sche.SchedulerCluster, target *core.StoreInfo) bool {
	stores := cluster.GetStores()
	expected := (len(stores) + 1) / 2
	targetSlowTrend := target.GetSlowTrend()
	if targetSlowTrend == nil {
		storeSlowTrendActionStatusGauge.WithLabelValues("candidate", "check_faster_no_data").Inc()
		return false
	}
	fasterThanStores := 0
	for _, store := range stores {
		if store.IsRemoved() {
			continue
		}
		if !(store.IsPreparing() || store.IsServing()) {
			continue
		}
		if store.GetID() == target.GetID() {
			continue
		}
		slowTrend := store.GetSlowTrend()
		// Greater `CauseValue` means slower
		if slowTrend != nil && targetSlowTrend.CauseValue <= slowTrend.CauseValue*1.1 &&
			slowTrend.CauseValue > alterEpsilon && targetSlowTrend.CauseValue > alterEpsilon {
			fasterThanStores += 1
		}
	}
	storeSlowTrendMiscGauge.WithLabelValues("store", "check_faster_count").Set(float64(fasterThanStores))
	storeSlowTrendMiscGauge.WithLabelValues("store", "check_faster_expected").Set(float64(expected))
	return fasterThanStores >= expected
}

// DurationSinceAsSecs returns the duration gap since the given startTS, unit: s.
func DurationSinceAsSecs(startTS time.Time) uint64 {
	return uint64(time.Since(startTS).Seconds())
}
