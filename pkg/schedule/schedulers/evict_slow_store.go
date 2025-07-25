// Copyright 2021 TiKV Project Authors.
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
	"github.com/tikv/pd/pkg/utils/keyutil"
)

const (
	slowStoreEvictThreshold   = 100
	slowStoreRecoverThreshold = 1
)

type evictSlowStoreSchedulerConfig struct {
	baseDefaultSchedulerConfig

	cluster *core.BasicCluster
	// Last timestamp of the chosen slow store for eviction.
	lastSlowStoreCaptureTS time.Time
	isRecovered            bool
	// Duration gap for recovering the candidate, unit: s.
	RecoveryDurationGap uint64   `json:"recovery-duration"`
	EvictedStores       []uint64 `json:"evict-stores"`
	// TODO: We only add batch for evict-slow-store-scheduler now.
	// If necessary, we also need to support evict-slow-trend-scheduler.
	Batch int `json:"batch"`
}

func initEvictSlowStoreSchedulerConfig() *evictSlowStoreSchedulerConfig {
	return &evictSlowStoreSchedulerConfig{
		baseDefaultSchedulerConfig: newBaseDefaultSchedulerConfig(),
		lastSlowStoreCaptureTS:     time.Time{},
		RecoveryDurationGap:        defaultRecoveryDurationGap,
		EvictedStores:              make([]uint64, 0),
		Batch:                      EvictLeaderBatchSize,
	}
}

func (conf *evictSlowStoreSchedulerConfig) clone() *evictSlowStoreSchedulerConfig {
	conf.RLock()
	defer conf.RUnlock()
	return &evictSlowStoreSchedulerConfig{
		RecoveryDurationGap: conf.RecoveryDurationGap,
		Batch:               conf.Batch,
	}
}

func (conf *evictSlowStoreSchedulerConfig) getStores() []uint64 {
	conf.RLock()
	defer conf.RUnlock()
	return conf.EvictedStores
}

func (conf *evictSlowStoreSchedulerConfig) getKeyRangesByID(id uint64) []keyutil.KeyRange {
	if conf.evictStore() != id {
		return nil
	}
	return []keyutil.KeyRange{keyutil.NewKeyRange("", "")}
}

func (conf *evictSlowStoreSchedulerConfig) getBatch() int {
	conf.RLock()
	defer conf.RUnlock()
	return conf.Batch
}

func (conf *evictSlowStoreSchedulerConfig) evictStore() uint64 {
	if len(conf.getStores()) == 0 {
		return 0
	}
	return conf.getStores()[0]
}

// readyForRecovery checks whether the last captured candidate is ready for recovery.
func (conf *evictSlowStoreSchedulerConfig) readyForRecovery() bool {
	conf.RLock()
	defer conf.RUnlock()
	recoveryDurationGap := conf.RecoveryDurationGap
	failpoint.Inject("transientRecoveryGap", func() {
		recoveryDurationGap = 0
	})
	return uint64(time.Since(conf.lastSlowStoreCaptureTS).Seconds()) >= recoveryDurationGap
}

func (conf *evictSlowStoreSchedulerConfig) setStoreAndPersist(id uint64) error {
	conf.Lock()
	defer conf.Unlock()
	conf.EvictedStores = []uint64{id}
	conf.lastSlowStoreCaptureTS = time.Now()
	return conf.save()
}

func (conf *evictSlowStoreSchedulerConfig) tryUpdateRecoverStatus(isRecovered bool) error {
	conf.RLock()
	if conf.isRecovered == isRecovered {
		conf.RUnlock()
		return nil
	}
	conf.RUnlock()

	conf.Lock()
	defer conf.Unlock()
	conf.lastSlowStoreCaptureTS = time.Now()
	conf.isRecovered = isRecovered
	return conf.save()
}

func (conf *evictSlowStoreSchedulerConfig) clearAndPersist() (oldID uint64, err error) {
	oldID = conf.evictStore()
	conf.Lock()
	defer conf.Unlock()
	if oldID > 0 {
		conf.EvictedStores = []uint64{}
		conf.lastSlowStoreCaptureTS = time.Time{}
		err = conf.save()
	}
	return
}

type evictSlowStoreHandler struct {
	rd     *render.Render
	config *evictSlowStoreSchedulerConfig
}

func newEvictSlowStoreHandler(config *evictSlowStoreSchedulerConfig) http.Handler {
	h := &evictSlowStoreHandler{
		config: config,
		rd:     render.New(render.Options{IndentJSON: true}),
	}
	router := mux.NewRouter()
	router.HandleFunc("/config", h.updateConfig).Methods(http.MethodPost)
	router.HandleFunc("/list", h.listConfig).Methods(http.MethodGet)
	return router
}

func (handler *evictSlowStoreHandler) updateConfig(w http.ResponseWriter, r *http.Request) {
	var input map[string]any
	if err := apiutil.ReadJSONRespondError(handler.rd, w, r.Body, &input); err != nil {
		return
	}
	recoveryDurationGapFloat, ok := input["recovery-duration"].(float64)
	if input["recovery-duration"] != nil && !ok {
		handler.rd.JSON(w, http.StatusInternalServerError, errors.New("invalid argument for 'recovery-duration'").Error())
		return
	}

	batch := handler.config.getBatch()
	batchFloat, ok := input["batch"].(float64)
	if input["batch"] != nil && !ok {
		handler.rd.JSON(w, http.StatusInternalServerError, errors.New("invalid argument for 'batch'").Error())
		return
	}
	if ok {
		if batchFloat < 1 || batchFloat > 10 {
			handler.rd.JSON(w, http.StatusBadRequest, "batch is invalid, it should be in [1, 10]")
			return
		}
		batch = (int)(batchFloat)
	}

	handler.config.Lock()
	defer handler.config.Unlock()
	prevRecoveryDurationGap := handler.config.RecoveryDurationGap
	prevBatch := handler.config.Batch
	recoveryDurationGap := uint64(recoveryDurationGapFloat)
	handler.config.RecoveryDurationGap = recoveryDurationGap
	handler.config.Batch = batch
	if err := handler.config.save(); err != nil {
		handler.rd.JSON(w, http.StatusInternalServerError, err.Error())
		handler.config.RecoveryDurationGap = prevRecoveryDurationGap
		handler.config.Batch = prevBatch
		return
	}
	log.Info("evict-slow-store-scheduler update config", zap.Uint64("prev-recovery-duration", prevRecoveryDurationGap), zap.Uint64("cur-recovery-duration", recoveryDurationGap), zap.Int("prev-batch", prevBatch), zap.Int("cur-batch", batch))
	handler.rd.JSON(w, http.StatusOK, "Config updated.")
}

func (handler *evictSlowStoreHandler) listConfig(w http.ResponseWriter, _ *http.Request) {
	conf := handler.config.clone()
	handler.rd.JSON(w, http.StatusOK, conf)
}

type evictSlowStoreScheduler struct {
	*BaseScheduler
	conf    *evictSlowStoreSchedulerConfig
	handler http.Handler
}

// ServeHTTP implements the http.Handler interface.
func (s *evictSlowStoreScheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

// EncodeConfig implements the Scheduler interface.
func (s *evictSlowStoreScheduler) EncodeConfig() ([]byte, error) {
	return EncodeConfig(s.conf)
}

// ReloadConfig implements the Scheduler interface.
func (s *evictSlowStoreScheduler) ReloadConfig() error {
	s.conf.Lock()
	defer s.conf.Unlock()

	newCfg := &evictSlowStoreSchedulerConfig{}
	if err := s.conf.load(newCfg); err != nil {
		return err
	}
	if newCfg.Batch == 0 {
		newCfg.Batch = EvictLeaderBatchSize
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
	s.conf.Batch = newCfg.Batch
	return nil
}

// PrepareConfig implements the Scheduler interface.
func (s *evictSlowStoreScheduler) PrepareConfig(cluster sche.SchedulerCluster) error {
	evictStore := s.conf.evictStore()
	if evictStore != 0 {
		return cluster.SlowStoreEvicted(evictStore)
	}
	return nil
}

// CleanConfig implements the Scheduler interface.
func (s *evictSlowStoreScheduler) CleanConfig(cluster sche.SchedulerCluster) {
	s.cleanupEvictLeader(cluster)
}

func (s *evictSlowStoreScheduler) prepareEvictLeader(cluster sche.SchedulerCluster, storeID uint64) error {
	err := s.conf.setStoreAndPersist(storeID)
	if err != nil {
		log.Info("evict-slow-store-scheduler persist config failed", zap.Uint64("store-id", storeID))
		return err
	}

	return cluster.SlowStoreEvicted(storeID)
}

func (s *evictSlowStoreScheduler) cleanupEvictLeader(cluster sche.SchedulerCluster) {
	evictSlowStore, err := s.conf.clearAndPersist()
	if err != nil {
		log.Info("evict-slow-store-scheduler persist config failed", zap.Uint64("store-id", evictSlowStore))
	}
	if evictSlowStore == 0 {
		return
	}
	cluster.SlowStoreRecovered(evictSlowStore)
}

func (s *evictSlowStoreScheduler) schedulerEvictLeader(cluster sche.SchedulerCluster) []*operator.Operator {
	return scheduleEvictLeaderBatch(s.R, s.GetName(), cluster, s.conf)
}

// IsScheduleAllowed implements the Scheduler interface.
func (s *evictSlowStoreScheduler) IsScheduleAllowed(cluster sche.SchedulerCluster) bool {
	if s.conf.evictStore() != 0 {
		allowed := s.OpController.OperatorCount(operator.OpLeader) < cluster.GetSchedulerConfig().GetLeaderScheduleLimit()
		if !allowed {
			operator.IncOperatorLimitCounter(s.GetType(), operator.OpLeader)
		}
		return allowed
	}
	return true
}

// Schedule implements the Scheduler interface.
func (s *evictSlowStoreScheduler) Schedule(cluster sche.SchedulerCluster, _ bool) ([]*operator.Operator, []plan.Plan) {
	evictSlowStoreCounter.Inc()

	if s.conf.evictStore() != 0 {
		store := cluster.GetStore(s.conf.evictStore())
		storeIDStr := strconv.FormatUint(store.GetID(), 10)
		if store == nil || store.IsRemoved() {
			// Previous slow store had been removed, remove the scheduler and check
			// slow node next time.
			log.Info("slow store has been removed",
				zap.Uint64("store-id", store.GetID()))
			evictedSlowStoreStatusGauge.DeleteLabelValues(s.GetName(), storeIDStr)
			s.cleanupEvictLeader(cluster)
			return nil, nil
		}
		// recover slow store if its score is below the threshold.
		if store.GetSlowScore() <= slowStoreRecoverThreshold {
			if err := s.conf.tryUpdateRecoverStatus(true); err != nil {
				log.Info("evict-slow-store-scheduler persist config failed", zap.Uint64("store-id", store.GetID()), zap.Error(err))
				return nil, nil
			}

			if !s.conf.readyForRecovery() {
				return nil, nil
			}

			log.Info("slow store has been recovered",
				zap.Uint64("store-id", store.GetID()))
			evictedSlowStoreStatusGauge.DeleteLabelValues(s.GetName(), storeIDStr)
			s.cleanupEvictLeader(cluster)
			return nil, nil
		}
		// If the slow store is still slow or slow again, we can continue to evict leaders from it.
		if err := s.conf.tryUpdateRecoverStatus(false); err != nil {
			log.Info("evict-slow-store-scheduler persist config failed", zap.Uint64("store-id", store.GetID()), zap.Error(err))
			return nil, nil
		}
		return s.schedulerEvictLeader(cluster), nil
	}

	var slowStore *core.StoreInfo

	for _, store := range cluster.GetStores() {
		if store.IsRemoved() {
			continue
		}

		if (store.IsPreparing() || store.IsServing()) && store.IsSlow() {
			// Do nothing if there is more than one slow store.
			if slowStore != nil {
				return nil, nil
			}
			slowStore = store
		}
	}

	if slowStore == nil || slowStore.GetSlowScore() < slowStoreEvictThreshold {
		return nil, nil
	}

	// If there is only one slow store, evict leaders from that store.
	log.Info("detected slow store, start to evict leaders",
		zap.Uint64("store-id", slowStore.GetID()))
	err := s.prepareEvictLeader(cluster, slowStore.GetID())
	if err != nil {
		log.Info("prepare for evicting leader failed", zap.Error(err), zap.Uint64("store-id", slowStore.GetID()))
		return nil, nil
	}
	// Record the slow store evicted status.
	storeIDStr := strconv.FormatUint(slowStore.GetID(), 10)
	evictedSlowStoreStatusGauge.WithLabelValues(s.GetName(), storeIDStr).Set(1)
	return s.schedulerEvictLeader(cluster), nil
}

// newEvictSlowStoreScheduler creates a scheduler that detects and evicts slow stores.
func newEvictSlowStoreScheduler(opController *operator.Controller, conf *evictSlowStoreSchedulerConfig) Scheduler {
	handler := newEvictSlowStoreHandler(conf)
	return &evictSlowStoreScheduler{
		BaseScheduler: NewBaseScheduler(opController, types.EvictSlowStoreScheduler, conf),
		conf:          conf,
		handler:       handler,
	}
}
