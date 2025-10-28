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
	"time"

	"github.com/gorilla/mux"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/core"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/plan"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/unrolled/render"
	"go.uber.org/zap"
)

const (
	// EvictSlowStoreName is evict leader scheduler name.
	EvictSlowStoreName = "evict-slow-store-scheduler"
	// EvictSlowStoreType is evict leader scheduler type.
	EvictSlowStoreType = "evict-slow-store"

	slowStoreEvictThreshold   = 100
	slowStoreRecoverThreshold = 1
)

// WithLabelValues is a heavy operation, define variable to avoid call it every time.
var evictSlowStoreCounter = schedulerCounter.WithLabelValues(EvictSlowStoreName, "schedule")

type evictSlowStoreSchedulerConfig struct {
	syncutil.RWMutex
	cluster *core.BasicCluster
	storage endpoint.ConfigStorage
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

func initEvictSlowStoreSchedulerConfig(storage endpoint.ConfigStorage) *evictSlowStoreSchedulerConfig {
	return &evictSlowStoreSchedulerConfig{
		storage:                storage,
		lastSlowStoreCaptureTS: time.Time{},
		RecoveryDurationGap:    defaultRecoveryDurationGap,
		EvictedStores:          make([]uint64, 0),
		Batch:                  EvictLeaderBatchSize,
	}
}

func (conf *evictSlowStoreSchedulerConfig) Clone() *evictSlowStoreSchedulerConfig {
	conf.RLock()
	defer conf.RUnlock()
	return &evictSlowStoreSchedulerConfig{
		RecoveryDurationGap: conf.RecoveryDurationGap,
		Batch:               conf.Batch,
	}
}

func (conf *evictSlowStoreSchedulerConfig) getBatch() int {
	conf.RLock()
	defer conf.RUnlock()
	return conf.Batch
}

func (conf *evictSlowStoreSchedulerConfig) persistLocked() error {
	name := EvictSlowStoreName
	data, err := EncodeConfig(conf)
	failpoint.Inject("persistFail", func() {
		err = errors.New("fail to persist")
	})
	if err != nil {
		return err
	}
	return conf.storage.SaveSchedulerConfig(name, data)
}

func (conf *evictSlowStoreSchedulerConfig) getStores() []uint64 {
	conf.RLock()
	defer conf.RUnlock()
	return conf.EvictedStores
}

func (conf *evictSlowStoreSchedulerConfig) getKeyRangesByID(id uint64) []core.KeyRange {
	if conf.evictStore() != id {
		return nil
	}
	return []core.KeyRange{core.NewKeyRange("", "")}
}

func (conf *evictSlowStoreSchedulerConfig) evictStore() uint64 {
	if len(conf.getStores()) == 0 {
		return 0
	}
	return conf.getStores()[0]
}

// readyForRecovery checks whether the last cpatured candidate is ready for recovery.
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
	conf.isRecovered = false
	return conf.persistLocked()
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
	if conf.isRecovered == isRecovered {
		return nil
	}
	conf.lastSlowStoreCaptureTS = time.Now()
	conf.isRecovered = isRecovered
	return conf.persistLocked()
}

func (conf *evictSlowStoreSchedulerConfig) clearAndPersist() (oldID uint64, err error) {
	oldID = conf.evictStore()
	conf.Lock()
	defer conf.Unlock()
	if oldID > 0 {
		conf.EvictedStores = []uint64{}
		conf.lastSlowStoreCaptureTS = time.Time{}
		conf.isRecovered = false
		err = conf.persistLocked()
	}
	return
}

func (conf *evictSlowStoreSchedulerConfig) save() error {
	conf.Lock()
	defer conf.Unlock()
	return conf.persistLocked()
}

func (conf *evictSlowStoreSchedulerConfig) load(target *evictSlowStoreSchedulerConfig) error {
	name := EvictSlowStoreName
	cfgData, err := conf.storage.LoadSchedulerConfig(name)
	if err != nil {
		return err
	}
	if len(cfgData) == 0 {
		return nil
	}
	return DecodeConfig([]byte(cfgData), target)
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
	router.HandleFunc("/config", h.UpdateConfig).Methods(http.MethodPost)
	router.HandleFunc("/list", h.ListConfig).Methods(http.MethodGet)
	return router
}

func (handler *evictSlowStoreHandler) UpdateConfig(w http.ResponseWriter, r *http.Request) {
	var input map[string]interface{}
	if err := apiutil.ReadJSONRespondError(handler.rd, w, r.Body, &input); err != nil {
		return
	}

	var (
		recoveryDurationGap  uint64
		shouldUpdateRecovery bool
	)
	if value, exists := input["recovery-duration"]; exists && value != nil {
		gapFloat, ok := value.(float64)
		if !ok {
			handler.rd.JSON(w, http.StatusInternalServerError, errors.New("invalid argument for 'recovery-duration'").Error())
			return
		}
		recoveryDurationGap = uint64(gapFloat)
		shouldUpdateRecovery = true
	}

	batch := handler.config.getBatch()
	if value, exists := input["batch"]; exists && value != nil {
		batchFloat, ok := value.(float64)
		if !ok {
			handler.rd.JSON(w, http.StatusInternalServerError, errors.New("invalid argument for 'batch'").Error())
			return
		}
		if batchFloat < 1 || batchFloat > 10 {
			handler.rd.JSON(w, http.StatusBadRequest, "batch is invalid, it should be in [1, 10]")
			return
		}
		batch = int(batchFloat)
	}

	handler.config.Lock()
	defer handler.config.Unlock()
	prevRecoveryDurationGap := handler.config.RecoveryDurationGap
	prevBatch := handler.config.Batch
	if shouldUpdateRecovery {
		handler.config.RecoveryDurationGap = recoveryDurationGap
	}
	handler.config.Batch = batch
	if err := handler.config.persistLocked(); err != nil {
		handler.rd.JSON(w, http.StatusInternalServerError, err.Error())
		if shouldUpdateRecovery {
			handler.config.RecoveryDurationGap = prevRecoveryDurationGap
		}
		handler.config.Batch = prevBatch
		return
	}
	log.Info("evict-slow-store-scheduler update config", zap.Uint64("prev-recovery-duration", prevRecoveryDurationGap), zap.Uint64("cur-recovery-duration", handler.config.RecoveryDurationGap), zap.Int("prev-batch", prevBatch), zap.Int("cur-batch", handler.config.Batch))
	handler.rd.JSON(w, http.StatusOK, "Config updated.")
}

func (handler *evictSlowStoreHandler) ListConfig(w http.ResponseWriter, r *http.Request) {
	conf := handler.config.Clone()
	handler.rd.JSON(w, http.StatusOK, conf)
}

type evictSlowStoreScheduler struct {
	*BaseScheduler
	conf    *evictSlowStoreSchedulerConfig
	handler http.Handler
}

func (s *evictSlowStoreScheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

func (s *evictSlowStoreScheduler) GetName() string {
	return EvictSlowStoreName
}

func (s *evictSlowStoreScheduler) GetType() string {
	return EvictSlowStoreType
}

func (s *evictSlowStoreScheduler) EncodeConfig() ([]byte, error) {
	return EncodeConfig(s.conf)
}

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
	pauseAndResumeLeaderTransfer(s.conf.cluster, old, new)
	s.conf.RecoveryDurationGap = newCfg.RecoveryDurationGap
	s.conf.EvictedStores = newCfg.EvictedStores
	s.conf.Batch = newCfg.Batch
	return nil
}

func (s *evictSlowStoreScheduler) PrepareConfig(cluster sche.SchedulerCluster) error {
	evictStore := s.conf.evictStore()
	if evictStore != 0 {
		return cluster.SlowStoreEvicted(evictStore)
	}
	return nil
}

func (s *evictSlowStoreScheduler) Cleanup(cluster sche.SchedulerCluster) {
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
	batch := s.conf.getBatch()
	if batch <= 0 {
		batch = EvictLeaderBatchSize
	}
	return scheduleEvictLeaderBatch(s.R, s.GetName(), s.GetType(), cluster, s.conf, batch)
}

func (s *evictSlowStoreScheduler) IsScheduleAllowed(cluster sche.SchedulerCluster) bool {
	if s.conf.evictStore() != 0 {
		allowed := s.OpController.OperatorCount(operator.OpLeader) < cluster.GetSchedulerConfig().GetLeaderScheduleLimit()
		if !allowed {
			operator.OperatorLimitCounter.WithLabelValues(s.GetType(), operator.OpLeader.String()).Inc()
		}
		return allowed
	}
	return true
}

func (s *evictSlowStoreScheduler) Schedule(cluster sche.SchedulerCluster, dryRun bool) ([]*operator.Operator, []plan.Plan) {
	evictSlowStoreCounter.Inc()

	if s.conf.evictStore() != 0 {
		store := cluster.GetStore(s.conf.evictStore())
		if store == nil || store.IsRemoved() {
			// Previous slow store had been removed, remove the scheduler and check
			// slow node next time.
			log.Info("slow store has been removed",
				zap.Uint64("store-id", store.GetID()))
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
	return s.schedulerEvictLeader(cluster), nil
}

// newEvictSlowStoreScheduler creates a scheduler that detects and evicts slow stores.
func newEvictSlowStoreScheduler(opController *operator.Controller, conf *evictSlowStoreSchedulerConfig) Scheduler {
	handler := newEvictSlowStoreHandler(conf)
	return &evictSlowStoreScheduler{
		BaseScheduler: NewBaseScheduler(opController),
		conf:          conf,
		handler:       handler,
	}
}
