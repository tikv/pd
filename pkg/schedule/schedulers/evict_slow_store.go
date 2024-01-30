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
	// Duration gap for recovering the candidate, unit: s.
	RecoveryDurationGap uint64   `json:"recovery-duration"`
	EvictedStores       []uint64 `json:"evict-stores"`
}

func initEvictSlowStoreSchedulerConfig(storage endpoint.ConfigStorage) *evictSlowStoreSchedulerConfig {
	return &evictSlowStoreSchedulerConfig{
		storage:                storage,
		lastSlowStoreCaptureTS: time.Time{},
		RecoveryDurationGap:    defaultRecoveryDurationGap,
		EvictedStores:          make([]uint64, 0),
	}
}

func (conf *evictSlowStoreSchedulerConfig) Clone() *evictSlowStoreSchedulerConfig {
	conf.RLock()
	defer conf.RUnlock()
	return &evictSlowStoreSchedulerConfig{
		RecoveryDurationGap: conf.RecoveryDurationGap,
	}
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
	return conf.persistLocked()
}

func (conf *evictSlowStoreSchedulerConfig) clearAndPersist() (oldID uint64, err error) {
	oldID = conf.evictStore()
	conf.Lock()
	defer conf.Unlock()
	if oldID > 0 {
		conf.EvictedStores = []uint64{}
		conf.lastSlowStoreCaptureTS = time.Time{}
		err = conf.persistLocked()
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
	router.HandleFunc("/config", h.UpdateConfig).Methods(http.MethodPost)
	router.HandleFunc("/list", h.ListConfig).Methods(http.MethodGet)
	return router
}

func (handler *evictSlowStoreHandler) UpdateConfig(w http.ResponseWriter, r *http.Request) {
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
	if err := handler.config.persistLocked(); err != nil {
		handler.rd.JSON(w, http.StatusInternalServerError, err.Error())
		handler.config.RecoveryDurationGap = prevRecoveryDurationGap
		return
	}
	log.Info("evict-slow-store-scheduler update 'recovery-duration' - unit: s", zap.Uint64("prev", prevRecoveryDurationGap), zap.Uint64("cur", recoveryDurationGap))
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
	cfgData, err := s.conf.storage.LoadSchedulerConfig(s.GetName())
	if err != nil {
		return err
	}
	if len(cfgData) == 0 {
		return nil
	}
	newCfg := &evictSlowStoreSchedulerConfig{}
	if err = DecodeConfig([]byte(cfgData), newCfg); err != nil {
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
	pauseAndResumeLeaderTransfer(s.conf.cluster, old, new)
	s.conf.RecoveryDurationGap = newCfg.RecoveryDurationGap
	s.conf.EvictedStores = newCfg.EvictedStores
	return nil
}

func (s *evictSlowStoreScheduler) PrepareConfig(cluster sche.SchedulerCluster) error {
	evictStore := s.conf.evictStore()
	if evictStore != 0 {
		return cluster.SlowStoreEvicted(evictStore)
	}
	return nil
}

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
	return scheduleEvictLeaderBatch(s.GetName(), s.GetType(), cluster, s.conf, EvictLeaderBatchSize)
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
	var ops []*operator.Operator

	if s.conf.evictStore() != 0 {
		store := cluster.GetStore(s.conf.evictStore())
		if store == nil || store.IsRemoved() {
			// Previous slow store had been removed, remove the scheduler and check
			// slow node next time.
			log.Info("slow store has been removed",
				zap.Uint64("store-id", store.GetID()))
		} else if store.GetSlowScore() <= slowStoreRecoverThreshold && s.conf.readyForRecovery() {
			log.Info("slow store has been recovered",
				zap.Uint64("store-id", store.GetID()))
		} else {
			return s.schedulerEvictLeader(cluster), nil
		}
		s.cleanupEvictLeader(cluster)
		return ops, nil
	}

	var slowStore *core.StoreInfo

	for _, store := range cluster.GetStores() {
		if store.IsRemoved() {
			continue
		}

		if (store.IsPreparing() || store.IsServing()) && store.IsSlow() {
			// Do nothing if there is more than one slow store.
			if slowStore != nil {
				return ops, nil
			}
			slowStore = store
		}
	}

	if slowStore == nil || slowStore.GetSlowScore() < slowStoreEvictThreshold {
		return ops, nil
	}

	// If there is only one slow store, evict leaders from that store.
	log.Info("detected slow store, start to evict leaders",
		zap.Uint64("store-id", slowStore.GetID()))
	err := s.prepareEvictLeader(cluster, slowStore.GetID())
	if err != nil {
		log.Info("prepare for evicting leader failed", zap.Error(err), zap.Uint64("store-id", slowStore.GetID()))
		return ops, nil
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
