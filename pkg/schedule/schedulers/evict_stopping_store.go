// Copyright 2025 TiKV Project Authors.
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

	"github.com/gorilla/mux"
	"github.com/unrolled/render"
	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/plan"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

type stoppingStoreType string

const (
	gracefulShutdownStore stoppingStoreType = "graceful-shutdown"
)

type evictStoppingStoreSchedulerConfig struct {
	baseDefaultSchedulerConfig

	cluster *core.BasicCluster
	// EvictedStores stores the stopping stores that are being evicted
	EvictedStores []uint64 `json:"evicted-stores"`
	// Batch is used to generate multiple operators by one scheduling
	Batch int `json:"batch"`
}

func initEvictStoppingStoreSchedulerConfig() *evictStoppingStoreSchedulerConfig {
	return &evictStoppingStoreSchedulerConfig{
		baseDefaultSchedulerConfig: newBaseDefaultSchedulerConfig(),
		EvictedStores:              make([]uint64, 0),
		Batch:                      EvictLeaderBatchSize,
	}
}

func (conf *evictStoppingStoreSchedulerConfig) persistLocked(updateFn func()) error {
	var (
		oldEvictedStores = conf.EvictedStores
		oldBatch         = conf.Batch
	)
	updateFn()
	if err := conf.save(); err != nil {
		conf.EvictedStores = oldEvictedStores
		conf.Batch = oldBatch
		return err
	}
	return nil
}

func (conf *evictStoppingStoreSchedulerConfig) getStores() []uint64 {
	conf.RLock()
	defer conf.RUnlock()
	return conf.EvictedStores
}

func (*evictStoppingStoreSchedulerConfig) getKeyRangesByID(uint64) []keyutil.KeyRange {
	return []keyutil.KeyRange{keyutil.NewKeyRange("", "")}
}

func (conf *evictStoppingStoreSchedulerConfig) getBatch() int {
	conf.RLock()
	defer conf.RUnlock()
	return conf.Batch
}

func (conf *evictStoppingStoreSchedulerConfig) evictStore() uint64 {
	conf.RLock()
	defer conf.RUnlock()
	if len(conf.EvictedStores) == 0 {
		return 0
	}
	return conf.EvictedStores[0]
}

func (conf *evictStoppingStoreSchedulerConfig) setStoreAndPersist(id uint64) error {
	conf.Lock()
	defer conf.Unlock()
	return conf.persistLocked(func() {
		conf.EvictedStores = []uint64{id}
	})
}

func (conf *evictStoppingStoreSchedulerConfig) clearEvictedAndPersist() (oldID uint64, err error) {
	oldID = conf.evictStore()
	conf.Lock()
	defer conf.Unlock()
	if oldID > 0 {
		err = conf.persistLocked(func() {
			conf.EvictedStores = []uint64{}
		})
	}
	return
}

type evictStoppingStoreHandler struct {
	rd     *render.Render
	config *evictStoppingStoreSchedulerConfig
}

func newEvictStoppingStoreHandler(config *evictStoppingStoreSchedulerConfig) http.Handler {
	h := &evictStoppingStoreHandler{
		config: config,
		rd:     render.New(render.Options{IndentJSON: true}),
	}
	router := mux.NewRouter()
	router.HandleFunc("/config", h.updateConfig).Methods(http.MethodPost)
	router.HandleFunc("/list", h.listConfig).Methods(http.MethodGet)
	return router
}

func (handler *evictStoppingStoreHandler) updateConfig(w http.ResponseWriter, _ *http.Request) {
	handler.rd.JSON(w, http.StatusOK, "Config updated.")
}

func (handler *evictStoppingStoreHandler) listConfig(w http.ResponseWriter, _ *http.Request) {
	handler.config.RLock()
	defer handler.config.RUnlock()
	conf := &evictStoppingStoreSchedulerConfig{
		Batch:         handler.config.Batch,
		EvictedStores: handler.config.EvictedStores,
	}
	handler.rd.JSON(w, http.StatusOK, conf)
}

type evictStoppingStoreScheduler struct {
	*BaseScheduler
	conf    *evictStoppingStoreSchedulerConfig
	handler http.Handler
}

// ServeHTTP implements the http.Handler interface.
func (s *evictStoppingStoreScheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

// EncodeConfig implements the Scheduler interface.
func (s *evictStoppingStoreScheduler) EncodeConfig() ([]byte, error) {
	return EncodeConfig(s.conf)
}

// ReloadConfig implements the Scheduler interface.
func (s *evictStoppingStoreScheduler) ReloadConfig() error {
	s.conf.Lock()
	defer s.conf.Unlock()

	newCfg := &evictStoppingStoreSchedulerConfig{}
	if err := s.conf.load(newCfg); err != nil {
		return err
	}
	if newCfg.Batch == 0 {
		newCfg.Batch = EvictLeaderBatchSize
	}

	s.conf.EvictedStores = newCfg.EvictedStores
	s.conf.Batch = newCfg.Batch
	return nil
}

// PrepareConfig implements the Scheduler interface.
func (s *evictStoppingStoreScheduler) PrepareConfig(cluster sche.SchedulerCluster) error {
	evictStore := s.conf.evictStore()
	if evictStore != 0 {
		if err := cluster.SlowStoppingStoreEvicted(evictStore); err != nil {
			return err
		}
	}
	return nil
}

// CleanConfig implements the Scheduler interface.
func (s *evictStoppingStoreScheduler) CleanConfig(cluster sche.SchedulerCluster) {
	s.cleanupEvictLeader(cluster)
}

func (s *evictStoppingStoreScheduler) prepareEvictLeader(cluster sche.SchedulerCluster, storeID uint64) error {
	if err := cluster.SlowStoppingStoreEvicted(storeID); err != nil {
		log.Info("failed to evict stopping store", zap.Uint64("store-id", storeID), zap.Error(err))
		return err
	}

	if err := s.conf.setStoreAndPersist(storeID); err != nil {
		log.Info("failed to persist evicted stopping store", zap.Uint64("store-id", storeID), zap.Error(err))
		cluster.SlowStoreRecovered(storeID)
		return err
	}

	return nil
}

func (s *evictStoppingStoreScheduler) cleanupEvictLeader(cluster sche.SchedulerCluster) {
	evictStoppingStore, err := s.conf.clearEvictedAndPersist()
	if err != nil {
		log.Info("evict-stopping-store-scheduler persist config failed", zap.Uint64("store-id", evictStoppingStore))
	}
	if evictStoppingStore == 0 {
		return
	}
	cluster.SlowStoreRecovered(evictStoppingStore)
}

func (s *evictStoppingStoreScheduler) schedulerEvictLeader(cluster sche.SchedulerCluster) []*operator.Operator {
	return scheduleEvictLeaderBatch(s.R, s.GetName(), cluster, s.conf)
}

// IsScheduleAllowed implements the Scheduler interface.
func (s *evictStoppingStoreScheduler) IsScheduleAllowed(cluster sche.SchedulerCluster) bool {
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
func (s *evictStoppingStoreScheduler) Schedule(cluster sche.SchedulerCluster, _ bool) ([]*operator.Operator, []plan.Plan) {
	evictStoppingStoreCounter.Inc()
	s.scheduleStoppingStore(cluster)
	return s.schedulerEvictLeader(cluster), nil
}

func (s *evictStoppingStoreScheduler) scheduleStoppingStore(cluster sche.SchedulerCluster) {
	if s.conf.evictStore() != 0 {
		store := cluster.GetStore(s.conf.evictStore())
		if store == nil || store.IsRemoved() {
			// Previous stopping store had been removed, remove the scheduler and check next time.
			log.Info("stopping store has been removed", zap.Uint64("store-id", s.conf.evictStore()))
			s.cleanupEvictLeader(cluster)
			return
		}

		// recover stopping store if it's no longer in stopping state.
		if !store.IsStopping() {
			log.Info("stopping store has been recovered", zap.Uint64("store-id", store.GetID()))
			s.cleanupEvictLeader(cluster)
			return
		}
		return
	}

	var stoppingStore *core.StoreInfo

	for _, store := range cluster.GetStores() {
		if store.IsRemoved() {
			continue
		}

		if (store.IsPreparing() || store.IsServing()) && store.IsStopping() {
			// Do nothing if there is more than one stopping store.
			if stoppingStore != nil {
				return
			}
			stoppingStore = store
		}
	}

	if stoppingStore == nil {
		return
	}

	// If there is only one stopping store, evict leaders from that store.
	stoppingStoreID := stoppingStore.GetID()
	log.Info("detected stopping store, start to evict leaders", zap.Uint64("store-id", stoppingStoreID))
	err := s.prepareEvictLeader(cluster, stoppingStoreID)
	if err != nil {
		log.Info("prepare for evicting leader failed", zap.Error(err), zap.Uint64("store-id", stoppingStoreID))
		return
	}
	// Record the stopping store evicted status.
	storeIDStr := strconv.FormatUint(stoppingStoreID, 10)
	evictedStoppingStoreStatusGauge.WithLabelValues(storeIDStr, string(gracefulShutdownStore)).Set(1)
}

// newEvictStoppingStoreScheduler creates a scheduler that detects and evicts stopping stores.
func newEvictStoppingStoreScheduler(opController *operator.Controller, conf *evictStoppingStoreSchedulerConfig) Scheduler {
	handler := newEvictStoppingStoreHandler(conf)
	return &evictStoppingStoreScheduler{
		BaseScheduler: NewBaseScheduler(opController, types.EvictStoppingStoreScheduler, conf),
		conf:          conf,
		handler:       handler,
	}
}
