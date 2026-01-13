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
	"errors"
	"net/http"
	"slices"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/unrolled/render"
	"go.uber.org/zap"

	perrors "github.com/pingcap/errors"
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
	slowStoreEvictThreshold = 100
	// For network slow store, its slow scores include the scores from each tikv(If the score
	// is equal to 1, it will not report to PD). These scores should meet the following
	// conditions and then will be considered as a slow store:
	// 1. At least one network slow score is greater than networkSlowStoreIssueThreshold.
	// 2. A network with a slow score exceeding networkSlowStoreIssueThreshold is considered
	//    a problem network. Since it is impossible to directly identify which side of the
	//    network has a problem, the score is removed from the stores on both sides.
	// 3. After removing the problem network slow score, if the number of scores are greater
	//    than or equal to networkSlowStoreFluctuationThreshold is greater than or equal
	//    to 2 in one store, it is considered a slow store.
	networkSlowStoreIssueThreshold       = 95 // Threshold for detecting network issues with a specific store
	networkSlowStoreFluctuationThreshold = 10 // Threshold for detecting network fluctuations with other stores

	// Why 6? Store heartbeat reports a score every 10 seconds. Therefore, if the average
	// score of all six store heartbeats is above 99, it indicates that network jitter has
	// occurred within all six 10-second intervals.
	maxNetworkEvictThresholdTriggerCount = 6
	slowStoreRecoverThreshold            = 1
	// Currently only support one network slow store
	defaultMaxNetworkSlowStore = 1
)

type slowStoreType string

const (
	diskSlowStore       slowStoreType = "disk"
	networkSlowStore    slowStoreType = "network"
	networkEvictedStore slowStoreType = "network-evicted"
)

type evictSlowStoreSchedulerConfig struct {
	baseDefaultSchedulerConfig

	cluster *core.BasicCluster
	// Last timestamp of the chosen slow store for eviction.
	lastSlowStoreCaptureTS time.Time
	isRecovered            bool

	// The value should be nil when store is slow. Once the store's
	// score becomes 1, the store prepares to recover and record the
	// timestamp.
	networkSlowStoreRecoverStartAts map[uint64]*time.Time
	// Duration gap for recovering the candidate, unit: s.
	RecoverySec uint64 `json:"recovery-duration"`
	// EvictedStores is only used by disk slow store scheduler
	EvictedStores            []uint64 `json:"evict-stores"`
	EnableNetworkSlowStore   bool     `json:"enable-network-slow-store"`
	PausedNetworkSlowStores  []uint64 `json:"paused-network-slow-stores"`
	EvictedNetworkSlowStores []uint64 `json:"evicted-network-slow-stores"`
	// TODO: We only add batch for evict-slow-store-scheduler now.
	// If necessary, we also need to support evict-slow-trend-scheduler.
	Batch int `json:"batch"`
}

func initEvictSlowStoreSchedulerConfig() *evictSlowStoreSchedulerConfig {
	return &evictSlowStoreSchedulerConfig{
		baseDefaultSchedulerConfig:      newBaseDefaultSchedulerConfig(),
		lastSlowStoreCaptureTS:          time.Time{},
		RecoverySec:                     defaultRecoverySec,
		EvictedStores:                   make([]uint64, 0),
		Batch:                           EvictLeaderBatchSize,
		EnableNetworkSlowStore:          false,
		PausedNetworkSlowStores:         make([]uint64, 0),
		EvictedNetworkSlowStores:        make([]uint64, 0),
		networkSlowStoreRecoverStartAts: make(map[uint64]*time.Time),
	}
}

func (conf *evictSlowStoreSchedulerConfig) persistLocked(updateFn func()) error {
	var (
		oldLastSlowStoreCaptureTS          = conf.lastSlowStoreCaptureTS
		oldNetworkSlowStoreRecoverStartAts = make(map[uint64]*time.Time)
		oldIsRecovered                     = conf.isRecovered
		oldEvictedStores                   = conf.EvictedStores
		oldPausedNetworkSlowStores         = conf.PausedNetworkSlowStores
		oldEvictedNetworkSlowStores        = conf.EvictedNetworkSlowStores
		oldRecoverySec                     = conf.RecoverySec
		oldBatch                           = conf.Batch
	)
	for k, v := range conf.networkSlowStoreRecoverStartAts {
		oldNetworkSlowStoreRecoverStartAts[k] = v
	}
	updateFn()
	if err := conf.save(); err != nil {
		conf.lastSlowStoreCaptureTS = oldLastSlowStoreCaptureTS
		conf.networkSlowStoreRecoverStartAts = oldNetworkSlowStoreRecoverStartAts
		conf.isRecovered = oldIsRecovered
		conf.EvictedStores = oldEvictedStores
		conf.PausedNetworkSlowStores = oldPausedNetworkSlowStores
		conf.EvictedNetworkSlowStores = oldEvictedNetworkSlowStores
		conf.RecoverySec = oldRecoverySec
		conf.Batch = oldBatch
		return err
	}
	return nil
}

func (conf *evictSlowStoreSchedulerConfig) getStores() []uint64 {
	conf.RLock()
	defer conf.RUnlock()

	return append(conf.EvictedStores, conf.EvictedNetworkSlowStores...)
}

func (*evictSlowStoreSchedulerConfig) getKeyRangesByID(uint64) []keyutil.KeyRange {
	return []keyutil.KeyRange{keyutil.NewKeyRange("", "")}
}

func (conf *evictSlowStoreSchedulerConfig) getBatch() int {
	conf.RLock()
	defer conf.RUnlock()
	return conf.Batch
}

func (conf *evictSlowStoreSchedulerConfig) evictStore() uint64 {
	conf.RLock()
	defer conf.RUnlock()
	if len(conf.EvictedStores) == 0 {
		return 0
	}
	return conf.EvictedStores[0]
}

func (conf *evictSlowStoreSchedulerConfig) getPausedNetworkSlowStores() []uint64 {
	conf.RLock()
	defer conf.RUnlock()
	return conf.PausedNetworkSlowStores
}

func (conf *evictSlowStoreSchedulerConfig) getEvictNetworkSlowStores() []uint64 {
	conf.RLock()
	defer conf.RUnlock()
	return conf.EvictedNetworkSlowStores
}

func (conf *evictSlowStoreSchedulerConfig) deleteNetworkSlowStoreLocked(storeID uint64, cluster sche.SchedulerCluster) {
	log.Info("deleting network slow store",
		zap.Uint64("store-id", storeID))

	needRecoverEvict := slices.Contains(conf.EvictedNetworkSlowStores, storeID)
	if err := conf.persistLocked(func() {
		conf.PausedNetworkSlowStores = slices.DeleteFunc(conf.PausedNetworkSlowStores, func(val uint64) bool {
			return val == storeID
		})
		conf.EvictedNetworkSlowStores = slices.DeleteFunc(conf.EvictedNetworkSlowStores, func(val uint64) bool {
			return val == storeID
		})
	}); err != nil {
		log.Warn("failed to persist evict slow store config",
			zap.Uint64("store-id", storeID),
			zap.Error(err))
		return
	}
	cluster.ResumeLeaderTransfer(storeID, constant.In)
	if needRecoverEvict {
		log.Info("recovered network evicted slow store", zap.Uint64("store-id", storeID))
		cluster.SlowStoreRecovered(storeID)
		evictedSlowStoreStatusGauge.DeleteLabelValues(strconv.FormatUint(storeID, 10), string(networkEvictedStore))
	}

	evictedSlowStoreStatusGauge.DeleteLabelValues(strconv.FormatUint(storeID, 10), string(networkSlowStore))
	delete(conf.networkSlowStoreRecoverStartAts, storeID)
}

func (conf *evictSlowStoreSchedulerConfig) addNetworkSlowStoreLocked(storeID uint64, cluster sche.SchedulerCluster) {
	log.Info("detected network slow store, start to pause scheduler",
		zap.Uint64("store-id", storeID))
	if err := cluster.PauseLeaderTransfer(storeID, constant.In); err != nil {
		log.Warn("failed to pause leader transfer for network slow store",
			zap.Uint64("store-id", storeID),
			zap.Error(err))
		return
	}

	err := conf.persistLocked(func() {
		conf.PausedNetworkSlowStores = append(conf.PausedNetworkSlowStores, storeID)
		conf.networkSlowStoreRecoverStartAts[storeID] = nil
	})
	if err != nil {
		log.Warn("failed to persist evict slow store config",
			zap.Uint64("store-id", storeID),
			zap.Error(err))
		cluster.ResumeLeaderTransfer(storeID, constant.In)
	}
	evictedSlowStoreStatusGauge.WithLabelValues(strconv.FormatUint(storeID, 10), string(networkSlowStore)).Set(1)
}

// readyForRecovery checks whether the last captured candidate is ready for recovery.
func (conf *evictSlowStoreSchedulerConfig) readyForRecovery() bool {
	conf.RLock()
	defer conf.RUnlock()
	recoverySec := conf.RecoverySec
	failpoint.Inject("transientRecoveryGap", func() {
		recoverySec = 0
	})
	return uint64(time.Since(conf.lastSlowStoreCaptureTS).Seconds()) >= recoverySec
}

func (conf *evictSlowStoreSchedulerConfig) setStoreAndPersist(id uint64) error {
	conf.Lock()
	defer conf.Unlock()
	return conf.persistLocked(func() {
		conf.EvictedStores = []uint64{id}
		conf.lastSlowStoreCaptureTS = time.Now()
	})
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
	return conf.persistLocked(func() {
		conf.lastSlowStoreCaptureTS = time.Now()
		conf.isRecovered = isRecovered
	})
}

func (conf *evictSlowStoreSchedulerConfig) clearEvictedAndPersist() (oldID uint64, err error) {
	oldID = conf.evictStore()
	conf.Lock()
	defer conf.Unlock()
	if oldID > 0 {
		err = conf.persistLocked(func() {
			conf.EvictedStores = []uint64{}
			conf.lastSlowStoreCaptureTS = time.Time{}
		})
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
	recoveryDurationGapFloat, inputRecoveryDuration := input["recovery-duration"].(float64)
	if input["recovery-duration"] != nil && !inputRecoveryDuration {
		handler.rd.JSON(w, http.StatusBadRequest, perrors.New("invalid argument for 'recovery-duration'").Error())
		return
	}

	batchFloat, inputBatch := input["batch"].(float64)
	if input["batch"] != nil && !inputBatch {
		handler.rd.JSON(w, http.StatusBadRequest, perrors.New("invalid argument for 'batch'").Error())
		return
	}
	if inputBatch {
		if batchFloat < 1 || batchFloat > 10 {
			handler.rd.JSON(w, http.StatusBadRequest, "batch is invalid, it should be in [1, 10]")
			return
		}
	}

	enableNetworkSlowStore, inputEnableNetworkSlowStore := input["enable-network-slow-store"].(bool)
	if input["enable-network-slow-store"] != nil && !inputEnableNetworkSlowStore {
		handler.rd.JSON(w, http.StatusBadRequest, perrors.New("invalid argument for 'enable-network-slow-store'").Error())
		return
	}

	handler.config.Lock()
	defer handler.config.Unlock()
	recoverySec := uint64(recoveryDurationGapFloat)

	if err := handler.config.persistLocked(func() {
		if inputRecoveryDuration {
			handler.config.RecoverySec = recoverySec
		}
		if inputBatch {
			handler.config.Batch = int(batchFloat)
		}
		if inputEnableNetworkSlowStore {
			handler.config.EnableNetworkSlowStore = enableNetworkSlowStore
		}
	}); err != nil {
		handler.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	log.Info("evict-slow-store-scheduler update config",
		zap.Uint64("cur-recovery-duration", recoverySec),
		zap.Float64("cur-batch", batchFloat),
		zap.Bool("cur-enable-network-slow-store", enableNetworkSlowStore),
	)

	handler.rd.JSON(w, http.StatusOK, "Config updated.")
}

func (handler *evictSlowStoreHandler) listConfig(w http.ResponseWriter, _ *http.Request) {
	handler.config.RLock()
	defer handler.config.RUnlock()
	conf := &evictSlowStoreSchedulerConfig{
		RecoverySec:              handler.config.RecoverySec,
		Batch:                    handler.config.Batch,
		EvictedStores:            handler.config.EvictedStores,
		EnableNetworkSlowStore:   handler.config.EnableNetworkSlowStore,
		PausedNetworkSlowStores:  handler.config.PausedNetworkSlowStores,
		EvictedNetworkSlowStores: handler.config.EvictedNetworkSlowStores,
	}
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
	for _, id := range s.conf.PausedNetworkSlowStores {
		old[id] = struct{}{}
	}
	new := make(map[uint64]struct{})
	for _, id := range newCfg.EvictedStores {
		new[id] = struct{}{}
	}
	for _, id := range newCfg.PausedNetworkSlowStores {
		new[id] = struct{}{}
	}
	pauseAndResumeLeaderTransfer(s.conf.cluster, constant.In, old, new)
	s.conf.RecoverySec = newCfg.RecoverySec
	s.conf.EvictedStores = newCfg.EvictedStores
	s.conf.PausedNetworkSlowStores = newCfg.PausedNetworkSlowStores
	s.conf.EvictedNetworkSlowStores = newCfg.EvictedNetworkSlowStores
	s.conf.EnableNetworkSlowStore = newCfg.EnableNetworkSlowStore
	s.conf.Batch = newCfg.Batch
	return nil
}

// PrepareConfig implements the Scheduler interface.
func (s *evictSlowStoreScheduler) PrepareConfig(cluster sche.SchedulerCluster) error {
	errs := make([]error, 0)

	evictStore := s.conf.evictStore()
	if evictStore != 0 {
		if err := cluster.SlowStoreEvicted(evictStore); err != nil {
			errs = append(errs, err)
		}
	}
	for _, storeID := range s.conf.getPausedNetworkSlowStores() {
		s.conf.networkSlowStoreRecoverStartAts[storeID] = nil
		if err := cluster.PauseLeaderTransfer(storeID, constant.In); err != nil {
			errs = append(errs, err)
		}
	}
	for _, storeID := range s.conf.getEvictNetworkSlowStores() {
		if err := cluster.SlowStoreEvicted(storeID); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// CleanConfig implements the Scheduler interface.
func (s *evictSlowStoreScheduler) CleanConfig(cluster sche.SchedulerCluster) {
	s.cleanupEvictLeader(cluster)
	networkSlowStores := s.conf.getPausedNetworkSlowStores()
	s.conf.Lock()
	defer s.conf.Unlock()
	for _, storeID := range networkSlowStores {
		s.conf.deleteNetworkSlowStoreLocked(storeID, cluster)
	}
}

func (s *evictSlowStoreScheduler) prepareEvictLeader(cluster sche.SchedulerCluster, storeID uint64) error {
	if err := cluster.SlowStoreEvicted(storeID); err != nil {
		log.Info("failed to evict slow store", zap.Uint64("store-id", storeID), zap.Error(err))
		return err
	}

	if err := s.conf.setStoreAndPersist(storeID); err != nil {
		log.Info("failed to persist evicted slow store", zap.Uint64("store-id", storeID), zap.Error(err))
		cluster.SlowStoreRecovered(storeID)
		return err
	}

	return nil
}

func (s *evictSlowStoreScheduler) cleanupEvictLeader(cluster sche.SchedulerCluster) {
	evictSlowStore, err := s.conf.clearEvictedAndPersist()
	if err != nil {
		log.Info("evict-slow-store-scheduler persist config failed", zap.Uint64("store-id", evictSlowStore))
	}
	if evictSlowStore == 0 {
		return
	}
	cluster.SlowStoreRecovered(evictSlowStore)
}

func (s *evictSlowStoreScheduler) schedulerEvictLeader(cluster sche.SchedulerCluster) []*operator.Operator {
	return scheduleEvictLeaderBatch(s.GetName(), cluster, s.conf)
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
	s.scheduleNetworkSlowStore(cluster)
	s.scheduleDiskSlowStore(cluster)
	return s.schedulerEvictLeader(cluster), nil
}

func (s *evictSlowStoreScheduler) scheduleNetworkSlowStore(cluster sche.SchedulerCluster) {
	// If necessary, we can split the lock into finer granularity
	s.conf.Lock()
	defer s.conf.Unlock()

	if s.conf.EnableNetworkSlowStore {
		// Try to recover network slow stores that have become normal
		s.tryRecoverNetworkSlowStores(cluster)

		// Activate paused network slow stores if capacity allows
		s.activatePausedNetworkSlowStores(cluster)

		// Detect and handle new network slow stores
		s.detectAndHandleNetworkSlowStores(cluster)

		s.tryEvictLeaderFromNetworkSlowStores(cluster)
	} else {
		// Clear network slow stores if the feature is disabled
		for _, storeID := range s.conf.PausedNetworkSlowStores {
			s.conf.deleteNetworkSlowStoreLocked(storeID, cluster)
		}
		clear(s.conf.networkSlowStoreRecoverStartAts)
	}
}

func (s *evictSlowStoreScheduler) tryEvictLeaderFromNetworkSlowStores(cluster sche.SchedulerCluster) {
	for storeID, startTime := range s.conf.networkSlowStoreRecoverStartAts {
		if !slices.Contains(s.conf.PausedNetworkSlowStores, storeID) {
			continue
		}

		if startTime != nil {
			// Already recovering
			continue
		}

		if slices.Contains(s.conf.EvictedNetworkSlowStores, storeID) {
			continue
		}

		store := cluster.GetStore(storeID)
		if store == nil {
			continue
		}

		if store.GetNetworkSlowTriggers() >= maxNetworkEvictThresholdTriggerCount {
			if err := cluster.SlowStoreEvicted(storeID); err != nil {
				log.Info("failed to evict slow store",
					zap.Uint64("store-id", storeID),
					zap.Error(err))
				continue
			}
			if err := s.conf.persistLocked(func() {
				s.conf.EvictedNetworkSlowStores = append(s.conf.EvictedNetworkSlowStores, storeID)
			}); err != nil {
				log.Warn("failed to persist evicted network slow store",
					zap.Uint64("store-id", storeID),
					zap.Error(err))
				cluster.SlowStoreRecovered(storeID)
			}
			log.Info("evicted network slow store", zap.Uint64("store-id", storeID))
			evictedSlowStoreStatusGauge.WithLabelValues(strconv.FormatUint(storeID, 10), string(networkEvictedStore)).Set(1)
		}
	}
}

// tryRecoverNetworkSlowStores attempts to recover network slow stores that have returned to normal
func (s *evictSlowStoreScheduler) tryRecoverNetworkSlowStores(cluster sche.SchedulerCluster) {
	var (
		networkSlowStoreRecoverStartAts = s.conf.networkSlowStoreRecoverStartAts
		recoveryGap                     = s.conf.RecoverySec
		storesStillSlow                 = make([]uint64, 0)
		storesPrepareToRecover          = make([]uint64, 0)
	)
	failpoint.Inject("transientRecoveryGap", func() {
		recoveryGap = 0
	})

	for storeID, startTime := range networkSlowStoreRecoverStartAts {
		store := cluster.GetStore(storeID)
		if store == nil || store.IsRemoved() || store.IsRemoving() {
			log.Info("network slow store has been removed",
				zap.Uint64("store-id", storeID))
			s.conf.deleteNetworkSlowStoreLocked(storeID, cluster)
			continue
		}

		networkSlowScores := filterNetworkSlowScores(store.GetNetworkSlowScores(), networkSlowStoreRecoverStartAts)
		avgScore := calculateAvgScore(networkSlowScores)

		// Recover the slow store only if the recoveryGap time is
		// continuously less than or equal to slowStoreRecoverThreshold.
		if avgScore > slowStoreRecoverThreshold {
			storesStillSlow = append(storesStillSlow, storeID)
			continue
		}

		if startTime == nil {
			storesPrepareToRecover = append(storesPrepareToRecover, storeID)
			continue
		}

		if uint64(time.Since(*startTime).Seconds()) >= recoveryGap {
			s.conf.deleteNetworkSlowStoreLocked(storeID, cluster)
		}
	}

	for _, storeID := range storesStillSlow {
		s.conf.networkSlowStoreRecoverStartAts[storeID] = nil
	}
	for _, storeID := range storesPrepareToRecover {
		now := time.Now()
		s.conf.networkSlowStoreRecoverStartAts[storeID] = &now
	}
}

// activatePausedNetworkSlowStores activates paused network slow stores if capacity allows
func (s *evictSlowStoreScheduler) activatePausedNetworkSlowStores(cluster sche.SchedulerCluster) {
	pausedNetworkSlowStores := s.conf.PausedNetworkSlowStores
	networkSlowStoreRecoverStartAts := s.conf.networkSlowStoreRecoverStartAts

	if len(pausedNetworkSlowStores) < defaultMaxNetworkSlowStore && len(networkSlowStoreRecoverStartAts) > 0 {
		var slowStoreID uint64
		for storeID := range networkSlowStoreRecoverStartAts {
			slowStoreID = storeID
			break
		}
		s.conf.addNetworkSlowStoreLocked(slowStoreID, cluster)
	}
}

// detectAndHandleNetworkSlowStores detects new network slow stores and handles them
func (s *evictSlowStoreScheduler) detectAndHandleNetworkSlowStores(cluster sche.SchedulerCluster) {
	stores := cluster.GetStores()
	networkSlowStoreRecoverStartAts := s.conf.networkSlowStoreRecoverStartAts
	pausedNetworkSlowStores := s.conf.PausedNetworkSlowStores

	// Build problematic network map
	problematicNetwork := buildProblematicNetworkMap(stores, networkSlowStoreRecoverStartAts)

	// Evaluate each store for network slowness
	for _, store := range stores {
		if shouldSkipStoreEvaluation(store, problematicNetwork, networkSlowStoreRecoverStartAts) {
			continue
		}

		if isNetworkSlowStore(store, stores, problematicNetwork, networkSlowStoreRecoverStartAts) {
			storeID := store.GetID()

			if len(pausedNetworkSlowStores) >= defaultMaxNetworkSlowStore {
				failpoint.InjectCall("evictSlowStoreTriggerLimit")
				slowStoreTriggerLimitGauge.WithLabelValues(strconv.FormatUint(storeID, 10), string(networkSlowStore)).Inc()
				s.conf.networkSlowStoreRecoverStartAts[storeID] = nil
				continue
			}

			s.conf.addNetworkSlowStoreLocked(storeID, cluster)
		}
	}
}

// buildProblematicNetworkMap builds a map of stores with problematic network connections
func buildProblematicNetworkMap(
	stores []*core.StoreInfo,
	networkSlowStoreRecoverStartAts map[uint64]*time.Time,
) map[uint64]map[uint64]struct{} {
	problematicNetwork := make(map[uint64]map[uint64]struct{})

	for _, store := range stores {
		storeID := store.GetID()
		if _, exist := networkSlowStoreRecoverStartAts[storeID]; exist {
			continue
		}

		networkSlowScores := filterNetworkSlowScores(store.GetNetworkSlowScores(), networkSlowStoreRecoverStartAts)
		if len(networkSlowScores) < 2 {
			continue
		}

		potentialSlowStores := filterPotentialSlowStores(networkSlowScores, networkSlowStoreIssueThreshold)
		if len(potentialSlowStores) == 0 {
			continue
		}

		for potentialSlowStore := range potentialSlowStores {
			if _, ok := problematicNetwork[potentialSlowStore]; !ok {
				problematicNetwork[potentialSlowStore] = make(map[uint64]struct{})
			}
			if _, ok := problematicNetwork[storeID]; !ok {
				problematicNetwork[storeID] = make(map[uint64]struct{})
			}
			problematicNetwork[potentialSlowStore][storeID] = struct{}{}
			problematicNetwork[storeID][potentialSlowStore] = struct{}{}
		}
	}

	return problematicNetwork
}

// shouldSkipStoreEvaluation checks if a store should be skipped for network slowness evaluation
func shouldSkipStoreEvaluation(
	store *core.StoreInfo,
	problematicNetwork map[uint64]map[uint64]struct{},
	networkSlowStoreRecoverStartAts map[uint64]*time.Time,
) bool {
	storeID := store.GetID()
	_, hasProblematicNetwork := problematicNetwork[storeID]
	_, alreadySlowStore := networkSlowStoreRecoverStartAts[storeID]

	return !hasProblematicNetwork || alreadySlowStore
}

// isNetworkSlowStore determines if a store should be considered a network slow store
func isNetworkSlowStore(
	store *core.StoreInfo,
	allStores []*core.StoreInfo,
	problematicNetwork map[uint64]map[uint64]struct{},
	networkSlowStoreRecoverStartAts map[uint64]*time.Time,
) bool {
	storeID := store.GetID()
	networkSlowScores := filterNetworkSlowScores(store.GetNetworkSlowScores(), networkSlowStoreRecoverStartAts)

	// Can not detect slow stores with less than 3 scores
	if len(networkSlowScores) <= 2 {
		return false
	}

	// Check if all other stores report problems with this store
	if len(problematicNetwork[storeID]) >= len(allStores)-1-len(networkSlowStoreRecoverStartAts) {
		return true
	}

	// Check for network fluctuations with other peers
	fluctuationCount := 0
	problematicStores := problematicNetwork[storeID]
	for storeID, score := range networkSlowScores {
		// There is a network problem, but we don't know which side of the network has the problem.
		// To avoid misjudgment, filter it.
		if _, isProblematic := problematicStores[storeID]; isProblematic {
			continue
		}
		if score >= networkSlowStoreFluctuationThreshold {
			fluctuationCount++
		}
	}
	// At least 2 scores >= networkSlowStoreFluctuationThreshold (10) after removing
	// problematic network. This confirms the store has network fluctuations with other peers
	return fluctuationCount >= 2
}

// filterNetworkSlowScores removes already slow stores from network slow scores to avoid duplicate detection
func filterNetworkSlowScores(
	scores map[uint64]uint64,
	networkSlowStoreRecoverStartAts map[uint64]*time.Time,
) map[uint64]uint64 {
	filteredScores := make(map[uint64]uint64)
	for storeID, score := range scores {
		if _, isSlowStore := networkSlowStoreRecoverStartAts[storeID]; !isSlowStore {
			filteredScores[storeID] = score
		}
	}
	return filteredScores
}

func (s *evictSlowStoreScheduler) scheduleDiskSlowStore(cluster sche.SchedulerCluster) {
	if s.conf.evictStore() != 0 {
		store := cluster.GetStore(s.conf.evictStore())
		storeIDStr := strconv.FormatUint(store.GetID(), 10)
		if store == nil || store.IsRemoved() {
			// Previous slow store had been removed, remove the scheduler and check
			// slow node next time.
			log.Info("slow store has been removed",
				zap.Uint64("store-id", store.GetID()))
			evictedSlowStoreStatusGauge.DeleteLabelValues(storeIDStr, string(diskSlowStore))
			s.cleanupEvictLeader(cluster)
			return
		}
		// recover slow store if its score is below the threshold.
		if store.GetSlowScore() <= slowStoreRecoverThreshold {
			if err := s.conf.tryUpdateRecoverStatus(true); err != nil {
				log.Info("evict-slow-store-scheduler persist config failed", zap.Uint64("store-id", store.GetID()), zap.Error(err))
				return
			}

			if !s.conf.readyForRecovery() {
				return
			}

			log.Info("slow store has been recovered",
				zap.Uint64("store-id", store.GetID()))
			evictedSlowStoreStatusGauge.DeleteLabelValues(storeIDStr, string(diskSlowStore))
			s.cleanupEvictLeader(cluster)
			return
		}
		// If the slow store is still slow or slow again, we can continue to evict leaders from it.
		if err := s.conf.tryUpdateRecoverStatus(false); err != nil {
			log.Info("evict-slow-store-scheduler persist config failed", zap.Uint64("store-id", store.GetID()), zap.Error(err))
			return
		}
		return
	}

	var slowStore *core.StoreInfo

	for _, store := range cluster.GetStores() {
		if store.IsRemoved() {
			continue
		}

		if (store.IsPreparing() || store.IsServing()) && store.IsSlow() {
			// Do nothing if there is more than one slow store.
			if slowStore != nil {
				return
			}
			slowStore = store
		}
	}

	if slowStore == nil || slowStore.GetSlowScore() < slowStoreEvictThreshold {
		return
	}

	// If there is only one slow store, evict leaders from that store.
	log.Info("detected slow store, start to evict leaders",
		zap.Uint64("store-id", slowStore.GetID()))
	err := s.prepareEvictLeader(cluster, slowStore.GetID())
	if err != nil {
		log.Info("prepare for evicting leader failed", zap.Error(err), zap.Uint64("store-id", slowStore.GetID()))
		return
	}
	// Record the slow store evicted status.
	storeIDStr := strconv.FormatUint(slowStore.GetID(), 10)
	evictedSlowStoreStatusGauge.WithLabelValues(storeIDStr, string(diskSlowStore)).Set(1)
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

func calculateAvgScore(scores map[uint64]uint64) uint64 {
	if len(scores) == 0 {
		return 0
	}
	var sum uint64

	for _, score := range scores {
		sum += score
	}

	return sum / uint64(len(scores))
}

// filterPotentialSlowStores filters the potential stores that are considered slow based on the threshold
func filterPotentialSlowStores(scores map[uint64]uint64, threshold uint64) map[uint64]struct{} {
	potentialSlowStores := make(map[uint64]struct{})

	for storeID, score := range scores {
		if score >= threshold {
			potentialSlowStores[storeID] = struct{}{}
		}
	}
	return potentialSlowStores
}
