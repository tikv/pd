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
	"slices"
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
	slowStoreRecoverThreshold            = 1
	// Currently only support one network slow store
	defaultMaxNetworkSlowStore = 1
)

type slowStoreType string

const (
	diskSlowStore    slowStoreType = "disk"
	networkSlowStore slowStoreType = "network"
)

type evictSlowStoreSchedulerConfig struct {
	baseDefaultSchedulerConfig

	cluster *core.BasicCluster
	// Last timestamp of the chosen slow store for eviction.
	lastSlowStoreCaptureTS     time.Time
	isRecovered                bool
	networkSlowStoreCaptureTSs map[uint64]time.Time
	// Duration gap for recovering the candidate, unit: s.
	RecoverySec uint64 `json:"recovery-duration"`
	// EvictedStores is only used by disk slow store scheduler
	EvictedStores           []uint64 `json:"evict-stores"`
	EnableNetworkSlowStore  bool     `json:"enable-network-slow-store"`
	PausedNetworkSlowStores []uint64 `json:"network-slow-stores"`
	// TODO: We only add batch for evict-slow-store-scheduler now.
	// If necessary, we also need to support evict-slow-trend-scheduler.
	Batch int `json:"batch"`
}

func initEvictSlowStoreSchedulerConfig() *evictSlowStoreSchedulerConfig {
	return &evictSlowStoreSchedulerConfig{
		baseDefaultSchedulerConfig: newBaseDefaultSchedulerConfig(),
		lastSlowStoreCaptureTS:     time.Time{},
		RecoverySec:                defaultRecoverySec,
		EvictedStores:              make([]uint64, 0),
		Batch:                      EvictLeaderBatchSize,
		EnableNetworkSlowStore:     true,
		PausedNetworkSlowStores:    make([]uint64, 0),
		networkSlowStoreCaptureTSs: make(map[uint64]time.Time),
	}
}

func (conf *evictSlowStoreSchedulerConfig) clone() *evictSlowStoreSchedulerConfig {
	conf.RLock()
	defer conf.RUnlock()
	return &evictSlowStoreSchedulerConfig{
		RecoverySec:             conf.RecoverySec,
		Batch:                   conf.Batch,
		EvictedStores:           conf.EvictedStores,
		EnableNetworkSlowStore:  conf.EnableNetworkSlowStore,
		PausedNetworkSlowStores: conf.PausedNetworkSlowStores,
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

func (conf *evictSlowStoreSchedulerConfig) getPausedNetworkSlowStores() []uint64 {
	conf.RLock()
	defer conf.RUnlock()
	return conf.PausedNetworkSlowStores
}

func (conf *evictSlowStoreSchedulerConfig) deleteNetworkSlowStoreLocked(storeID uint64, cluster sche.SchedulerCluster) {
	log.Info("deleting network slow store",
		zap.Uint64("store-id", storeID))
	oldPausedNetworkSlowStores := conf.PausedNetworkSlowStores
	conf.PausedNetworkSlowStores = slices.DeleteFunc(conf.PausedNetworkSlowStores, func(val uint64) bool {
		return val == storeID
	})
	if err := conf.save(); err != nil {
		log.Warn("failed to persist evict slow store config",
			zap.Uint64("store-id", storeID),
			zap.Error(err))
		conf.PausedNetworkSlowStores = oldPausedNetworkSlowStores
		return
	}
	cluster.ResumeLeaderTransfer(storeID, constant.In)
	evictedSlowStoreStatusGauge.DeleteLabelValues(strconv.FormatUint(storeID, 10), string(networkSlowStore))
	delete(conf.networkSlowStoreCaptureTSs, storeID)
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

	oldValue := conf.PausedNetworkSlowStores
	conf.PausedNetworkSlowStores = append(conf.PausedNetworkSlowStores, storeID)
	oldCaptureTS, ok := conf.networkSlowStoreCaptureTSs[storeID]
	conf.networkSlowStoreCaptureTSs[storeID] = time.Now()

	err := conf.save()
	if err != nil {
		log.Warn("failed to persist evict slow store config",
			zap.Uint64("store-id", storeID),
			zap.Error(err))
		conf.PausedNetworkSlowStores = oldValue
		cluster.ResumeLeaderTransfer(storeID, constant.In)
		if !ok {
			delete(conf.networkSlowStoreCaptureTSs, storeID)
		} else {
			conf.networkSlowStoreCaptureTSs[storeID] = oldCaptureTS
		}
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

func (conf *evictSlowStoreSchedulerConfig) clearEvictedAndPersist() (oldID uint64, err error) {
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
	recoveryDurationGapFloat, inputRecoveryDuration := input["recovery-duration"].(float64)
	if input["recovery-duration"] != nil && !inputRecoveryDuration {
		handler.rd.JSON(w, http.StatusInternalServerError, errors.New("invalid argument for 'recovery-duration'").Error())
		return
	}

	batchFloat, inputBatch := input["batch"].(float64)
	if input["batch"] != nil && !inputBatch {
		handler.rd.JSON(w, http.StatusInternalServerError, errors.New("invalid argument for 'batch'").Error())
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
		handler.rd.JSON(w, http.StatusInternalServerError, errors.New("invalid argument for 'enable-network-slow-store'").Error())
		return
	}

	handler.config.Lock()
	defer handler.config.Unlock()
	if !inputRecoveryDuration {
		recoveryDurationGapFloat = float64(handler.config.RecoverySec)
	}
	if !inputBatch {
		batchFloat = float64(handler.config.Batch)
	}
	if !inputEnableNetworkSlowStore {
		enableNetworkSlowStore = handler.config.EnableNetworkSlowStore
	}
	prevRecoverySec := handler.config.RecoverySec
	prevBatch := handler.config.Batch
	prevEnableNetworkSlowStore := handler.config.EnableNetworkSlowStore
	recoverySec := uint64(recoveryDurationGapFloat)
	handler.config.RecoverySec = recoverySec
	handler.config.Batch = int(batchFloat)
	handler.config.EnableNetworkSlowStore = enableNetworkSlowStore
	if err := handler.config.save(); err != nil {
		handler.rd.JSON(w, http.StatusInternalServerError, err.Error())
		handler.config.RecoverySec = prevRecoverySec
		handler.config.Batch = prevBatch
		handler.config.EnableNetworkSlowStore = prevEnableNetworkSlowStore
		return
	}
	log.Info("evict-slow-store-scheduler update config",
		zap.Uint64("prev-recovery-duration", prevRecoverySec),
		zap.Uint64("cur-recovery-duration", recoverySec),
		zap.Int("prev-batch", prevBatch),
		zap.Float64("cur-batch", batchFloat),
		zap.Bool("prev-enable-network-slow-store", prevEnableNetworkSlowStore),
		zap.Bool("cur-enable-network-slow-store", enableNetworkSlowStore),
	)

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
	s.conf.EnableNetworkSlowStore = newCfg.EnableNetworkSlowStore
	s.conf.Batch = newCfg.Batch
	return nil
}

// PrepareConfig implements the Scheduler interface.
func (s *evictSlowStoreScheduler) PrepareConfig(cluster sche.SchedulerCluster) error {
	evictStore := s.conf.evictStore()
	if evictStore != 0 {
		return cluster.SlowStoreEvicted(evictStore)
	}
	for _, storeID := range s.conf.getPausedNetworkSlowStores() {
		// only support one network slow store now
		return cluster.PauseLeaderTransfer(storeID, constant.In)
	}
	return nil
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
	err := s.conf.setStoreAndPersist(storeID)
	if err != nil {
		log.Info("evict-slow-store-scheduler persist config failed", zap.Uint64("store-id", storeID))
		return err
	}

	return cluster.SlowStoreEvicted(storeID)
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
	s.scheduleNetworkSlowStore(cluster)
	return s.scheduleDiskSlowStore(cluster), nil
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
	} else {
		// Clear network slow stores if the feature is disabled
		for _, storeID := range s.conf.PausedNetworkSlowStores {
			s.conf.deleteNetworkSlowStoreLocked(storeID, cluster)
		}
		clear(s.conf.networkSlowStoreCaptureTSs)
	}
}

// tryRecoverNetworkSlowStores attempts to recover network slow stores that have returned to normal
func (s *evictSlowStoreScheduler) tryRecoverNetworkSlowStores(cluster sche.SchedulerCluster) {
	networkSlowStoreCaptureTSs := s.conf.networkSlowStoreCaptureTSs
	recoveryGap := s.conf.RecoverySec
	failpoint.Inject("transientRecoveryGap", func() {
		recoveryGap = 0
	})

	for storeID, startTime := range networkSlowStoreCaptureTSs {
		store := cluster.GetStore(storeID)
		if store == nil || store.IsRemoved() || store.IsRemoving() {
			log.Info("network slow store has been removed",
				zap.Uint64("store-id", storeID))
			s.conf.deleteNetworkSlowStoreLocked(storeID, cluster)
			continue
		}

		if shouldRecoverNetworkSlowStore(store, startTime, networkSlowStoreCaptureTSs, recoveryGap) {
			s.conf.deleteNetworkSlowStoreLocked(storeID, cluster)
		}
	}
}

// shouldRecoverNetworkSlowStore checks if a network slow store should be recovered
func shouldRecoverNetworkSlowStore(
	store *core.StoreInfo,
	startTime time.Time,
	networkSlowStoreCaptureTSs map[uint64]time.Time,
	recoveryGap uint64,
) bool {
	networkSlowScores := filterNetworkSlowScores(store.GetNetworkSlowScores(), networkSlowStoreCaptureTSs)
	return calculateAvgScore(networkSlowScores) <= slowStoreRecoverThreshold &&
		uint64(time.Since(startTime).Seconds()) >= recoveryGap
}

// activatePausedNetworkSlowStores activates paused network slow stores if capacity allows
func (s *evictSlowStoreScheduler) activatePausedNetworkSlowStores(cluster sche.SchedulerCluster) {
	pausedNetworkSlowStores := s.conf.PausedNetworkSlowStores
	networkSlowStoreCaptureTSs := s.conf.networkSlowStoreCaptureTSs

	if len(pausedNetworkSlowStores) < defaultMaxNetworkSlowStore && len(networkSlowStoreCaptureTSs) > 0 {
		var slowStoreID uint64
		for storeID := range networkSlowStoreCaptureTSs {
			slowStoreID = storeID
			break
		}
		s.conf.addNetworkSlowStoreLocked(slowStoreID, cluster)
	}
}

// detectAndHandleNetworkSlowStores detects new network slow stores and handles them
func (s *evictSlowStoreScheduler) detectAndHandleNetworkSlowStores(cluster sche.SchedulerCluster) {
	stores := cluster.GetStores()
	networkSlowStoreCaptureTSs := s.conf.networkSlowStoreCaptureTSs
	pausedNetworkSlowStores := s.conf.PausedNetworkSlowStores

	// Build problematic network map
	problematicNetwork := buildProblematicNetworkMap(stores, networkSlowStoreCaptureTSs)

	// Evaluate each store for network slowness
	for _, store := range stores {
		if shouldSkipStoreEvaluation(store, problematicNetwork, networkSlowStoreCaptureTSs) {
			continue
		}

		if isNetworkSlowStore(store, stores, problematicNetwork, networkSlowStoreCaptureTSs) {
			storeID := store.GetID()

			if len(pausedNetworkSlowStores) >= defaultMaxNetworkSlowStore {
				failpoint.InjectCall("evictSlowStoreTriggerLimit")
				slowStoreTriggerLimitGauge.WithLabelValues(strconv.FormatUint(storeID, 10), string(networkSlowStore)).Inc()
				s.conf.networkSlowStoreCaptureTSs[storeID] = time.Now()
				continue
			}

			s.conf.addNetworkSlowStoreLocked(storeID, cluster)
		}
	}
}

// buildProblematicNetworkMap builds a map of stores with problematic network connections
func buildProblematicNetworkMap(
	stores []*core.StoreInfo,
	networkSlowStoreCaptureTSs map[uint64]time.Time,
) map[uint64]map[uint64]struct{} {
	problematicNetwork := make(map[uint64]map[uint64]struct{})

	for _, store := range stores {
		storeID := store.GetID()
		if _, exist := networkSlowStoreCaptureTSs[storeID]; exist {
			continue
		}

		networkSlowScores := filterNetworkSlowScores(store.GetNetworkSlowScores(), networkSlowStoreCaptureTSs)
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
	networkSlowStoreCaptureTSs map[uint64]time.Time,
) bool {
	storeID := store.GetID()
	_, hasProblematicNetwork := problematicNetwork[storeID]
	_, alreadySlowStore := networkSlowStoreCaptureTSs[storeID]

	return !hasProblematicNetwork || alreadySlowStore
}

// isNetworkSlowStore determines if a store should be considered a network slow store
func isNetworkSlowStore(
	store *core.StoreInfo,
	allStores []*core.StoreInfo,
	problematicNetwork map[uint64]map[uint64]struct{},
	networkSlowStoreCaptureTSs map[uint64]time.Time,
) bool {
	storeID := store.GetID()
	networkSlowScores := filterNetworkSlowScores(store.GetNetworkSlowScores(), networkSlowStoreCaptureTSs)

	// Can not detect slow stores with less than 3 scores
	if len(networkSlowScores) <= 2 {
		return false
	}

	// Check if all other stores report problems with this store
	if len(problematicNetwork[storeID]) >= len(allStores)-1-len(networkSlowStoreCaptureTSs) {
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
	networkSlowStoreCaptureTSs map[uint64]time.Time,
) map[uint64]uint64 {
	filteredScores := make(map[uint64]uint64)
	for storeID, score := range scores {
		if _, isSlowStore := networkSlowStoreCaptureTSs[storeID]; !isSlowStore {
			filteredScores[storeID] = score
		}
	}
	return filteredScores
}

func (s *evictSlowStoreScheduler) scheduleDiskSlowStore(cluster sche.SchedulerCluster) []*operator.Operator {
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
			return nil
		}
		// recover slow store if its score is below the threshold.
		if store.GetSlowScore() <= slowStoreRecoverThreshold {
			if err := s.conf.tryUpdateRecoverStatus(true); err != nil {
				log.Info("evict-slow-store-scheduler persist config failed", zap.Uint64("store-id", store.GetID()), zap.Error(err))
				return nil
			}

			if !s.conf.readyForRecovery() {
				return nil
			}

			log.Info("slow store has been recovered",
				zap.Uint64("store-id", store.GetID()))
			evictedSlowStoreStatusGauge.DeleteLabelValues(storeIDStr, string(diskSlowStore))
			s.cleanupEvictLeader(cluster)
			return nil
		}
		// If the slow store is still slow or slow again, we can continue to evict leaders from it.
		if err := s.conf.tryUpdateRecoverStatus(false); err != nil {
			log.Info("evict-slow-store-scheduler persist config failed", zap.Uint64("store-id", store.GetID()), zap.Error(err))
			return nil
		}
		return s.schedulerEvictLeader(cluster)
	}

	var slowStore *core.StoreInfo

	for _, store := range cluster.GetStores() {
		if store.IsRemoved() {
			continue
		}

		if (store.IsPreparing() || store.IsServing()) && store.IsSlow() {
			// Do nothing if there is more than one slow store.
			if slowStore != nil {
				return nil
			}
			slowStore = store
		}
	}

	if slowStore == nil || slowStore.GetSlowScore() < slowStoreEvictThreshold {
		return nil
	}

	// If there is only one slow store, evict leaders from that store.
	log.Info("detected slow store, start to evict leaders",
		zap.Uint64("store-id", slowStore.GetID()))
	err := s.prepareEvictLeader(cluster, slowStore.GetID())
	if err != nil {
		log.Info("prepare for evicting leader failed", zap.Error(err), zap.Uint64("store-id", slowStore.GetID()))
		return nil
	}
	// Record the slow store evicted status.
	storeIDStr := strconv.FormatUint(slowStore.GetID(), 10)
	evictedSlowStoreStatusGauge.WithLabelValues(storeIDStr, string(diskSlowStore)).Set(1)
	return s.schedulerEvictLeader(cluster)
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
