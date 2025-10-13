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
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/plan"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

const (
	// EvictSlowStoreName is evict leader scheduler name.
	EvictSlowStoreName = "evict-slow-store-scheduler"
	// EvictSlowStoreType is evict leader scheduler type.
	EvictSlowStoreType = "evict-slow-store"

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

// WithLabelValues is a heavy operation, define variable to avoid call it every time.
var evictSlowStoreCounter = schedulerCounter.WithLabelValues(EvictSlowStoreName, "schedule")

type evictSlowStoreSchedulerConfig struct {
	mu      syncutil.RWMutex
	storage endpoint.ConfigStorage
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
}

func (conf *evictSlowStoreSchedulerConfig) Persist() error {
	name := conf.getSchedulerName()
	data, err := EncodeConfig(conf)
	failpoint.Inject("persistFail", func() {
		err = errors.New("fail to persist")
	})
	if err != nil {
		return err
	}
	return conf.storage.SaveSchedulerConfig(name, data)
}

func initEvictSlowStoreSchedulerConfig() *evictSlowStoreSchedulerConfig {
	return &evictSlowStoreSchedulerConfig{
		lastSlowStoreCaptureTS:     time.Time{},
		RecoverySec:                defaultRecoveryDurationGap,
		EvictedStores:              make([]uint64, 0),
		EnableNetworkSlowStore:     true,
		PausedNetworkSlowStores:    make([]uint64, 0),
		networkSlowStoreCaptureTSs: make(map[uint64]time.Time),
	}
}

func (conf *evictSlowStoreSchedulerConfig) getSchedulerName() string {
	return EvictSlowStoreName
}

func (conf *evictSlowStoreSchedulerConfig) clone() *evictSlowStoreSchedulerConfig {
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	return &evictSlowStoreSchedulerConfig{
		RecoverySec: conf.RecoverySec,
	}
}

func (conf *evictSlowStoreSchedulerConfig) getStores() []uint64 {
	return conf.EvictedStores
}

func (conf *evictSlowStoreSchedulerConfig) getKeyRangesByID(id uint64) []core.KeyRange {
	if conf.evictStore() != id {
		return nil
	}
	return []core.KeyRange{core.NewKeyRange("", "")}
}

func (conf *evictSlowStoreSchedulerConfig) getEnableNetworkSlowStore() bool {
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	return conf.EnableNetworkSlowStore
}

func (conf *evictSlowStoreSchedulerConfig) evictStore() uint64 {
	if len(conf.EvictedStores) == 0 {
		return 0
	}
	return conf.getStores()[0]
}

func (conf *evictSlowStoreSchedulerConfig) getPausedNetworkSlowStores() []uint64 {
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	return conf.PausedNetworkSlowStores
}

func (conf *evictSlowStoreSchedulerConfig) pauseNetworkSlowStore(storeID uint64) error {
	conf.mu.Lock()
	defer conf.mu.Unlock()
	oldValue := conf.PausedNetworkSlowStores
	conf.PausedNetworkSlowStores = append(conf.PausedNetworkSlowStores, storeID)
	err := conf.Persist()
	if err != nil {
		log.Warn("failed to persist evict slow store config",
			zap.Uint64("store-id", storeID),
			zap.Error(err))
		conf.PausedNetworkSlowStores = oldValue
	}
	return err
}

func (conf *evictSlowStoreSchedulerConfig) deleteNetworkSlowStore(storeID uint64, cluster sche.SchedulerCluster) {
	conf.mu.Lock()
	defer conf.mu.Unlock()

	oldPausedNetworkSlowStores := conf.PausedNetworkSlowStores
	conf.PausedNetworkSlowStores = slices.DeleteFunc(conf.PausedNetworkSlowStores, func(val uint64) bool {
		return val == storeID
	})
	if err := conf.Persist(); err != nil {
		log.Warn("failed to persist evict slow store config",
			zap.Uint64("store-id", storeID),
			zap.Error(err))
		conf.PausedNetworkSlowStores = oldPausedNetworkSlowStores
		return
	}
	cluster.ResumeLeaderTransfer(storeID)
	evictedSlowStoreStatusGauge.DeleteLabelValues(strconv.FormatUint(storeID, 10), string(networkSlowStore))
	delete(conf.networkSlowStoreCaptureTSs, storeID)
}

func (conf *evictSlowStoreSchedulerConfig) addNetworkSlowStore(storeID uint64, cluster sche.SchedulerCluster) {
	log.Info("detected network slow store, start to pause scheduler",
		zap.Uint64("store-id", storeID))
	conf.networkSlowStoreCaptureTSs[storeID] = time.Now()
	if err := cluster.PauseLeaderTransfer(storeID); err != nil {
		log.Warn("failed to pause leader transfer for network slow store",
			zap.Uint64("store-id", storeID),
			zap.Error(err))
		return
	}
	if err := conf.pauseNetworkSlowStore(storeID); err != nil {
		log.Warn("failed to persist evict slow store config",
			zap.Uint64("store-id", storeID),
			zap.Error(err))
		cluster.ResumeLeaderTransfer(storeID)
		return
	}
	evictedSlowStoreStatusGauge.WithLabelValues(strconv.FormatUint(storeID, 10), string(networkSlowStore)).Set(1)
}

func (conf *evictSlowStoreSchedulerConfig) getRecoverySec() uint64 {
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	failpoint.Inject("transientRecoveryGap", func() {
		failpoint.Return(0)
	})
	return conf.RecoverySec
}

// readyForRecovery checks whether the last captured candidate is ready for recovery.
func (conf *evictSlowStoreSchedulerConfig) readyForRecovery() bool {
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	recoverySec := conf.RecoverySec
	failpoint.Inject("transientRecoveryGap", func() {
		recoverySec = 0
	})
	return uint64(time.Since(conf.lastSlowStoreCaptureTS).Seconds()) >= recoverySec
}

func (conf *evictSlowStoreSchedulerConfig) setStoreAndPersist(id uint64) error {
	conf.EvictedStores = []uint64{id}
	return conf.Persist()
}

func (conf *evictSlowStoreSchedulerConfig) clearEvictedAndPersist() (oldID uint64, err error) {
	oldID = conf.evictStore()
	if oldID > 0 {
		conf.EvictedStores = []uint64{}
		err = conf.Persist()
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

	handler.config.mu.Lock()
	defer handler.config.mu.Unlock()
	prevRecoverySec := handler.config.RecoverySec
	recoverySec := uint64(recoveryDurationGapFloat)
	handler.config.RecoverySec = recoverySec
	if err := handler.config.Persist(); err != nil {
		handler.rd.JSON(w, http.StatusInternalServerError, err.Error())
		handler.config.RecoverySec = prevRecoverySec
		return
	}
	log.Info("evict-slow-store-scheduler update config", zap.Uint64("prev-recovery-duration", prevRecoverySec), zap.Uint64("cur-recovery-duration", recoverySec))
	handler.rd.JSON(w, http.StatusOK, "Config updated.")
}

func (handler *evictSlowStoreHandler) listConfig(w http.ResponseWriter, _ *http.Request) {
	conf := handler.config.clone()
	handler.rd.JSON(w, http.StatusOK, conf)
}

type evictSlowStoreScheduler struct {
	*BaseScheduler
	conf *evictSlowStoreSchedulerConfig
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

// ReloadConfig implements the Scheduler interface.
func (s *evictSlowStoreScheduler) ReloadConfig() error {
	s.conf.mu.Lock()
	defer s.conf.mu.Unlock()

	cfgData, err := s.conf.storage.LoadSchedulerConfig(s.GetName())
	if err != nil {
		return err
	}
	newCfg := &evictSlowStoreSchedulerConfig{}
	if err = DecodeConfig([]byte(cfgData), newCfg); err != nil {
		return err
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
	pauseAndResumeLeaderTransfer(s.conf.cluster, old, new)
	s.conf.RecoverySec = newCfg.RecoverySec
	s.conf.EvictedStores = newCfg.EvictedStores
	s.conf.PausedNetworkSlowStores = newCfg.PausedNetworkSlowStores
	return nil
}

// Prepare implements the Scheduler interface.
func (s *evictSlowStoreScheduler) Prepare(cluster sche.SchedulerCluster) error {
	evictStore := s.conf.evictStore()
	if evictStore != 0 {
		return cluster.SlowStoreEvicted(evictStore)
	}
	for _, storeID := range s.conf.getPausedNetworkSlowStores() {
		// only support one network slow store now
		return cluster.PauseLeaderTransfer(storeID)
	}
	return nil
}

func (s *evictSlowStoreScheduler) Cleanup(cluster sche.SchedulerCluster) {
	s.cleanupEvictLeader(cluster)
	networkSlowStores := s.conf.getPausedNetworkSlowStores()
	for _, storeID := range networkSlowStores {
		s.conf.deleteNetworkSlowStore(storeID, cluster)
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
	return scheduleEvictLeaderBatch(s.R, s.GetName(), s.GetType(), cluster, s.conf, EvictLeaderBatchSize)
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
	if s.conf.getEnableNetworkSlowStore() {
		s.scheduleNetworkSlowStore(cluster)
	}
	return s.scheduleDiskSlowStore(cluster), nil
}

func (s *evictSlowStoreScheduler) scheduleNetworkSlowStore(cluster sche.SchedulerCluster) {
	// Try to recover network slow stores that have become normal
	s.tryRecoverNetworkSlowStores(cluster)

	// Activate paused network slow stores if capacity allows
	s.activatePausedNetworkSlowStores(cluster)

	// Detect and handle new network slow stores
	s.detectAndHandleNetworkSlowStores(cluster)
}

// tryRecoverNetworkSlowStores attempts to recover network slow stores that have returned to normal
func (s *evictSlowStoreScheduler) tryRecoverNetworkSlowStores(cluster sche.SchedulerCluster) {
	networkSlowStoreCaptureTSs := s.conf.networkSlowStoreCaptureTSs
	recoveryGap := s.conf.getRecoverySec()

	for storeID, startTime := range networkSlowStoreCaptureTSs {
		store := cluster.GetStore(storeID)
		if store == nil || store.IsRemoved() || store.IsRemoving() {
			log.Info("network slow store has been removed",
				zap.Uint64("store-id", storeID))
			s.conf.deleteNetworkSlowStore(storeID, cluster)
			continue
		}

		if shouldRecoverNetworkSlowStore(store, startTime, networkSlowStoreCaptureTSs, recoveryGap) {
			s.conf.deleteNetworkSlowStore(storeID, cluster)
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
	pausedNetworkSlowStores := s.conf.getPausedNetworkSlowStores()
	networkSlowStoreCaptureTSs := s.conf.networkSlowStoreCaptureTSs

	if len(pausedNetworkSlowStores) < defaultMaxNetworkSlowStore && len(networkSlowStoreCaptureTSs) > 0 {
		var slowStoreID uint64
		for storeID := range networkSlowStoreCaptureTSs {
			slowStoreID = storeID
			break
		}
		s.conf.addNetworkSlowStore(slowStoreID, cluster)
	}
}

// detectAndHandleNetworkSlowStores detects new network slow stores and handles them
func (s *evictSlowStoreScheduler) detectAndHandleNetworkSlowStores(cluster sche.SchedulerCluster) {
	stores := cluster.GetStores()
	networkSlowStoreCaptureTSs := s.conf.networkSlowStoreCaptureTSs
	pausedNetworkSlowStores := s.conf.getPausedNetworkSlowStores()

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

			s.conf.addNetworkSlowStore(storeID, cluster)
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
		if store == nil || store.IsRemoved() {
			// Previous slow store had been removed, remove the scheduler and check
			// slow node next time.
			log.Info("slow store has been removed",
				zap.Uint64("store-id", store.GetID()))
		} else if store.GetSlowScore() <= slowStoreRecoverThreshold {
			log.Info("slow store has been recovered",
				zap.Uint64("store-id", store.GetID()))
		} else {
			return s.schedulerEvictLeader(cluster)
		}
		s.cleanupEvictLeader(cluster)
		return nil
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
	return s.schedulerEvictLeader(cluster)
}

// newEvictSlowStoreScheduler creates a scheduler that detects and evicts slow stores.
func newEvictSlowStoreScheduler(opController *operator.Controller, conf *evictSlowStoreSchedulerConfig) Scheduler {
	base := NewBaseScheduler(opController)

	s := &evictSlowStoreScheduler{
		BaseScheduler: base,
		conf:          conf,
	}
	return s
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
