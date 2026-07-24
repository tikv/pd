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
	"strings"
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
	"github.com/tikv/pd/pkg/schedule/filter"
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
	EvictedStores []uint64 `json:"evict-stores"`
	// GroupEvictionLabels are the location labels (e.g. ["zone"] or ["zone","rack"]) that
	// define a drainable failure domain. When set, the disk slow store scheduler evicts
	// leaders from all slow stores that share these label values. Empty disables grouping
	// and falls back to single-store eviction.
	GroupEvictionLabels      []string `json:"group-eviction-labels"`
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
		GroupEvictionLabels:             make([]string, 0),
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
		oldGroupEvictionLabels             = conf.GroupEvictionLabels
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
		conf.GroupEvictionLabels = oldGroupEvictionLabels
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

// evictedStores returns a copy of the disk slow stores currently being evicted.
func (conf *evictSlowStoreSchedulerConfig) evictedStores() []uint64 {
	conf.RLock()
	defer conf.RUnlock()
	res := make([]uint64, len(conf.EvictedStores))
	copy(res, conf.EvictedStores)
	return res
}

// isEvictingDiskSlowStore reports whether any disk slow store is being evicted.
func (conf *evictSlowStoreSchedulerConfig) isEvictingDiskSlowStore() bool {
	conf.RLock()
	defer conf.RUnlock()
	return len(conf.EvictedStores) > 0
}

// getGroupEvictionLabels returns a copy of the configured failure-domain labels.
func (conf *evictSlowStoreSchedulerConfig) getGroupEvictionLabels() []string {
	conf.RLock()
	defer conf.RUnlock()
	return slices.Clone(conf.GroupEvictionLabels)
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

// setEvictedStoresAndPersist replaces the disk slow store eviction set with ids
// and resets the capture timestamp (the group is freshly slow).
func (conf *evictSlowStoreSchedulerConfig) setEvictedStoresAndPersist(ids []uint64) error {
	conf.Lock()
	defer conf.Unlock()
	return conf.persistLocked(func() {
		conf.EvictedStores = ids
		// Reset the recovery clock so as for multi store issues we need to wait whole zone is stable
		conf.lastSlowStoreCaptureTS = time.Now()
	})
}

// removeEvictedAndPersist drops the given ids from the eviction set (only those
// actually present) and returns the ids removed. Used when an evicted store
// leaves the cluster mid-drain; the remaining group keeps its recovery clock.
func (conf *evictSlowStoreSchedulerConfig) removeEvictedAndPersist(ids []uint64) (removed []uint64, err error) {
	conf.Lock()
	defer conf.Unlock()
	drop := make(map[uint64]struct{}, len(ids))
	for _, id := range ids {
		drop[id] = struct{}{}
	}
	kept := make([]uint64, 0, len(conf.EvictedStores))
	for _, id := range conf.EvictedStores {
		if _, ok := drop[id]; ok {
			removed = append(removed, id)
			continue
		}
		kept = append(kept, id)
	}
	if len(removed) == 0 {
		return nil, nil
	}
	if err = conf.persistLocked(func() {
		conf.EvictedStores = kept
	}); err != nil {
		return nil, err
	}
	return removed, nil
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

// clearEvictedAndPersist releases the whole disk slow store eviction set and
// returns the ids that were evicted.
func (conf *evictSlowStoreSchedulerConfig) clearEvictedAndPersist() (oldIDs []uint64, err error) {
	conf.Lock()
	defer conf.Unlock()
	if len(conf.EvictedStores) == 0 {
		return nil, nil
	}
	oldIDs = make([]uint64, len(conf.EvictedStores))
	copy(oldIDs, conf.EvictedStores)
	err = conf.persistLocked(func() {
		conf.EvictedStores = []uint64{}
		conf.lastSlowStoreCaptureTS = time.Time{}
	})
	if err != nil {
		return nil, err
	}
	return oldIDs, nil
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

// parseGroupEvictionLabels splits a comma-separated label string (e.g. "zone,rack") into a
// trimmed, non-empty label slice. An empty/blank string yields an empty slice (disabled).
func parseGroupEvictionLabels(s string) []string {
	labels := make([]string, 0)
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			labels = append(labels, part)
		}
	}
	return labels
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
	if inputBatch && !isValidEvictLeaderBatchSize(batchFloat) {
		handler.rd.JSON(w, http.StatusBadRequest, invalidEvictLeaderBatchSizeMsg)
		return
	}

	enableNetworkSlowStore, inputEnableNetworkSlowStore := input["enable-network-slow-store"].(bool)
	if input["enable-network-slow-store"] != nil && !inputEnableNetworkSlowStore {
		handler.rd.JSON(w, http.StatusBadRequest, perrors.New("invalid argument for 'enable-network-slow-store'").Error())
		return
	}

	groupEvictionLabelsStr, inputGroupEvictionLabels := input["group-eviction-labels"].(string)
	if input["group-eviction-labels"] != nil && !inputGroupEvictionLabels {
		handler.rd.JSON(w, http.StatusBadRequest, perrors.New("invalid argument for 'group-eviction-labels'").Error())
		return
	}
	groupEvictionLabels := parseGroupEvictionLabels(groupEvictionLabelsStr)

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
		if inputGroupEvictionLabels {
			handler.config.GroupEvictionLabels = groupEvictionLabels
		}
	}); err != nil {
		handler.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	log.Info("evict-slow-store-scheduler update config",
		zap.Uint64("cur-recovery-duration", recoverySec),
		zap.Float64("cur-batch", batchFloat),
		zap.Bool("cur-enable-network-slow-store", enableNetworkSlowStore),
		zap.Strings("cur-group-eviction-labels", groupEvictionLabels),
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
		GroupEvictionLabels:      handler.config.GroupEvictionLabels,
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
	s.conf.GroupEvictionLabels = newCfg.GroupEvictionLabels
	s.conf.PausedNetworkSlowStores = newCfg.PausedNetworkSlowStores
	s.conf.EvictedNetworkSlowStores = newCfg.EvictedNetworkSlowStores
	s.conf.EnableNetworkSlowStore = newCfg.EnableNetworkSlowStore
	s.conf.Batch = newCfg.Batch
	return nil
}

// PrepareConfig implements the Scheduler interface.
func (s *evictSlowStoreScheduler) PrepareConfig(cluster sche.SchedulerCluster) error {
	errs := make([]error, 0)

	for _, evictStore := range s.conf.evictedStores() {
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

// prepareEvictLeader marks the given newly-detected stores as evicted (each is
// expected to not already be evicted) and persists the resulting set. On any
// failure it rolls back the marks it added so the slow-evicted counter stays
// balanced.
func (s *evictSlowStoreScheduler) prepareEvictLeader(cluster sche.SchedulerCluster, addIDs []uint64) error {
	if len(addIDs) == 0 {
		return nil
	}
	marked := make([]uint64, 0, len(addIDs))
	for _, storeID := range addIDs {
		if err := cluster.SlowStoreEvicted(storeID); err != nil {
			log.Info("failed to evict slow store", zap.Uint64("store-id", storeID), zap.Error(err))
			for _, id := range marked {
				cluster.SlowStoreRecovered(id)
			}
			return err
		}
		marked = append(marked, storeID)
	}

	newSet := append(s.conf.evictedStores(), addIDs...)
	if err := s.conf.setEvictedStoresAndPersist(newSet); err != nil {
		log.Info("failed to persist evicted slow store", zap.Uint64s("store-ids", addIDs), zap.Error(err))
		for _, id := range addIDs {
			cluster.SlowStoreRecovered(id)
		}
		return err
	}

	return nil
}

func (s *evictSlowStoreScheduler) cleanupEvictLeader(cluster sche.SchedulerCluster) {
	evictSlowStores, err := s.conf.clearEvictedAndPersist()
	if err != nil {
		log.Info("evict-slow-store-scheduler persist config failed", zap.Uint64s("store-ids", evictSlowStores))
	}
	for _, storeID := range evictSlowStores {
		cluster.SlowStoreRecovered(storeID)
		evictedSlowStoreStatusGauge.DeleteLabelValues(strconv.FormatUint(storeID, 10), string(diskSlowStore))
	}
}

func (s *evictSlowStoreScheduler) schedulerEvictLeader(cluster sche.SchedulerCluster) []*operator.Operator {
	return scheduleEvictLeaderBatch(s.GetName(), cluster, s.conf)
}

// IsScheduleAllowed implements the Scheduler interface.
func (s *evictSlowStoreScheduler) IsScheduleAllowed(cluster sche.SchedulerCluster) bool {
	if s.conf.isEvictingDiskSlowStore() {
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

// minRemainingHealthyDomains is the number of distinct failure domains (by the broadest
// configured label, besides the one being drained) that must still be able to host leaders
// before PD will group-evict. It prevents draining a domain on a too-small topology where
// all leaders would pile onto a single survivor.
const minRemainingHealthyDomains = 2

// collectDiskSlowStores returns the serving/preparing stores that are currently slow.
func collectDiskSlowStores(cluster sche.SchedulerCluster) []*core.StoreInfo {
	var slowStores []*core.StoreInfo
	for _, store := range cluster.GetStores() {
		if store.IsRemoved() {
			continue
		}
		if (store.IsPreparing() || store.IsServing()) && store.IsSlow() {
			slowStores = append(slowStores, store)
		}
	}
	return slowStores
}

func storeIDs(stores []*core.StoreInfo) []uint64 {
	ids := make([]uint64, 0, len(stores))
	for _, store := range stores {
		ids = append(ids, store.GetID())
	}
	return ids
}

// hasAllLabels reports whether the store has a non-empty value for every label.
func hasAllLabels(store *core.StoreInfo, labels []string) bool {
	for _, l := range labels {
		if store.GetLabelValue(l) == "" {
			return false
		}
	}
	return true
}

// sameLocation reports whether store is in the same failure domain as ref for the
// configured labels. It requires store to have all labels set, so an unlabeled store is
// never treated as co-located (CompareLocation alone would treat a missing label as a
// wildcard match).
func sameLocation(ref, store *core.StoreInfo, labels []string) bool {
	return hasAllLabels(store, labels) && ref.CompareLocation(store, labels) == -1
}

// eligibleHealthyLeaderTargets returns the stores that could actually receive leaders,
// applying the same target-store checks as a leader transfer (StoreStateFilter with
// TransferLeader set) so the domain counting matches what the operator layer will accept.
// It additionally drops disk-slow stores, which the leader-target filter does not reject.
func eligibleHealthyLeaderTargets(cluster sche.SchedulerCluster, stores []*core.StoreInfo) []*core.StoreInfo {
	candidates := filter.NewCandidates(stores).
		FilterTarget(
			cluster.GetSchedulerConfig(),
			nil,
			nil,
			&filter.StoreStateFilter{
				ActionScope:    types.EvictSlowStoreScheduler.String(),
				TransferLeader: true,
				OperatorLevel:  constant.Urgent,
			},
		).
		PickAll()

	healthy := candidates[:0]
	for _, store := range candidates {
		if store.IsSlow() {
			continue
		}
		healthy = append(healthy, store)
	}
	return healthy
}

// hasEnoughHealthyDomains reports whether at least minRemainingHealthyDomains distinct
// failure domains (by all configured labels) other than the drained store's domain still
// have a store able to host leaders. Domains are deduplicated with CompareLocation so the
// counting stays consistent with the grouping decision.
func hasEnoughHealthyDomains(cluster sche.SchedulerCluster, labels []string, drained *core.StoreInfo) bool {
	reps := make([]*core.StoreInfo, 0, minRemainingHealthyDomains)
	for _, store := range eligibleHealthyLeaderTargets(cluster, cluster.GetStores()) {
		if !hasAllLabels(store, labels) || drained.CompareLocation(store, labels) == -1 {
			continue
		}
		isNewDomain := true
		for _, rep := range reps {
			if rep.CompareLocation(store, labels) == -1 {
				isNewDomain = false
				break
			}
		}
		if isNewDomain {
			reps = append(reps, store)
			if len(reps) >= minRemainingHealthyDomains {
				return true
			}
		}
	}
	return false
}

// startEvictDiskSlowStores marks the given stores as evicted and records their status.
func (s *evictSlowStoreScheduler) startEvictDiskSlowStores(cluster sche.SchedulerCluster, stores []*core.StoreInfo) {
	ids := storeIDs(stores)
	if err := s.prepareEvictLeader(cluster, ids); err != nil {
		log.Info("prepare for evicting leader failed", zap.Error(err), zap.Uint64s("store-ids", ids))
		return
	}
	for _, id := range ids {
		evictedSlowStoreStatusGauge.WithLabelValues(strconv.FormatUint(id, 10), string(diskSlowStore)).Set(1)
	}
}

func (s *evictSlowStoreScheduler) scheduleDiskSlowStore(cluster sche.SchedulerCluster) {
	slowStores := collectDiskSlowStores(cluster)

	if s.conf.isEvictingDiskSlowStore() {
		s.reconcileDiskSlowStoreGroup(cluster, slowStores)
		return
	}

	// Detection: nothing is being evicted yet.
	if len(slowStores) == 0 {
		return
	}
	if len(slowStores) == 1 {
		// Single slow store: unchanged behavior.
		if slowStores[0].GetSlowScore() >= slowStoreEvictThreshold {
			log.Info("detected slow store, start to evict leaders",
				zap.Uint64("store-id", slowStores[0].GetID()))
			s.startEvictDiskSlowStores(cluster, slowStores[:1])
		}
		return
	}

	// Multiple slow stores: group eviction only when group-eviction-labels is configured and
	// every slow store shares that failure domain.
	labels := s.conf.getGroupEvictionLabels()
	if len(labels) == 0 {
		// Grouping disabled; keep the conservative behavior (do nothing for 2+ slow stores).
		return
	}
	ref := slowStores[0]
	if !hasAllLabels(ref, labels) {
		// Reference store is missing a configured label; cannot determine its domain.
		return
	}
	for _, store := range slowStores[1:] {
		if !sameLocation(ref, store, labels) {
			// Slow stores span more than one domain (or one lacks the labels); never
			// drain across failure domains.
			return
		}
	}
	if !hasEnoughHealthyDomains(cluster, labels, ref) {
		return
	}

	var toEvict []*core.StoreInfo
	for _, store := range slowStores {
		if store.GetSlowScore() >= slowStoreEvictThreshold {
			toEvict = append(toEvict, store)
		}
	}
	if len(toEvict) == 0 {
		// All same-domain slow stores are still below the evict threshold; wait.
		return
	}
	log.Info("detected slow stores in a single failure domain, start to evict leaders",
		zap.Strings("group-eviction-labels", labels),
		zap.Uint64s("store-ids", storeIDs(toEvict)))
	s.startEvictDiskSlowStores(cluster, toEvict)
}

// reconcileDiskSlowStoreGroup runs each cycle while a domain is being drained: it
// releases stores that left the cluster, releases the whole group once every
// evicted store has recovered for the recovery duration, and (unless a slow store
// has appeared outside the locked domain) adds newly-slow same-domain stores.
func (s *evictSlowStoreScheduler) reconcileDiskSlowStoreGroup(cluster sche.SchedulerCluster, slowStores []*core.StoreInfo) {
	evicted := s.conf.evictedStores()

	// Single pass: drop stores that have left the cluster; compute maxScore and pick a
	// surviving store as the locked-domain reference (all survivors share one domain by
	// construction) so we only call GetStore once per evicted id.
	var (
		gone       []uint64
		maxScore   uint64
		survivorID uint64
	)
	for _, id := range evicted {
		store := cluster.GetStore(id)
		if store == nil || store.IsRemoved() {
			gone = append(gone, id)
			continue
		}
		if score := store.GetSlowScore(); score > maxScore {
			maxScore = score
		}
		survivorID = id
	}
	if len(gone) > 0 {
		removed, err := s.conf.removeEvictedAndPersist(gone)
		if err != nil {
			log.Info("evict-slow-store-scheduler persist config failed", zap.Uint64s("store-ids", gone), zap.Error(err))
			return
		}
		for _, id := range removed {
			log.Info("evicted slow store has been removed", zap.Uint64("store-id", id))
			cluster.SlowStoreRecovered(id)
			evictedSlowStoreStatusGauge.DeleteLabelValues(strconv.FormatUint(id, 10), string(diskSlowStore))
		}
	}
	if !s.conf.isEvictingDiskSlowStore() {
		// All evicted stores have left the cluster.
		return
	}

	// Group recovery: release the whole group only when every evicted store has
	// recovered below the threshold for the recovery duration.
	if maxScore <= slowStoreRecoverThreshold {
		if err := s.conf.tryUpdateRecoverStatus(true); err != nil {
			log.Info("evict-slow-store-scheduler persist config failed", zap.Error(err))
			return
		}
		if !s.conf.readyForRecovery() {
			return
		}
		log.Info("slow store group has been recovered", zap.Uint64s("store-ids", s.conf.evictedStores()))
		s.cleanupEvictLeader(cluster)
		return
	}
	if err := s.conf.tryUpdateRecoverStatus(false); err != nil {
		log.Info("evict-slow-store-scheduler persist config failed", zap.Error(err))
		return
	}

	// Expansion is config-driven. If group-eviction-labels was cleared (or the locked store no
	// longer carries the labels), keep draining the current group but do not expand.
	// survivorID is always set here because isEvictingDiskSlowStore returned true and
	// every surviving store was visited in the loop above.
	labels := s.conf.getGroupEvictionLabels()
	if len(labels) == 0 {
		return
	}
	lockedRef := cluster.GetStore(survivorID)
	if lockedRef == nil || !hasAllLabels(lockedRef, labels) {
		return
	}

	// Freeze brake: if any slow store sits outside the locked domain, stop expanding
	// the eviction set (we never start draining a second domain).
	for _, store := range slowStores {
		if !sameLocation(lockedRef, store, labels) {
			return
		}
	}

	// Only expand if the topology can still absorb draining the locked domain.
	// This prevents the single-store → reconcile-add bypass of the domain guard.
	if !hasEnoughHealthyDomains(cluster, labels, lockedRef) {
		return
	}

	// Reconcile: add newly-slow same-domain stores that reached the evict threshold.
	// EvictedAsSlowStore() is the authoritative "already marked" check; it covers
	// both disk- and network-evicted stores, so no separate set is needed.
	var toAdd []*core.StoreInfo
	for _, store := range slowStores {
		if store.EvictedAsSlowStore() {
			continue
		}
		if sameLocation(lockedRef, store, labels) && store.GetSlowScore() >= slowStoreEvictThreshold {
			toAdd = append(toAdd, store)
		}
	}
	if len(toAdd) > 0 {
		log.Info("adding newly-slow stores in the locked domain to eviction",
			zap.Strings("group-eviction-labels", labels), zap.Uint64s("store-ids", storeIDs(toAdd)))
		s.startEvictDiskSlowStores(cluster, toAdd)
	}
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
