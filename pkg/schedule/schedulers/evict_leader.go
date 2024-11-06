// Copyright 2017 TiKV Project Authors.
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
	"math/rand"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/errs"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/plan"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/unrolled/render"
	"go.uber.org/zap"
)

const (
	// EvictLeaderBatchSize is the number of operators to transfer
	// leaders by one scheduling
	EvictLeaderBatchSize = 3
	lastStoreDeleteInfo  = "The last store has been deleted"
)

type evictLeaderSchedulerConfig struct {
	syncutil.RWMutex
	schedulerConfig

	StoreIDWithRanges map[uint64][]core.KeyRange `json:"store-id-ranges"`
	// Batch is used to generate multiple operators by one scheduling
	Batch             int `json:"batch"`
	cluster           *core.BasicCluster
	removeSchedulerCb func(string) error
}

func (conf *evictLeaderSchedulerConfig) getStores() []uint64 {
	conf.RLock()
	defer conf.RUnlock()
	stores := make([]uint64, 0, len(conf.StoreIDWithRanges))
	for storeID := range conf.StoreIDWithRanges {
		stores = append(stores, storeID)
	}
	return stores
}

func (conf *evictLeaderSchedulerConfig) getBatch() int {
	conf.RLock()
	defer conf.RUnlock()
	return conf.Batch
}

func (conf *evictLeaderSchedulerConfig) clone() *evictLeaderSchedulerConfig {
	conf.RLock()
	defer conf.RUnlock()
	storeIDWithRanges := make(map[uint64][]core.KeyRange)
	for id, ranges := range conf.StoreIDWithRanges {
		storeIDWithRanges[id] = append(storeIDWithRanges[id], ranges...)
	}
	return &evictLeaderSchedulerConfig{
		StoreIDWithRanges: storeIDWithRanges,
		Batch:             conf.Batch,
	}
}

func (conf *evictLeaderSchedulerConfig) getRanges(id uint64) []string {
	conf.RLock()
	defer conf.RUnlock()
	ranges := conf.StoreIDWithRanges[id]
	res := make([]string, 0, len(ranges)*2)
	for index := range ranges {
		res = append(res, (string)(ranges[index].StartKey), (string)(ranges[index].EndKey))
	}
	return res
}

func (conf *evictLeaderSchedulerConfig) removeStoreLocked(id uint64) (bool, error) {
	_, exists := conf.StoreIDWithRanges[id]
	if exists {
		delete(conf.StoreIDWithRanges, id)
		conf.cluster.ResumeLeaderTransfer(id)
		return len(conf.StoreIDWithRanges) == 0, nil
	}
	return false, errs.ErrScheduleConfigNotExist.FastGenByArgs()
}

func (conf *evictLeaderSchedulerConfig) resetStoreLocked(id uint64, keyRange []core.KeyRange) {
	if err := conf.cluster.PauseLeaderTransfer(id); err != nil {
		log.Error("pause leader transfer failed", zap.Uint64("store-id", id), errs.ZapError(err))
	}
	conf.StoreIDWithRanges[id] = keyRange
}

func (conf *evictLeaderSchedulerConfig) resetStore(id uint64, keyRange []core.KeyRange) {
	conf.Lock()
	defer conf.Unlock()
	conf.resetStoreLocked(id, keyRange)
}

func (conf *evictLeaderSchedulerConfig) getKeyRangesByID(id uint64) []core.KeyRange {
	conf.RLock()
	defer conf.RUnlock()
	if ranges, exist := conf.StoreIDWithRanges[id]; exist {
		return ranges
	}
	return nil
}

func (conf *evictLeaderSchedulerConfig) encodeConfig() ([]byte, error) {
	conf.RLock()
	defer conf.RUnlock()
	return EncodeConfig(conf)
}

func (conf *evictLeaderSchedulerConfig) reloadConfig() error {
	conf.Lock()
	defer conf.Unlock()
	newCfg := &evictLeaderSchedulerConfig{}
	if err := conf.load(newCfg); err != nil {
		return err
	}
	pauseAndResumeLeaderTransfer(conf.cluster, conf.StoreIDWithRanges, newCfg.StoreIDWithRanges)
	conf.StoreIDWithRanges = newCfg.StoreIDWithRanges
	conf.Batch = newCfg.Batch
	return nil
}

func (conf *evictLeaderSchedulerConfig) pauseLeaderTransfer(cluster sche.SchedulerCluster) error {
	conf.RLock()
	defer conf.RUnlock()
	var res error
	for id := range conf.StoreIDWithRanges {
		if err := cluster.PauseLeaderTransfer(id); err != nil {
			res = err
		}
	}
	return res
}

func (conf *evictLeaderSchedulerConfig) resumeLeaderTransfer(cluster sche.SchedulerCluster) {
	conf.RLock()
	defer conf.RUnlock()
	for id := range conf.StoreIDWithRanges {
		cluster.ResumeLeaderTransfer(id)
	}
}

func (conf *evictLeaderSchedulerConfig) pauseLeaderTransferIfStoreNotExist(id uint64) (bool, error) {
	conf.RLock()
	defer conf.RUnlock()
	if _, exist := conf.StoreIDWithRanges[id]; !exist {
		if err := conf.cluster.PauseLeaderTransfer(id); err != nil {
			return exist, err
		}
	}
	return true, nil
}

func (conf *evictLeaderSchedulerConfig) resumeLeaderTransferIfExist(id uint64) {
	conf.RLock()
	defer conf.RUnlock()
	conf.cluster.ResumeLeaderTransfer(id)
}

func (conf *evictLeaderSchedulerConfig) update(id uint64, newRanges []core.KeyRange, batch int) error {
	conf.Lock()
	defer conf.Unlock()
	if id != 0 {
		conf.StoreIDWithRanges[id] = newRanges
	}
	conf.Batch = batch
	err := conf.save()
	if err != nil && id != 0 {
		_, _ = conf.removeStoreLocked(id)
	}
	return err
}

func (conf *evictLeaderSchedulerConfig) delete(id uint64) (any, error) {
	conf.Lock()
	var resp any
	last, err := conf.removeStoreLocked(id)
	if err != nil {
		conf.Unlock()
		return resp, err
	}

	keyRanges := conf.StoreIDWithRanges[id]
	err = conf.save()
	if err != nil {
		conf.resetStoreLocked(id, keyRanges)
		conf.Unlock()
		return resp, err
	}
	if !last {
		conf.Unlock()
		return resp, nil
	}
	conf.Unlock()
	if err := conf.removeSchedulerCb(types.EvictLeaderScheduler.String()); err != nil {
		if !errors.ErrorEqual(err, errs.ErrSchedulerNotFound.FastGenByArgs()) {
			conf.resetStore(id, keyRanges)
		}
		return resp, err
	}
	resp = lastStoreDeleteInfo
	return resp, nil
}

type evictLeaderScheduler struct {
	*BaseScheduler
	conf    *evictLeaderSchedulerConfig
	handler http.Handler
}

// newEvictLeaderScheduler creates an admin scheduler that transfers all leaders
// out of a store.
func newEvictLeaderScheduler(opController *operator.Controller, conf *evictLeaderSchedulerConfig) Scheduler {
	handler := newEvictLeaderHandler(conf)
	return &evictLeaderScheduler{
		BaseScheduler: NewBaseScheduler(opController, types.EvictLeaderScheduler, conf),
		conf:          conf,
		handler:       handler,
	}
}

// EvictStoreIDs returns the IDs of the evict-stores.
func (s *evictLeaderScheduler) EvictStoreIDs() []uint64 {
	return s.conf.getStores()
}

// ServeHTTP implements the http.Handler interface.
func (s *evictLeaderScheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

// GetName implements the Scheduler interface.
func (s *evictLeaderScheduler) EncodeConfig() ([]byte, error) {
	return s.conf.encodeConfig()
}

// ReloadConfig reloads the config from the storage.
func (s *evictLeaderScheduler) ReloadConfig() error {
	return s.conf.reloadConfig()
}

// PrepareConfig implements the Scheduler interface.
func (s *evictLeaderScheduler) PrepareConfig(cluster sche.SchedulerCluster) error {
	return s.conf.pauseLeaderTransfer(cluster)
}

// CleanConfig implements the Scheduler interface.
func (s *evictLeaderScheduler) CleanConfig(cluster sche.SchedulerCluster) {
	s.conf.resumeLeaderTransfer(cluster)
}

// IsScheduleAllowed implements the Scheduler interface.
func (s *evictLeaderScheduler) IsScheduleAllowed(cluster sche.SchedulerCluster) bool {
	allowed := s.OpController.OperatorCount(operator.OpLeader) < cluster.GetSchedulerConfig().GetLeaderScheduleLimit()
	if !allowed {
		operator.IncOperatorLimitCounter(s.GetType(), operator.OpLeader)
	}
	return allowed
}

// Schedule implements the Scheduler interface.
func (s *evictLeaderScheduler) Schedule(cluster sche.SchedulerCluster, _ bool) ([]*operator.Operator, []plan.Plan) {
	evictLeaderCounter.Inc()
	return scheduleEvictLeaderBatch(s.R, s.GetName(), cluster, s.conf), nil
}

func uniqueAppendOperator(dst []*operator.Operator, src ...*operator.Operator) []*operator.Operator {
	regionIDs := make(map[uint64]struct{})
	for i := range dst {
		regionIDs[dst[i].RegionID()] = struct{}{}
	}
	for i := range src {
		if _, ok := regionIDs[src[i].RegionID()]; ok {
			continue
		}
		regionIDs[src[i].RegionID()] = struct{}{}
		dst = append(dst, src[i])
	}
	return dst
}

type evictLeaderStoresConf interface {
	getStores() []uint64
	getKeyRangesByID(id uint64) []core.KeyRange
	getBatch() int
}

func scheduleEvictLeaderBatch(r *rand.Rand, name string, cluster sche.SchedulerCluster, conf evictLeaderStoresConf) []*operator.Operator {
	var ops []*operator.Operator
	batchSize := conf.getBatch()
	for range batchSize {
		once := scheduleEvictLeaderOnce(r, name, cluster, conf)
		// no more regions
		if len(once) == 0 {
			break
		}
		ops = uniqueAppendOperator(ops, once...)
		// the batch has been fulfilled
		if len(ops) > batchSize {
			break
		}
	}
	return ops
}

func scheduleEvictLeaderOnce(r *rand.Rand, name string, cluster sche.SchedulerCluster, conf evictLeaderStoresConf) []*operator.Operator {
	stores := conf.getStores()
	ops := make([]*operator.Operator, 0, len(stores))
	for _, storeID := range stores {
		ranges := conf.getKeyRangesByID(storeID)
		if len(ranges) == 0 {
			continue
		}
		var filters []filter.Filter
		pendingFilter := filter.NewRegionPendingFilter()
		downFilter := filter.NewRegionDownFilter()
		region := filter.SelectOneRegion(cluster.RandLeaderRegions(storeID, ranges), nil, pendingFilter, downFilter)
		if region == nil {
			// try to pick unhealthy region
			region = filter.SelectOneRegion(cluster.RandLeaderRegions(storeID, ranges), nil)
			if region == nil {
				evictLeaderNoLeaderCounter.Inc()
				continue
			}
			evictLeaderPickUnhealthyCounter.Inc()
			unhealthyPeerStores := make(map[uint64]struct{})
			for _, peer := range region.GetDownPeers() {
				unhealthyPeerStores[peer.GetPeer().GetStoreId()] = struct{}{}
			}
			for _, peer := range region.GetPendingPeers() {
				unhealthyPeerStores[peer.GetStoreId()] = struct{}{}
			}
			filters = append(filters, filter.NewExcludedFilter(name, nil, unhealthyPeerStores))
		}

		filters = append(filters, &filter.StoreStateFilter{ActionScope: name, TransferLeader: true, OperatorLevel: constant.Urgent})
		candidates := filter.NewCandidates(r, cluster.GetFollowerStores(region)).
			FilterTarget(cluster.GetSchedulerConfig(), nil, nil, filters...)
		// Compatible with old TiKV transfer leader logic.
		target := candidates.RandomPick()
		targets := candidates.PickAll()
		// `targets` MUST contains `target`, so only needs to check if `target` is nil here.
		if target == nil {
			evictLeaderNoTargetStoreCounter.Inc()
			continue
		}
		targetIDs := make([]uint64, 0, len(targets))
		for _, t := range targets {
			targetIDs = append(targetIDs, t.GetID())
		}
		op, err := operator.CreateTransferLeaderOperator(name, cluster, region, target.GetID(), targetIDs, operator.OpLeader)
		if err != nil {
			log.Debug("fail to create evict leader operator", errs.ZapError(err))
			continue
		}
		op.SetPriorityLevel(constant.Urgent)
		op.Counters = append(op.Counters, evictLeaderNewOperatorCounter)
		ops = append(ops, op)
	}
	return ops
}

type evictLeaderHandler struct {
	rd     *render.Render
	config *evictLeaderSchedulerConfig
}

func (handler *evictLeaderHandler) updateConfig(w http.ResponseWriter, r *http.Request) {
	var input map[string]any
	if err := apiutil.ReadJSONRespondError(handler.rd, w, r.Body, &input); err != nil {
		return
	}
	var (
		exist     bool
		err       error
		id        uint64
		newRanges []core.KeyRange
	)
	idFloat, inputHasStoreID := input["store_id"].(float64)
	if inputHasStoreID {
		id = (uint64)(idFloat)
		exist, err = handler.config.pauseLeaderTransferIfStoreNotExist(id)
		if err != nil {
			handler.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	}

	batch := handler.config.getBatch()
	batchFloat, ok := input["batch"].(float64)
	if ok {
		if batchFloat < 1 || batchFloat > 10 {
			handler.config.resumeLeaderTransferIfExist(id)
			handler.rd.JSON(w, http.StatusBadRequest, "batch is invalid, it should be in [1, 10]")
			return
		}
		batch = (int)(batchFloat)
	}

	ranges, ok := (input["ranges"]).([]string)
	if ok {
		if !inputHasStoreID {
			handler.config.resumeLeaderTransferIfExist(id)
			handler.rd.JSON(w, http.StatusInternalServerError, errs.ErrSchedulerConfig.FastGenByArgs("id"))
			return
		}
	} else if exist {
		ranges = handler.config.getRanges(id)
	}

	newRanges, err = getKeyRanges(ranges)
	if err != nil {
		handler.config.resumeLeaderTransferIfExist(id)
		handler.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	// StoreIDWithRanges is only changed in update function.
	err = handler.config.update(id, newRanges, batch)
	if err != nil {
		handler.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	handler.rd.JSON(w, http.StatusOK, "The scheduler has been applied to the store.")
}

func (handler *evictLeaderHandler) listConfig(w http.ResponseWriter, _ *http.Request) {
	conf := handler.config.clone()
	handler.rd.JSON(w, http.StatusOK, conf)
}

func (handler *evictLeaderHandler) deleteConfig(w http.ResponseWriter, r *http.Request) {
	idStr := mux.Vars(r)["store_id"]
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		handler.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	resp, err := handler.config.delete(id)
	if err != nil {
		if errors.ErrorEqual(err, errs.ErrSchedulerNotFound.FastGenByArgs()) || errors.ErrorEqual(err, errs.ErrScheduleConfigNotExist.FastGenByArgs()) {
			handler.rd.JSON(w, http.StatusNotFound, err.Error())
		} else {
			handler.rd.JSON(w, http.StatusInternalServerError, err.Error())
		}
		return
	}

	handler.rd.JSON(w, http.StatusOK, resp)
}

func newEvictLeaderHandler(config *evictLeaderSchedulerConfig) http.Handler {
	h := &evictLeaderHandler{
		config: config,
		rd:     render.New(render.Options{IndentJSON: true}),
	}
	router := mux.NewRouter()
	router.HandleFunc("/config", h.updateConfig).Methods(http.MethodPost)
	router.HandleFunc("/list", h.listConfig).Methods(http.MethodGet)
	router.HandleFunc("/delete/{store_id}", h.deleteConfig).Methods(http.MethodDelete)
	return router
}
