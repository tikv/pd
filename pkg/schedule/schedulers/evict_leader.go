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
	"fmt"
	"math"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/unrolled/render"
	"go.uber.org/zap"

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
	"github.com/tikv/pd/pkg/utils/keyutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

const (
	// EvictLeaderBatchSize is the number of operators to transfer
	// leaders by one scheduling
	EvictLeaderBatchSize    = 3
	maxEvictLeaderBatchSize = 100
	lastStoreDeleteInfo     = "The last store has been deleted"
)

var invalidEvictLeaderBatchSizeMsg = "batch must be an integer in [1, " + strconv.Itoa(maxEvictLeaderBatchSize) + "]"

func isValidEvictLeaderBatchSize(batchFloat float64) bool {
	return batchFloat >= 1 &&
		batchFloat <= maxEvictLeaderBatchSize &&
		batchFloat == float64(int(batchFloat))
}

type evictLeaderSchedulerConfig struct {
	syncutil.RWMutex
	schedulerConfig

	StoreIDWithRanges map[uint64][]keyutil.KeyRange `json:"store-id-ranges"`
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
	storeIDWithRanges := make(map[uint64][]keyutil.KeyRange)
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
		conf.cluster.ResumeLeaderTransfer(id, constant.In)
		return len(conf.StoreIDWithRanges) == 0, nil
	}
	return false, errs.ErrScheduleConfigNotExist.FastGenByArgs()
}

func (conf *evictLeaderSchedulerConfig) resetStoreLocked(id uint64, keyRange []keyutil.KeyRange) {
	if err := conf.cluster.PauseLeaderTransfer(id, constant.In); err != nil {
		log.Error("pause leader transfer failed", zap.Uint64("store-id", id), errs.ZapError(err))
	}
	conf.StoreIDWithRanges[id] = keyRange
}

func (conf *evictLeaderSchedulerConfig) resetStore(id uint64, keyRange []keyutil.KeyRange) {
	conf.Lock()
	defer conf.Unlock()
	conf.resetStoreLocked(id, keyRange)
}

func (conf *evictLeaderSchedulerConfig) getKeyRangesByID(id uint64) []keyutil.KeyRange {
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
	if newCfg.Batch == 0 {
		newCfg.Batch = EvictLeaderBatchSize
	}
	pauseAndResumeLeaderTransfer(conf.cluster, constant.In, conf.StoreIDWithRanges, newCfg.StoreIDWithRanges)
	conf.StoreIDWithRanges = newCfg.StoreIDWithRanges
	conf.Batch = newCfg.Batch
	return nil
}

func (conf *evictLeaderSchedulerConfig) pauseLeaderTransfer(cluster sche.SchedulerCluster) error {
	conf.RLock()
	defer conf.RUnlock()
	var res error
	for id := range conf.StoreIDWithRanges {
		if err := cluster.PauseLeaderTransfer(id, constant.In); err != nil {
			res = err
		}
	}
	return res
}

func (conf *evictLeaderSchedulerConfig) resumeLeaderTransfer(cluster sche.SchedulerCluster) {
	conf.RLock()
	defer conf.RUnlock()
	for id := range conf.StoreIDWithRanges {
		cluster.ResumeLeaderTransfer(id, constant.In)
	}
}

// applyStoreIDs pauses leader transfer for any newly added store, resolves
// each store's target ranges, and persists everything with a single save —
// all under one lock, so the whole call is atomic with respect to concurrent
// config updates on this scheduler. ids may be a single store (a store_id
// request), several (a store_ids batch), or empty (a batch-size-only
// update). When hasExplicitRanges is false, a store that already existed
// keeps its current ranges untouched; a brand-new store defaults to the
// whole key space. On failure, previously existing stores are restored to
// their prior ranges and newly added stores (and their leader-transfer
// pause) are rolled back entirely; Batch is restored too.
func (conf *evictLeaderSchedulerConfig) applyStoreIDs(ids []uint64, explicitRanges []keyutil.KeyRange, hasExplicitRanges bool, batch int) error {
	conf.Lock()
	defer conf.Unlock()

	prevBatch := conf.Batch
	prevRanges := make(map[uint64][]keyutil.KeyRange)
	var pausedIDs []uint64

	// rollback undoes everything applied to StoreIDWithRanges so far in this
	// call: newly added stores are removed (and un-paused), and previously
	// existing stores get their old ranges back. It's shared by both failure
	// points below so a mid-batch pause failure rolls back exactly as
	// completely as a later save failure does.
	rollback := func() {
		for _, id := range pausedIDs {
			delete(conf.StoreIDWithRanges, id)
			conf.cluster.ResumeLeaderTransfer(id, constant.In)
		}
		for id, ranges := range prevRanges {
			conf.StoreIDWithRanges[id] = ranges
		}
	}

	for _, id := range ids {
		old, existed := conf.StoreIDWithRanges[id]
		if !existed {
			if err := conf.cluster.PauseLeaderTransfer(id, constant.In); err != nil {
				rollback()
				return err
			}
			pausedIDs = append(pausedIDs, id)
			if hasExplicitRanges {
				conf.StoreIDWithRanges[id] = append([]keyutil.KeyRange(nil), explicitRanges...)
			} else {
				conf.StoreIDWithRanges[id] = []keyutil.KeyRange{keyutil.NewKeyRange("", "")}
			}
			continue
		}
		if !hasExplicitRanges {
			// Keep the existing store's current ranges untouched.
			continue
		}
		prevRanges[id] = old
		conf.StoreIDWithRanges[id] = append([]keyutil.KeyRange(nil), explicitRanges...)
	}
	conf.Batch = batch

	if err := conf.save(); err != nil {
		conf.Batch = prevBatch
		rollback()
		return err
	}
	return nil
}

func (conf *evictLeaderSchedulerConfig) delete(id uint64) (any, error) {
	conf.Lock()
	var resp any
	keyRanges := conf.StoreIDWithRanges[id]
	last, err := conf.removeStoreLocked(id)
	if err != nil {
		conf.Unlock()
		return resp, err
	}

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

// EncodeConfig encodes the config to a byte array.
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
	return scheduleEvictLeaderBatch(s.GetName(), cluster, s.conf), nil
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
	getKeyRangesByID(id uint64) []keyutil.KeyRange
	getBatch() int
}

func scheduleEvictLeaderBatch(name string, cluster sche.SchedulerCluster, conf evictLeaderStoresConf) []*operator.Operator {
	var ops []*operator.Operator
	batchSize := conf.getBatch()
	for range batchSize {
		once := scheduleEvictLeaderOnce(name, cluster, conf)
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

func scheduleEvictLeaderOnce(name string, cluster sche.SchedulerCluster, conf evictLeaderStoresConf) []*operator.Operator {
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
		candidates := filter.NewCandidates(cluster.GetFollowerStores(region)).
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

// updateConfig handles both a single "store_id" (backward compatible) and a
// "store_ids" array (batch) the same way: it resolves the target store list
// first, then calls applyStoreIDs once. No leader-transfer pausing happens
// until that single call, so an early validation failure (bad batch, bad
// ranges) never needs to roll anything back — nothing has been touched yet.
func (handler *evictLeaderHandler) updateConfig(w http.ResponseWriter, r *http.Request) {
	var input map[string]any
	if err := apiutil.ReadJSONRespondError(handler.rd, w, r.Body, &input); err != nil {
		return
	}

	ids, err := parseUpdateStoreIDs(input)
	if err != nil {
		handler.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	batch := handler.config.getBatch()
	batchFloat, inputBatch := input["batch"].(float64)
	if input["batch"] != nil && !inputBatch {
		handler.rd.JSON(w, http.StatusBadRequest, fmt.Sprintf("invalid argument for 'batch': expected a number, got %T", input["batch"]))
		return
	}
	if inputBatch {
		if !isValidEvictLeaderBatchSize(batchFloat) {
			handler.rd.JSON(w, http.StatusBadRequest, invalidEvictLeaderBatchSizeMsg)
			return
		}
		batch = (int)(batchFloat)
	}

	rawRanges, hasRanges := input["ranges"]
	var explicitRanges []keyutil.KeyRange
	if hasRanges {
		if len(ids) == 0 {
			handler.rd.JSON(w, http.StatusBadRequest, errs.ErrSchedulerConfig.FastGenByArgs("id"))
			return
		}
		strs, ok := decodeStringSlice(rawRanges)
		if !ok {
			handler.rd.JSON(w, http.StatusBadRequest, fmt.Sprintf("invalid argument for 'ranges': expected an array of strings, got %T", rawRanges))
			return
		}
		// An empty "ranges" array carries no range pairs to apply; treat it
		// the same as an omitted field instead of letting getKeyRanges
		// default it to the whole key space and overwrite existing stores'
		// custom ranges.
		if len(strs) > 0 {
			explicitRanges, err = getKeyRanges(strs)
			if err != nil {
				handler.rd.JSON(w, http.StatusBadRequest, err.Error())
				return
			}
		} else {
			hasRanges = false
		}
	}

	if err := handler.config.applyStoreIDs(ids, explicitRanges, hasRanges, batch); err != nil {
		handler.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	handler.rd.JSON(w, http.StatusOK, "The scheduler has been applied to the store.")
}

// parseUpdateStoreIDs reads the target store IDs for a config update from the
// request body. It accepts a single "store_id" (backward compatible with
// existing clients), a "store_ids" array to update several stores in one
// call, or neither (a batch-size-only update).
func parseUpdateStoreIDs(input map[string]any) ([]uint64, error) {
	_, hasStoreID := input["store_id"]
	rawStoreIDs, hasStoreIDs := input["store_ids"]
	switch {
	case hasStoreID && hasStoreIDs:
		return nil, errors.New("only one of store_id and store_ids can be set")
	case hasStoreIDs:
		arr, ok := rawStoreIDs.([]any)
		if !ok || len(arr) == 0 {
			return nil, errors.New("please input a right store id")
		}
		ids := make([]uint64, 0, len(arr))
		seen := make(map[uint64]struct{}, len(arr))
		for _, v := range arr {
			f, ok := v.(float64)
			if !ok {
				return nil, errors.New("please input a right store id")
			}
			id, ok := storeIDFromFloat(f)
			if !ok {
				return nil, errors.New("please input a right store id")
			}
			if _, dup := seen[id]; dup {
				continue
			}
			seen[id] = struct{}{}
			ids = append(ids, id)
		}
		return ids, nil
	case hasStoreID:
		f, ok := input["store_id"].(float64)
		if !ok {
			return nil, errors.New("please input a right store id")
		}
		id, ok := storeIDFromFloat(f)
		if !ok {
			return nil, errors.New("please input a right store id")
		}
		return []uint64{id}, nil
	default:
		return nil, nil
	}
}

// storeIDFromFloat converts a JSON-decoded number into a store ID, rejecting
// fractional, negative, or out-of-range values.
func storeIDFromFloat(f float64) (uint64, bool) {
	if f < 0 || f != math.Trunc(f) || f > float64(math.MaxUint64) {
		return 0, false
	}
	return uint64(f), true
}

// decodeStringSlice converts a JSON-decoded value into []string. A JSON
// array always decodes as []any (never []string), so this checks the
// element types explicitly instead of relying on a direct type assertion.
func decodeStringSlice(v any) ([]string, bool) {
	arr, ok := v.([]any)
	if !ok {
		return nil, false
	}
	strs := make([]string, 0, len(arr))
	for _, item := range arr {
		s, ok := item.(string)
		if !ok {
			return nil, false
		}
		strs = append(strs, s)
	}
	return strs, true
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
