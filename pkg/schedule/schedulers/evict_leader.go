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
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/errs"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/plan"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/unrolled/render"
)

const (
	// EvictLeaderName is evict leader scheduler name.
	EvictLeaderName = "evict-leader-scheduler"
	// EvictLeaderType is evict leader scheduler type.
	EvictLeaderType = "evict-leader"
	// EvictLeaderBatchSize is the number of operators to to transfer
	// leaders by one scheduling
	EvictLeaderBatchSize = 3
	lastStoreDeleteInfo  = "The last store has been deleted"
)

var (
	// WithLabelValues is a heavy operation, define variable to avoid call it every time.
	evictLeaderCounter              = schedulerCounter.WithLabelValues(EvictLeaderName, "schedule")
	evictLeaderNoLeaderCounter      = schedulerCounter.WithLabelValues(EvictLeaderName, "no-leader")
	evictLeaderPickUnhealthyCounter = schedulerCounter.WithLabelValues(EvictLeaderName, "pick-unhealthy-region")
	evictLeaderNoTargetStoreCounter = schedulerCounter.WithLabelValues(EvictLeaderName, "no-target-store")
	evictLeaderNewOperatorCounter   = schedulerCounter.WithLabelValues(EvictLeaderName, "new-operator")
)

type evictLeaderSchedulerConfig struct {
	syncutil.RWMutex
	storage           endpoint.ConfigStorage
	StoreIDWithRanges map[uint64][]core.KeyRange `json:"store-id-ranges"`
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

func (conf *evictLeaderSchedulerConfig) BuildWithArgs(args []string) error {
	if len(args) != 1 {
		return errs.ErrSchedulerConfig.FastGenByArgs("id")
	}

	id, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		return errs.ErrStrconvParseUint.Wrap(err)
	}
	ranges, err := getKeyRanges(args[1:])
	if err != nil {
		return err
	}
	conf.Lock()
	defer conf.Unlock()
	conf.StoreIDWithRanges[id] = ranges
	return nil
}

func (conf *evictLeaderSchedulerConfig) Clone() *evictLeaderSchedulerConfig {
	conf.RLock()
	defer conf.RUnlock()
	storeIDWithRanges := make(map[uint64][]core.KeyRange)
	for id, ranges := range conf.StoreIDWithRanges {
		storeIDWithRanges[id] = append(storeIDWithRanges[id], ranges...)
	}
	return &evictLeaderSchedulerConfig{
		StoreIDWithRanges: storeIDWithRanges,
	}
}

func (conf *evictLeaderSchedulerConfig) Persist() error {
	name := conf.getSchedulerName()
	conf.RLock()
	defer conf.RUnlock()
	data, err := EncodeConfig(conf)
	failpoint.Inject("persistFail", func() {
		err = errors.New("fail to persist")
	})
	if err != nil {
		return err
	}
	return conf.storage.SaveSchedulerConfig(name, data)
}

func (conf *evictLeaderSchedulerConfig) getSchedulerName() string {
	return EvictLeaderName
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

func (conf *evictLeaderSchedulerConfig) removeStore(id uint64) (succ bool, last bool) {
	conf.Lock()
	defer conf.Unlock()
	_, exists := conf.StoreIDWithRanges[id]
	succ, last = false, false
	if exists {
		delete(conf.StoreIDWithRanges, id)
		conf.cluster.ResumeLeaderTransfer(id)
		succ = true
		last = len(conf.StoreIDWithRanges) == 0
	}
	return succ, last
}

func (conf *evictLeaderSchedulerConfig) resetStore(id uint64, keyRange []core.KeyRange) {
	conf.Lock()
	defer conf.Unlock()
	conf.cluster.PauseLeaderTransfer(id)
	conf.StoreIDWithRanges[id] = keyRange
}

func (conf *evictLeaderSchedulerConfig) getKeyRangesByID(id uint64) []core.KeyRange {
	conf.RLock()
	defer conf.RUnlock()
	if ranges, exist := conf.StoreIDWithRanges[id]; exist {
		return ranges
	}
	return nil
}

type evictLeaderScheduler struct {
	*BaseScheduler
	conf    *evictLeaderSchedulerConfig
	handler http.Handler
}

// newEvictLeaderScheduler creates an admin scheduler that transfers all leaders
// out of a store.
func newEvictLeaderScheduler(opController *operator.Controller, conf *evictLeaderSchedulerConfig) Scheduler {
	base := NewBaseScheduler(opController)
	handler := newEvictLeaderHandler(conf)
	return &evictLeaderScheduler{
		BaseScheduler: base,
		conf:          conf,
		handler:       handler,
	}
}

// EvictStoreIDs returns the IDs of the evict-stores.
func (s *evictLeaderScheduler) EvictStoreIDs() []uint64 {
	return s.conf.getStores()
}

func (s *evictLeaderScheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

func (s *evictLeaderScheduler) GetName() string {
	return EvictLeaderName
}

func (s *evictLeaderScheduler) GetType() string {
	return EvictLeaderType
}

func (s *evictLeaderScheduler) EncodeConfig() ([]byte, error) {
	s.conf.RLock()
	defer s.conf.RUnlock()
	return EncodeConfig(s.conf)
}

func (s *evictLeaderScheduler) ReloadConfig() error {
	s.conf.Lock()
	defer s.conf.Unlock()
	cfgData, err := s.conf.storage.LoadSchedulerConfig(s.GetName())
	if err != nil {
		return err
	}
	if len(cfgData) == 0 {
		return nil
	}
	newCfg := &evictLeaderSchedulerConfig{}
	if err = DecodeConfig([]byte(cfgData), newCfg); err != nil {
		return err
	}
	pauseAndResumeLeaderTransfer(s.conf.cluster, s.conf.StoreIDWithRanges, newCfg.StoreIDWithRanges)
	s.conf.StoreIDWithRanges = newCfg.StoreIDWithRanges
	return nil
}

func (s *evictLeaderScheduler) PrepareConfig(cluster sche.SchedulerCluster) error {
	s.conf.RLock()
	defer s.conf.RUnlock()
	var res error
	for id := range s.conf.StoreIDWithRanges {
		if err := cluster.PauseLeaderTransfer(id); err != nil {
			res = err
		}
	}
	return res
}

func (s *evictLeaderScheduler) CleanConfig(cluster sche.SchedulerCluster) {
	s.conf.RLock()
	defer s.conf.RUnlock()
	for id := range s.conf.StoreIDWithRanges {
		cluster.ResumeLeaderTransfer(id)
	}
}

func (s *evictLeaderScheduler) IsScheduleAllowed(cluster sche.SchedulerCluster) bool {
	allowed := s.OpController.OperatorCount(operator.OpLeader) < cluster.GetSchedulerConfig().GetLeaderScheduleLimit()
	if !allowed {
		operator.OperatorLimitCounter.WithLabelValues(s.GetType(), operator.OpLeader.String()).Inc()
	}
	return allowed
}

func (s *evictLeaderScheduler) Schedule(cluster sche.SchedulerCluster, dryRun bool) ([]*operator.Operator, []plan.Plan) {
	evictLeaderCounter.Inc()
	return scheduleEvictLeaderBatch(s.GetName(), s.GetType(), cluster, s.conf, EvictLeaderBatchSize), nil
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
}

func scheduleEvictLeaderBatch(name, typ string, cluster sche.SchedulerCluster, conf evictLeaderStoresConf, batchSize int) []*operator.Operator {
	var ops []*operator.Operator
	for i := 0; i < batchSize; i++ {
		once := scheduleEvictLeaderOnce(name, typ, cluster, conf)
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

func scheduleEvictLeaderOnce(name, typ string, cluster sche.SchedulerCluster, conf evictLeaderStoresConf) []*operator.Operator {
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
		op, err := operator.CreateTransferLeaderOperator(typ, cluster, region, region.GetLeader().GetStoreId(), target.GetID(), targetIDs, operator.OpLeader)
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

func (handler *evictLeaderHandler) UpdateConfig(w http.ResponseWriter, r *http.Request) {
	var input map[string]any
	if err := apiutil.ReadJSONRespondError(handler.rd, w, r.Body, &input); err != nil {
		return
	}
	var args []string
	var exists bool
	var id uint64
	idFloat, ok := input["store_id"].(float64)
	if ok {
		id = (uint64)(idFloat)
		handler.config.RLock()
		if _, exists = handler.config.StoreIDWithRanges[id]; !exists {
			if err := handler.config.cluster.PauseLeaderTransfer(id); err != nil {
				handler.config.RUnlock()
				handler.rd.JSON(w, http.StatusInternalServerError, err.Error())
				return
			}
		}
		handler.config.RUnlock()
		args = append(args, strconv.FormatUint(id, 10))
	}

	ranges, ok := (input["ranges"]).([]string)
	if ok {
		args = append(args, ranges...)
	} else if exists {
		args = append(args, handler.config.getRanges(id)...)
	}

	handler.config.BuildWithArgs(args)
	err := handler.config.Persist()
	if err != nil {
		handler.config.removeStore(id)
		handler.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	handler.rd.JSON(w, http.StatusOK, "The scheduler has been applied to the store.")
}

func (handler *evictLeaderHandler) ListConfig(w http.ResponseWriter, r *http.Request) {
	conf := handler.config.Clone()
	handler.rd.JSON(w, http.StatusOK, conf)
}

func (handler *evictLeaderHandler) DeleteConfig(w http.ResponseWriter, r *http.Request) {
	idStr := mux.Vars(r)["store_id"]
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		handler.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	var resp any
	keyRanges := handler.config.getKeyRangesByID(id)
	succ, last := handler.config.removeStore(id)
	if succ {
		err = handler.config.Persist()
		if err != nil {
			handler.config.resetStore(id, keyRanges)
			handler.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
		if last {
			if err := handler.config.removeSchedulerCb(EvictLeaderName); err != nil {
				if errors.ErrorEqual(err, errs.ErrSchedulerNotFound.FastGenByArgs()) {
					handler.rd.JSON(w, http.StatusNotFound, err.Error())
				} else {
					handler.config.resetStore(id, keyRanges)
					handler.rd.JSON(w, http.StatusInternalServerError, err.Error())
				}
				return
			}
			resp = lastStoreDeleteInfo
		}
		handler.rd.JSON(w, http.StatusOK, resp)
		return
	}

	handler.rd.JSON(w, http.StatusNotFound, errs.ErrScheduleConfigNotExist.FastGenByArgs().Error())
}

func newEvictLeaderHandler(config *evictLeaderSchedulerConfig) http.Handler {
	h := &evictLeaderHandler{
		config: config,
		rd:     render.New(render.Options{IndentJSON: true}),
	}
	router := mux.NewRouter()
	router.HandleFunc("/config", h.UpdateConfig).Methods(http.MethodPost)
	router.HandleFunc("/list", h.ListConfig).Methods(http.MethodGet)
	router.HandleFunc("/delete/{store_id}", h.DeleteConfig).Methods(http.MethodDelete)
	return router
}
