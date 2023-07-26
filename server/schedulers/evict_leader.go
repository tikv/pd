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
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"net/http"
	"strconv"
	"sync"

	"github.com/gorilla/mux"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/apiutil"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/filter"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/opt"
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

func init() {
	schedule.RegisterSliceDecoderBuilder(EvictLeaderType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			if len(args) != 1 {
				return errs.ErrSchedulerConfig.FastGenByArgs("id")
			}
			conf, ok := v.(*evictLeaderSchedulerConfig)
			if !ok {
				return errs.ErrScheduleConfigNotExist.FastGenByArgs()
			}

			id, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return errs.ErrStrconvParseUint.Wrap(err).FastGenWithCause()
			}
			ranges, err := getKeyRanges(args[1:])
			if err != nil {
				return err
			}
			conf.StoreIDWithRanges[id] = ranges
			return nil

		}
	})

	schedule.RegisterScheduler(EvictLeaderType, func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &evictLeaderSchedulerConfig{StoreIDWithRanges: make(map[uint64][]core.KeyRange), storage: storage}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		conf.cluster = opController.GetCluster()
		return newEvictLeaderScheduler(opController, conf), nil
	})
}

type evictLeaderSchedulerConfig struct {
	mu                sync.RWMutex
	storage           *core.Storage
	StoreIDWithRanges map[uint64][]core.KeyRange `json:"store-id-ranges"`
	cluster           opt.Cluster
}

func (conf *evictLeaderSchedulerConfig) BuildWithArgs(args []string) error {
	if len(args) != 1 {
		return errs.ErrSchedulerConfig.FastGenByArgs("id")
	}

	id, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		return errs.ErrStrconvParseUint.Wrap(err).FastGenWithCause()
	}
	ranges, err := getKeyRanges(args[1:])
	if err != nil {
		return err
	}
	conf.mu.Lock()
	defer conf.mu.Unlock()
	conf.StoreIDWithRanges[id] = ranges
	return nil
}

func (conf *evictLeaderSchedulerConfig) Clone() *evictLeaderSchedulerConfig {
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	return &evictLeaderSchedulerConfig{
		StoreIDWithRanges: conf.StoreIDWithRanges,
	}
}

func (conf *evictLeaderSchedulerConfig) Persist() error {
	name := conf.getSchedulerName()
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	data, err := schedule.EncodeConfig(conf)
	if err != nil {
		return err
	}
	return conf.storage.SaveScheduleConfig(name, data)
}

func (conf *evictLeaderSchedulerConfig) getSchedulerName() string {
	return EvictLeaderName
}

func (conf *evictLeaderSchedulerConfig) getRanges(id uint64) []string {
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	var res []string
	ranges := conf.StoreIDWithRanges[id]
	for index := range ranges {
		res = append(res, (string)(ranges[index].StartKey))
		res = append(res, (string)(ranges[index].EndKey))
	}
	return res
}

func (conf *evictLeaderSchedulerConfig) mayBeRemoveStoreFromConfig(id uint64) (succ bool, last bool) {
	conf.mu.Lock()
	defer conf.mu.Unlock()
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

type evictLeaderScheduler struct {
	*BaseScheduler
	conf    *evictLeaderSchedulerConfig
	handler http.Handler
}

// newEvictLeaderScheduler creates an admin scheduler that transfers all leaders
// out of a store.
func newEvictLeaderScheduler(opController *schedule.OperatorController, conf *evictLeaderSchedulerConfig) schedule.Scheduler {
	base := NewBaseScheduler(opController)
	handler := newEvictLeaderHandler(conf)
	return &evictLeaderScheduler{
		BaseScheduler: base,
		conf:          conf,
		handler:       handler,
	}
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
	s.conf.mu.RLock()
	defer s.conf.mu.RUnlock()
	return schedule.EncodeConfig(s.conf)
}

func (s *evictLeaderScheduler) Prepare(cluster opt.Cluster) error {
	s.conf.mu.RLock()
	defer s.conf.mu.RUnlock()
	var res error
	for id := range s.conf.StoreIDWithRanges {
		if err := cluster.PauseLeaderTransfer(id); err != nil {
			res = err
		}
	}
	return res
}

func (s *evictLeaderScheduler) Cleanup(cluster opt.Cluster) {
	s.conf.mu.RLock()
	defer s.conf.mu.RUnlock()
	for id := range s.conf.StoreIDWithRanges {
		cluster.ResumeLeaderTransfer(id)
	}
}

func (s *evictLeaderScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.OpController.OperatorCount(operator.OpLeader) < cluster.GetOpts().GetLeaderScheduleLimit()
}

func (s *evictLeaderScheduler) scheduleOnce(cluster opt.Cluster) []*operator.Operator {
	var ops []*operator.Operator
	for id, ranges := range s.conf.StoreIDWithRanges {
		region := cluster.RandLeaderRegion(id, ranges, opt.HealthRegion(cluster))
		if region == nil {
			schedulerCounter.WithLabelValues(s.GetName(), "no-leader").Inc()
			continue
		}

		target := filter.NewCandidates(cluster.GetFollowerStores(region)).
			FilterTarget(cluster.GetOpts(), filter.StoreStateFilter{ActionScope: EvictLeaderName, TransferLeader: true}).
			RandomPick()
		if target == nil {
			schedulerCounter.WithLabelValues(s.GetName(), "no-target-store").Inc()
			continue
		}
		op, err := operator.CreateTransferLeaderOperator(EvictLeaderType, cluster, region, region.GetLeader().GetStoreId(), target.GetID(), operator.OpLeader)
		if err != nil {
			log.Debug("fail to create evict leader operator", errs.ZapError(err))
			continue
		}
		op.SetPriorityLevel(core.HighPriority)
		op.Counters = append(op.Counters, schedulerCounter.WithLabelValues(s.GetName(), "new-operator"))
		ops = append(ops, op)
	}
	return ops
}

func (s *evictLeaderScheduler) uniqueAppend(dst []*operator.Operator, src ...*operator.Operator) []*operator.Operator {
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

func (s *evictLeaderScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	var ops []*operator.Operator
	s.conf.mu.RLock()
	defer s.conf.mu.RUnlock()

	for i := 0; i < EvictLeaderBatchSize; i++ {
		once := s.scheduleOnce(cluster)
		// no more regions
		if len(once) == 0 {
			break
		}
		ops = s.uniqueAppend(ops, once...)
		// the batch has been fulfilled
		if len(ops) > EvictLeaderBatchSize {
			break
		}
	}

	return ops
}

type evictLeaderHandler struct {
	rd     *render.Render
	config *evictLeaderSchedulerConfig
}

func (handler *evictLeaderHandler) UpdateConfig(w http.ResponseWriter, r *http.Request) {
	var input map[string]interface{}
	if err := apiutil.ReadJSONRespondError(handler.rd, w, r.Body, &input); err != nil {
		return
	}
	var args []string
	var exists bool
	var id uint64
	idFloat, ok := input["store_id"].(float64)
	if ok {
		id = (uint64)(idFloat)
		if _, exists = handler.config.StoreIDWithRanges[id]; !exists {
			if err := handler.config.cluster.PauseLeaderTransfer(id); err != nil {
				handler.rd.JSON(w, http.StatusInternalServerError, err.Error())
				return
			}
		}
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
		handler.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	handler.rd.JSON(w, http.StatusOK, nil)
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

	var resp interface{}
	succ, last := handler.config.mayBeRemoveStoreFromConfig(id)
	if succ {
		err = handler.config.Persist()
		if err != nil {
			handler.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
		if last {
			if err := handler.config.cluster.RemoveScheduler(EvictLeaderName); err != nil {
				if errors.ErrorEqual(err, errs.ErrSchedulerNotFound.FastGenByArgs()) {
					handler.rd.JSON(w, http.StatusNotFound, err.Error())
				} else {
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
	router.HandleFunc("/config", h.UpdateConfig).Methods("POST")
	router.HandleFunc("/list", h.ListConfig).Methods("GET")
	router.HandleFunc("/delete/{store_id}", h.DeleteConfig).Methods("DELETE")
	return router
}
