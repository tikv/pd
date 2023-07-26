// Copyright 2019 TiKV Project Authors.
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

package main

import (
	"net/http"
	"net/url"
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
	"github.com/tikv/pd/server/schedulers"
	"github.com/unrolled/render"
)

const (
	// EvictLeaderName is evict leader scheduler name.
	EvictLeaderName = "user-evict-leader-scheduler"
	// EvictLeaderType is evict leader scheduler type.
	EvictLeaderType        = "user-evict-leader"
	noStoreInSchedulerInfo = "No store in user-evict-leader-scheduler-config"
)

func init() {
	schedule.RegisterSliceDecoderBuilder(EvictLeaderType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			if len(args) != 1 {
				return errors.New("should specify the store-id")
			}
			conf, ok := v.(*evictLeaderSchedulerConfig)
			if !ok {
				return errors.New("the config does not exist")
			}

			id, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return errors.WithStack(err)
			}
			ranges, err := getKeyRanges(args[1:])
			if err != nil {
				return errors.WithStack(err)
			}
			conf.StoreIDWitRanges[id] = ranges
			return nil

		}
	})

	schedule.RegisterScheduler(EvictLeaderType, func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &evictLeaderSchedulerConfig{StoreIDWitRanges: make(map[uint64][]core.KeyRange), storage: storage}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		conf.cluster = opController.GetCluster()
		return newEvictLeaderScheduler(opController, conf), nil
	})
}

// SchedulerType returns the type of the scheduler
//nolint
func SchedulerType() string {
	return EvictLeaderType
}

// SchedulerArgs returns the args for the scheduler
//nolint
func SchedulerArgs() []string {
	args := []string{"1"}
	return args
}

type evictLeaderSchedulerConfig struct {
	mu               sync.RWMutex
	storage          *core.Storage
	StoreIDWitRanges map[uint64][]core.KeyRange `json:"store-id-ranges"`
	cluster          opt.Cluster
}

func (conf *evictLeaderSchedulerConfig) BuildWithArgs(args []string) error {
	if len(args) != 1 {
		return errors.New("should specify the store-id")
	}

	id, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		return errors.WithStack(err)
	}
	ranges, err := getKeyRanges(args[1:])
	if err != nil {
		return errors.WithStack(err)
	}
	conf.mu.Lock()
	defer conf.mu.Unlock()
	conf.StoreIDWitRanges[id] = ranges
	return nil
}

func (conf *evictLeaderSchedulerConfig) Clone() *evictLeaderSchedulerConfig {
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	return &evictLeaderSchedulerConfig{
		StoreIDWitRanges: conf.StoreIDWitRanges,
	}
}

func (conf *evictLeaderSchedulerConfig) Persist() error {
	name := conf.getScheduleName()
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	data, err := schedule.EncodeConfig(conf)
	if err != nil {
		return err
	}
	return conf.storage.SaveScheduleConfig(name, data)
}

func (conf *evictLeaderSchedulerConfig) getScheduleName() string {
	return EvictLeaderName
}

func (conf *evictLeaderSchedulerConfig) getRanges(id uint64) []string {
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	var res []string
	for index := range conf.StoreIDWitRanges[id] {
		res = append(res, (string)(conf.StoreIDWitRanges[id][index].StartKey))
		res = append(res, (string)(conf.StoreIDWitRanges[id][index].EndKey))
	}
	return res
}

type evictLeaderScheduler struct {
	*schedulers.BaseScheduler
	conf    *evictLeaderSchedulerConfig
	handler http.Handler
}

// newEvictLeaderScheduler creates an admin scheduler that transfers all leaders
// out of a store.
func newEvictLeaderScheduler(opController *schedule.OperatorController, conf *evictLeaderSchedulerConfig) schedule.Scheduler {
	base := schedulers.NewBaseScheduler(opController)
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
	for id := range s.conf.StoreIDWitRanges {
		if err := cluster.PauseLeaderTransfer(id); err != nil {
			res = err
		}
	}
	return res
}

func (s *evictLeaderScheduler) Cleanup(cluster opt.Cluster) {
	s.conf.mu.RLock()
	defer s.conf.mu.RUnlock()
	for id := range s.conf.StoreIDWitRanges {
		cluster.ResumeLeaderTransfer(id)
	}
}

func (s *evictLeaderScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.OpController.OperatorCount(operator.OpLeader) < cluster.GetOpts().GetLeaderScheduleLimit()
}

func (s *evictLeaderScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	var ops []*operator.Operator
	s.conf.mu.RLock()
	defer s.conf.mu.RUnlock()
	for id, ranges := range s.conf.StoreIDWitRanges {
		region := cluster.RandLeaderRegion(id, ranges, opt.HealthRegion(cluster))
		if region == nil {
			continue
		}
		target := filter.NewCandidates(cluster.GetFollowerStores(region)).
			FilterTarget(cluster.GetOpts(), filter.StoreStateFilter{ActionScope: EvictLeaderName, TransferLeader: true}).
			RandomPick()
		if target == nil {
			continue
		}
		op, err := operator.CreateTransferLeaderOperator(EvictLeaderType, cluster, region, region.GetLeader().GetStoreId(), target.GetID(), operator.OpLeader)
		if err != nil {
			log.Debug("fail to create evict leader operator", errs.ZapError(err))
			continue

		}
		op.SetPriorityLevel(core.HighPriority)
		ops = append(ops, op)
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
		if _, exists = handler.config.StoreIDWitRanges[id]; !exists {
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

	handler.config.mu.Lock()
	defer handler.config.mu.Unlock()
	_, exists := handler.config.StoreIDWitRanges[id]
	if exists {
		delete(handler.config.StoreIDWitRanges, id)
		handler.config.cluster.ResumeLeaderTransfer(id)

		handler.config.mu.Unlock()
		handler.config.Persist()
		handler.config.mu.Lock()

		var resp interface{}
		if len(handler.config.StoreIDWitRanges) == 0 {
			resp = noStoreInSchedulerInfo
		}
		handler.rd.JSON(w, http.StatusOK, resp)
		return
	}

	handler.rd.JSON(w, http.StatusInternalServerError, errors.New("the config does not exist"))
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

func getKeyRanges(args []string) ([]core.KeyRange, error) {
	var ranges []core.KeyRange
	for len(args) > 1 {
		startKey, err := url.QueryUnescape(args[0])
		if err != nil {
			return nil, err
		}
		endKey, err := url.QueryUnescape(args[1])
		if err != nil {
			return nil, err
		}
		args = args[2:]
		ranges = append(ranges, core.NewKeyRange(startKey, endKey))
	}
	if len(ranges) == 0 {
		return []core.KeyRange{core.NewKeyRange("", "")}, nil
	}
	return ranges, nil
}
