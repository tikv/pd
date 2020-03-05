// Copyright 2017 PingCAP, Inc.
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
	"github.com/pingcap/log"
	"github.com/pingcap/pd/v4/pkg/apiutil"
	"github.com/pingcap/pd/v4/server/core"
	"github.com/pingcap/pd/v4/server/schedule"
	"github.com/pingcap/pd/v4/server/schedule/operator"
	"github.com/pingcap/pd/v4/server/schedule/opt"
	"github.com/pkg/errors"
	"github.com/unrolled/render"
	"go.uber.org/zap"
)

const (
	// GrantLeaderName is grant leader scheduler name.
	GrantLeaderName = "grant-leader-scheduler"
	// GrantLeaderType is grant leader scheduler type.
	GrantLeaderType = "grant-leader"
)

func init() {
	schedule.RegisterSliceDecoderBuilder(GrantLeaderType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			if len(args) != 1 {
				return errors.New("should specify the store-id")
			}

			conf, ok := v.(*grantLeaderSchedulerConfig)
			if !ok {
				return ErrScheduleConfigNotExist
			}

			id, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return errors.WithStack(err)
			}
			ranges, err := getKeyRanges(args[1:])
			if err != nil {
				return errors.WithStack(err)
			}
			conf.StoreIDWithRanges[id] = ranges
			return nil
		}
	})

	schedule.RegisterScheduler(GrantLeaderType, func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &grantLeaderSchedulerConfig{StoreIDWithRanges: make(map[uint64][]core.KeyRange), storage: storage}
		conf.cluster = opController.GetCluster()
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newGrantLeaderScheduler(opController, conf), nil
	})
}

type grantLeaderSchedulerConfig struct {
	mu                sync.RWMutex
	storage           *core.Storage
	StoreIDWithRanges map[uint64][]core.KeyRange `json:"store-id-ranges"`
	cluster           opt.Cluster
}

func (conf *grantLeaderSchedulerConfig) BuildWithArgs(args []string) error {
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
	conf.StoreIDWithRanges[id] = ranges
	return nil
}

func (conf *grantLeaderSchedulerConfig) Clone() *grantLeaderSchedulerConfig {
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	return &grantLeaderSchedulerConfig{
		StoreIDWithRanges: conf.StoreIDWithRanges,
	}
}

func (conf *grantLeaderSchedulerConfig) Persist() error {
	name := conf.getSchedulerName()
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	data, err := schedule.EncodeConfig(conf)
	if err != nil {
		return err
	}
	conf.storage.SaveScheduleConfig(name, data)
	return nil
}

func (conf *grantLeaderSchedulerConfig) getSchedulerName() string {
	return GrantLeaderName
}

func (conf *grantLeaderSchedulerConfig) getRanges(id uint64) []string {
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

func (conf *grantLeaderSchedulerConfig) mayBeRemoveStoreFromConfig(id uint64) (succ bool, last bool) {
	conf.mu.Lock()
	defer conf.mu.Unlock()
	_, exists := conf.StoreIDWithRanges[id]
	succ, last = false, false
	if exists {
		delete(conf.StoreIDWithRanges, id)
		conf.cluster.UnblockStore(id)
		succ = true
		last = len(conf.StoreIDWithRanges) == 0
	}
	return succ, last
}

// grantLeaderScheduler transfers all leaders to peers in the store.
type grantLeaderScheduler struct {
	*BaseScheduler
	conf    *grantLeaderSchedulerConfig
	handler http.Handler
}

// newGrantLeaderScheduler creates an admin scheduler that transfers all leaders
// to a store.
func newGrantLeaderScheduler(opController *schedule.OperatorController, conf *grantLeaderSchedulerConfig) schedule.Scheduler {
	base := NewBaseScheduler(opController)
	handler := newGrantLeaderHandler(conf)
	return &grantLeaderScheduler{
		BaseScheduler: base,
		conf:          conf,
		handler:       handler,
	}
}

func (s *grantLeaderScheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

func (s *grantLeaderScheduler) GetName() string {
	return GrantLeaderName
}

func (s *grantLeaderScheduler) GetType() string {
	return GrantLeaderType
}

func (s *grantLeaderScheduler) EncodeConfig() ([]byte, error) {
	return schedule.EncodeConfig(s.conf)
}

func (s *grantLeaderScheduler) Prepare(cluster opt.Cluster) error {
	s.conf.mu.RLock()
	defer s.conf.mu.RUnlock()
	var res error
	for id := range s.conf.StoreIDWithRanges {
		if err := cluster.BlockStore(id); err != nil {
			res = err
		}
	}
	return res
}

func (s *grantLeaderScheduler) Cleanup(cluster opt.Cluster) {
	s.conf.mu.RLock()
	defer s.conf.mu.RUnlock()
	for id := range s.conf.StoreIDWithRanges {
		cluster.UnblockStore(id)
	}
}

func (s *grantLeaderScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.OpController.OperatorCount(operator.OpLeader) < cluster.GetLeaderScheduleLimit()
}

func (s *grantLeaderScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	var ops []*operator.Operator
	s.conf.mu.RLock()
	defer s.conf.mu.RUnlock()
	for id, ranges := range s.conf.StoreIDWithRanges {
		region := cluster.RandFollowerRegion(id, ranges, opt.HealthRegion(cluster))
		if region == nil {
			schedulerCounter.WithLabelValues(s.GetName(), "no-follower").Inc()
			continue
		}

		op, err := operator.CreateTransferLeaderOperator(GrantLeaderType, cluster, region, region.GetLeader().GetStoreId(), id, operator.OpLeader)
		if err != nil {
			log.Debug("fail to create grant leader operator", zap.Error(err))
			continue
		}
		op.Counters = append(op.Counters, schedulerCounter.WithLabelValues(s.GetName(), "new-operator"))
		op.SetPriorityLevel(core.HighPriority)
		ops = append(ops, op)
	}

	return ops
}

type grantLeaderHandler struct {
	rd     *render.Render
	config *grantLeaderSchedulerConfig
}

func (handler *grantLeaderHandler) UpdateConfig(w http.ResponseWriter, r *http.Request) {
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
			if err := handler.config.cluster.BlockStore(id); err != nil {
				handler.rd.JSON(w, http.StatusInternalServerError, err)
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
		handler.rd.JSON(w, http.StatusInternalServerError, err)
		return
	}
	handler.rd.JSON(w, http.StatusOK, nil)
}

func (handler *grantLeaderHandler) ListConfig(w http.ResponseWriter, r *http.Request) {
	conf := handler.config.Clone()
	handler.rd.JSON(w, http.StatusOK, conf)
}

func (handler *grantLeaderHandler) DeleteConfig(w http.ResponseWriter, r *http.Request) {
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
			handler.rd.JSON(w, http.StatusInternalServerError, err)
			return
		}
		if last {
			if err := handler.config.cluster.RemoveScheduler(GrantLeaderName); err != nil {
				handler.rd.JSON(w, http.StatusInternalServerError, err)
				return
			}
			resp = lastStoreDeleteInfo
		}
		handler.rd.JSON(w, http.StatusOK, resp)
		return
	}

	handler.rd.JSON(w, http.StatusInternalServerError, ErrScheduleConfigNotExist)
}

func newGrantLeaderHandler(config *grantLeaderSchedulerConfig) http.Handler {
	h := &grantLeaderHandler{
		config: config,
		rd:     render.New(render.Options{IndentJSON: true}),
	}
	router := mux.NewRouter()
	router.HandleFunc("/config", h.UpdateConfig).Methods("POST")
	router.HandleFunc("/list", h.ListConfig).Methods("GET")
	router.HandleFunc("/delete/{store_id}", h.DeleteConfig).Methods("DELETE")
	return router
}
