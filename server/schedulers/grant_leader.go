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
	"fmt"
	"strconv"

	"github.com/pkg/errors"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule"
)

func init() {
	schedule.RegisterScheduler("grant-leader", func(opController *schedule.OperatorController, args []string) (schedule.Scheduler, error) {
		if len(args) != 1 {
			return nil, errors.New("grant-leader needs 1 argument")
		}
		id, err := strconv.ParseUint(args[0], 10, 64)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return newGrantLeaderScheduler(opController, id), nil
	})
}

// grantLeaderScheduler transfers all leaders to peers in the store.
type grantLeaderScheduler struct {
	*baseScheduler
	name    string
	storeID uint64
}

// newGrantLeaderScheduler creates an admin scheduler that transfers all leaders
// to a store.
func newGrantLeaderScheduler(opController *schedule.OperatorController, storeID uint64) schedule.Scheduler {
	base := newBaseScheduler(opController)
	return &grantLeaderScheduler{
		baseScheduler: base,
		name:          fmt.Sprintf("grant-leader-scheduler-%d", storeID),
		storeID:       storeID,
	}
}

func (s *grantLeaderScheduler) GetName() string {
	return s.name
}

func (s *grantLeaderScheduler) GetType() string {
	return "grant-leader"
}
func (s *grantLeaderScheduler) Prepare(cluster schedule.Cluster) error {
	return cluster.BlockStore(s.storeID)
}

func (s *grantLeaderScheduler) Cleanup(cluster schedule.Cluster) {
	cluster.UnblockStore(s.storeID)
}

func (s *grantLeaderScheduler) IsScheduleAllowed(cluster schedule.Cluster) bool {
	return s.opController.OperatorCount(schedule.OpLeader) < cluster.GetLeaderScheduleLimit()
}

func (s *grantLeaderScheduler) Schedule(cluster schedule.Cluster) []*schedule.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
<<<<<<< HEAD
	region := cluster.RandFollowerRegion(s.storeID, core.HealthRegion())
	if region == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no_follower").Inc()
		return nil
	}
	schedulerCounter.WithLabelValues(s.GetName(), "new_operator").Inc()
	op := schedule.CreateTransferLeaderOperator("grant-leader", region, region.GetLeader().GetStoreId(), s.storeID, schedule.OpLeader)
	op.SetPriorityLevel(core.HighPriority)
	return []*schedule.Operator{op}
=======
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
			log.Debug("fail to create grant leader operator", errs.ZapError(err))
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
			handler.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
		if last {
			if err := handler.config.cluster.RemoveScheduler(GrantLeaderName); err != nil {
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
>>>>>>> 3e31744... fix empty http response in scheduler (#2869)
}
