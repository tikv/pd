// Copyright 2018 TiKV Project Authors.
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
	"net/url"

	"github.com/pkg/errors"
	"github.com/tikv/pd/server/schedule"
)

func init() {
	schedule.RegisterScheduler("scatter-range", func(opController *schedule.OperatorController, args []string) (schedule.Scheduler, error) {
		if len(args) != 3 {
			return nil, errors.New("should specify the range and the name")
		}
		startKey, err := url.QueryUnescape(args[0])
		if err != nil {
			return nil, err
		}
		endKey, err := url.QueryUnescape(args[1])
		if err != nil {
			return nil, err
		}
		name := args[2]
		return newScatterRangeScheduler(opController, []string{startKey, endKey, name}), nil
	})
}

type scatterRangeScheduler struct {
	*baseScheduler
	rangeName     string
	startKey      []byte
	endKey        []byte
	balanceLeader schedule.Scheduler
	balanceRegion schedule.Scheduler
}

// newScatterRangeScheduler creates a scheduler that balances the distribution of leaders and regions that in the specified key range.
func newScatterRangeScheduler(opController *schedule.OperatorController, args []string) schedule.Scheduler {
	base := newBaseScheduler(opController)
	return &scatterRangeScheduler{
		baseScheduler: base,
		startKey:      []byte(args[0]),
		endKey:        []byte(args[1]),
		rangeName:     args[2],
		balanceLeader: newBalanceLeaderScheduler(
			opController,
			WithBalanceLeaderName("scatter-range-leader"),
			WithBalanceLeaderCounter(scatterRangeLeaderCounter),
		),
		balanceRegion: newBalanceRegionScheduler(
			opController,
			WithBalanceRegionName("scatter-range-region"),
			WithBalanceRegionCounter(scatterRangeRegionCounter),
		),
	}
}

func (l *scatterRangeScheduler) GetName() string {
	return fmt.Sprintf("scatter-range-%s", l.rangeName)
}

func (l *scatterRangeScheduler) GetType() string {
	return "scatter-range"
}

func (l *scatterRangeScheduler) IsScheduleAllowed(cluster schedule.Cluster) bool {
	return l.opController.OperatorCount(schedule.OpRange) < cluster.GetRegionScheduleLimit()
}

func (l *scatterRangeScheduler) Schedule(cluster schedule.Cluster) []*schedule.Operator {
	schedulerCounter.WithLabelValues(l.GetName(), "schedule").Inc()
	// isolate a new cluster according to the key range
	c := schedule.GenRangeCluster(cluster, l.startKey, l.endKey)
	c.SetTolerantSizeRatio(2)
	ops := l.balanceLeader.Schedule(c)
	if len(ops) > 0 {
		ops[0].SetDesc(fmt.Sprintf("scatter-range-leader-%s", l.rangeName))
		ops[0].AttachKind(schedule.OpRange)
		schedulerCounter.WithLabelValues(l.GetName(), "new-leader-operator").Inc()
		return ops
	}
	ops = l.balanceRegion.Schedule(c)
	if len(ops) > 0 {
		ops[0].SetDesc(fmt.Sprintf("scatter-range-region-%s", l.rangeName))
		ops[0].AttachKind(schedule.OpRange)
		schedulerCounter.WithLabelValues(l.GetName(), "new-region-operator").Inc()
		return ops
	}
	schedulerCounter.WithLabelValues(l.GetName(), "no-need").Inc()
	return nil
}
<<<<<<< HEAD
=======

type scatterRangeHandler struct {
	rd     *render.Render
	config *scatterRangeSchedulerConfig
}

func (handler *scatterRangeHandler) UpdateConfig(w http.ResponseWriter, r *http.Request) {
	var input map[string]interface{}
	if err := apiutil.ReadJSONRespondError(handler.rd, w, r.Body, &input); err != nil {
		return
	}
	var args []string
	name, ok := input["range-name"].(string)
	if ok {
		if name != handler.config.GetRangeName() {
			handler.rd.JSON(w, http.StatusInternalServerError, errors.New("Cannot change the range name, please delete this schedule").Error())
			return
		}
		args = append(args, name)
	} else {
		args = append(args, handler.config.GetRangeName())
	}

	startKey, ok := input["start-key"].(string)
	if ok {
		args = append(args, startKey)
	} else {
		args = append(args, string(handler.config.GetStartKey()))
	}

	endKey, ok := input["end-key"].(string)
	if ok {
		args = append(args, endKey)
	} else {
		args = append(args, string(handler.config.GetEndKey()))
	}
	handler.config.BuildWithArgs(args)
	err := handler.config.Persist()
	if err != nil {
		handler.rd.JSON(w, http.StatusInternalServerError, err.Error())
	}
	handler.rd.JSON(w, http.StatusOK, nil)
}

func (handler *scatterRangeHandler) ListConfig(w http.ResponseWriter, r *http.Request) {
	conf := handler.config.Clone()
	handler.rd.JSON(w, http.StatusOK, conf)
}

func newScatterRangeHandler(config *scatterRangeSchedulerConfig) http.Handler {
	h := &scatterRangeHandler{
		config: config,
		rd:     render.New(render.Options{IndentJSON: true}),
	}
	router := mux.NewRouter()
	router.HandleFunc("/config", h.UpdateConfig).Methods("POST")
	router.HandleFunc("/list", h.ListConfig).Methods("GET")
	return router
}
>>>>>>> 3e31744... fix empty http response in scheduler (#2869)
