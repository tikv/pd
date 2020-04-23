// Copyright 2016 PingCAP, Inc.
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

package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	"github.com/pingcap/pd/v4/pkg/apiutil"
	"github.com/pingcap/pd/v4/server"
	"github.com/pingcap/pd/v4/server/schedulers"
	"github.com/unrolled/render"
)

const schedulerConfigPrefix = "pd/api/v1/scheduler-config"

type schedulerHandler struct {
	*server.Handler
	svr *server.Server
	r   *render.Render
}

func newSchedulerHandler(svr *server.Server, r *render.Render) *schedulerHandler {
	return &schedulerHandler{
		Handler: svr.GetHandler(),
		r:       r,
		svr:     svr,
	}
}

// @Tags scheduler
// @Summary List running schedulers.
// @Produce json
// @Success 200 {array} string
// @Failure 500 {string} string "PD server failed to proceed the request."
// @Router /schedulers [get]
func (h *schedulerHandler) List(w http.ResponseWriter, r *http.Request) {
	schedulers, err := h.GetSchedulers()
	if err != nil {
		h.r.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.r.JSON(w, http.StatusOK, schedulers)
}

// FIXME: details of input json body params
// @Tags scheduler
// @Summary Create a scheduler.
// @Accept json
// @Param body body object true "json params"
// @Produce json
// @Success 200 {string} string "The scheduler is created."
// @Failure 400 {string} string "Bad format request."
// @Failure 500 {string} string "PD server failed to proceed the request."
// @Router /schedulers [post]
func (h *schedulerHandler) Post(w http.ResponseWriter, r *http.Request) {
	var input map[string]interface{}
	if err := apiutil.ReadJSONRespondError(h.r, w, r.Body, &input); err != nil {
		return
	}

	name, ok := input["name"].(string)
	if !ok {
		h.r.JSON(w, http.StatusBadRequest, "missing scheduler name")
		return
	}

	switch name {
	case schedulers.BalanceLeaderName:
		if err := h.AddBalanceLeaderScheduler(); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case schedulers.HotRegionName:
		if err := h.AddBalanceHotRegionScheduler(); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case schedulers.BalanceRegionName:
		if err := h.AddBalanceRegionScheduler(); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case schedulers.LabelName:
		if err := h.AddLabelScheduler(); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case schedulers.ScatterRangeName:
		var args []string

		collector := func(v string) {
			args = append(args, v)
		}
		if err := collectEscapeStringOption("start_key", input, collector); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}

		if err := collectEscapeStringOption("end_key", input, collector); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}

		if err := collectStringOption("range_name", input, collector); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
		if err := h.AddScatterRangeScheduler(args...); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}

	case schedulers.AdjacentRegionName:
		var args []string
		leaderLimit, ok := input["leader_limit"].(string)
		if ok {
			args = append(args, leaderLimit)
		}
		peerLimit, ok := input["peer_limit"].(string)
		if ok {
			args = append(args, peerLimit)
		}

		if err := h.AddAdjacentRegionScheduler(args...); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case schedulers.GrantLeaderName:
		storeID, ok := input["store_id"].(float64)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "missing store id")
			return
		}
		err := h.AddGrantLeaderScheduler(uint64(storeID))
		if err == schedulers.ErrSchedulerExisted {
			if err := h.redirectSchedulerUpdate(schedulers.GrantLeaderName, storeID); err != nil {
				h.r.JSON(w, http.StatusInternalServerError, err.Error())
				return
			}
		}
		if err != nil && err != schedulers.ErrSchedulerExisted {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case schedulers.EvictLeaderName:
		storeID, ok := input["store_id"].(float64)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "missing store id")
			return
		}
		err := h.AddEvictLeaderScheduler(uint64(storeID))
		if err == schedulers.ErrSchedulerExisted {
			if err := h.redirectSchedulerUpdate(schedulers.EvictLeaderName, storeID); err != nil {
				h.r.JSON(w, http.StatusInternalServerError, err.Error())
				return
			}
		}
		if err != nil && err != schedulers.ErrSchedulerExisted {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case schedulers.ShuffleLeaderName:
		if err := h.AddShuffleLeaderScheduler(); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case schedulers.ShuffleRegionName:
		if err := h.AddShuffleRegionScheduler(); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case schedulers.RandomMergeName:
		if err := h.AddRandomMergeScheduler(); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case schedulers.ShuffleHotRegionName:
		limit := uint64(1)
		l, ok := input["limit"].(float64)
		if ok {
			limit = uint64(l)
		}
		if err := h.AddShuffleHotRegionScheduler(limit); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	default:
		h.r.JSON(w, http.StatusBadRequest, "unknown scheduler")
		return
	}

	h.r.JSON(w, http.StatusOK, nil)
}

func (h *schedulerHandler) redirectSchedulerUpdate(name string, storeID float64) error {
	input := make(map[string]interface{})
	input["name"] = name
	input["store_id"] = storeID
	updateURL := fmt.Sprintf("%s/%s/%s/config", h.GetAddr(), schedulerConfigPrefix, name)
	body, err := json.Marshal(input)
	if err != nil {
		return err
	}
	return postJSON(h.svr.GetHTTPClient(), updateURL, body)
}

// @Tags scheduler
// @Summary Delete a scheduler.
// @Param name path string true "The name of the scheduler."
// @Produce json
// @Success 200 {string} string "The scheduler is removed."
// @Failure 404 {string} string "The scheduler is not found."
// @Failure 500 {string} string "PD server failed to proceed the request."
// @Router /schedulers/{name} [delete]
func (h *schedulerHandler) Delete(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]
	switch {
	case strings.HasPrefix(name, schedulers.EvictLeaderName) && name != schedulers.EvictLeaderName:
		if err := h.redirectSchedulerDelete(name, schedulers.EvictLeaderName); err != nil {
			h.handleErr(w, err)
			return
		}
	case strings.HasPrefix(name, schedulers.GrantLeaderName) && name != schedulers.GrantLeaderName:
		if err := h.redirectSchedulerDelete(name, schedulers.GrantLeaderName); err != nil {
			h.handleErr(w, err)
			return
		}
	default:
		if err := h.RemoveScheduler(name); err != nil {
			h.handleErr(w, err)
			return
		}
	}
	h.r.JSON(w, http.StatusOK, nil)
}

func (h *schedulerHandler) handleErr(w http.ResponseWriter, err error) {
	if err == schedulers.ErrSchedulerNotFound {
		h.r.JSON(w, http.StatusNotFound, err.Error())
	} else {
		h.r.JSON(w, http.StatusInternalServerError, err.Error())
	}
}

func (h *schedulerHandler) redirectSchedulerDelete(name, schedulerName string) error {
	args := strings.Split(name, "-")
	args = args[len(args)-1:]
	url := fmt.Sprintf("%s/%s/%s/delete/%s", h.GetAddr(), schedulerConfigPrefix, schedulerName, args[0])
	resp, err := doDelete(h.svr.GetHTTPClient(), url)
	if resp.StatusCode != 200 {
		return schedulers.ErrSchedulerNotFound
	}
	if err != nil {
		return err
	}
	return nil
}

// FIXME: details of input json body params
// @Tags scheduler
// @Summary Pause or resume a scheduler.
// @Accept json
// @Param name path string true "The name of the scheduler."
// @Param body body object true "json params"
// @Produce json
// @Success 200 {string} string "Pause or resume the scheduler success."
// @Failure 400 {string} string "Bad format request."
// @Failure 500 {string} string "PD server failed to proceed the request."
// @Router /schedulers/{name} [post]
func (h *schedulerHandler) PauseOrResume(w http.ResponseWriter, r *http.Request) {
	var input map[string]int
	if err := apiutil.ReadJSONRespondError(h.r, w, r.Body, &input); err != nil {
		return
	}

	name := mux.Vars(r)["name"]
	t, ok := input["delay"]
	if !ok {
		h.r.JSON(w, http.StatusBadRequest, "missing pause time")
		return
	}
	if err := h.PauseOrResumeScheduler(name, int64(t)); err != nil {
		h.r.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.r.JSON(w, http.StatusOK, nil)
}

type schedulerConfigHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newSchedulerConfigHandler(svr *server.Server, rd *render.Render) *schedulerConfigHandler {
	return &schedulerConfigHandler{
		svr: svr,
		rd:  rd,
	}
}

func (h *schedulerConfigHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	handler := h.svr.GetHandler()
	sh := handler.GetSchedulerConfigHandler()
	if sh != nil {
		sh.ServeHTTP(w, r)
		return
	}
	h.rd.JSON(w, http.StatusNotAcceptable, errNoImplement)
}
