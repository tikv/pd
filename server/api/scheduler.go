// Copyright 2016 TiKV Project Authors.
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

package api

import (
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	"github.com/unrolled/render"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/server"
)

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

// @Tags     scheduler
// @Summary  List all created schedulers by status.
// @Produce  json
// @Success  200  {array}   string
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /schedulers [get]
func (h *schedulerHandler) GetSchedulers(w http.ResponseWriter, r *http.Request) {
	status := r.URL.Query().Get("status")
	_, needTS := r.URL.Query()["timestamp"]
	output, err := h.Handler.GetSchedulerByStatus(status, needTS)
	if err != nil {
		h.r.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.r.JSON(w, http.StatusOK, output)
}

// FIXME: details of input json body params
// @Tags     scheduler
// @Summary  Create a scheduler.
// @Accept   json
// @Param    body  body  object  true  "json params"
// @Produce  json
// @Success  200  {string}  string  "The scheduler is created."
// @Failure  400  {string}  string  "Bad format request."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /schedulers [post]
func (h *schedulerHandler) CreateScheduler(w http.ResponseWriter, r *http.Request) {
	var input map[string]any
	if err := apiutil.ReadJSONRespondError(h.r, w, r.Body, &input); err != nil {
		return
	}

	name, ok := input["name"].(string)
	if !ok {
		h.r.JSON(w, http.StatusBadRequest, "missing scheduler name")
		return
	}

	tp, ok := types.StringToSchedulerType[name]
	if !ok {
		h.r.JSON(w, http.StatusBadRequest, "unknown scheduler")
		return
	}
	var args []string
	collector := func(v string) {
		args = append(args, v)
	}

	switch tp {
	case types.BalanceRangeScheduler:
		exist, _ := h.IsSchedulerExisted(name)
		if exist {
			h.r.JSON(w, http.StatusBadRequest, "The scheduler already exists, pls remove the exist scheduler first.")
			return
		}
		if err := apiutil.CollectStringOption("role", input, collector); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
		if err := apiutil.CollectStringOption("engine", input, collector); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}

		defaultTimeout := "1h"
		if err := apiutil.CollectStringOption("timeout", input, collector); err != nil {
			if errors.ErrorEqual(err, errs.ErrOptionNotExist) {
				collector(defaultTimeout)
			} else {
				h.r.JSON(w, http.StatusInternalServerError, err.Error())
				return
			}
		}

		if err := apiutil.CollectStringOption("alias", input, collector); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}

		if err := apiutil.CollectEscapeStringOption("start-key", input, collector); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}

		if err := apiutil.CollectEscapeStringOption("end-key", input, collector); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case types.ScatterRangeScheduler:
		if err := apiutil.CollectEscapeStringOption("start_key", input, collector); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}

		if err := apiutil.CollectEscapeStringOption("end_key", input, collector); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}

		if err := apiutil.CollectStringOption("range_name", input, collector); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case types.GrantLeaderScheduler, types.EvictLeaderScheduler:
		_, ok := input["store_id"]
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "missing store id")
			return
		}
		storeID, ok := input["store_id"].(float64)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "please input a right store id")
			return
		}
		var (
			exist bool
			err   error
		)
		if exist, err = h.IsSchedulerExisted(name); exist {
			if err := h.RedirectSchedulerUpdate(name, storeID); err != nil {
				h.r.JSON(w, http.StatusInternalServerError, err.Error())
				return
			}
			log.Info("update scheduler", zap.String("scheduler-name", name), zap.Uint64("store-id", uint64(storeID)))
			h.r.JSON(w, http.StatusOK, "The scheduler has been applied to the store.")
			return
		}
		if err != nil && !errors.ErrorEqual(err, errs.ErrSchedulerNotFound.FastGenByArgs()) {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}

		collector(strconv.FormatUint(uint64(storeID), 10))
	case types.ShuffleHotRegionScheduler:
		limit := uint64(1)
		l, ok := input["limit"].(float64)
		if ok {
			limit = uint64(l)
		}
		collector(strconv.FormatUint(limit, 10))
	case types.GrantHotRegionScheduler:
		isExist, err := h.isSchedulerExist(types.BalanceHotRegionScheduler)
		if err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
		if isExist {
			h.r.JSON(w, http.StatusBadRequest, "balance-hot-region-scheduler is running, please remove it first")
			return
		}
		leaderID, ok := input["store-leader-id"].(string)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "missing leader id")
			return
		}
		peerIDs, ok := input["store-id"].(string)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "missing store id")
			return
		}
		collector(leaderID)
		collector(peerIDs)
	case types.BalanceHotRegionScheduler:
		isExist, err := h.isSchedulerExist(types.GrantHotRegionScheduler)
		if err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
		if isExist {
			h.r.JSON(w, http.StatusBadRequest, "grant-hot-region-scheduler is running, please remove it first")
			return
		}
	}

	if err := h.AddScheduler(tp, args...); err != nil {
		h.r.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.r.JSON(w, http.StatusOK, "The scheduler is created.")
}

// @Tags     scheduler
// @Summary  Delete a scheduler.
// @Param    name  path  string  true  "The name of the scheduler."
// @Produce  json
// @Success  200  {string}  string  "The scheduler is removed."
// @Failure  404  {string}  string  "The scheduler is not found."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /schedulers/{name} [delete]
func (h *schedulerHandler) DeleteScheduler(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]
	switch {
	case strings.HasPrefix(name, types.EvictLeaderScheduler.String()) && name != types.EvictLeaderScheduler.String():
		h.redirectSchedulerDelete(w, name, types.EvictLeaderScheduler.String())
		return
	case strings.HasPrefix(name, types.GrantLeaderScheduler.String()) && name != types.GrantLeaderScheduler.String():
		h.redirectSchedulerDelete(w, name, types.GrantLeaderScheduler.String())
		return
	default:
		if err := h.RemoveScheduler(name); err != nil {
			h.handleErr(w, err)
			return
		}
	}
	h.r.JSON(w, http.StatusOK, "The scheduler is removed.")
}

func (h *schedulerHandler) handleErr(w http.ResponseWriter, err error) {
	if errors.ErrorEqual(err, errs.ErrSchedulerNotFound.FastGenByArgs()) {
		h.r.JSON(w, http.StatusNotFound, err.Error())
	} else {
		h.r.JSON(w, http.StatusInternalServerError, err.Error())
	}
}

func (h *schedulerHandler) redirectSchedulerDelete(w http.ResponseWriter, name, schedulerName string) {
	args := strings.Split(name, "-")
	args = args[len(args)-1:]
	deleteURL, err := url.JoinPath(h.GetAddr(), "pd", server.SchedulerConfigHandlerPath, schedulerName, "delete", args[0])
	if err != nil {
		h.r.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	resp, err := apiutil.DoDelete(h.svr.GetHTTPClient(), deleteURL)
	if err != nil {
		h.r.JSON(w, resp.StatusCode, err.Error())
		return
	}
	defer resp.Body.Close()
	h.r.JSON(w, resp.StatusCode, nil)
}

// FIXME: details of input json body params
// @Tags     scheduler
// @Summary  Pause or resume a scheduler.
// @Accept   json
// @Param    name  path  string  true  "The name of the scheduler."
// @Param    body  body  object  true  "json params"
// @Produce  json
// @Success  200  {string}  string  "Pause or resume the scheduler successfully."
// @Failure  400  {string}  string  "Bad format request."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /schedulers/{name} [post]
func (h *schedulerHandler) PauseOrResumeScheduler(w http.ResponseWriter, r *http.Request) {
	var input map[string]int64
	if err := apiutil.ReadJSONRespondError(h.r, w, r.Body, &input); err != nil {
		return
	}

	name := mux.Vars(r)["name"]
	t, ok := input["delay"]
	if !ok {
		h.r.JSON(w, http.StatusBadRequest, "missing pause time")
		return
	}
	if err := h.Handler.PauseOrResumeScheduler(name, t); err != nil {
		h.r.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.r.JSON(w, http.StatusOK, "Pause or resume the scheduler successfully.")
}

func (h *schedulerHandler) isSchedulerExist(scheduler types.CheckerSchedulerType) (bool, error) {
	rc, err := h.GetRaftCluster()
	if err != nil {
		return false, err
	}
	if rc.IsServiceIndependent(constant.SchedulingServiceName) {
		handlers := rc.GetSchedulerHandlers()
		_, ok := handlers[scheduler.String()]
		return ok, nil
	}
	schedulers := rc.GetSchedulers()
	if slice.Contains(schedulers, scheduler.String()) {
		return !rc.GetSchedulerConfig().IsSchedulerDisabled(scheduler), nil
	}
	return false, nil
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

func (h *schedulerConfigHandler) handleSchedulerConfig(w http.ResponseWriter, r *http.Request) {
	handler := h.svr.GetHandler()
	sh, err := handler.GetSchedulerConfigHandler()
	if err == nil && sh != nil {
		sh.ServeHTTP(w, r)
		return
	}
	h.rd.JSON(w, http.StatusNotAcceptable, err.Error())
}
