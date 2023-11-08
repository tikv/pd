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
	"strings"

	"github.com/gorilla/mux"
	"github.com/pingcap/errors"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/schedulers"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/server"
	"github.com/unrolled/render"
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
	case schedulers.BalanceWitnessName:
		if err := h.AddBalanceWitnessScheduler(); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case schedulers.TransferWitnessLeaderName:
		if err := h.AddTransferWitnessLeaderScheduler(); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case schedulers.HotRegionName:
		if err := h.AddBalanceHotRegionScheduler(); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case schedulers.EvictSlowTrendName:
		if err := h.AddEvictSlowTrendScheduler(); err != nil {
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
		if err := h.AddScatterRangeScheduler(args...); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}

	case schedulers.GrantLeaderName:
		h.addEvictOrGrant(w, input, schedulers.GrantLeaderName)
	case schedulers.EvictLeaderName:
		h.addEvictOrGrant(w, input, schedulers.EvictLeaderName)
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
	case schedulers.EvictSlowStoreName:
		if err := h.AddEvictSlowStoreScheduler(); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case schedulers.SplitBucketName:
		if err := h.AddSplitBucketScheduler(); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case schedulers.GrantHotRegionName:
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
		if err := h.AddGrantHotRegionScheduler(leaderID, peerIDs); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	default:
		h.r.JSON(w, http.StatusBadRequest, "unknown scheduler")
		return
	}

	h.r.JSON(w, http.StatusOK, "The scheduler is created.")
}

func (h *schedulerHandler) addEvictOrGrant(w http.ResponseWriter, input map[string]interface{}, name string) {
	storeID, ok := input["store_id"].(float64)
	if !ok {
		h.r.JSON(w, http.StatusBadRequest, "missing store id")
		return
	}
	err := h.AddEvictOrGrant(storeID, name)
	if err != nil {
		h.r.JSON(w, http.StatusInternalServerError, err.Error())
	}
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
	case strings.HasPrefix(name, schedulers.EvictLeaderName) && name != schedulers.EvictLeaderName:
		h.redirectSchedulerDelete(w, name, schedulers.EvictLeaderName)
		return
	case strings.HasPrefix(name, schedulers.GrantLeaderName) && name != schedulers.GrantLeaderName:
		h.redirectSchedulerDelete(w, name, schedulers.GrantLeaderName)
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

func (h *schedulerConfigHandler) GetSchedulerConfig(w http.ResponseWriter, r *http.Request) {
	handler := h.svr.GetHandler()
	sh, err := handler.GetSchedulerConfigHandler()
	if err == nil && sh != nil {
		sh.ServeHTTP(w, r)
		return
	}
	h.rd.JSON(w, http.StatusNotAcceptable, err.Error())
}
