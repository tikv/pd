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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/tikv/pd/pkg/apiutil"
	"github.com/tikv/pd/pkg/ratelimit"
	"github.com/tikv/pd/server"
	"github.com/unrolled/render"
)

type adminHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newAdminHandler(svr *server.Server, rd *render.Render) *adminHandler {
	return &adminHandler{
		svr: svr,
		rd:  rd,
	}
}

// @Tags admin
// @Summary Drop a specific region from cache.
// @Param id path integer true "Region Id"
// @Produce json
// @Success 200 {string} string "The region is removed from server cache."
// @Failure 400 {string} string "The input is invalid."
// @Router /admin/cache/region/{id} [delete]
func (h *adminHandler) DeleteRegionCache(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r)
	vars := mux.Vars(r)
	regionIDStr := vars["id"]
	regionID, err := strconv.ParseUint(regionIDStr, 10, 64)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	rc.DropCacheRegion(regionID)
	h.rd.JSON(w, http.StatusOK, "The region is removed from server cache.")
}

// FIXME: details of input json body params
// @Tags admin
// @Summary Reset the ts.
// @Accept json
// @Param body body object true "json params"
// @Produce json
// @Success 200 {string} string "Reset ts successfully."
// @Failure 400 {string} string "The input is invalid."
// @Failure 403 {string} string "Reset ts is forbidden."
// @Failure 500 {string} string "PD server failed to proceed the request."
// @Router /admin/reset-ts [post]
func (h *adminHandler) ResetTS(w http.ResponseWriter, r *http.Request) {
	handler := h.svr.GetHandler()
	var input map[string]interface{}
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &input); err != nil {
		return
	}
	tsValue, ok := input["tso"].(string)
	if !ok || len(tsValue) == 0 {
		h.rd.JSON(w, http.StatusBadRequest, "invalid tso value")
		return
	}
	ts, err := strconv.ParseUint(tsValue, 10, 64)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, "invalid tso value")
		return
	}

	if err = handler.ResetTS(ts); err != nil {
		if err == server.ErrServerNotStarted {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		} else {
			h.rd.JSON(w, http.StatusForbidden, err.Error())
		}
	}
	h.rd.JSON(w, http.StatusOK, "Reset ts successfully.")
}

// Intentionally no swagger mark as it is supposed to be only used in
// server-to-server. For security reason, it only accepts JSON formatted data.
func (h *adminHandler) SavePersistFile(w http.ResponseWriter, r *http.Request) {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		h.rd.Text(w, http.StatusInternalServerError, "")
		return
	}
	defer r.Body.Close()
	if !json.Valid(data) {
		h.rd.Text(w, http.StatusBadRequest, "body should be json format")
		return
	}
	err = h.svr.PersistFile(mux.Vars(r)["file_name"], data)
	if err != nil {
		h.rd.Text(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.Text(w, http.StatusOK, "")
}

// Intentionally no swagger mark as it is supposed to be only used in
// server-to-server.
func (h *adminHandler) UpdateWaitAsyncTime(w http.ResponseWriter, r *http.Request) {
	var input map[string]interface{}
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &input); err != nil {
		return
	}
	memberIDValue, ok := input["member_id"].(string)
	if !ok || len(memberIDValue) == 0 {
		h.rd.JSON(w, http.StatusBadRequest, "invalid member id")
		return
	}
	memberID, err := strconv.ParseUint(memberIDValue, 10, 64)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, "invalid member id")
		return
	}
	cluster := getCluster(r)
	cluster.GetReplicationMode().UpdateMemberWaitAsyncTime(memberID)
	h.rd.JSON(w, http.StatusOK, nil)
}

// @Tags admin
// @Summary switch ratelimit middleware
// @Param enable query string true "enable" Enums(true, false)
// @Produce json
// @Success 200 {string} string ""
// @Failure 400 {string} string ""
// @Router /admin/ratelimit/config [POST]
func (h *adminHandler) SetRatelimitConfig(w http.ResponseWriter, r *http.Request) {
	var input map[string]interface{}
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &input); err != nil {
		return
	}
	typeStr, ok := input["type"].(string)
	if !ok {
		h.rd.JSON(w, http.StatusBadRequest, "The type is empty.")
		return
	}
	var serviceLabel string
	switch typeStr {
	case "label":
		serviceLabel, ok = input["label"].(string)
		if !ok || len(serviceLabel) == 0 {
			h.rd.JSON(w, http.StatusBadRequest, "The label is empty.")
			return
		}
		if len(h.svr.GetServiceLabels(serviceLabel)) == 0 {
			h.rd.JSON(w, http.StatusBadRequest, "There is no label matched.")
			return
		}
	case "path":
		method, _ := input["method"].(string)
		path, ok := input["path"].(string)
		if !ok || len(path) == 0 {
			h.rd.JSON(w, http.StatusBadRequest, "The path is empty.")
			return
		}
		serviceLabel = h.svr.GetAPIAccessServiceLabel(apiutil.NewAccessPath(path, method))
		if len(serviceLabel) == 0 {
			h.rd.JSON(w, http.StatusBadRequest, "There is no label matched.")
			return
		}
	default:
		h.rd.JSON(w, http.StatusBadRequest, "The type is invalid.")
		return
	}
	if h.svr.IsInRateLimitBlockList(serviceLabel) {
		h.rd.JSON(w, http.StatusBadRequest, "This service is in block list.")
		return
	}
	cfg := &ratelimit.DimensionConfig{}
	// update concurrency limiter
	concurrencyUpdatedFlag := "Concurrency limiter is not changed."
	concurrencyFloat, okc := input["concurrency"].(float64)
	if okc {
		concurrency := uint64(concurrencyFloat)
		if concurrency > 0 {
			cfg.ConcurrencyLimit = concurrency
			h.svr.UpdateServiceRateLimiter(serviceLabel, ratelimit.UpdateConcurrencyLimiter(concurrency))
			concurrencyUpdatedFlag = "Concurrency limiter is changed."
		} else {
			h.svr.DeteleServiceConcurrencyLimiter(serviceLabel)
			concurrencyUpdatedFlag = "Concurrency limiter is deleted."
		}
	}
	// update qps rate limiter
	qpsRateUpdatedFlag := "QPS rate limiter is not changed."
	qps, okq := input["qps"].(float64)
	if okq {
		if qps > 0 {
			brust := 1
			if int(qps) > 1 {
				brust = int(qps)
			}
			cfg.QPS = qps
			cfg.QPSBrust = brust
			h.svr.UpdateServiceRateLimiter(serviceLabel, ratelimit.UpdateQPSLimiter(qps, brust))
			qpsRateUpdatedFlag = "QPS rate limiter is changed."
		} else {
			h.svr.DeleteServiceQPSLimiter(serviceLabel)
			qpsRateUpdatedFlag = "QPS rate limiter is deleted."
		}
	}
	if !okc && !okq {
		h.rd.JSON(w, http.StatusOK, "No changed.")
	} else {
		updateRateLimitConfig(h.svr, "rate-limit-config", cfg)
		h.rd.JSON(w, http.StatusOK, fmt.Sprintf("%s %s", concurrencyUpdatedFlag, qpsRateUpdatedFlag))
	}
}
