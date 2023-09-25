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
	"io"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/tikv/pd/pkg/apiutil"
	"github.com/tikv/pd/pkg/errs"
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

// @Tags     admin
// @Summary  Drop a specific region from cache.
// @Param    id  path  integer  true  "Region Id"
// @Produce  json
// @Success  200  {string}  string  "The region is removed from server cache."
// @Failure  400  {string}  string  "The input is invalid."
// @Router   /admin/cache/region/{id} [delete]
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

// @Tags     admin
// @Summary  Drop all regions from cache.
// @Produce  json
// @Success  200  {string}  string  "All regions are removed from server cache."
// @Router   /admin/cache/regions [delete]
func (h *adminHandler) DeleteAllRegionCache(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r)
	rc.DropCacheAllRegion()
	h.rd.JSON(w, http.StatusOK, "All regions are removed from server cache.")
}

// ResetTS
// FIXME: details of input json body params
// @Tags     admin
// @Summary  Reset the ts.
// @Accept   json
// @Param    body  body  object  true  "json params"
// @Produce  json
// @Success  200  {string}  string  "Reset ts successfully."
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  403  {string}  string  "Reset ts is forbidden."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /admin/reset-ts [post]
// if force-use-larger=true:
//
//	reset ts to max(current ts, input ts).
//
// else:
//
//	reset ts to input ts if it > current ts and < upper bound, error if not in that range
//
// during EBS based restore, we call this to make sure ts of pd >= resolved_ts in backup.
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

	forceUseLarger := false
	forceUseLargerVal, contains := input["force-use-larger"]
	if contains {
		if forceUseLarger, ok = forceUseLargerVal.(bool); !ok {
			h.rd.JSON(w, http.StatusBadRequest, "invalid force-use-larger value")
			return
		}
	}
	var ignoreSmaller, skipUpperBoundCheck bool
	if forceUseLarger {
		ignoreSmaller, skipUpperBoundCheck = true, true
	}

	if err = handler.ResetTS(ts, ignoreSmaller, skipUpperBoundCheck); err != nil {
		if err == server.ErrServerNotStarted {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		} else {
			h.rd.JSON(w, http.StatusForbidden, err.Error())
		}
		return
	}
	h.rd.JSON(w, http.StatusOK, "Reset ts successfully.")
}

// Intentionally no swagger mark as it is supposed to be only used in
// server-to-server.
// For security reason,
//   - it only accepts JSON formatted data.
//   - it only accepts file name which is `DrStatusFile`.
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

func (h *adminHandler) MarkSnapshotRecovering(w http.ResponseWriter, r *http.Request) {
	if err := h.svr.MarkSnapshotRecovering(); err != nil {
		_ = h.rd.Text(w, http.StatusInternalServerError, err.Error())
		return
	}
	_ = h.rd.Text(w, http.StatusOK, "")
}

func (h *adminHandler) IsSnapshotRecovering(w http.ResponseWriter, r *http.Request) {
	marked, err := h.svr.IsSnapshotRecovering(r.Context())
	if err != nil {
		_ = h.rd.Text(w, http.StatusInternalServerError, err.Error())
		return
	}
	type resStruct struct {
		Marked bool `json:"marked"`
	}
	_ = h.rd.JSON(w, http.StatusOK, &resStruct{Marked: marked})
}

func (h *adminHandler) UnmarkSnapshotRecovering(w http.ResponseWriter, r *http.Request) {
	if err := h.svr.UnmarkSnapshotRecovering(r.Context()); err != nil {
		_ = h.rd.Text(w, http.StatusInternalServerError, err.Error())
		return
	}
	_ = h.rd.Text(w, http.StatusOK, "")
}

// RecoverAllocID recover base alloc id
// body should be in {"id": "123"} format
func (h *adminHandler) RecoverAllocID(w http.ResponseWriter, r *http.Request) {
	var input map[string]interface{}
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &input); err != nil {
		return
	}
	idValue, ok := input["id"].(string)
	if !ok || len(idValue) == 0 {
		_ = h.rd.Text(w, http.StatusBadRequest, "invalid id value")
		return
	}
	newID, err := strconv.ParseUint(idValue, 10, 64)
	if err != nil {
		_ = h.rd.Text(w, http.StatusBadRequest, err.Error())
		return
	}
	marked, err := h.svr.IsSnapshotRecovering(r.Context())
	if err != nil {
		_ = h.rd.Text(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !marked {
		_ = h.rd.Text(w, http.StatusForbidden, "can only recover alloc id when recovering mark marked")
		return
	}

	leader := h.svr.GetLeader()
	if leader == nil {
		_ = h.rd.Text(w, http.StatusServiceUnavailable, errs.ErrLeaderNil.FastGenByArgs().Error())
		return
	}
	if err = h.svr.RecoverAllocID(r.Context(), newID); err != nil {
		_ = h.rd.Text(w, http.StatusInternalServerError, err.Error())
	}

	_ = h.rd.Text(w, http.StatusOK, "")
}
