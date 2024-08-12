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
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/server"
	"github.com/unrolled/render"
	"go.uber.org/zap"
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
	rc.RemoveRegionIfExist(regionID)
	msg := "The region is removed from server cache."
	if rc.IsServiceIndependent(constant.SchedulingServiceName) {
		err = h.deleteRegionCacheInSchedulingServer(regionID)
		if err != nil {
			msg = buildMsg(err)
		}
	}
	h.rd.JSON(w, http.StatusOK, msg)
}

// @Tags     admin
// @Summary  Remove target region from region cache and storage.
// @Param    id  path  integer  true  "Region Id"
// @Produce  json
// @Success  200  {string}  string  "The region is removed from server storage."
// @Failure  400  {string}  string  "The input is invalid."
// @Router   /admin/storage/region/{id} [delete]
func (h *adminHandler) DeleteRegionStorage(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r)
	vars := mux.Vars(r)
	regionIDStr := vars["id"]
	regionID, err := strconv.ParseUint(regionIDStr, 10, 64)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	targetRegion := rc.GetRegion(regionID)
	if targetRegion == nil {
		h.rd.JSON(w, http.StatusBadRequest, "failed to get target region from cache")
		return
	}

	// Remove region from storage
	if err = rc.GetStorage().DeleteRegion(targetRegion.GetMeta()); err != nil {
		log.Error("failed to delete region from storage",
			zap.Uint64("region-id", targetRegion.GetID()),
			zap.Stringer("region-meta", core.RegionToHexMeta(targetRegion.GetMeta())),
			errs.ZapError(err))
		h.rd.JSON(w, http.StatusOK, "failed to delete region from storage.")
		return
	}
	// Remove region from cache.
	rc.RemoveRegionIfExist(regionID)
	msg := "The region is removed from server cache and region meta storage."
	if rc.IsServiceIndependent(constant.SchedulingServiceName) {
		err = h.deleteRegionCacheInSchedulingServer(regionID)
		if err != nil {
			msg = buildMsg(err)
		}
	}

	h.rd.JSON(w, http.StatusOK, msg)
}

// @Tags     admin
// @Summary  Drop all regions from cache.
// @Produce  json
// @Success  200  {string}  string  "All regions are removed from server cache."
// @Router   /admin/cache/regions [delete]
func (h *adminHandler) DeleteAllRegionCache(w http.ResponseWriter, r *http.Request) {
	var err error
	rc := getCluster(r)
	rc.ResetRegionCache()
	msg := "All regions are removed from server cache."
	if rc.IsServiceIndependent(constant.SchedulingServiceName) {
		err = h.deleteRegionCacheInSchedulingServer()
		if err != nil {
			msg = buildMsg(err)
		}
	}

	h.rd.JSON(w, http.StatusOK, msg)
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

func (h *adminHandler) markSnapshotRecovering(w http.ResponseWriter, _ *http.Request) {
	if err := h.svr.MarkSnapshotRecovering(); err != nil {
		h.rd.Text(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.Text(w, http.StatusOK, "")
}

func (h *adminHandler) isSnapshotRecovering(w http.ResponseWriter, r *http.Request) {
	marked, err := h.svr.IsSnapshotRecovering(r.Context())
	if err != nil {
		h.rd.Text(w, http.StatusInternalServerError, err.Error())
		return
	}
	type resStruct struct {
		Marked bool `json:"marked"`
	}
	h.rd.JSON(w, http.StatusOK, &resStruct{Marked: marked})
}

func (h *adminHandler) unmarkSnapshotRecovering(w http.ResponseWriter, r *http.Request) {
	if err := h.svr.UnmarkSnapshotRecovering(r.Context()); err != nil {
		h.rd.Text(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.Text(w, http.StatusOK, "")
}

// RecoverAllocID recover base alloc id
// body should be in {"id": "123"} format
func (h *adminHandler) recoverAllocID(w http.ResponseWriter, r *http.Request) {
	var input map[string]any
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &input); err != nil {
		return
	}
	idValue, ok := input["id"].(string)
	if !ok || len(idValue) == 0 {
		h.rd.Text(w, http.StatusBadRequest, "invalid id value")
		return
	}
	newID, err := strconv.ParseUint(idValue, 10, 64)
	if err != nil {
		h.rd.Text(w, http.StatusBadRequest, err.Error())
		return
	}
	marked, err := h.svr.IsSnapshotRecovering(r.Context())
	if err != nil {
		h.rd.Text(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !marked {
		h.rd.Text(w, http.StatusForbidden, "can only recover alloc id when recovering mark marked")
		return
	}

	leader := h.svr.GetLeader()
	if leader == nil {
		h.rd.Text(w, http.StatusServiceUnavailable, errs.ErrLeaderNil.FastGenByArgs().Error())
		return
	}
	if err = h.svr.RecoverAllocID(r.Context(), newID); err != nil {
		h.rd.Text(w, http.StatusInternalServerError, err.Error())
	}

	h.rd.Text(w, http.StatusOK, "")
}

func (h *adminHandler) deleteRegionCacheInSchedulingServer(id ...uint64) error {
	addr, ok := h.svr.GetServicePrimaryAddr(h.svr.Context(), constant.SchedulingServiceName)
	if !ok {
		return errs.ErrNotFoundSchedulingAddr.FastGenByArgs()
	}
	var idStr string
	if len(id) > 0 {
		idStr = strconv.FormatUint(id[0], 10)
	}
	url := fmt.Sprintf("%s/scheduling/api/v1/admin/cache/regions/%s", addr, idStr)
	req, err := http.NewRequest(http.MethodDelete, url, http.NoBody)
	if err != nil {
		return err
	}
	resp, err := h.svr.GetHTTPClient().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return errs.ErrSchedulingServer.FastGenByArgs(resp.StatusCode)
	}
	return nil
}

func buildMsg(err error) string {
	return fmt.Sprintf("This operation was executed in API server but needs to be re-executed on scheduling server due to the following error: %s", err.Error())
}
