// Copyright 2021 TiKV Project Authors.
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

	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/server"
	"github.com/unrolled/render"
)

type unsafeOperationHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newUnsafeOperationHandler(svr *server.Server, rd *render.Render) *unsafeOperationHandler {
	return &unsafeOperationHandler{
		svr: svr,
		rd:  rd,
	}
}

// @Tags     unsafe
// @Summary  Remove failed stores unsafely.
// @Accept   json
// @Param    body  body  object  true  "json params"
// @Produce  json
// Success 200 {string} string "Request has been accepted."
// Failure 400 {string} string "The input is invalid."
// Failure 500 {string} string "PD server failed to proceed the request."
// @Router   /admin/unsafe/remove-failed-stores [post]
func (h *unsafeOperationHandler) RemoveFailedStores(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r)
	var input map[string]any
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &input); err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	stores := make(map[uint64]struct{})
	autoDetect, exists := input["auto-detect"].(bool)
	if !exists || !autoDetect {
		storeSlice, ok := typeutil.JSONToUint64Slice(input["stores"])
		if !ok {
			h.rd.JSON(w, http.StatusBadRequest, "Store ids are invalid")
			return
		}
		for _, store := range storeSlice {
			stores[store] = struct{}{}
		}
	}

	timeout := uint64(600)
	if rawTimeout, exists := input["timeout"].(float64); exists {
		timeout = uint64(rawTimeout)
	}

	if err := rc.GetUnsafeRecoveryController().RemoveFailedStores(stores, timeout, autoDetect); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, "Request has been accepted.")
}

// @Tags     unsafe
// @Summary  Show the current status of failed stores removal.
// @Produce  json
// Success 200 {object} []StageOutput
// @Router   /admin/unsafe/remove-failed-stores/show [get]
func (h *unsafeOperationHandler) GetFailedStoresRemovalStatus(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r)
	h.rd.JSON(w, http.StatusOK, rc.GetUnsafeRecoveryController().Show())
}
