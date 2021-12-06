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

	"github.com/tikv/pd/pkg/apiutil"
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

// @Tags unsafe
// @Summary Remove failed stores unsafely.
// @Produce json
// Success 200 {string} string "Request has been accepted."
// Failure 400 {string} string "The input is invalid."
// Failure 500 {string} string "PD server failed to proceed the request."
// @Router /admin/unsafe/remove-failed-stores [POST]
func (h *unsafeOperationHandler) RemoveFailedStores(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r)
	var stores map[uint64]string
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &stores); err != nil {
		return
	}
	if len(stores) == 0 {
		h.rd.JSON(w, http.StatusBadRequest, "No store specified")
		return
	}
	if err := rc.GetUnsafeRecoveryController().RemoveFailedStores(stores); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, "Request has been accepted.")
}

// @Tags unsafe
// @Summary Show the current status of failed stores removal.
// @Produce json
// Success 200 {object} []string
// @Router /admin/unsafe/remove-failed-stores/show [GET]
func (h *unsafeOperationHandler) GetFailedStoresRemovalStatus(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r)
	h.rd.JSON(w, http.StatusOK, rc.GetUnsafeRecoveryController().Show())
}

// @Tags unsafe
// @Summary Show the history of failed stores removal.
// @Produce json
// Success 200 {object} []string
// @Router /admin/unsafe/remove-failed-stores/history [GET]
func (h *unsafeOperationHandler) GetFailedStoresRemovalHistory(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r)
	h.rd.JSON(w, http.StatusOK, rc.GetUnsafeRecoveryController().History())
}
