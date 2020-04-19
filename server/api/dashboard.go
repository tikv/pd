// Copyright 2020 PingCAP, Inc.
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
	"net/http"

	"github.com/pingcap/errcode"
	"github.com/pingcap/pd/v4/pkg/apiutil"
	"github.com/pingcap/pd/v4/server"
	"github.com/pkg/errors"
	"github.com/unrolled/render"
	dashboardUtil "github.com/pingcap-incubator/tidb-dashboard/pkg/utils"
)

type dashboardHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newDashboardHandler(svr *server.Server, rd *render.Render) *dashboardHandler {
	return &dashboardHandler{
		svr: svr,
		rd:  rd,
	}
}

// @Tags dashboard
// @Summary reset tikv mode auth password.
// @Produce json
// @Success 200 {string} string
// @Failure 500 {string} string "empty password not allow."
// @Router /dashboard/auth/kvmode [post]
func (h *dashboardHandler) Reset(w http.ResponseWriter, r *http.Request) {
	input := make(map[string]string)
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &input); err != nil {
		return
	}
	password, ok := input["password"]
	if !ok {
		apiutil.ErrorResp(h.rd, w, errcode.NewInvalidInputErr(errors.New("not set password")))
		return
	}
	c := h.svr.GetClient()
	if dashboardUtil.ResetKvModeAuthKey(c, password) != nil {
		h.rd.JSON(w, http.StatusBadRequest, "Failed to reset kv mode pass")
		return
	}
	h.rd.JSON(w, http.StatusOK, nil)
}

// @Tags dashboard
// @Summary delete tikv mode auth password.
// @Produce json
// @Success 200 {string} string
// @Router /dashboard/auth/kvmode [delete]
func (h *dashboardHandler) Delete(w http.ResponseWriter, r *http.Request) {
	if dashboardUtil.ClearKvModeAuthKey(h.svr.GetClient()) != nil {
		h.rd.JSON(w, http.StatusBadRequest, "Failed to clear kv mode pass")
		return
	}
	h.rd.JSON(w, http.StatusOK, nil)
}
