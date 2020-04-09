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

	"github.com/pingcap/pd/v4/server"
	"github.com/unrolled/render"
)

type replicateHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newReplicateHandler(svr *server.Server, rd *render.Render) *replicateHandler {
	return &replicateHandler{
		svr: svr,
		rd:  rd,
	}
}

// @Tags replicate_mode
// @Summary Get status of replicate mode
// @Produce json
// @Success 200 {object} replicate.HTTPReplicateStatus
// @Router /replicate/status [get]
func (h *replicateHandler) GetStatus(w http.ResponseWriter, r *http.Request) {
	h.rd.JSON(w, http.StatusOK, getCluster(r.Context()).GetReplicateMode().GetReplicateStatusHTTP())
}
