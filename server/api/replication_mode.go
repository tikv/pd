// Copyright 2020 TiKV Project Authors.
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

	"github.com/tikv/pd/server"
	"github.com/unrolled/render"
)

type replicationModeHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newReplicationModeHandler(svr *server.Server, rd *render.Render) *replicationModeHandler {
	return &replicationModeHandler{
		svr: svr,
		rd:  rd,
	}
}

// @Tags replication_mode
// @Summary Get status of replication mode
// @Produce json
// @Success 200 {object} replication.HTTPReplicationStatus
// @Router /replication_mode/status [get]
func (h *replicationModeHandler) GetStatus(w http.ResponseWriter, r *http.Request) {
	h.rd.JSON(w, http.StatusOK, getCluster(r).GetReplicationMode().GetReplicationStatusHTTP())
}
