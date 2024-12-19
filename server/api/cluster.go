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

	"github.com/unrolled/render"

	"github.com/tikv/pd/server"
)

type clusterHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newClusterHandler(svr *server.Server, rd *render.Render) *clusterHandler {
	return &clusterHandler{
		svr: svr,
		rd:  rd,
	}
}

// @Tags     cluster
// @Summary  Get cluster info.
// @Produce  json
// @Success  200  {object}  metapb.Cluster
// @Router   /cluster [get]
func (h *clusterHandler) GetCluster(w http.ResponseWriter, _ *http.Request) {
	h.rd.JSON(w, http.StatusOK, h.svr.GetCluster())
}

// @Tags     cluster
// @Summary  Get cluster status.
// @Produce  json
// @Success  200  {object}  cluster.Status
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /cluster/status [get]
func (h *clusterHandler) GetClusterStatus(w http.ResponseWriter, _ *http.Request) {
	status, err := h.svr.GetClusterStatus()
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, status)
}
