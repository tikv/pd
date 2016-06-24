// Copyright 2016 PingCAP, Inc.
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

	"github.com/pingcap/pd/server"
	"github.com/unrolled/render"
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

func (h *clusterHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	cluster, err := h.svr.GetRaftCluster()
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err)
		return
	}
	if cluster == nil {
		return
	}

	h.rd.JSON(w, http.StatusOK, cluster.GetConfig())
}
