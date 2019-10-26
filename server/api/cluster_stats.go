// Copyright 2017 PingCAP, Inc.
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

	"github.com/pingcap/pd/pkg/nodeutil"

	"github.com/gorilla/mux"

	"github.com/pingcap/pd/server"
	"github.com/unrolled/render"
)

const (
	nodeInfo       = "pd_node_info_cluster"
	statsInfo      = "pd_stats_info_cluster"
	networkLatency = "pd_network_latency_cluster"
)

type clusterStatsHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newStatsInfoHandler(svr *server.Server, rd *render.Render) *clusterStatsHandler {
	return &clusterStatsHandler{
		svr: svr,
		rd:  rd,
	}
}

func (h *clusterStatsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	tableName := mux.Vars(r)["table_name"]
	switch tableName {
	case nodeInfo:
		h.rd.JSON(w, http.StatusOK, nodeutil.GetStaticStatsInfo())
	case statsInfo:
		h.rd.JSON(w, http.StatusOK, nodeutil.GetStatsInfo())
	case networkLatency:
		h.rd.JSON(w, http.StatusOK, nodeutil.GetNetworkLatency())
	}
	h.rd.JSON(w, http.StatusOK, nil)
}
