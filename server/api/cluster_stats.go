// Copyright 2019 PingCAP, Inc.
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
	// request table_name
	nodeInfo       = "node_info"
	statsInfo      = "stats_info"
	networkLatency = "network_latency"
)

type clusterStatsHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newClusterStatsHandler(svr *server.Server, rd *render.Render) *clusterStatsHandler {
	return &clusterStatsHandler{
		svr: svr,
		rd:  rd,
	}
}

func (h *clusterStatsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	tableName := mux.Vars(r)["table_name"]
	switch tableName {
	case nodeInfo:
		ssi, err := nodeutil.GetStaticStatsInfo(h.svr)
		if err != nil {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
		h.rd.JSON(w, http.StatusOK, ssi)
	case statsInfo:
		csi, err := nodeutil.GetCurrStatsInfo(h.svr)
		if err != nil {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
		h.rd.JSON(w, http.StatusOK, csi)
	case networkLatency:
		nl, err := nodeutil.GetNetworkLatency(h.svr)
		if err != nil {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
		h.rd.JSON(w, http.StatusOK, nl)
	default:
		h.rd.JSON(w, http.StatusOK, "")
	}
}
