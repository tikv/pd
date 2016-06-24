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
	"strconv"

	"github.com/gorilla/mux"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server"
	"github.com/unrolled/render"
)

type regionInfo struct {
	Count   int              `json:"count"`
	Regions []*metapb.Region `json:"regions"`
}

type regionHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newRegionHandler(svr *server.Server, rd *render.Render) *regionHandler {
	return &regionHandler{
		svr: svr,
		rd:  rd,
	}
}

func (h *regionHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	cluster, err := h.svr.GetRaftCluster()
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err)
		return
	}
	if cluster == nil {
		h.rd.JSON(w, http.StatusOK, nil)
		return
	}

	vars := mux.Vars(r)
	regionIDStr := vars["id"]
	regionID, err := strconv.ParseUint(regionIDStr, 10, 64)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err)
		return
	}

	region := cluster.GetRegionByID(regionID)
	h.rd.JSON(w, http.StatusOK, region)
}

type regionsHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newRegionsHandler(svr *server.Server, rd *render.Render) *regionsHandler {
	return &regionsHandler{
		svr: svr,
		rd:  rd,
	}
}

func (h *regionsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	cluster, err := h.svr.GetRaftCluster()
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err)
		return
	}
	if cluster == nil {
		h.rd.JSON(w, http.StatusOK, nil)
		return
	}

	regions := cluster.GetRegions()
	regionsInfo := &regionInfo{
		Count:   len(regions),
		Regions: regions,
	}
	h.rd.JSON(w, http.StatusOK, regionsInfo)
}
