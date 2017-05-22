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

	"github.com/pingcap/pd/server"
	"github.com/unrolled/render"
)

type feedHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newFeedHandler(svr *server.Server, rd *render.Render) *feedHandler {
	return &feedHandler{
		svr: svr,
		rd:  rd,
	}
}

func (h *feedHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	cluster := h.svr.GetRaftCluster()
	if cluster == nil {
		h.rd.JSON(w, http.StatusInternalServerError, server.ErrNotBootstrapped.Error())
		return
	}

	offsetStr := r.URL.Query().Get("offset")
	if len(offsetStr) == 0 {
		h.rd.JSON(w, http.StatusOK, nil)
		return
	}

	offset, err := strconv.ParseUint(offsetStr, 10, 64)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	evts := cluster.FetchEvents(offset, false)
	h.rd.JSON(w, http.StatusOK, evts)
}

type eventsHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newEventsHandler(svr *server.Server, rd *render.Render) *eventsHandler {
	return &eventsHandler{
		svr: svr,
		rd:  rd,
	}
}

func (h *eventsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	cluster := h.svr.GetRaftCluster()
	if cluster == nil {
		h.rd.JSON(w, http.StatusInternalServerError, server.ErrNotBootstrapped.Error())
		return
	}

	evts := cluster.FetchEvents(0, true)
	h.rd.JSON(w, http.StatusOK, evts)
}
