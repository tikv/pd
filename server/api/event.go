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
	cluster, err := h.svr.GetRaftCluster()
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err)
		return
	}
	if cluster == nil {
		h.rd.JSON(w, http.StatusOK, nil)
		return
	}

	keyStr := r.URL.Query().Get("key")
	if len(keyStr) == 0 {
		h.rd.JSON(w, http.StatusOK, nil)
		return
	}

	key, err := strconv.ParseUint(keyStr, 10, 64)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err)
		return
	}

	evts := cluster.FetchEvents(key, false)
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
	cluster, err := h.svr.GetRaftCluster()
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err)
		return
	}
	if cluster == nil {
		h.rd.JSON(w, http.StatusOK, nil)
		return
	}

	evts := cluster.FetchEvents(0, true)
	h.rd.JSON(w, http.StatusOK, evts)
}
