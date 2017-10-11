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
	"strconv"

	"github.com/gorilla/mux"
	"github.com/juju/errors"
	"github.com/pingcap/pd/server"
	"github.com/unrolled/render"
)

type storeNsHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newStoreNsHandler(svr *server.Server, rd *render.Render) *storeNsHandler {
	return &storeNsHandler{
		svr: svr,
		rd:  rd,
	}
}

func (h *storeNsHandler) SetNamespace(w http.ResponseWriter, r *http.Request) {
	cluster := h.svr.GetRaftCluster()
	if cluster == nil {
		h.rd.JSON(w, http.StatusInternalServerError, server.ErrNotBootstrapped.Error())
	}

	vars := mux.Vars(r)
	storeIDStr := vars["id"]
	storeID, err := strconv.ParseUint(storeIDStr, 10, 64)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	var input map[string]string
	if err := readJSON(r.Body, &input); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	ns := input["namespace"]
	action, ok := input["action"]
	if !ok {
		h.rd.JSON(w, http.StatusBadRequest, errors.New("missing parameters"))
	}

	switch action {
	case "add":
		// append store id to namespace
		if err := cluster.AddNamespaceStoreID(ns, storeID); err != nil {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case "remove":
		// remove store id from namespace
		if err := cluster.RemoveNamespaceStoreID(ns, storeID); err != nil {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	default:
		h.rd.JSON(w, http.StatusBadRequest, errors.New("unknown action"))
		return
	}

	h.rd.JSON(w, http.StatusOK, nil)
}
