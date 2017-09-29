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

	"github.com/juju/errors"
	"github.com/pingcap/pd/server"
	"github.com/unrolled/render"
)

type namespacesInfo struct {
	Count      int                 `json:"count"`
	Namespaces []*server.Namespace `json:"namespaces"`
}

type namespaceHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newNamespaceHandler(svr *server.Server, rd *render.Render) *namespaceHandler {
	return &namespaceHandler{
		svr: svr,
		rd:  rd,
	}
}

// Get lists namespace mapping
func (h *namespaceHandler) Get(w http.ResponseWriter, r *http.Request) {
	cluster := h.svr.GetRaftCluster()
	if cluster == nil {
		h.rd.JSON(w, http.StatusInternalServerError, server.ErrNotBootstrapped.Error())
		return
	}

	namespaces := cluster.GetNamespaces()
	nsInfo := &namespacesInfo{
		Count:      len(namespaces),
		Namespaces: namespaces,
	}
	h.rd.JSON(w, http.StatusOK, nsInfo)
}

// Post creates a namespace
func (h *namespaceHandler) Post(w http.ResponseWriter, r *http.Request) {
	cluster := h.svr.GetRaftCluster()
	if cluster == nil {
		h.rd.JSON(w, http.StatusInternalServerError, server.ErrNotBootstrapped.Error())
		return
	}

	var input map[string]string
	if err := readJSON(r.Body, &input); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	ns := input["namespace"]

	// create namespace
	if err := cluster.CreateNamespace(ns); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.rd.JSON(w, http.StatusOK, nil)
}

func (h *namespaceHandler) Update(w http.ResponseWriter, r *http.Request) {
	cluster := h.svr.GetRaftCluster()
	if cluster == nil {
		h.rd.JSON(w, http.StatusInternalServerError, server.ErrNotBootstrapped.Error())
		return
	}

	var input map[string]string
	if err := readJSON(r.Body, &input); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	tableIDStr := input["table_id"]
	tableID, err := strconv.ParseInt(tableIDStr, 10, 64)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	ns := input["namespace"]
	action, ok := input["action"]
	if !ok {
		h.rd.JSON(w, http.StatusBadRequest, errors.New("missing parameters"))
		return
	}

	switch action {
	case "add":
		// append table id to namespace
		if err := cluster.AddNamespaceTableID(ns, tableID); err != nil {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case "remove":
		// remove table id from namespace
		if err := cluster.RemoveNamespaceTableID(ns, tableID); err != nil {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	default:
		h.rd.JSON(w, http.StatusBadRequest, errors.New("unknown action"))
		return
	}

	h.rd.JSON(w, http.StatusOK, nil)
}
