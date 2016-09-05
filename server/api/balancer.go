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
	"encoding/json"
	"net/http"

	"github.com/pingcap/pd/server"
	"github.com/unrolled/render"
)

type balancersInfo struct {
	Count     int               `json:"count"`
	Balancers []server.Operator `json:"balancers"`
}

type balancerHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newBalancerHandler(svr *server.Server, rd *render.Render) *balancerHandler {
	return &balancerHandler{
		svr: svr,
		rd:  rd,
	}
}

func (h *balancerHandler) Get(w http.ResponseWriter, r *http.Request) {
	cluster := h.svr.GetRaftCluster()
	if cluster == nil {
		h.rd.JSON(w, http.StatusInternalServerError, errNotBootstrapped.Error())
		return
	}

	balancers := cluster.GetBalanceOperators()
	balancersInfo := &balancersInfo{
		Count:     len(balancers),
		Balancers: make([]server.Operator, 0, len(balancers)),
	}

	for _, balancer := range balancers {
		balancersInfo.Balancers = append(balancersInfo.Balancers, balancer)
	}

	h.rd.JSON(w, http.StatusOK, balancersInfo)
}

func (h *balancerHandler) Post(w http.ResponseWriter, r *http.Request) {
	cluster := h.svr.GetRaftCluster()
	if cluster == nil {
		h.rd.JSON(w, http.StatusInternalServerError, errNotBootstrapped.Error())
		return
	}

	var input []json.RawMessage
	if err := readJSON(r.Body, &input); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	ops := make(map[uint64][]server.Operator)
	for _, message := range input {
		id, op, err := newOperator(cluster, message)
		if err != nil {
			h.rd.JSON(w, http.StatusInternalServerError, err)
			return
		}
		ops[id] = append(ops[id], op)
	}

	for regionID, regionOps := range ops {
		if err := cluster.SetAdminOperator(regionID, regionOps); err != nil {
			h.rd.JSON(w, http.StatusInternalServerError, err)
			return
		}
	}

	h.rd.JSON(w, http.StatusOK, nil)
}

type historyOperatorHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newHistoryOperatorHandler(svr *server.Server, rd *render.Render) *historyOperatorHandler {
	return &historyOperatorHandler{
		svr: svr,
		rd:  rd,
	}
}

func (h *historyOperatorHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	cluster := h.svr.GetRaftCluster()
	if cluster == nil {
		h.rd.JSON(w, http.StatusInternalServerError, errNotBootstrapped.Error())
		return
	}

	balancers := cluster.GetHistoryOperators()
	balancersInfo := &balancersInfo{
		Count:     len(balancers),
		Balancers: make([]server.Operator, 0, len(balancers)),
	}

	for _, balancer := range balancers {
		balancersInfo.Balancers = append(balancersInfo.Balancers, balancer)
	}

	h.rd.JSON(w, http.StatusOK, balancersInfo)
}
