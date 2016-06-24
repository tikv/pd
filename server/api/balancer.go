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

func (h *balancerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	cluster, err := h.svr.GetRaftCluster()
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err)
		return
	}
	if cluster == nil {
		h.rd.JSON(w, http.StatusOK, nil)
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
	cluster, err := h.svr.GetRaftCluster()
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err)
		return
	}
	if cluster == nil {
		h.rd.JSON(w, http.StatusOK, nil)
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
