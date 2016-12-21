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
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/server"
	"github.com/unrolled/render"
	"golang.org/x/net/context"
)

const defaultDialTimeout = 5 * time.Second

type memberListHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newMemberListHandler(svr *server.Server, rd *render.Render) *memberListHandler {
	return &memberListHandler{
		svr: svr,
		rd:  rd,
	}
}

func (h *memberListHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	client := h.svr.GetClient()

	members, err := server.GetPDMembers(client)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	ret := make(map[string][]*pdpb.PDMember)
	ret["members"] = members
	h.rd.JSON(w, http.StatusOK, ret)
}

type memberDeleteHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newMemberDeleteHandler(svr *server.Server, rd *render.Render) *memberDeleteHandler {
	return &memberDeleteHandler{
		svr: svr,
		rd:  rd,
	}
}

func (h *memberDeleteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	client := h.svr.GetClient()

	// step 1. get etcd id
	var id uint64
	name := (mux.Vars(r))["name"]
	ctx, cancel := context.WithTimeout(client.Ctx(), defaultDialTimeout)
	defer cancel()
	listResp, err := client.MemberList(ctx)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	for _, m := range listResp.Members {
		if name == m.Name {
			id = m.ID
			break
		}
	}
	if id == 0 {
		h.rd.JSON(w, http.StatusNotFound, fmt.Sprintf("not found, pd: %s", name))
		return
	}

	// step 2. remove member by id
	ctx, cancel = context.WithTimeout(client.Ctx(), defaultDialTimeout)
	defer cancel()
	_, err = client.MemberRemove(ctx, id)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, fmt.Sprintf("removed, pd: %s", name))
}

type leaderInfo struct {
	Addr string `json:"addr"`
	ID   uint64 `json:"id"`
}

type leaderHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newLeaderHandler(svr *server.Server, rd *render.Render) *leaderHandler {
	return &leaderHandler{
		svr: svr,
		rd:  rd,
	}
}

func (h *leaderHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	leader, err := h.svr.GetLeader()
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	ret := leaderInfo{
		Addr: leader.Addr,
		ID:   leader.Id,
	}
	h.rd.JSON(w, http.StatusOK, ret)
}
