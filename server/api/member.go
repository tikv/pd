// Copyright 2016 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"context"
	"fmt"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/unrolled/render"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/server"
)

type memberHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newMemberHandler(svr *server.Server, rd *render.Render) *memberHandler {
	return &memberHandler{
		svr: svr,
		rd:  rd,
	}
}

// @Tags     member
// @Summary  List all PD servers in the cluster.
// @Produce  json
// @Success  200  {object}  pdpb.GetMembersResponse
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /members [get]
func (h *memberHandler) GetMembers(w http.ResponseWriter, _ *http.Request) {
	members, err := getMembers(h.svr)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, members)
}

func getMembers(svr *server.Server) (*pdpb.GetMembersResponse, error) {
	req := &pdpb.GetMembersRequest{Header: &pdpb.RequestHeader{ClusterId: keypath.ClusterID()}}
	grpcServer := &server.GrpcServer{Server: svr}
	members, err := grpcServer.GetMembers(context.Background(), req)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if members.GetHeader().GetError() != nil {
		return nil, errors.WithStack(errors.New(members.GetHeader().GetError().String()))
	}

	for _, m := range members.GetMembers() {
		var e error
		m.BinaryVersion, e = svr.GetMember().GetMemberBinaryVersion(m.GetMemberId())
		if e != nil {
			log.Error("failed to load binary version", zap.Uint64("member", m.GetMemberId()), errs.ZapError(e))
		}
		m.DeployPath, e = svr.GetMember().GetMemberDeployPath(m.GetMemberId())
		if e != nil {
			log.Error("failed to load deploy path", zap.Uint64("member", m.GetMemberId()), errs.ZapError(e))
		}
		if svr.GetMember().GetEtcdLeader() == 0 {
			log.Warn("no etcd leader, skip get leader priority", zap.Uint64("member", m.GetMemberId()))
			continue
		}
		leaderPriority, e := svr.GetMember().GetMemberLeaderPriority(m.GetMemberId())
		if e != nil {
			log.Error("failed to load leader priority", zap.Uint64("member", m.GetMemberId()), errs.ZapError(e))
			continue
		}
		m.LeaderPriority = int32(leaderPriority)
		m.GitHash, e = svr.GetMember().GetMemberGitHash(m.GetMemberId())
		if e != nil {
			log.Error("failed to load git hash", zap.Uint64("member", m.GetMemberId()), errs.ZapError(e))
			continue
		}
	}
	return members, nil
}

// @Tags     member
// @Summary  Remove a PD server from the cluster.
// @Param    name  path  string  true  "PD server name"
// @Produce  json
// @Success  200  {string}  string  "The PD server is successfully removed."
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  404  {string}  string  "The member does not exist."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /members/name/{name} [delete]
func (h *memberHandler) DeleteMemberByName(w http.ResponseWriter, r *http.Request) {
	client := h.svr.GetClient()

	// Get etcd ID by name.
	var id uint64
	name := mux.Vars(r)["name"]
	listResp, err := etcdutil.ListEtcdMembers(client.Ctx(), client)
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

	// Delete config.
	err = h.svr.GetMember().DeleteMemberLeaderPriority(id)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Remove member by id
	_, err = etcdutil.RemoveEtcdMember(client, id)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, fmt.Sprintf("removed, pd: %s", name))
}

// @Tags     member
// @Summary  Remove a PD server from the cluster.
// @Param    id  path  integer  true  "PD server Id"
// @Produce  json
// @Success  200  {string}  string  "The PD server is successfully removed."
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /members/id/{id} [delete]
func (h *memberHandler) DeleteMemberByID(w http.ResponseWriter, r *http.Request) {
	idStr := mux.Vars(r)["id"]
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	// Delete config.
	err = h.svr.GetMember().DeleteMemberLeaderPriority(id)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	client := h.svr.GetClient()
	_, err = etcdutil.RemoveEtcdMember(client, id)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, fmt.Sprintf("removed, pd: %v", id))
}

// FIXME: details of input json body params
// @Tags     member
// @Summary  Set leader priority of a PD member.
// @Accept   json
// @Param    name  path  string  true  "PD server name"
// @Param    body  body  object  true  "json params"
// @Produce  json
// @Success  200  {string}  string  "The leader priority is updated."
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  404  {string}  string  "The member does not exist."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /members/name/{name} [post]
func (h *memberHandler) SetMemberPropertyByName(w http.ResponseWriter, r *http.Request) {
	members, membersErr := getMembers(h.svr)
	if membersErr != nil {
		h.rd.JSON(w, http.StatusInternalServerError, membersErr.Error())
		return
	}

	var memberID uint64
	name := mux.Vars(r)["name"]
	for _, m := range members.GetMembers() {
		if m.GetName() == name {
			memberID = m.GetMemberId()
			break
		}
	}
	if memberID == 0 {
		h.rd.JSON(w, http.StatusNotFound, fmt.Sprintf("not found, pd: %s", name))
		return
	}

	var input map[string]any
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &input); err != nil {
		return
	}
	for k, v := range input {
		if k == "leader-priority" {
			priority, ok := v.(float64)
			if !ok {
				h.rd.JSON(w, http.StatusBadRequest, "bad format leader priority")
				return
			}
			err := h.svr.GetMember().SetMemberLeaderPriority(memberID, int(priority))
			if err != nil {
				h.rd.JSON(w, http.StatusInternalServerError, err.Error())
				return
			}
		}
	}
	h.rd.JSON(w, http.StatusOK, "success")
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

// @Tags     leader
// @Summary  Get the leader PD server of the cluster.
// @Produce  json
// @Success  200  {object}  pdpb.Member
// @Router   /leader [get]
func (h *leaderHandler) GetLeader(w http.ResponseWriter, _ *http.Request) {
	h.rd.JSON(w, http.StatusOK, h.svr.GetLeader())
}

// @Tags     leader
// @Summary  Transfer etcd leadership to another PD server.
// @Produce  json
// @Success  200  {string}  string  "The resign command is submitted."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /leader/resign [post]
func (h *leaderHandler) ResignLeader(w http.ResponseWriter, _ *http.Request) {
	err := h.svr.GetMember().ResignEtcdLeader(h.svr.Context(), h.svr.Name(), "")
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.rd.JSON(w, http.StatusOK, "The resign command is submitted.")
}

// @Tags     leader
// @Summary  Transfer etcd leadership to the specific PD server.
// @Param    nextLeader  path  string  true  "PD server that transfer leader to"
// @Produce  json
// @Success  200  {string}  string  "The transfer command is submitted."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /leader/transfer/{nextLeader} [post]
func (h *leaderHandler) TransferLeader(w http.ResponseWriter, r *http.Request) {
	err := h.svr.GetMember().ResignEtcdLeader(h.svr.Context(), h.svr.Name(), mux.Vars(r)["next_leader"])
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.rd.JSON(w, http.StatusOK, "The transfer command is submitted.")
}
