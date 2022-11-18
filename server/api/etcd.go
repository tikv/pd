// Copyright 2022 TiKV Project Authors.
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
	"net/http"

	"github.com/tikv/pd/server"
	"github.com/unrolled/render"
	"go.etcd.io/etcd/etcdserver"
)

type etcdHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newEtcdHandler(svr *server.Server, rd *render.Render) *etcdHandler {
	return &etcdHandler{
		svr: svr,
		rd:  rd,
	}
}

// etcdReadiness reflects current PD instance's etcd readiness.
type etcdReadiness struct {
	// IsLearner indicates whether the etcd member is a learner.
	IsLearner bool `json:"is_learner"`
	// CurrIndex is the applied raft index for current PD.
	CurrIndex uint64 `json:"curr_index"`
	// LeaderIndex is the committed raft index for the leader.
	LeaderIndex uint64 `json:"leader_index"`
}

// @Summary  ETCD readiness status of the PD instance.
// @Produce  json
// @Success  200  {object}  etcdReadiness
// @Failure  400  {object}  etcdReadiness
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /ready [get]
func (h *etcdHandler) GetReadyStatus(w http.ResponseWriter, r *http.Request) {
	client := h.svr.GetClient()

	var leaderIndex uint64
	if h.svr.GetLeader() == nil || len(h.svr.GetLeader().PeerUrls) == 0 {
		h.rd.JSON(w, http.StatusInternalServerError, "failed to find etcd leader url")
		return
	}
	if leaderStatus, err := client.Maintenance.Status(client.Ctx(), h.svr.GetLeader().PeerUrls[0]); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	} else {
		leaderIndex = leaderStatus.RaftIndex
	}

	if h.svr.GetMember() == nil || h.svr.GetMember().Etcd() == nil || h.svr.GetMember().Etcd().Server == nil {
		h.rd.JSON(w, http.StatusInternalServerError, "failed to find PD's etcd server")
		return
	}
	currServer := h.svr.GetMember().Etcd().Server

	status := &etcdReadiness{
		IsLearner:   currServer.IsLearner(),
		CurrIndex:   currServer.AppliedIndex(),
		LeaderIndex: leaderIndex,
	}
	if currServer.IsLearner() || currServer.AppliedIndex()+etcdserver.DefaultSnapshotCatchUpEntries < leaderIndex {
		h.rd.JSON(w, http.StatusBadRequest, status)
	}

	h.rd.JSON(w, http.StatusOK, status)
}
