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

	"github.com/tikv/pd/pkg/etcdutil"
	"github.com/tikv/pd/server"
	"github.com/unrolled/render"
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

// @Summary  ETCD readiness status of the PD instance.
// @Produce  json
// @Success  200  {string}  string  "ok"
// @Failure  400  {string}  string  "not ready"
// @Failure  400  {string}  string  "not found"
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /ready [get]
func (h *etcdHandler) GetReadyStatus(w http.ResponseWriter, r *http.Request) {
	client := h.svr.GetClient()
	resp, err := etcdutil.ListEtcdMembers(client)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	for _, member := range resp.Members {
		if member.GetID() != h.svr.GetMember().ID() {
			continue
		}
		// readiness check failed if the member is still a learner
		// after it is promoted to a leader or follower, the PD instance is ready
		if member.IsLearner {
			h.rd.Text(w, http.StatusBadRequest, "not ready")
			return
		}
		h.rd.Text(w, http.StatusOK, "ok")
		return
	}
	h.rd.Text(w, http.StatusNotFound, "not found")
}
