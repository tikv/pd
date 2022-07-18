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

	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server"
	"github.com/unrolled/render"
)

type minResolvedTSHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newMinResolvedTSHandler(svr *server.Server, rd *render.Render) *minResolvedTSHandler {
	return &minResolvedTSHandler{
		svr: svr,
		rd:  rd,
	}
}

// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type minResolvedTS struct {
	IsRealTime      bool              `json:"is_real_time,omitempty"`
	MinResolvedTS   uint64            `json:"min_resolved_ts"`
	PersistInterval typeutil.Duration `json:"persist_interval,omitempty"`
}

// @Tags     min_resolved_ts
// @Summary  Get cluster-level min resolved ts.
// @Produce  json
// @Success  200  {array}   minResolvedTS
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /min-resolved-ts [get]
func (h *minResolvedTSHandler) GetMinResolvedTS(w http.ResponseWriter, r *http.Request) {
	c := h.svr.GetRaftCluster()
	value := c.GetMinResolvedTS()
	persistInterval := c.GetOpts().GetPDServerConfig().MinResolvedTSPersistenceInterval
	h.rd.JSON(w, http.StatusOK, minResolvedTS{
		MinResolvedTS:   value,
		PersistInterval: persistInterval,
		IsRealTime:      persistInterval.Duration != 0,
	})
}
