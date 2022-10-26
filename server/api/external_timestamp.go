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
)

type externalTimestampHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newExternalTSHandler(svr *server.Server, rd *render.Render) *externalTimestampHandler {
	return &externalTimestampHandler{
		svr: svr,
		rd:  rd,
	}
}

// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type externalTimestamp struct {
	ExternalTimestamp uint64 `json:"external_timestamp"`
}

// @Tags     external_timestamp
// @Summary  Get external timestamp.
// @Produce  json
// @Success  200  {array}   ExternalTimestamp
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /external-timestamp [get]
func (h *externalTimestampHandler) GetExternalTS(w http.ResponseWriter, r *http.Request) {
	value := h.svr.GetExternalTS()
	h.rd.JSON(w, http.StatusOK, externalTimestamp{
		ExternalTimestamp: value,
	})
}
