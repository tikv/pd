// Copyright 2021 TiKV Project Authors.
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

type tsoHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newTSOHandler(svr *server.Server, rd *render.Render) *tsoHandler {
	return &tsoHandler{
		svr: svr,
		rd:  rd,
	}
}

// @Tags     tso
// @Summary  Transfer Local TSO Allocator
// @Accept   json
// @Param    name  path  string  true  "PD server name"
// @Param    body  body  object  true  "json params"
// @Produce  json
// @Router   /tso/allocator/transfer/{name} [post]
func (h *tsoHandler) TransferLocalTSOAllocator(w http.ResponseWriter, _ *http.Request) {
	h.rd.JSON(w, http.StatusOK, "The transfer command is deprecated.")
}
