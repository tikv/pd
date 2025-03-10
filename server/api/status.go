// Copyright 2017 TiKV Project Authors.
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

	"github.com/unrolled/render"

	"github.com/tikv/pd/pkg/versioninfo"
	"github.com/tikv/pd/server"
)

type statusHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newStatusHandler(svr *server.Server, rd *render.Render) *statusHandler {
	return &statusHandler{
		svr: svr,
		rd:  rd,
	}
}

// @Summary  Get the build info of PD server.
// @Produce  json
// @Success  200  {object}  versioninfo.Status
// @Router   /status [get]
func (h *statusHandler) GetPDStatus(w http.ResponseWriter, _ *http.Request) {
	version := versioninfo.Status{
		BuildTS:        versioninfo.PDBuildTS,
		GitHash:        versioninfo.PDGitHash,
		Version:        versioninfo.PDReleaseVersion,
		StartTimestamp: h.svr.StartTimestamp(),
	}

	h.rd.JSON(w, http.StatusOK, version)
}
