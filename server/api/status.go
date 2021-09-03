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

	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/versioninfo"
	"github.com/unrolled/render"
)

type statusHandler struct {
	svr *server.Server
	rd  *render.Render
}

type status struct {
	BuildTS        string `json:"build_ts"`
	Version        string `json:"version"`
	GitHash        string `json:"git_hash"`
	StartTimestamp int64  `json:"start_timestamp"`
}

func newStatusHandler(svr *server.Server, rd *render.Render) *statusHandler {
	return &statusHandler{
		svr: svr,
		rd:  rd,
	}
}

// @Summary Get the build info of PD server.
// @Produce json
// @Success 200 {object} status
// @Router /status [get]
func (h *statusHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	version := status{
		BuildTS:        versioninfo.PDBuildTS,
		GitHash:        versioninfo.PDGitHash,
		Version:        versioninfo.PDReleaseVersion,
		StartTimestamp: h.svr.StartTimestamp(),
	}

	h.rd.JSON(w, http.StatusOK, version)
}
