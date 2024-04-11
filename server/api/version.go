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

	"github.com/tikv/pd/pkg/versioninfo"
	"github.com/unrolled/render"
)

// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type version struct {
	Version   string `json:"version"`
	BuildTime string `json:"build_time"`
	Hash      string `json:"hash"`
	Branch    string `json:"branch"`
}

type versionHandler struct {
	rd *render.Render
}

func newVersionHandler(rd *render.Render) *versionHandler {
	return &versionHandler{
		rd: rd,
	}
}

// @Summary  Get the version of PD server.
// @Produce  json
// @Success  200  {object}  version
// @Router   /version [get]
func (h *versionHandler) GetVersion(w http.ResponseWriter, _ *http.Request) {
	version := &version{
		Version: versioninfo.PDReleaseVersion,
	}
	h.rd.JSON(w, http.StatusOK, version)
}
