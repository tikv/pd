// Copyright 2018 TiKV Project Authors.
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
	"encoding/json"
	"io"
	"net/http"

	"github.com/unrolled/render"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/server"
)

type logHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newLogHandler(svr *server.Server, rd *render.Render) *logHandler {
	return &logHandler{
		svr: svr,
		rd:  rd,
	}
}

// @Tags     admin
// @Summary  Set log level.
// @Accept   json
// @Param    level  body  string  true  "json params"
// @Produce  json
// @Success  200  {string}  string  "The log level is updated."
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Failure  503  {string}  string  "PD server has no leader."
// @Router   /admin/log [post]
func (h *logHandler) SetLogLevel(w http.ResponseWriter, r *http.Request) {
	var level string
	data, err := io.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	err = json.Unmarshal(data, &level)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	err = h.svr.SetLogLevel(level)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	log.SetLevel(logutil.StringToZapLogLevel(level))

	h.rd.JSON(w, http.StatusOK, "The log level is updated.")
}
