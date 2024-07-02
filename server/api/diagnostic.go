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

	"github.com/gorilla/mux"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/server"
	"github.com/unrolled/render"
	"go.uber.org/zap"
)

type diagnosticHandler struct {
	handler *server.Handler
	svr     *server.Server
	rd      *render.Render
}

func newDiagnosticHandler(svr *server.Server, rd *render.Render) *diagnosticHandler {
	return &diagnosticHandler{
		handler: svr.GetHandler(),
		svr:     svr,
		rd:      rd,
	}
}

func (h *diagnosticHandler) GetDiagnosticResult(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]
	schedulerName, err := config.ConvertSchedulerStr2Name(name)
	if err != nil {
		log.Error("failed to convert scheduler name",
			zap.String("scheduler", name),
			errs.ZapError(err))
		h.rd.JSON(w, http.StatusNotFound, errs.ErrSchedulerNotFound.GenWithStackByArgs().Error())
		return
	}
	result, err := h.handler.GetDiagnosticResult(schedulerName)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, result)
}
