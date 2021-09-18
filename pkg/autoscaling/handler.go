// Copyright 2020 TiKV Project Authors.
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

package autoscaling

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server"
	"github.com/unrolled/render"
	"go.uber.org/zap"
)

// HTTPHandler is a handler to handle the auto scaling HTTP request.
type HTTPHandler struct {
	svr *server.Server
	rd  *render.Render
}

// NewHTTPHandler creates a HTTPHandler.
func NewHTTPHandler(svr *server.Server, rd *render.Render) *HTTPHandler {
	return &HTTPHandler{
		svr: svr,
		rd:  rd,
	}
}

func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rc := h.svr.GetRaftCluster()
	if rc == nil {
		h.rd.JSON(w, http.StatusInternalServerError, errs.ErrNotBootstrapped.FastGenByArgs().Error())
		return
	}
	data, err := io.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	log.Debug("get http body completed", zap.String("strategy", string(data)))

	strategy := Strategy{}
	if err = json.Unmarshal(data, &strategy); err != nil {
		log.Error("unmarshall strategy failed", errs.ZapError(err))
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	plans, err := calculate(rc, &strategy)
	if err != nil {
		log.Error("calculate plans failed", errs.ZapError(err))
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	data, err = json.Marshal(plans)
	if err != nil {
		log.Error("marshal plans failed", errs.ZapError(err))
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	} else {
		log.Debug("marshal plans completed", zap.String("plans", string(data)))
		h.rd.JSON(w, http.StatusOK, plans)
	}
}
