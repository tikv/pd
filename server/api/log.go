// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/pingcap/log"
	"github.com/pingcap/pd/v4/pkg/logutil"
	"github.com/pingcap/pd/v4/server"
	"github.com/unrolled/render"
)

type logHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newlogHandler(svr *server.Server, rd *render.Render) *logHandler {
	return &logHandler{
		svr: svr,
		rd:  rd,
	}
}

func (h *logHandler) Handle(w http.ResponseWriter, r *http.Request) {
	if h.svr.GetConfig().EnableDynamicConfig {
		cm := h.svr.GetConfigManager()
		var str string
		json.NewDecoder(r.Body).Decode(&str)
		entries := []*entry{{key: "log.level", value: fmt.Sprintf("level = \"%v\"", str)}}
		client := h.svr.GetConfigClient()
		if client == nil {
			h.rd.JSON(w, http.StatusServiceUnavailable, "no leader")
			return
		}
		err := redirectUpdateReq(h.svr.Context(), client, cm, entries)
		if err != nil {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
		h.rd.JSON(w, http.StatusOK, nil)
		return
	}
	var level string
	data, err := ioutil.ReadAll(r.Body)
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

	h.svr.SetLogLevel(level)
	log.SetLevel(logutil.StringToZapLogLevel(level))

	h.rd.JSON(w, http.StatusOK, nil)
}
