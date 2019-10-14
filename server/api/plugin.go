// Copyright 2016 PingCAP, Inc.
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
	"net/http"

	"github.com/pingcap/pd/server"
	"github.com/unrolled/render"
)

type pluginHandler struct {
	*server.Handler
	rd *render.Render
}

func newPluginHandler(handler *server.Handler, rd *render.Render) *pluginHandler {
	return &pluginHandler{
		Handler: handler,
		rd:      rd,
	}
}

func (h *pluginHandler) LoadPlugin(w http.ResponseWriter, r *http.Request) {
	var input map[string]interface{}
	if err := readJSONRespondError(h.rd, w, r.Body, &input); err != nil {
		return
	}

	var err error
	err = h.PluginLoad(input["plugin-path"].(string))
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, nil)
}

func (h *pluginHandler) UpdatePlugin(w http.ResponseWriter, r *http.Request) {
	var input map[string]interface{}
	if err := readJSONRespondError(h.rd, w, r.Body, &input); err != nil {
		return
	}

	var err error
	err = h.PluginUpdate(input["plugin-path"].(string))
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, nil)
}

func (h *pluginHandler) UnloadPlugin(w http.ResponseWriter, r *http.Request) {
	var input map[string]interface{}
	if err := readJSONRespondError(h.rd, w, r.Body, &input); err != nil {
		return
	}

	var err error
	err = h.PluginUnload(input["plugin-path"].(string))
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, nil)
}
