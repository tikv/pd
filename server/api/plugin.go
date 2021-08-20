// Copyright 2019 TiKV Project Authors.
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
	"errors"
	"net/http"
	"os"

	"github.com/tikv/pd/pkg/apiutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/cluster"
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

// FIXME: details of input json body params
// @Tags plugin
// @Summary Load plugin.
// @Accept json
// @Param body body object true "json params"
// @Produce json
// @Success 200 {string} string "Load plugin success."
// @Failure 400 {string} string "The input is invalid."
// @Failure 500 {string} string "PD server failed to proceed the request."
// @Router /plugin [post]
func (h *pluginHandler) LoadPlugin(w http.ResponseWriter, r *http.Request) {
	h.processPluginCommand(w, r, cluster.PluginLoad)
}

// FIXME: details of input json body params
// @Tags plugin
// @Summary Unload plugin.
// @Accept json
// @Param body body object true "json params"
// @Produce json
// @Success 200 {string} string "Load/Unload plugin successfully."
// @Failure 400 {string} string "The input is invalid."
// @Failure 500 {string} string "PD server failed to proceed the request."
// @Router /plugin [delete]
func (h *pluginHandler) UnloadPlugin(w http.ResponseWriter, r *http.Request) {
	h.processPluginCommand(w, r, cluster.PluginUnload)
}

func (h *pluginHandler) processPluginCommand(w http.ResponseWriter, r *http.Request, action string) {
	data := make(map[string]string)
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &data); err != nil {
		return
	}
	path := data["plugin-path"]
	if exist, err := pathExists(path); !exist {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	var err error
	switch action {
	case cluster.PluginLoad:
		err = h.PluginLoad(path)
		if err != nil {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
		h.rd.JSON(w, http.StatusOK, "Load plugin successfully.")
	case cluster.PluginUnload:
		err = h.PluginUnload(path)
		if err != nil {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
		h.rd.JSON(w, http.StatusOK, "Unload plugin successfully.")
	default:
		h.rd.JSON(w, http.StatusBadRequest, "unknown action")
	}
}

func pathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		err = errors.New("file is not exists")
		return false, err
	}
	return false, err
}
