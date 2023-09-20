// Copyright 2023 TiKV Project Authors.
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

//go:build !with_plugin
// +build !with_plugin

package api

import (
	"net/http"

	"github.com/tikv/pd/server"
	"github.com/unrolled/render"
)

type pluginHandler struct{}

func newPluginHandler(_ *server.Handler, _ *render.Render) *pluginHandler {
	return &pluginHandler{}
}

func (h *pluginHandler) LoadPlugin(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
	w.Write([]byte("load plugin is disabled, please `make pd-server-plugin` first"))
}

func (h *pluginHandler) UnloadPlugin(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
	w.Write([]byte("unload plugin is disabled, please `make pd-server-plugin` first"))
}
