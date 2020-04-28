// Copyright 2020 PingCAP, Inc.
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

	"github.com/gorilla/mux"
	"github.com/pingcap/errcode"
	"github.com/pingcap/pd/v4/pkg/apiutil"
	"github.com/pingcap/pd/v4/server"
	"github.com/pkg/errors"
	"github.com/unrolled/render"
)

// Addresses is mapping from component to addresses.
type Addresses map[string][]string

type componentHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newComponentHandler(svr *server.Server, rd *render.Render) *componentHandler {
	return &componentHandler{
		svr: svr,
		rd:  rd,
	}
}

// @Tags component
// @Summary Register component address.
// @Produce json
// @Success 200 {string} string
// @Failure 400 {string} string "The input is invalid."
// @Failure 500 {string} string "PD server failed to proceed the request."
// @Router /component [post]
func (h *componentHandler) Register(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r.Context())
	input := make(map[string]string)
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &input); err != nil {
		return
	}
	component, ok := input["component"]
	if !ok {
		apiutil.ErrorResp(h.rd, w, errcode.NewInvalidInputErr(errors.New("not set component")))
		return
	}
	addr, ok := input["addr"]
	if !ok {
		apiutil.ErrorResp(h.rd, w, errcode.NewInvalidInputErr(errors.New("not set addr")))
		return
	}
	if err := rc.GetComponentManager().Register(component, addr); err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, nil)
}

// @Tags component
// @Summary Unregister component address.
// @Produce json
// @Success 200 {string} string
// @Failure 400 {string} string "The input is invalid."
// @Router /component [delete]
func (h *componentHandler) UnRegister(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r.Context())
	vars := mux.Vars(r)
	component := vars["component"]
	addr := vars["addr"]
	if err := rc.GetComponentManager().UnRegister(component, addr); err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, nil)
}

// @Tags component
// @Summary List all component addresses
// @Produce json
// @Success 200 {object} Addresses
// @Router /component [get]
func (h *componentHandler) GetAllAddress(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r.Context())
	addrs := rc.GetComponentManager().GetAllComponentAddrs()
	h.rd.JSON(w, http.StatusOK, addrs)
}

// @Tags component
// @Summary List component addresses
// @Produce json
// @Success 200 {array} string
// @Failure 404 {string} string "The component does not exist."
// @Router /component/{type} [get]
func (h *componentHandler) GetAddress(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r.Context())
	vars := mux.Vars(r)
	component := vars["type"]
	addrs := rc.GetComponentManager().GetComponentAddrs(component)

	if len(addrs) == 0 {
		h.rd.JSON(w, http.StatusNotFound, "component not found")
		return
	}
	h.rd.JSON(w, http.StatusOK, addrs)
}
