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

	"github.com/gorilla/mux"
	"github.com/unrolled/render"

	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/server"
)

type checkerHandler struct {
	*server.Handler
	r *render.Render
}

func newCheckerHandler(svr *server.Server, r *render.Render) *checkerHandler {
	return &checkerHandler{
		Handler: svr.GetHandler(),
		r:       r,
	}
}

// FIXME: details of input json body params
// @Tags     checker
// @Summary  Pause or resume region merge.
// @Accept   json
// @Param    name  path  string  true  "The name of the checker."
// @Param    body  body  object  true  "json params"
// @Produce  json
// @Success  200  {string}  string  "Pause or resume the scheduler successfully."
// @Failure  400  {string}  string  "Bad format request."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /checker/{name} [post]
func (c *checkerHandler) PauseOrResumeChecker(w http.ResponseWriter, r *http.Request) {
	var input map[string]int
	if err := apiutil.ReadJSONRespondError(c.r, w, r.Body, &input); err != nil {
		return
	}

	name := mux.Vars(r)["name"]
	t, ok := input["delay"]
	if !ok {
		c.r.JSON(w, http.StatusBadRequest, "missing pause time")
		return
	}
	if t < 0 {
		c.r.JSON(w, http.StatusBadRequest, "delay cannot be negative")
		return
	}
	if err := c.Handler.PauseOrResumeChecker(name, int64(t)); err != nil {
		c.r.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	if t == 0 {
		c.r.JSON(w, http.StatusOK, "Resume the checker successfully.")
	} else {
		c.r.JSON(w, http.StatusOK, "Pause the checker successfully.")
	}
}

// FIXME: details of input json body params
// @Tags     checker
// @Summary  Get if checker is paused
// @Param    name  path  string  true  "The name of the scheduler."
// @Produce  json
// @Success  200  {string}  string  "Pause or resume the scheduler successfully."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /checker/{name} [get]
func (c *checkerHandler) GetCheckerStatus(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]
	output, err := c.Handler.GetCheckerStatus(name)
	if err != nil {
		c.r.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	c.r.JSON(w, http.StatusOK, output)
}
