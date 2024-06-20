// Copyright 2016 TiKV Project Authors.
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
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/server"
	"github.com/unrolled/render"
)

type operatorHandler struct {
	*server.Handler
	r *render.Render
}

func newOperatorHandler(handler *server.Handler, r *render.Render) *operatorHandler {
	return &operatorHandler{
		Handler: handler,
		r:       r,
	}
}

// @Tags     operator
// @Summary  Get a Region's pending operator.
// @Param    region_id  path  int  true  "A Region's Id"
// @Produce  json
// @Success  200  {object}  operator.OpWithStatus
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /operators/{region_id} [get]
func (h *operatorHandler) GetOperatorsByRegion(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["region_id"]

	regionID, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		h.r.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	op, err := h.GetOperatorStatus(regionID)
	if err != nil {
		h.r.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.r.JSON(w, http.StatusOK, op)
}

// @Tags     operator
// @Summary  List pending operators.
// @Param    kind   query  string  false  "Specify the operator kind."  Enums(admin, leader, region)
// @Param    object query  bool    false  "Whether to return as JSON object."
// @Produce  json
// @Success  200  {array}   operator.Operator
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /operators [get]
func (h *operatorHandler) GetOperators(w http.ResponseWriter, r *http.Request) {
	var (
		results []*operator.Operator
		err     error
	)

	kinds, ok := r.URL.Query()["kind"]
	_, objectFlag := r.URL.Query()["object"]
	if !ok {
		results, err = h.Handler.GetOperators()
	} else {
		results, err = h.Handler.GetOperatorsByKinds(kinds)
	}

	if err != nil {
		h.r.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	if objectFlag {
		objResults := make([]*operator.OpObject, len(results))
		for i, op := range results {
			objResults[i] = op.ToJSONObject()
		}
		h.r.JSON(w, http.StatusOK, objResults)
	} else {
		h.r.JSON(w, http.StatusOK, results)
	}
}

// @Tags     operator
// @Summary  Cancel all pending operators.
// @Produce  json
// @Success  200  {string}  string  "All pending operators are canceled."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /operators [delete]
func (h *operatorHandler) DeleteOperators(w http.ResponseWriter, _ *http.Request) {
	if err := h.RemoveOperators(); err != nil {
		h.r.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.r.JSON(w, http.StatusOK, "All pending operators are canceled.")
}

// FIXME: details of input json body params
// @Tags     operator
// @Summary  Create an operator.
// @Accept   json
// @Param    body  body  object  true  "json params"
// @Produce  json
// @Success  200  {string}  string  "The operator is created."
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /operators [post]
func (h *operatorHandler) CreateOperator(w http.ResponseWriter, r *http.Request) {
	var input map[string]any
	if err := apiutil.ReadJSONRespondError(h.r, w, r.Body, &input); err != nil {
		return
	}

	statusCode, result, err := h.HandleOperatorCreation(input)
	if err != nil {
		h.r.JSON(w, statusCode, err.Error())
		return
	}
	if statusCode == http.StatusOK && result == nil {
		h.r.JSON(w, http.StatusOK, "The operator is created.")
		return
	}
	h.r.JSON(w, statusCode, result)
}

// @Tags     operator
// @Summary  Cancel a Region's pending operator.
// @Param    region_id  path  int  true  "A Region's Id"
// @Produce  json
// @Success  200  {string}  string  "The pending operator is canceled."
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /operators/{region_id} [delete]
func (h *operatorHandler) DeleteOperatorByRegion(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["region_id"]

	regionID, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		h.r.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	if err = h.RemoveOperator(regionID); err != nil {
		h.r.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.r.JSON(w, http.StatusOK, "The pending operator is canceled.")
}

// @Tags     operator
// @Summary  lists the finished operators since the given timestamp in second.
// @Param    from  query  integer  false  "From Unix timestamp"
// @Produce  json
// @Success  200  {object}  []operator.OpRecord
// @Failure  400  {string}  string  "The request is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /operators/records [get]
func (h *operatorHandler) GetOperatorRecords(w http.ResponseWriter, r *http.Request) {
	var (
		from time.Time
		err  error
	)
	if froms := r.URL.Query()["from"]; len(froms) > 0 {
		from, err = apiutil.ParseTime(froms[0])
		if err != nil {
			h.r.JSON(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	records, err := h.GetRecords(from)
	if err != nil {
		h.r.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.r.JSON(w, http.StatusOK, records)
}
