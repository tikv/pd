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

package tso

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/unrolled/render"
)

// Handler defines the common behaviors of a basic tso handler.
type Handler interface {
	ResetTS(ts uint64, ignoreSmaller, skipUpperBoundCheck bool, keyspaceGroupID uint32) error
}

// AdminHandler wrap the basic tso handler to provide http service.
type AdminHandler struct {
	handler Handler
	rd      *render.Render
}

// NewAdminHandler returns a new admin handler.
func NewAdminHandler(handler Handler, rd *render.Render) *AdminHandler {
	return &AdminHandler{
		handler: handler,
		rd:      rd,
	}
}

// ResetTS is the http.HandlerFunc of ResetTS
// FIXME: details of input json body params
// @Tags     admin
// @Summary  Reset the ts.
// @Accept   json
// @Param    body  body  object  true  "json params"
// @Produce  json
// @Success  200  {string}  string  "Reset ts successfully."
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  403  {string}  string  "Reset ts is forbidden."
// @Failure  500  {string}  string  "TSO server failed to proceed the request."
// @Failure  503  {string}  string  "It's a temporary failure, please retry."
// @Router   /admin/reset-ts [post]
// if force-use-larger=true:
//
//	reset ts to max(current ts, input ts).
//
// else:
//
//	reset ts to input ts if it > current ts and < upper bound, error if not in that range
//
// during EBS based restore, we call this to make sure ts of pd >= resolved_ts in backup.
func (h *AdminHandler) ResetTS(w http.ResponseWriter, r *http.Request) {
	handler := h.handler
	var input map[string]any
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &input); err != nil {
		return
	}
	tsValue, ok := input["tso"].(string)
	if !ok || len(tsValue) == 0 {
		h.rd.JSON(w, http.StatusBadRequest, "invalid tso value")
		return
	}
	ts, err := strconv.ParseUint(tsValue, 10, 64)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, "invalid tso value")
		return
	}

	forceUseLarger := false
	forceUseLargerVal, contains := input["force-use-larger"]
	if contains {
		if forceUseLarger, ok = forceUseLargerVal.(bool); !ok {
			h.rd.JSON(w, http.StatusBadRequest, "invalid force-use-larger value")
			return
		}
	}
	var ignoreSmaller, skipUpperBoundCheck bool
	if forceUseLarger {
		ignoreSmaller, skipUpperBoundCheck = true, true
	}

	if err = handler.ResetTS(ts, ignoreSmaller, skipUpperBoundCheck, 0); err != nil {
		if err == errs.ErrServerNotStarted {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		} else if err == errs.ErrEtcdTxnConflict {
			// If the error is ErrEtcdTxnConflict, it means there is a temporary failure.
			// Return 503 to let the client retry.
			// Ref: https://datatracker.ietf.org/doc/html/rfc7231#section-6.6.4
			h.rd.JSON(w, http.StatusServiceUnavailable,
				fmt.Sprintf("It's a temporary failure with error %s, please retry.", err.Error()))
		} else {
			h.rd.JSON(w, http.StatusForbidden, err.Error())
		}
		return
	}
	h.rd.JSON(w, http.StatusOK, "Reset ts successfully.")
}
