// Copyright 2026 TiKV Project Authors.
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

package apiutil

import (
	"net/http"

	"github.com/unrolled/render"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
)

// ErrorResp writes a structured response for HTTP errors, including legacy
// errcode.ErrorCode values. Other errors produce HTTP status 500 and a JSON string.
// A nil error also produces HTTP status 500 and is logged at the error level.
func ErrorResp(rd *render.Render, w http.ResponseWriter, err error) {
	if err == nil {
		log.Error("nil is given to errorResp")
		rd.JSON(w, http.StatusInternalServerError, "nil error")
		return
	}
	httpErr := errs.FindHTTPError(err)
	if httpErr == nil {
		rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("TiDB-Error-Code", httpErr.Code())
	rd.JSON(w, httpErr.HTTPCode(), newHTTPErrorResponse(httpErr))
}

type httpErrorResponse struct {
	Code      string              `json:"code"`
	Msg       string              `json:"msg"`
	Data      any                 `json:"data"`
	Operation string              `json:"operation,omitempty"`
	Stack     errors.StackTrace   `json:"stack,omitempty"`
	Others    []httpErrorResponse `json:"others,omitempty"`
}

func newHTTPErrorResponse(err errs.HTTPError) httpErrorResponse {
	response := httpErrorResponse{Code: err.Code(), Msg: err.Error(), Data: err.GetClientData()}
	for _, value := range []any{err, response.Data} {
		if operation, ok := value.(interface{ GetOperation() string }); ok {
			response.Operation = operation.GetOperation()
			if response.Operation != "" {
				break
			}
		}
	}
	if errs.IsInternalHTTPError(err) {
		if tracer := errs.GetStackTracer(err); tracer != nil {
			response.Stack = tracer.StackTrace()
		}
	}
	if group, ok := err.(errors.ErrorGroup); ok {
		items := group.Errors()
		for _, item := range items[1:] {
			if other, ok := item.(errs.HTTPError); ok {
				response.Others = append(response.Others, newHTTPErrorResponse(other))
			}
		}
	}
	return response
}
