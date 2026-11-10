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

// ErrorResp writes a structured response for errors marked by errs.NewInvalidInputErr
// or errs.NewInternalErr. Other errors produce HTTP status 500 and a JSON string.
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
	var stack errors.StackTrace
	if httpErr.HTTPCode() >= http.StatusInternalServerError {
		if tracer := errs.GetStackTracer(err); tracer != nil {
			stack = tracer.StackTrace()
		}
	}
	w.Header().Set("TiDB-Error-Code", httpErr.Code())
	rd.JSON(w, httpErr.HTTPCode(), struct {
		Code  string            `json:"code"`
		Msg   string            `json:"msg"`
		Data  any               `json:"data"`
		Stack errors.StackTrace `json:"stack,omitempty"`
	}{
		Code:  httpErr.Code(),
		Msg:   err.Error(),
		Data:  httpErr.GetClientData(),
		Stack: stack,
	})
}
