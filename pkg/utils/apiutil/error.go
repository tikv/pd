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
	"github.com/pingcap/errors"

	"github.com/tikv/pd/pkg/errs"
)

type httpErrorResponse struct {
	Code      string              `json:"code"`
	Msg       string              `json:"msg"`
	Data      any                 `json:"data"`
	Operation string              `json:"operation,omitempty"`
	Stack     errors.StackTrace   `json:"stack,omitempty"`
	Others    []httpErrorResponse `json:"others,omitempty"`
}

// newHTTPErrorResponse preserves the legacy JSON fields and additional-error order.
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
		if len(items) > 1 {
			for _, item := range items[1:] {
				if other := errs.AsHTTPError(item); other != nil {
					response.Others = append(response.Others, newHTTPErrorResponse(other))
				}
			}
		}
	}
	return response
}
