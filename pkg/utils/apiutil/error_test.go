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
	"encoding/json"
	stderrors "errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unrolled/render"

	"github.com/pingcap/errors"
)

type responseTestError struct {
	Detail string `json:"detail"`
}

func (e *responseTestError) Error() string { return e.Detail }

func TestErrorResp(t *testing.T) {
	plainErr := stderrors.New("request failed")
	typeErr := &responseTestError{Detail: "invalid store ID"}
	for _, tc := range []struct {
		name    string
		err     error
		status  int
		code    string
		message string
		data    string
		stack   bool
	}{
		{name: "nil", status: http.StatusInternalServerError, message: "nil error"},
		{name: "plain", err: plainErr, status: http.StatusInternalServerError, message: plainErr.Error()},
		{name: "invalid input", err: NewInvalidInputErr(plainErr), status: http.StatusBadRequest, code: "input", message: plainErr.Error(), data: `{}`},
		{name: "typed input", err: NewInvalidInputErr(typeErr), status: http.StatusBadRequest, code: "input", message: typeErr.Error(), data: `{"detail":"invalid store ID"}`},
		{name: "annotated input", err: errors.Annotate(NewInvalidInputErr(plainErr), "store"), status: http.StatusBadRequest, code: "input", message: "store: request failed", data: `{}`},
		{name: "wrapped input", err: fmt.Errorf("store: %w", NewInvalidInputErr(plainErr)), status: http.StatusBadRequest, code: "input", message: "store: request failed", data: `{}`},
		{name: "internal", err: NewInternalErr(plainErr), status: http.StatusInternalServerError, code: "internal", message: plainErr.Error(), data: `{}`, stack: true},
		{name: "wrapped internal", err: errors.Annotate(NewInternalErr(plainErr), "config"), status: http.StatusInternalServerError, code: "internal", message: "config: request failed", data: `{}`, stack: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			response := httptest.NewRecorder()
			ErrorResp(render.New(), response, tc.err)
			re.Equal(tc.status, response.Code)
			re.Equal(tc.code, response.Header().Get("TiDB-Error-Code"))
			if tc.code == "" {
				var message string
				re.NoError(json.Unmarshal(response.Body.Bytes(), &message))
				re.Equal(tc.message, message)
				return
			}
			var body map[string]json.RawMessage
			re.NoError(json.Unmarshal(response.Body.Bytes(), &body))
			re.JSONEq(fmt.Sprintf("%q", tc.code), string(body["code"]))
			re.JSONEq(fmt.Sprintf("%q", tc.message), string(body["msg"]))
			re.JSONEq(tc.data, string(body["data"]))
			if tc.stack {
				var stack []uintptr
				re.NoError(json.Unmarshal(body["stack"], &stack))
				re.NotEmpty(stack)
				re.Len(body, 4)
			} else {
				re.NotContains(body, "stack")
				re.Len(body, 3)
			}
		})
	}
}

func TestInternalErrorPreservesStack(t *testing.T) {
	re := require.New(t)
	original := errors.New("storage failed")
	wrapped := NewInternalErr(original)
	re.ErrorIs(wrapped, original)
	re.Equal(original, errors.Cause(wrapped))
	response := httptest.NewRecorder()
	ErrorResp(render.New(), response, wrapped)
	var body struct {
		Stack json.RawMessage `json:"stack"`
	}
	re.NoError(json.Unmarshal(response.Body.Bytes(), &body))
	stack, err := json.Marshal(errors.GetStackTracer(original).StackTrace())
	re.NoError(err)
	re.JSONEq(string(stack), string(body.Stack))
}
