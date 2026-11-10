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

	"github.com/tikv/pd/pkg/errs"
)

type responseTestError struct {
	Detail string `json:"detail"`
}

func (e *responseTestError) Error() string { return e.Detail }

type causeOnlyResponseError struct{ error }

// Cause exposes a legacy wrapper that has no Unwrap method.
func (e causeOnlyResponseError) Cause() error { return e.error }

// TestErrorResp checks response metadata through legacy and standard wrappers.
func TestErrorResp(t *testing.T) {
	plainErr := stderrors.New("request failed")
	normalizedErr := errs.ErrStrconvParseInt.FastGenByArgs("invalid value")
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
		{name: "normalized error", err: normalizedErr, status: http.StatusInternalServerError, message: normalizedErr.Error()},
		{name: "invalid input", err: errs.NewInvalidInputErr(plainErr), status: http.StatusBadRequest, code: "input", message: plainErr.Error(), data: `{}`},
		{name: "typed input", err: errs.NewInvalidInputErr(typeErr), status: http.StatusBadRequest, code: "input", message: typeErr.Error(), data: `{"detail":"invalid store ID"}`},
		{name: "annotated input", err: errors.Annotate(errs.NewInvalidInputErr(plainErr), "store"), status: http.StatusBadRequest, code: "input", message: "store: request failed", data: `{}`},
		{name: "wrapped input", err: fmt.Errorf("store: %w", errs.NewInvalidInputErr(plainErr)), status: http.StatusBadRequest, code: "input", message: "store: request failed", data: `{}`},
		{name: "cause input", err: causeOnlyResponseError{errs.NewInvalidInputErr(plainErr)}, status: http.StatusBadRequest, code: "input", message: plainErr.Error(), data: `{}`},
		{name: "mixed input chain", err: fmt.Errorf("store: %w", causeOnlyResponseError{errs.NewInvalidInputErr(typeErr)}), status: http.StatusBadRequest, code: "input", message: "store: invalid store ID", data: `{"detail":"invalid store ID"}`},
		{name: "repeated typed input", err: errs.NewInvalidInputErr(errs.NewInvalidInputErr(typeErr)), status: http.StatusBadRequest, code: "input", message: typeErr.Error(), data: `{"detail":"invalid store ID"}`},
		{name: "repeated typed internal", err: errs.NewInternalErr(errs.NewInternalErr(typeErr)), status: http.StatusInternalServerError, code: "internal", message: typeErr.Error(), data: `{"detail":"invalid store ID"}`, stack: true},
		{name: "cause internal", err: causeOnlyResponseError{errs.NewInternalErr(plainErr)}, status: http.StatusInternalServerError, code: "internal", message: plainErr.Error(), data: `{}`, stack: true},
		{name: "internal", err: errs.NewInternalErr(plainErr), status: http.StatusInternalServerError, code: "internal", message: plainErr.Error(), data: `{}`, stack: true},
		{name: "wrapped internal", err: errors.Annotate(errs.NewInternalErr(plainErr), "config"), status: http.StatusInternalServerError, code: "internal", message: "config: request failed", data: `{}`, stack: true},
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

// TestInternalErrorPreservesStack prevents wrapping from replacing the cause's trace.
func TestInternalErrorPreservesStack(t *testing.T) {
	re := require.New(t)
	original := errors.New("storage failed")
	wrapped := errs.NewInternalErr(original)
	re.Implements((*errors.StackTracer)(nil), wrapped)
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

// These fixtures retain the legacy Code() signature without importing errcode.
type legacyResponseCode struct {
	name   legacyResponseCodeStr
	status int
}

type legacyResponseCodeStr string

func (c legacyResponseCode) CodeStr() legacyResponseCodeStr { return c.name }
func (c legacyResponseCode) HTTPCode() int                  { return c.status }

type legacyResponseError struct {
	code      legacyResponseCode
	data      any
	cause     error
	operation string
}

func (*legacyResponseError) Error() string              { return "request failed" }
func (e *legacyResponseError) Code() legacyResponseCode { return e.code }
func (e *legacyResponseError) GetClientData() any       { return e.data }
func (e *legacyResponseError) Cause() error             { return e.cause }
func (e *legacyResponseError) GetOperation() string     { return e.operation }

type legacyResponseGroup struct {
	first *legacyResponseError
	other *legacyResponseError
}

func (e legacyResponseGroup) Error() string {
	return e.first.Error() + "; " + e.other.Error()
}
func (e legacyResponseGroup) Code() legacyResponseCode { return e.first.Code() }
func (e legacyResponseGroup) Cause() error             { return e.first }
func (e legacyResponseGroup) GetClientData() any       { return e.first.GetClientData() }
func (e legacyResponseGroup) Errors() []error          { return []error{e.first, e.other} }

type operationResponseData struct {
	Detail string `json:"detail"`
}

func (operationResponseData) GetOperation() string { return "update store" }

// TestLegacyErrorResp checks legacy values without restoring the errcode dependency.
func TestLegacyErrorResp(t *testing.T) {
	typed := &responseTestError{Detail: "invalid store ID"}
	legacy := func(code string, status int) *legacyResponseError {
		return &legacyResponseError{code: legacyResponseCode{legacyResponseCodeStr(code), status}, data: typed}
	}
	input := legacy("input", http.StatusBadRequest)
	missing := legacy("missing", http.StatusNotFound)
	internal := legacy("internal.unimplemented", http.StatusNotImplemented)
	internal.cause = errors.New("storage failed")
	serverState := legacy("state.unavailable", http.StatusServiceUnavailable)
	serverState.cause = internal.cause
	withOperation := legacy("input", http.StatusBadRequest)
	withOperation.operation = "update store"
	withDataOperation := legacy("input", http.StatusBadRequest)
	withDataOperation.data = operationResponseData{Detail: typed.Detail}

	for _, tc := range []struct {
		name      string
		err       error
		status    int
		code      string
		message   string
		operation string
		stack     bool
		others    []string
	}{
		{name: "input", err: input, status: http.StatusBadRequest, code: "input"},
		{name: "missing", err: missing, status: http.StatusNotFound, code: "missing"},
		{name: "unauthenticated", err: legacy("auth.unauthenticated", http.StatusUnauthorized), status: http.StatusUnauthorized, code: "auth.unauthenticated"},
		{name: "forbidden", err: legacy("auth.forbidden", http.StatusForbidden), status: http.StatusForbidden, code: "auth.forbidden"},
		{name: "conflict", err: legacy("state.exists", http.StatusConflict), status: http.StatusConflict, code: "state.exists"},
		{name: "custom", err: legacy("quota.exceeded", http.StatusTooManyRequests), status: http.StatusTooManyRequests, code: "quota.exceeded"},
		{name: "internal child", err: internal, status: http.StatusNotImplemented, code: "internal.unimplemented", stack: true},
		{name: "non-internal server error", err: serverState, status: http.StatusServiceUnavailable, code: "state.unavailable"},
		{name: "cause", err: causeOnlyResponseError{input}, status: http.StatusBadRequest, code: "input"},
		{name: "mixed chain", err: fmt.Errorf("store: %w", causeOnlyResponseError{input}), status: http.StatusBadRequest, code: "input", message: "store: request failed"},
		{name: "operation", err: withOperation, status: http.StatusBadRequest, code: "input", operation: "update store"},
		{name: "data operation", err: withDataOperation, status: http.StatusBadRequest, code: "input", operation: "update store"},
		{name: "preserve custom input code", err: errs.NewInvalidInputErr(missing), status: http.StatusNotFound, code: "missing"},
		{name: "preserve internal child", err: errs.NewInternalErr(internal), status: http.StatusNotImplemented, code: "internal.unimplemented", stack: true},
		{name: "group", err: legacyResponseGroup{input, missing}, status: http.StatusBadRequest, code: "input", message: "request failed; request failed", others: []string{"missing"}},
		{name: "overridden code", err: errs.NewInternalErr(input), status: http.StatusInternalServerError, code: "internal", message: "request failed; request failed; request failed", stack: true, others: []string{"internal", "input"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			response := httptest.NewRecorder()
			ErrorResp(render.New(), response, tc.err)
			re.Equal(tc.status, response.Code)
			re.Equal(tc.code, response.Header().Get("TiDB-Error-Code"))
			var body struct {
				Code      string                  `json:"code"`
				Msg       string                  `json:"msg"`
				Data      json.RawMessage         `json:"data"`
				Operation string                  `json:"operation"`
				Stack     []uintptr               `json:"stack"`
				Others    []struct{ Code string } `json:"others"`
			}
			re.NoError(json.Unmarshal(response.Body.Bytes(), &body))
			re.Equal(tc.code, body.Code)
			message := tc.message
			if message == "" {
				message = tc.err.Error()
			}
			re.Equal(message, body.Msg)
			re.JSONEq(`{"detail":"invalid store ID"}`, string(body.Data))
			re.Equal(tc.operation, body.Operation)
			re.Equal(tc.stack, len(body.Stack) > 0)
			var codes []string
			for _, other := range body.Others {
				codes = append(codes, other.Code)
			}
			re.Equal(tc.others, codes)
		})
	}
}

type legacyResponseMembers struct {
	*legacyResponseError
	members []error
}

func (e *legacyResponseMembers) Errors() []error { return e.members }

// TestLegacyErrorGroupMembers protects empty groups and the primary member's slot.
func TestLegacyErrorGroupMembers(t *testing.T) {
	data := &responseTestError{Detail: "invalid store ID"}
	input := &legacyResponseError{code: legacyResponseCode{"input", http.StatusBadRequest}, data: data}
	missing := &legacyResponseError{code: legacyResponseCode{"missing", http.StatusNotFound}, data: data}
	plain := stderrors.New("uncoded member")
	for _, tc := range []struct {
		name    string
		members []error
		others  []string
	}{
		{name: "empty"},
		{name: "nil member", members: []error{nil}},
		{name: "uncoded primary", members: []error{plain}},
		{name: "all uncoded", members: []error{plain, plain}},
		{name: "single coded", members: []error{input}},
		{name: "uncoded before coded", members: []error{plain, missing}, others: []string{"missing"}},
		{name: "nil before coded", members: []error{nil, missing}, others: []string{"missing"}},
		{name: "uncoded between coded", members: []error{input, plain, missing}, others: []string{"missing"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			response := httptest.NewRecorder()
			group := &legacyResponseMembers{legacyResponseError: input, members: tc.members}
			re.NotPanics(func() { ErrorResp(render.New(), response, group) })
			re.Equal(http.StatusBadRequest, response.Code)
			re.Equal("input", response.Header().Get("TiDB-Error-Code"))
			var body struct {
				Code   string                  `json:"code"`
				Msg    string                  `json:"msg"`
				Data   json.RawMessage         `json:"data"`
				Others []struct{ Code string } `json:"others"`
			}
			re.NoError(json.Unmarshal(response.Body.Bytes(), &body))
			re.Equal("input", body.Code)
			re.Equal(group.Error(), body.Msg)
			re.JSONEq(`{"detail":"invalid store ID"}`, string(body.Data))
			var codes []string
			for _, other := range body.Others {
				codes = append(codes, other.Code)
			}
			re.Equal(tc.others, codes)
		})
	}
}

type responseErrorGroup struct{ members []error }

func (*responseErrorGroup) Error() string     { return "multiple errors" }
func (e *responseErrorGroup) Errors() []error { return e.members }

type nativeResponseError struct {
	*responseTestError
	code   string
	status int
}

func (e *nativeResponseError) Code() string       { return e.code }
func (e *nativeResponseError) HTTPCode() int      { return e.status }
func (e *nativeResponseError) GetClientData() any { return e.responseTestError }

// TestResponseErrorGroups checks grouped lookups, public HTTPError values, and stack reuse.
func TestResponseErrorGroups(t *testing.T) {
	data := &responseTestError{Detail: "invalid store ID"}
	input := &legacyResponseError{code: legacyResponseCode{"input", http.StatusBadRequest}, data: data}
	missing := &legacyResponseError{code: legacyResponseCode{"missing", http.StatusNotFound}, data: data}
	plain := stderrors.New("uncoded member")
	for _, tc := range []struct {
		name    string
		members []error
		status  int
		code    string
	}{
		{name: "empty", status: http.StatusInternalServerError},
		{name: "uncoded", members: []error{plain}, status: http.StatusInternalServerError},
		{name: "single coded", members: []error{input}, status: http.StatusBadRequest, code: "input"},
		{name: "multiple coded", members: []error{input, missing}, status: http.StatusBadRequest, code: "input"},
		{name: "mixed", members: []error{plain, input, missing}, status: http.StatusBadRequest, code: "input"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			group := &responseErrorGroup{members: tc.members}
			response := httptest.NewRecorder()
			ErrorResp(render.New(), response, group)
			re.Equal(tc.status, response.Code)
			re.Equal(tc.code, response.Header().Get("TiDB-Error-Code"))
			if tc.code == "" {
				var message string
				re.NoError(json.Unmarshal(response.Body.Bytes(), &message))
				re.Equal(group.Error(), message)
				return
			}
			var body map[string]json.RawMessage
			re.NoError(json.Unmarshal(response.Body.Bytes(), &body))
			re.JSONEq(`"input"`, string(body["code"]))
			re.JSONEq(`"multiple errors"`, string(body["msg"]))
			re.JSONEq(`{"detail":"invalid store ID"}`, string(body["data"]))
			re.Len(body, 3)
		})
	}
	t.Run("public metadata", func(t *testing.T) {
		re := require.New(t)
		original := &nativeResponseError{responseTestError: data, code: "quota.exceeded", status: http.StatusTooManyRequests}
		wrapped := errs.NewInvalidInputErr(original)
		response := httptest.NewRecorder()
		ErrorResp(render.New(), response, wrapped)
		re.Equal(http.StatusTooManyRequests, response.Code)
		re.Equal("quota.exceeded", response.Header().Get("TiDB-Error-Code"))
		re.Equal(data, errs.FindHTTPError(wrapped).GetClientData())
	})
	t.Run("grouped stack", func(t *testing.T) {
		re := require.New(t)
		traced := errors.New("storage failed")
		group := &responseErrorGroup{members: []error{plain, traced}}
		wrapped := errs.NewInternalErr(group)
		re.Equal(errors.GetStackTracer(traced).StackTrace(), errs.GetStackTracer(wrapped).StackTrace())
		re.Nil(errs.GetStackTracer(&responseErrorGroup{members: []error{plain}}))
	})
}
