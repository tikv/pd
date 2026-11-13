// Copyright 2022 TiKV Project Authors.
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
	"bytes"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unrolled/render"
	"go.uber.org/goleak"

	"github.com/pingcap/errors"

	"github.com/tikv/pd/pkg/errs"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestJsonRespondErrorOk(t *testing.T) {
	re := require.New(t)
	rd := render.New(render.Options{
		IndentJSON: true,
	})
	response := httptest.NewRecorder()
	body := io.NopCloser(bytes.NewBufferString("{\"zone\":\"cn\", \"host\":\"local\"}"))
	var input map[string]string
	output := map[string]string{"zone": "cn", "host": "local"}
	err := ReadJSONRespondError(rd, response, body, &input)
	re.NoError(err)
	re.Equal(output["zone"], input["zone"])
	re.Equal(output["host"], input["host"])
	result := response.Result()
	defer result.Body.Close()
	re.Equal(200, result.StatusCode)
}

func TestJsonRespondErrorBadInput(t *testing.T) {
	for _, tc := range []struct {
		name    string
		body    string
		message string
	}{
		{name: "type mismatch", body: `{"zone":"cn","host":"local"}`, message: "json: cannot unmarshal object into Go value of type []string"},
		{name: "invalid syntax", body: `{"zone":"cn",`, message: "unexpected end of JSON input"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			response := httptest.NewRecorder()
			var input []string
			err := ReadJSONRespondError(render.New(), response, io.NopCloser(bytes.NewBufferString(tc.body)), &input)
			re.EqualError(err, tc.message)
			re.Equal(http.StatusBadRequest, response.Code)
			re.Equal("input", response.Header().Get("TiDB-Error-Code"))
			var body struct {
				Code string          `json:"code"`
				Msg  string          `json:"msg"`
				Data json.RawMessage `json:"data"`
			}
			re.NoError(json.Unmarshal(response.Body.Bytes(), &body))
			re.Equal("input", body.Code)
			re.Equal(tc.message, body.Msg)
			re.NotEmpty(body.Data)
		})
	}
}

func TestGetIPPortFromHTTPRequest(t *testing.T) {
	re := require.New(t)

	testCases := []struct {
		r    *http.Request
		ip   string
		port string
		err  error
	}{
		// IPv4 "X-Forwarded-For" with port
		{
			r: &http.Request{
				Header: map[string][]string{
					XForwardedForHeader: {"127.0.0.1:5299"},
				},
			},
			ip:   "127.0.0.1",
			port: "5299",
		},
		// IPv4 "X-Forwarded-For" without port
		{
			r: &http.Request{
				Header: map[string][]string{
					XForwardedForHeader:  {"127.0.0.1"},
					XForwardedPortHeader: {"5299"},
				},
			},
			ip:   "127.0.0.1",
			port: "5299",
		},
		// IPv4 "X-Real-Ip" with port
		{
			r: &http.Request{
				Header: map[string][]string{
					XRealIPHeader: {"127.0.0.1:5299"},
				},
			},
			ip:   "127.0.0.1",
			port: "5299",
		},
		// IPv4 "X-Real-Ip" without port
		{
			r: &http.Request{
				Header: map[string][]string{
					XForwardedForHeader:  {"127.0.0.1"},
					XForwardedPortHeader: {"5299"},
				},
			},
			ip:   "127.0.0.1",
			port: "5299",
		},
		// IPv4 RemoteAddr with port
		{
			r: &http.Request{
				RemoteAddr: "127.0.0.1:5299",
			},
			ip:   "127.0.0.1",
			port: "5299",
		},
		// IPv4 RemoteAddr without port
		{
			r: &http.Request{
				RemoteAddr: "127.0.0.1",
			},
			ip:   "127.0.0.1",
			port: "",
		},
		// IPv6 "X-Forwarded-For" with port
		{
			r: &http.Request{
				Header: map[string][]string{
					XForwardedForHeader: {"[::1]:5299"},
				},
			},
			ip:   "::1",
			port: "5299",
		},
		// IPv6 "X-Forwarded-For" without port
		{
			r: &http.Request{
				Header: map[string][]string{
					XForwardedForHeader: {"::1"},
				},
			},
			ip:   "::1",
			port: "",
		},
		// IPv6 "X-Real-Ip" with port
		{
			r: &http.Request{
				Header: map[string][]string{
					XRealIPHeader: {"[::1]:5299"},
				},
			},
			ip:   "::1",
			port: "5299",
		},
		// IPv6 "X-Real-Ip" without port
		{
			r: &http.Request{
				Header: map[string][]string{
					XForwardedForHeader: {"::1"},
				},
			},
			ip:   "::1",
			port: "",
		},
		// IPv6 RemoteAddr with port
		{
			r: &http.Request{
				RemoteAddr: "[::1]:5299",
			},
			ip:   "::1",
			port: "5299",
		},
		// IPv6 RemoteAddr without port
		{
			r: &http.Request{
				RemoteAddr: "::1",
			},
			ip:   "::1",
			port: "",
		},
		// Abnormal case
		{
			r:    &http.Request{},
			ip:   "",
			port: "",
		},
	}
	for idx, testCase := range testCases {
		ip, port := GetIPPortFromHTTPRequest(testCase.r)
		re.Equal(testCase.ip, ip, "case %d", idx)
		re.Equal(testCase.port, port, "case %d", idx)
	}
}

func TestParseHexKeys(t *testing.T) {
	re := require.New(t)
	// Test for hex format
	hexBytes := [][]byte{[]byte(""), []byte("67"), []byte("0001020304050607"), []byte("08090a0b0c0d0e0f"), []byte("f0f1f2f3f4f5f6f7")}
	parseKeys, err := ParseHexKeys("hex", hexBytes)
	re.NoError(err)
	expectedBytes := [][]byte{[]byte(""), []byte("g"), []byte("\x00\x01\x02\x03\x04\x05\x06\x07"), []byte("\x08\t\n\x0b\x0c\r\x0e\x0f"), []byte("\xf0\xf1\xf2\xf3\xf4\xf5\xf6\xf7")}
	re.Equal(expectedBytes, parseKeys)
	// Test for other format NOT hex
	hexBytes = [][]byte{[]byte("hello")}
	parseKeys, err = ParseHexKeys("other", hexBytes)
	re.NoError(err)
	re.Len(parseKeys, 1)
	re.Equal([]byte("hello"), parseKeys[0])
	// Test for wrong key
	hexBytes = [][]byte{[]byte("world")}
	parseKeys, err = ParseHexKeys("hex", hexBytes)
	re.Error(err)
	re.Len(parseKeys, 1)
	re.Equal([]byte("world"), parseKeys[0])
	// Test for the first key is not valid, but the second key is valid
	hexBytes = [][]byte{[]byte("world"), []byte("0001020304050607")}
	parseKeys, err = ParseHexKeys("hex", hexBytes)
	re.Error(err)
	re.Len(parseKeys, 2)
	re.Equal([]byte("world"), parseKeys[0])
	re.NotEqual([]byte("\x00\x01\x02\x03\x04\x05\x06\x07"), parseKeys[1])
	// Test for the first key is valid, but the second key is not valid
	hexBytes = [][]byte{[]byte("0001020304050607"), []byte("world")}
	parseKeys, err = ParseHexKeys("hex", hexBytes)
	re.Error(err)
	re.Len(parseKeys, 2)
	re.NotEqual([]byte("\x00\x01\x02\x03\x04\x05\x06\x07"), parseKeys[0])
	re.Equal([]byte("world"), parseKeys[1])
}

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

// Coded fixtures exercise response metadata without depending on errcode.
type codedResponseCode struct {
	name   codedResponseCodeStr
	status int
}

type codedResponseCodeStr string

func (c codedResponseCode) CodeStr() codedResponseCodeStr { return c.name }
func (c codedResponseCode) HTTPCode() int                 { return c.status }

type unrelatedResponseError struct{ code codedResponseCode }

func (unrelatedResponseError) Error() string             { return "request failed" }
func (e unrelatedResponseError) Code() codedResponseCode { return e.code }

// TestUnrelatedErrorCode keeps method lookalikes on the ordinary error path.
func TestUnrelatedErrorCode(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, 0} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			re := require.New(t)
			err := unrelatedResponseError{code: codedResponseCode{"quota.exceeded", status}}
			response := httptest.NewRecorder()
			re.NotPanics(func() { ErrorResp(render.New(), response, err) })
			re.Equal(http.StatusInternalServerError, response.Code)
			re.Empty(response.Header().Get("TiDB-Error-Code"))
			re.JSONEq(`"request failed"`, response.Body.String())
		})
	}
}

type codedResponseError struct {
	code      codedResponseCode
	data      any
	cause     error
	operation string
}

func (*codedResponseError) Error() string          { return "request failed" }
func (e *codedResponseError) Code() string         { return string(e.code.name) }
func (e *codedResponseError) HTTPCode() int        { return e.code.status }
func (e *codedResponseError) GetClientData() any   { return e.data }
func (e *codedResponseError) Cause() error         { return e.cause }
func (e *codedResponseError) GetOperation() string { return e.operation }

type codedResponseGroup struct {
	first *codedResponseError
	other *codedResponseError
}

func (e codedResponseGroup) Error() string {
	return e.first.Error() + "; " + e.other.Error()
}
func (e codedResponseGroup) Code() string       { return e.first.Code() }
func (e codedResponseGroup) HTTPCode() int      { return e.first.HTTPCode() }
func (e codedResponseGroup) Cause() error       { return e.first }
func (e codedResponseGroup) GetClientData() any { return e.first.GetClientData() }
func (e codedResponseGroup) Errors() []error    { return []error{e.first, e.other} }

type operationResponseData struct {
	Detail string `json:"detail"`
}

func (operationResponseData) GetOperation() string { return "update store" }

// TestCodedErrorResp checks native codes, metadata, and constructor behavior.
func TestCodedErrorResp(t *testing.T) {
	typed := &responseTestError{Detail: "invalid store ID"}
	coded := func(code string, status int) *codedResponseError {
		return &codedResponseError{code: codedResponseCode{codedResponseCodeStr(code), status}, data: typed}
	}
	input := coded("input", http.StatusBadRequest)
	missing := coded("missing", http.StatusNotFound)
	internal := coded("internal.unimplemented", http.StatusNotImplemented)
	internal.cause = errors.New("storage failed")
	serverState := coded("state.unavailable", http.StatusServiceUnavailable)
	serverState.cause = internal.cause
	withOperation := coded("input", http.StatusBadRequest)
	withOperation.operation = "update store"
	withDataOperation := coded("input", http.StatusBadRequest)
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
		{name: "unauthenticated", err: coded("auth.unauthenticated", http.StatusUnauthorized), status: http.StatusUnauthorized, code: "auth.unauthenticated"},
		{name: "forbidden", err: coded("auth.forbidden", http.StatusForbidden), status: http.StatusForbidden, code: "auth.forbidden"},
		{name: "conflict", err: coded("state.exists", http.StatusConflict), status: http.StatusConflict, code: "state.exists"},
		{name: "custom", err: coded("quota.exceeded", http.StatusTooManyRequests), status: http.StatusTooManyRequests, code: "quota.exceeded"},
		{name: "internal child", err: internal, status: http.StatusNotImplemented, code: "internal.unimplemented", stack: true},
		{name: "non-internal server error", err: serverState, status: http.StatusServiceUnavailable, code: "state.unavailable"},
		{name: "cause", err: causeOnlyResponseError{input}, status: http.StatusBadRequest, code: "input"},
		{name: "mixed chain", err: fmt.Errorf("store: %w", causeOnlyResponseError{input}), status: http.StatusBadRequest, code: "input", message: "store: request failed"},
		{name: "operation", err: withOperation, status: http.StatusBadRequest, code: "input", operation: "update store"},
		{name: "data operation", err: withDataOperation, status: http.StatusBadRequest, code: "input", operation: "update store"},
		{name: "preserve custom input code", err: errs.NewInvalidInputErr(missing), status: http.StatusNotFound, code: "missing"},
		{name: "preserve internal child", err: errs.NewInternalErr(internal), status: http.StatusNotImplemented, code: "internal.unimplemented", stack: true},
		{name: "group", err: codedResponseGroup{input, missing}, status: http.StatusBadRequest, code: "input", message: "request failed; request failed", others: []string{"missing"}},
		{name: "annotated group", err: errors.Annotate(codedResponseGroup{input, missing}, "store"), status: http.StatusBadRequest, code: "input", message: "store: request failed; request failed", others: []string{"missing"}},
		{name: "wrapped group", err: fmt.Errorf("store: %w", codedResponseGroup{input, missing}), status: http.StatusBadRequest, code: "input", message: "store: request failed; request failed", others: []string{"missing"}},
		{name: "mixed group chain", err: fmt.Errorf("store: %w", errors.Annotate(codedResponseGroup{input, missing}, "update")), status: http.StatusBadRequest, code: "input", message: "store: update: request failed; request failed", others: []string{"missing"}},
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

type codedResponseMembers struct {
	*codedResponseError
	members []error
}

func (e *codedResponseMembers) Errors() []error { return e.members }

// TestCodedErrorGroupMembers protects empty groups and the primary member's slot.
func TestCodedErrorGroupMembers(t *testing.T) {
	data := &responseTestError{Detail: "invalid store ID"}
	input := &codedResponseError{code: codedResponseCode{"input", http.StatusBadRequest}, data: data}
	missing := &codedResponseError{code: codedResponseCode{"missing", http.StatusNotFound}, data: data}
	plain := stderrors.New("uncoded member")
	for _, tc := range []struct {
		name         string
		members      []error
		others       []string
		otherMessage string
	}{
		{name: "empty"},
		{name: "nil member", members: []error{nil}},
		{name: "uncoded primary", members: []error{plain}},
		{name: "all uncoded", members: []error{plain, plain}},
		{name: "single coded", members: []error{input}},
		{name: "uncoded before coded", members: []error{plain, missing}, others: []string{"missing"}},
		{name: "nil before coded", members: []error{nil, missing}, others: []string{"missing"}},
		{name: "uncoded between coded", members: []error{input, plain, missing}, others: []string{"missing"}},
		{name: "annotated member", members: []error{input, errors.Annotate(missing, "store")}, others: []string{"missing"}, otherMessage: "store: request failed"},
		{name: "wrapped member", members: []error{input, fmt.Errorf("store: %w", missing)}, others: []string{"missing"}, otherMessage: "store: request failed"},
		{name: "mixed member chain", members: []error{input, fmt.Errorf("store: %w", errors.Annotate(missing, "update"))}, others: []string{"missing"}, otherMessage: "store: update: request failed"},
		{name: "wrapped overridden member", members: []error{input, fmt.Errorf("store: %w", errs.NewInternalErr(missing))}, others: []string{"internal"}, otherMessage: "store: request failed; store: request failed; store: request failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			response := httptest.NewRecorder()
			group := &codedResponseMembers{codedResponseError: input, members: tc.members}
			re.NotPanics(func() { ErrorResp(render.New(), response, group) })
			re.Equal(http.StatusBadRequest, response.Code)
			re.Equal("input", response.Header().Get("TiDB-Error-Code"))
			var body struct {
				Code   string          `json:"code"`
				Msg    string          `json:"msg"`
				Data   json.RawMessage `json:"data"`
				Others []struct {
					Code string          `json:"code"`
					Msg  string          `json:"msg"`
					Data json.RawMessage `json:"data"`
				} `json:"others"`
			}
			re.NoError(json.Unmarshal(response.Body.Bytes(), &body))
			re.Equal("input", body.Code)
			re.Equal(group.Error(), body.Msg)
			re.JSONEq(`{"detail":"invalid store ID"}`, string(body.Data))
			var codes []string
			for _, other := range body.Others {
				codes = append(codes, other.Code)
				message := tc.otherMessage
				if message == "" {
					message = missing.Error()
				}
				re.Equal(message, other.Msg)
				re.JSONEq(`{"detail":"invalid store ID"}`, string(other.Data))
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

type sliceResponseError []string

func (sliceResponseError) Error() string        { return "invalid values" }
func (sliceResponseError) Code() string         { return "input" }
func (sliceResponseError) HTTPCode() int        { return http.StatusBadRequest }
func (e sliceResponseError) GetClientData() any { return []string(e) }

type structResponseError struct{ sliceResponseError }

type nativeResponseGroup struct {
	errs.HTTPError
	members []error
}

func (e nativeResponseGroup) Errors() []error { return e.members }

// TestNonComparableErrorResp preserves responses for slice and struct values.
func TestNonComparableErrorResp(t *testing.T) {
	value := sliceResponseError{"invalid store ID"}
	for _, tc := range []struct {
		name string
		err  errs.HTTPError
	}{
		{name: "slice", err: value},
		{name: "struct", err: structResponseError{value}},
	} {
		for _, wrapped := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/wrapped=%t", tc.name, wrapped), func(t *testing.T) {
				re := require.New(t)
				var err error = tc.err
				message := tc.err.Error()
				if wrapped {
					err = fmt.Errorf("store: %w", err)
					message = "store: " + message
				}
				response := httptest.NewRecorder()
				re.NotPanics(func() { ErrorResp(render.New(), response, err) })
				re.Equal(http.StatusBadRequest, response.Code)
				re.Equal("input", response.Header().Get("TiDB-Error-Code"))
				re.JSONEq(fmt.Sprintf(`{"code":"input","msg":%q,"data":["invalid store ID"]}`, message), response.Body.String())
			})
		}
		t.Run(tc.name+"/group", func(t *testing.T) {
			re := require.New(t)
			other := &nativeResponseError{responseTestError: &responseTestError{Detail: "not found"}, code: "missing", status: http.StatusNotFound}
			group := nativeResponseGroup{HTTPError: tc.err, members: []error{tc.err, other}}
			response := httptest.NewRecorder()
			re.NotPanics(func() { ErrorResp(render.New(), response, group) })
			re.Equal(http.StatusBadRequest, response.Code)
			re.Equal("input", response.Header().Get("TiDB-Error-Code"))
			re.JSONEq(`{"code":"input","msg":"invalid values","data":["invalid store ID"],"others":[{"code":"missing","msg":"not found","data":{"detail":"not found"}}]}`, response.Body.String())
		})
	}
}

// TestResponseErrorGroups checks grouped lookups, public HTTPError values, and stack reuse.
func TestResponseErrorGroups(t *testing.T) {
	data := &responseTestError{Detail: "invalid store ID"}
	input := &codedResponseError{code: codedResponseCode{"input", http.StatusBadRequest}, data: data}
	missing := &codedResponseError{code: codedResponseCode{"missing", http.StatusNotFound}, data: data}
	plain := stderrors.New("uncoded member")
	for _, tc := range []struct {
		name    string
		members []error
		status  int
		code    string
		others  []string
	}{
		{name: "empty", status: http.StatusInternalServerError},
		{name: "uncoded", members: []error{plain}, status: http.StatusInternalServerError},
		{name: "single coded", members: []error{input}, status: http.StatusBadRequest, code: "input"},
		{name: "multiple coded", members: []error{input, missing}, status: http.StatusBadRequest, code: "input", others: []string{"missing"}},
		{name: "mixed", members: []error{plain, input, missing}, status: http.StatusBadRequest, code: "input", others: []string{"missing"}},
		{name: "wrapped primary", members: []error{fmt.Errorf("store: %w", input), missing}, status: http.StatusBadRequest, code: "input", others: []string{"missing"}},
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
			if len(tc.others) == 0 {
				re.Len(body, 3)
				re.NotContains(body, "others")
			} else {
				re.Len(body, 4)
				var others []struct{ Code string }
				re.NoError(json.Unmarshal(body["others"], &others))
				var codes []string
				for _, other := range others {
					codes = append(codes, other.Code)
				}
				re.Equal(tc.others, codes)
			}
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
