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
	"testing"
	"testing/iotest"

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
		data    string
	}{
		{name: "type mismatch", body: `{"zone":"cn","host":"local"}`, message: "json: cannot unmarshal object into Go value of type []string", data: `{"Value":"object","Type":{},"Offset":1,"Struct":"","Field":"","Err":null}`},
		{name: "invalid syntax", body: `{"zone":"cn",`, message: "unexpected end of JSON input", data: `{"Offset":13}`},
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
			re.JSONEq(tc.data, string(body.Data))
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
		{name: "internal from input", err: errs.NewInternalErr(errs.NewInvalidInputErr(plainErr)), status: http.StatusInternalServerError, code: "internal", message: plainErr.Error(), data: `{}`, stack: true},
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

func TestJSONRespondReadError(t *testing.T) {
	re := require.New(t)
	response := httptest.NewRecorder()
	var input []string
	err := ReadJSONRespondError(render.New(), response, io.NopCloser(iotest.ErrReader(io.ErrUnexpectedEOF)), &input)
	re.ErrorIs(err, io.ErrUnexpectedEOF)
	re.Equal(http.StatusInternalServerError, response.Code)
	re.Equal("internal", response.Header().Get("TiDB-Error-Code"))
	var body struct {
		Code  string    `json:"code"`
		Msg   string    `json:"msg"`
		Stack []uintptr `json:"stack"`
	}
	re.NoError(json.Unmarshal(response.Body.Bytes(), &body))
	re.Equal("internal", body.Code)
	re.Equal(io.ErrUnexpectedEOF.Error(), body.Msg)
	re.NotEmpty(body.Stack)
}
