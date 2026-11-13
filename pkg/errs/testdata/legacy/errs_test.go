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

package legacy_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unrolled/render"

	"github.com/pingcap/errcode"
	"github.com/pingcap/errors"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/apiutil"
)

type clientError struct {
	Detail string `json:"detail"`
}

func (e *clientError) Error() string { return e.Detail }

type operationError struct{ errcode.CodedError }

func (operationError) GetOperation() string { return "update store" }

func TestLegacyHTTPError(t *testing.T) {
	data := &clientError{Detail: "store limit reached"}
	quota := errcode.NewCode("quota").SetHTTP(http.StatusTooManyRequests).Child("quota.exceeded")
	for _, code := range []errcode.Code{errcode.InvalidInputCode, quota, errcode.UnimplementedCode} {
		t.Run(string(code.CodeStr()), func(t *testing.T) {
			re := require.New(t)
			original := operationError{errcode.NewCodedError(data, code)}
			_, native := any(original).(errs.HTTPError)
			re.False(native, "the real legacy Code() signature must use reflection")
			adapted := errs.AsHTTPError(original)
			re.NotNil(adapted)
			re.Equal(string(code.CodeStr()), adapted.Code())
			re.Equal(code.HTTPCode(), adapted.HTTPCode())
			re.Same(data, adapted.GetClientData())
			re.Equal(original.Error(), adapted.Error())
			operation, ok := adapted.(interface{ GetOperation() string })
			re.True(ok)
			re.Equal("update store", operation.GetOperation())
			for _, wrapped := range []error{errors.Annotate(original, "store"), fmt.Errorf("store: %w", original)} {
				found := errs.FindHTTPError(wrapped)
				re.NotNil(found)
				re.Equal(adapted.Code(), found.Code())
				re.Equal(adapted.HTTPCode(), found.HTTPCode())
				re.Same(data, found.GetClientData())
				re.Equal(wrapped.Error(), found.Error())
			}
		})
	}
}

func TestLegacyHTTPErrorGroup(t *testing.T) {
	re := require.New(t)
	data := &clientError{Detail: "invalid store"}
	input := errcode.NewInvalidInputErr(data)
	missing := errcode.NewNotFoundErr(data)
	group := errcode.Combine(input, missing)
	for _, original := range []error{group, errors.Annotate(group, "store"), fmt.Errorf("store: %w", group)} {
		found := errs.FindHTTPError(original)
		re.NotNil(found)
		re.Equal("input", found.Code())
		re.Equal(http.StatusBadRequest, found.HTTPCode())
		re.Equal(original.Error(), found.Error())
		re.Same(data, found.GetClientData())
		members := errors.Errors(found)
		re.Len(members, 2)
		re.Equal("input", errs.FindHTTPError(members[0]).Code())
		re.Equal("missing", errs.FindHTTPError(members[1]).Code())
	}
}

type mixedGroup struct {
	errs.HTTPError
	members []error
}

func (e mixedGroup) Errors() []error { return e.members }

type legacyGroup struct {
	errcode.ErrorCode
	members []error
}

func (e legacyGroup) Errors() []error { return e.members }

func (e legacyGroup) GetClientData() any { return errcode.ClientData(e.ErrorCode) }

type sliceError []string

func (sliceError) Error() string      { return "invalid values" }
func (sliceError) Code() errcode.Code { return errcode.InvalidInputCode }

func TestLegacyErrorResp(t *testing.T) {
	data := &clientError{Detail: "invalid store"}
	input := errcode.NewInvalidInputErr(data)
	missing := errcode.NewNotFoundErr(data)
	native := errs.FindHTTPError(errs.NewInvalidInputErr(data))
	group := errcode.Combine(input, missing)
	for _, tc := range []struct {
		name      string
		err       error
		status    int
		code      string
		data      string
		operation string
		stack     bool
		others    []string
	}{
		{name: "input", err: input, status: http.StatusBadRequest, code: "input"},
		{name: "not found", err: missing, status: http.StatusNotFound, code: "missing"},
		{name: "internal", err: errcode.NewInternalErr(data), status: http.StatusInternalServerError, code: "internal", stack: true},
		{name: "internal child", err: errcode.NewUnimplementedErr(data), status: http.StatusNotImplemented, code: "internal.unimplemented", stack: true},
		{name: "operation", err: operationError{errcode.NewCodedError(data, errcode.InvalidInputCode)}, status: http.StatusBadRequest, code: "input", operation: "update store"},
		{name: "native constructor", err: errs.NewInvalidInputErr(missing), status: http.StatusNotFound, code: "missing"},
		{name: "annotated", err: errors.Annotate(missing, "store"), status: http.StatusNotFound, code: "missing"},
		{name: "wrapped", err: fmt.Errorf("store: %w", missing), status: http.StatusNotFound, code: "missing"},
		{name: "group", err: group, status: http.StatusBadRequest, code: "input", others: []string{"missing"}},
		{name: "wrapped group", err: fmt.Errorf("store: %w", group), status: http.StatusBadRequest, code: "input", others: []string{"missing"}},
		{name: "native and legacy", err: mixedGroup{native, []error{native, missing}}, status: http.StatusBadRequest, code: "input", others: []string{"missing"}},
		{name: "annotated legacy member", err: mixedGroup{native, []error{native, errors.Annotate(missing, "store")}}, status: http.StatusBadRequest, code: "input", others: []string{"missing"}},
		{name: "wrapped legacy member", err: mixedGroup{native, []error{native, fmt.Errorf("store: %w", missing)}}, status: http.StatusBadRequest, code: "input", others: []string{"missing"}},
		{name: "legacy and native", err: legacyGroup{input, []error{input, native}}, status: http.StatusBadRequest, code: "input", others: []string{"input"}},
		{name: "empty group", err: legacyGroup{input, nil}, status: http.StatusBadRequest, code: "input"},
		{name: "uncoded primary", err: legacyGroup{input, []error{errors.New("uncoded"), missing}}, status: http.StatusBadRequest, code: "input", others: []string{"missing"}},
		{name: "non-comparable", err: sliceError{"invalid store"}, status: http.StatusBadRequest, code: "input", data: `["invalid store"]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			response := httptest.NewRecorder()
			re.NotPanics(func() { apiutil.ErrorResp(render.New(), response, tc.err) })
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
			re.Equal(tc.err.Error(), body.Msg)
			wantData := tc.data
			if wantData == "" {
				wantData = `{"detail":"invalid store"}`
			}
			re.JSONEq(wantData, string(body.Data))
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

func TestLegacyCodeIdentity(t *testing.T) {
	re := require.New(t)
	first := errcode.InvalidInputCode.Child("detail")
	second := errcode.NewCode("input").Child("detail")
	re.Equal(first.CodeStr(), second.CodeStr())
	re.NotSame(first.Parent, second.Parent)
	original := errcode.CodedError{
		GetCode: first,
		Err:     errcode.NewCodedError(&clientError{Detail: "invalid store"}, second),
	}
	legacy := errcode.CodeChain(original)
	adapted := errs.FindHTTPError(original)
	re.NotNil(adapted)
	re.Equal(legacy.Error(), adapted.Error())
	re.Len(errors.Errors(adapted), len(errors.Errors(legacy)))
}
