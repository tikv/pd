// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package errcode_test

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/pingcap/pd/pkg/error_code"
)

// Test setting the HTTP code
type HTTPError struct{}

func (e HTTPError) Error() string { return "error" }

const httpCodeStr = "input.http"

var codeHttp900 = errcode.InvalidInputCode.Child(httpCodeStr).SetHTTP(900)

func (e HTTPError) Code() errcode.Code {
	return codeHttp900
}

func TestHttpErrorCode(t *testing.T) {
	http := HTTPError{}
	AssertHTTPCode(t, http, 900)
	ErrorEquals(t, http, "error")
	ClientDataEquals(t, http, http, httpCodeStr)
}

// Test a very simple error
type MinimalError struct{}

func (e MinimalError) Error() string { return "error" }

var _ errcode.ErrorCode = (*MinimalError)(nil) // assert implements interface

const codeString errcode.CodeStr = "input.testcode"

var registeredCode errcode.Code = errcode.InvalidInputCode.Child(codeString)

func (e MinimalError) Code() errcode.Code { return registeredCode }

func TestMinimalErrorCode(t *testing.T) {
	minimal := MinimalError{}
	AssertCodes(t, minimal)
	ErrorEquals(t, minimal, "error")
	ClientDataEquals(t, minimal, minimal)
}

// We don't prevent duplicate codes
var childPathOnlyCode errcode.Code = errcode.InvalidInputCode.Child("testcode")

type ChildOnlyError struct{}

func (e ChildOnlyError) Error() string { return "error" }

var _ errcode.ErrorCode = (*ChildOnlyError)(nil) // assert implements interface

func (e ChildOnlyError) Code() errcode.Code { return childPathOnlyCode }

func TestChildOnlyErrorCode(t *testing.T) {
	coe := ChildOnlyError{}
	AssertCodes(t, coe)
	ErrorEquals(t, coe, "error")
	ClientDataEquals(t, coe, coe)
}

// Test a top-level error
type TopError struct{}

func (e TopError) Error() string { return "error" }

var _ errcode.ErrorCode = (*TopError)(nil) // assert implements interface

const topCodeStr errcode.CodeStr = "top"

var topCode errcode.Code = errcode.NewCode(topCodeStr)

func (e TopError) Code() errcode.Code { return topCode }

func TestTopErrorCode(t *testing.T) {
	top := TopError{}
	AssertCodes(t, top, topCodeStr)
	ErrorEquals(t, top, "error")
	ClientDataEquals(t, top, top, topCodeStr)
}

// Test a deep hierarchy
type DeepError struct{}

func (e DeepError) Error() string { return "error" }

var _ errcode.ErrorCode = (*DeepError)(nil) // assert implements interface

const deepCodeStr errcode.CodeStr = "input.testcode.very.very.deep"

var intermediateCode = registeredCode.Child("input.testcode.very").SetHTTP(800)
var deepCode errcode.Code = intermediateCode.Child("input.testcode.very.very").Child(deepCodeStr)

func (e DeepError) Code() errcode.Code { return deepCode }

func TestDeepErrorCode(t *testing.T) {
	deep := DeepError{}
	AssertHTTPCode(t, deep, 800)
	AssertCode(t, deep, deepCodeStr)
	ErrorEquals(t, deep, "error")
	ClientDataEquals(t, deep, deep, deepCodeStr)
}

// Test an ErrorWrapper that has different error types placed into it
type ErrorWrapper struct{ Err error }

var _ errcode.ErrorCode = (*ErrorWrapper)(nil)     // assert implements interface
var _ errcode.HasClientData = (*ErrorWrapper)(nil) // assert implements interface

func (e ErrorWrapper) Code() errcode.Code {
	return registeredCode
}
func (e ErrorWrapper) Error() string {
	return e.Err.Error()
}
func (e ErrorWrapper) GetClientData() interface{} {
	return e.Err
}

type Struct1 struct{ A string }
type StructConstError1 struct{ A string }

func (e Struct1) Error() string {
	return e.A
}

func (e StructConstError1) Error() string {
	return "error"
}

type Struct2 struct {
	A string
	B string
}

func (e Struct2) Error() string {
	return fmt.Sprintf("error A & B %s & %s", e.A, e.B)
}

func TestErrorWrapperCode(t *testing.T) {
	wrapped := ErrorWrapper{Err: errors.New("error")}
	AssertCodes(t, wrapped)
	ErrorEquals(t, wrapped, "error")
	ClientDataEquals(t, wrapped, errors.New("error"))
	s2 := Struct2{A: "A", B: "B"}
	wrappedS2 := ErrorWrapper{Err: s2}
	AssertCodes(t, wrappedS2)
	ErrorEquals(t, wrappedS2, "error A & B A & B")
	ClientDataEquals(t, wrappedS2, s2)
	s1 := Struct1{A: "A"}
	ClientDataEquals(t, ErrorWrapper{Err: s1}, s1)
	sconst := StructConstError1{A: "A"}
	ClientDataEquals(t, ErrorWrapper{Err: sconst}, sconst)
}

var internalChildCodeStr errcode.CodeStr = "internal.child.granchild"
var internalChild = errcode.InternalCode.Child("internal.child").SetHTTP(503).Child(internalChildCodeStr)

type InternalChild struct{}

func (ic InternalChild) Error() string      { return "internal child error" }
func (ic InternalChild) Code() errcode.Code { return internalChild }

func TestNewInvalidInputErr(t *testing.T) {
	err := errcode.NewInvalidInputErr(errors.New("new error"))
	AssertCodes(t, err, "input")
	ErrorEquals(t, err, "new error")
	ClientDataEquals(t, err, errors.New("new error"), "input")

	err = errcode.NewInvalidInputErr(MinimalError{})
	AssertCodes(t, err, "input.testcode")
	ErrorEquals(t, err, "error")
	ClientDataEquals(t, err, MinimalError{}, errcode.CodeStr("input.testcode"))

	internalErr := errcode.NewInternalErr(MinimalError{})
	err = errcode.NewInvalidInputErr(internalErr)
	internalCodeStr := errcode.CodeStr("internal")
	AssertCode(t, err, internalCodeStr)
	AssertHTTPCode(t, err, 500)
	ErrorEquals(t, err, "error")
	ClientDataEquals(t, err, MinimalError{}, internalCodeStr)

	err = errcode.NewInvalidInputErr(InternalChild{})
	AssertCode(t, err, internalChildCodeStr)
	AssertHTTPCode(t, err, 503)
	ErrorEquals(t, err, "internal child error")
	ClientDataEquals(t, err, InternalChild{}, internalChildCodeStr)
}

func TestNewInternalErr(t *testing.T) {
	internalCodeStr := errcode.CodeStr("internal")
	err := errcode.NewInternalErr(errors.New("new error"))
	AssertCode(t, err, internalCodeStr)
	AssertHTTPCode(t, err, 500)
	ErrorEquals(t, err, "new error")
	ClientDataEquals(t, err, errors.New("new error"), "internal")

	err = errcode.NewInternalErr(MinimalError{})
	AssertCode(t, err, internalCodeStr)
	AssertHTTPCode(t, err, 500)
	ErrorEquals(t, err, "error")
	ClientDataEquals(t, err, MinimalError{}, errcode.CodeStr("internal"))

	invalidErr := errcode.NewInvalidInputErr(MinimalError{})
	err = errcode.NewInternalErr(invalidErr)
	AssertCode(t, err, internalCodeStr)
	AssertHTTPCode(t, err, 500)
	ErrorEquals(t, err, "error")
	ClientDataEquals(t, err, MinimalError{}, internalCodeStr)
}

func AssertCodes(t *testing.T, code errcode.ErrorCode, codeStrs ...errcode.CodeStr) {
	t.Helper()
	AssertCode(t, code, codeStrs...)
	AssertHTTPCode(t, code, 400)
}

func AssertCode(t *testing.T, code errcode.ErrorCode, codeStrs ...errcode.CodeStr) {
	t.Helper()
	codeStr := codeString
	if len(codeStrs) > 0 {
		codeStr = codeStrs[0]
	}
	if code.Code().CodeStr() != codeStr {
		t.Errorf("code expected %v\ncode but got %v", codeStr, code.Code().CodeStr())
	}
}

func AssertHTTPCode(t *testing.T, code errcode.ErrorCode, httpCode int) {
	t.Helper()
	expected := code.Code().HTTPCode()
	if expected != httpCode {
		t.Errorf("excpected HTTP Code %v but got %v", httpCode, expected)
	}
}

func ErrorEquals(t *testing.T, err error, msg string) {
	if err.Error() != msg {
		t.Errorf("Expected error %v. Got error %v", msg, err.Error())
	}
}

func ClientDataEquals(t *testing.T, code errcode.ErrorCode, data interface{}, codeStrs ...errcode.CodeStr) {
	codeStr := codeString
	if len(codeStrs) > 0 {
		codeStr = codeStrs[0]
	}
	t.Helper()
	if !reflect.DeepEqual(errcode.ClientData(code), data) {
		t.Errorf("\nClientData expected: %#v\n ClientData but got: %#v", data, errcode.ClientData(code))
	}
	jsonExpected := errcode.JSONFormat{
		Data: data,
		Msg:  code.Error(),
		Code: codeStr,
	}
	if !reflect.DeepEqual(errcode.NewJSONFormat(code), jsonExpected) {
		t.Errorf("\nJSON expected: %+v\n JSON but got: %+v", jsonExpected, errcode.NewJSONFormat(code))
	}
}
