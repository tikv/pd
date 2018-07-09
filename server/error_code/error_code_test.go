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
	"testing"

	"github.com/pingcap/pd/server/error_code"
)

type MinimalError struct{}

const registeredCode errcode.RegisteredCode = "registeredCode"

var _ errcode.ErrorCode = (*MinimalError)(nil) // assert implements interface

func (e MinimalError) Code() errcode.RegisteredCode {
	return registeredCode
}
func (e MinimalError) Error() string {
	return "error"
}

type ErrorWrapper struct{ Err error }

var _ errcode.ErrorCode = (*ErrorWrapper)(nil) // assert implements interface

func (e ErrorWrapper) Code() errcode.RegisteredCode {
	return registeredCode
}
func (e ErrorWrapper) Error() string {
	return e.Err.Error()
}
func (e ErrorWrapper) GetClientData() interface{} {
	return errcode.ErrorData(e.Err)
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

var empty struct{}

func TestErrorData(t *testing.T) {
	ErrorDataEquals(t, errors.New("errors"), empty)
	s2 := Struct2{A: "A", B: "B"}
	ErrorDataEquals(t, s2, s2)
	s1 := Struct1{A: "A"}
	ErrorDataEquals(t, s1, empty)
	sconst := StructConstError1{A: "A"}
	ErrorDataEquals(t, sconst, sconst)
}

func ErrorDataEquals(t *testing.T, given error, expected interface{}) {
	t.Helper()
	data := errcode.ErrorData(given)
	if data != expected {
		t.Errorf("\nErrorData expected: %+v\n but got: %+v", expected, data)
	}
}

func TestMinimalErrorCode(t *testing.T) {
	minimal := MinimalError{}
	AssertCodes(t, minimal)
	ErrorEquals(t, minimal, "error")
	ClientDataEquals(t, minimal, empty)
	wrapped := ErrorWrapper{Err: errors.New("error")}
	AssertCodes(t, wrapped)
	ErrorEquals(t, wrapped, "error")
	ClientDataEquals(t, wrapped, empty)
	s2 := Struct2{A: "A", B: "B"}
	wrappedS2 := ErrorWrapper{Err: s2}
	AssertCodes(t, wrappedS2)
	ErrorEquals(t, wrappedS2, "error A & B A & B")
	ClientDataEquals(t, wrappedS2, s2)
}

func AssertCodes(t *testing.T, code errcode.ErrorCode) {
	t.Helper()
	if code.Code() != registeredCode {
		t.Error("bad code")
	}
	if errcode.HTTPCode(code) != 400 {
		t.Error("excpected HTTP Code 400")
	}
}

func ErrorEquals(t *testing.T, err error, msg string) {
	if err.Error() != msg {
		t.Errorf("Expected error %v. Got error %v", msg, err.Error())
	}
}

func ClientDataEquals(t *testing.T, code errcode.ErrorCode, data interface{}) {
	t.Helper()
	if errcode.ClientData(code) != data {
		t.Errorf("ClientData. expected %v. got: %v", data, errcode.ClientData(code))
	}
	jsonExpected := errcode.JSONFormat{Data: data, Msg: code.Error(), Code: registeredCode}
	if errcode.NewJSONFormat(code) != jsonExpected {
		t.Errorf("\nJSON expected: %+v\n JSON but got: %+v", jsonExpected, errcode.NewJSONFormat(code))
	}
}
