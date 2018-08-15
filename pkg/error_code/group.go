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

package errcode

// ErrorGroup is the same interface as provided by uber-go/multierr
// Other multi error packages provide similar interfaces.
//
// There are two concepts around multiple errors
// * Wrapping errors (We have this already with Causer)
// * multiple parallel/sibling errors: this is ErrorGroup
type ErrorGroup interface {
	Errors() []error
}

// Errors uses the ErrorGroup interface to return a slice of errors.
// If the ErrorGroup interface is not implemented it returns an array containing just the given error.
func Errors(err error) []error {
	if eg, ok := err.(ErrorGroup); ok {
		return eg.Errors()
	}
	return []error{err}
}

// ErrorCodes return all errors (from an ErrorGroup) that are of interface ErrorCode.
// It first calls the Errors function.
func ErrorCodes(err error) []ErrorCode {
	errors := Errors(err)
	errorCodes := make([]ErrorCode, len(errors))
	for i, errItem := range errors {
		if errcode, ok := errItem.(ErrorCode); ok {
			errorCodes[i] = errcode
		}
	}
	return errorCodes
}

// A MultiErrCode contains at least one ErrorCode and uses that to satisfy the ErrorCode and related interfaces
// The Error method will produce a string of all the errors with a semi-colon separation.
// Later code (such as a JSON response) needs to look for the ErrorGroup interface.
type MultiErrCode struct {
	ErrorCode ErrorCode
	all       []error
}

// Combine constructs a MultiErrCode.
// It will combine any other MultiErrCode into just one MultiErrCode.
func Combine(override ErrorCode, others ...ErrorCode) ErrorCode {
	all := Errors(override)
	for _, other := range others {
		all = append(all, Errors(other)...)
	}
	return MultiErrCode{
		ErrorCode: override,
		all:       all,
	}
}

var _ ErrorCode = (*MultiErrCode)(nil)     // assert implements interface
var _ HasClientData = (*MultiErrCode)(nil) // assert implements interface
var _ Causer = (*MultiErrCode)(nil)        // assert implements interface
var _ ErrorGroup = (*MultiErrCode)(nil)    // assert implements interface

func (e MultiErrCode) Error() string {
	output := e.ErrorCode.Error()
	for _, item := range e.all[1:] {
		output += "; " + item.Error()
	}
	return output
}

// Errors fullfills the ErrorGroup inteface
func (e MultiErrCode) Errors() []error {
	return e.all
}

// Code fullfills the ErrorCode inteface
func (e MultiErrCode) Code() Code {
	return e.ErrorCode.Code()
}

// Cause fullfills the Causer inteface
func (e MultiErrCode) Cause() error {
	return e.ErrorCode
}

// GetClientData fullfills the HasClientData inteface
func (e MultiErrCode) GetClientData() interface{} {
	return ClientData(e.ErrorCode)
}
