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

// Causer allows the abstract retrieval of the underlying error.
// This is the interface that pkg/errors does not export but is considered part of the stable public API.
//
// Types that wrap errors should implement this to allow viewing of the underlying error.
// Generally you would use this via pkg/errors.Cause or WrappedError.
// PreviousErrorCode and StackTrace use Causer to check if the underlying error is an ErrorCode or a StackTracer.
type Causer interface {
	Cause() error
}

// WrappedError retrieves the wrapped error via Causer.
// Unlike pkg/errors.Cause, it goes just one level deep and does not recursively traverse.
// Return nil is there is no wrapped error.
func WrappedError(err error) error {
	if hasCause, ok := err.(Causer); ok {
		return hasCause.Cause()
	}
	return nil
}

// EmbedErr is designed to be embedded into your existing error structs.
// It provides the Causer interface already, which can reduce your boilerplate.
type EmbedErr struct {
	Err error
}

// Cause implements the Causer interface
func (e EmbedErr) Cause() error {
	return e.Err
}

var _ Causer = (*EmbedErr)(nil) // assert implements interface

// PreviousErrorCode looks for a previous ErrorCode that has a different code.
// This helps construct a trace of all previous errors.
// It will return nil if no previous ErrorCode is found.
//
// To look for a previous ErrorCode it looks at Causer to see if they are an ErrorCode.
// Wrappers of errors like OpErrCode and CodedError implement Causer.
func PreviousErrorCode(err ErrorCode) ErrorCode {
	return previousErrorCodeCompare(err.Code(), err)
}

func previousErrorCodeCompare(code Code, err error) ErrorCode {
	prev := WrappedError(err)
	if prev == nil {
		return nil
	}
	if errcode, ok := prev.(ErrorCode); ok {
		if errcode.Code() != code {
			return errcode
		}
	}
	return previousErrorCodeCompare(code, prev)
}

// GetErrorCode tries to convert an error to an ErrorCode.
// If the error is not an ErrorCode,
// it looks for the first ErrorCode it can find via Causer.
// In that case it will retain the error message of the original error by returning a WrappedErrorCode.
func GetErrorCode(err error) ErrorCode {
	if errcode, ok := err.(ErrorCode); ok {
		return errcode
	}
	prev := WrappedError(err)
	for prev != nil {
		if errcode, ok := prev.(ErrorCode); ok {
			return WrappedErrorCode{errcode, err}
		}
		prev = WrappedError(err)
	}
	return nil
}

// WrappedErrorCode is returned by GetErrorCode to retain the full wrapped error message
type WrappedErrorCode struct {
	ErrCode ErrorCode
	Top     error
}

// Code satisfies the ErrorCode interface
func (err WrappedErrorCode) Code() Code {
	return err.ErrCode.Code()
}

// Error satisfies the Error interface
func (err WrappedErrorCode) Error() string {
	return err.Top.Error()
}

// Cause satisfies the Causer interface
func (err WrappedErrorCode) Cause() error {
	if wrapped := WrappedError(err.Top); wrapped != nil {
		return wrapped
	}
	// There should be a wrapped error and this should not be reached
	return err.ErrCode
}

// GetClientData satisfies the HasClientData interface
func (err WrappedErrorCode) GetClientData() interface{} {
	return ClientData(err.ErrCode)
}

var _ ErrorCode = (*WrappedErrorCode)(nil)
var _ HasClientData = (*WrappedErrorCode)(nil)
var _ Causer = (*WrappedErrorCode)(nil)
