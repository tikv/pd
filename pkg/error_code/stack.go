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

import (
	"github.com/pkg/errors"
)

// StackTracer is the interface defined but not exported from pkg/errors
// The StackTrace() function (not method) is a preferred way to access the StackTrace
//
// Generally you should only bother with stack traces for internal errors.
type StackTracer interface {
	StackTrace() errors.StackTrace
}

// StackTrace retrieves the errors.StackTrace from the error if it is present.
// If there is not StackTrace it will return nil
//
// StackTrace looks to see if the error is a StackTracer or if a Causer of the error is a StackTracer.
// It will return the stack trace from the deepest error it can find.
func StackTrace(err error) errors.StackTrace {
	if prev := WrappedError(err); prev != nil {
		if trace := StackTrace(prev); trace != nil {
			return trace
		}
	}
	if stackTraceErr, ok := err.(StackTracer); ok {
		return stackTraceErr.StackTrace()
	}
	return nil
}

// StackCode is an ErrorCode with stack trace information attached.
// This may be used as a convenience to record the strack trace information for the error.
// Generally stack traces aren't needed for user errors, but they are provided by NewInternalErr.
// Its also possible to define your own structures that satisfy the StackTracer interface.
type StackCode struct {
	Err           ErrorCode
	GetStackTrace errors.StackTrace
}

// StackTrace fulfills the StackTracer interface
func (e StackCode) StackTrace() errors.StackTrace {
	return e.GetStackTrace
}

// NewStackCode constructs a StackCode, which is an ErrorCode with stack trace information
// The second variable is an optional stack position gets rid of information about function calls to construct the stack trace.
// It is defaulted to 1 to remove this function call.
//
// NewStackCode first looks at the underlying error via WrappedError to see if it already has a StackTrace.
// If so, that StackTrace is used.
func NewStackCode(err ErrorCode, position ...int) StackCode {
	stackPosition := 1
	if len(position) > 0 {
		stackPosition = position[0]
	}

	// if there is an existing trace, take that: it should be deeper
	if trace := StackTrace(err); trace != nil {
		return StackCode{Err: err, GetStackTrace: trace}
	}

	// we must go through some contortions to get a stack trace from pkg/errors
	stackedErr := errors.WithStack(err)
	if stackTraceErr, ok := stackedErr.(StackTracer); ok {
		return StackCode{Err: err, GetStackTrace: stackTraceErr.StackTrace()[stackPosition:]}
	}
	panic("NewStackCode: pkg/errors WithStack StackTrace interface changed")
}

// Cause satisfies the Causer interface
func (e StackCode) Cause() error {
	return e.Err
}

// Error ignores the stack and gives the underlying Err Error.
func (e StackCode) Error() string {
	return e.Err.Error()
}

// Code returns the underlying Code of Err.
func (e StackCode) Code() Code {
	return e.Err.Code()
}

// GetClientData returns the ClientData of the underlying Err.
func (e StackCode) GetClientData() interface{} {
	return ClientData(e.Err)
}

var _ ErrorCode = (*StackCode)(nil)     // assert implements interface
var _ HasClientData = (*StackCode)(nil) // assert implements interface
var _ Causer = (*StackCode)(nil)        // assert implements interface
