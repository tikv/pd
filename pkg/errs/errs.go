// Copyright 2020 TiKV Project Authors.
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

package errs

import (
	stderrors "errors"
	"net/http"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/pingcap/errors"
)

// ZapError is used to make the log output easier.
func ZapError(err error, causeError ...error) zap.Field {
	if err == nil {
		return zap.Skip()
	}
	if e, ok := err.(*errors.Error); ok {
		if len(causeError) >= 1 {
			err = e.Wrap(causeError[0])
		} else {
			err = e.FastGenByArgs()
		}
	}
	return zap.Field{Key: "error", Type: zapcore.ErrorType, Interface: err}
}

// IsLeaderChanged returns true if the error is due to leader changed.
func IsLeaderChanged(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), NotLeaderErr)
}

// HTTPError carries the metadata used by structured HTTP error responses.
type HTTPError interface {
	error
	Code() string
	HTTPCode() int
	GetClientData() any
}

type httpError struct {
	err        error
	code       string
	statusCode int
}

func (e *httpError) Error() string { return e.err.Error() }
func (e *httpError) Unwrap() error { return e.err }

// Cause returns the underlying error for github.com/pingcap/errors.
func (e *httpError) Cause() error { return e.err }

// Code returns the error code sent in the response header and body.
func (e *httpError) Code() string { return e.code }

// HTTPCode returns the response status code.
func (e *httpError) HTTPCode() int { return e.statusCode }

// GetClientData preserves the original error data across HTTP error wrappers.
func (e *httpError) GetClientData() any {
	if nested, ok := e.err.(HTTPError); ok {
		return nested.GetClientData()
	}
	return e.err
}

type internalHTTPError struct {
	*httpError
	errors.StackTracer
}

// NewInvalidInputErr marks err as invalid input with HTTP status 400.
// An existing HTTPError retains its code and status.
func NewInvalidInputErr(err error) error {
	if err == nil {
		return nil
	}
	code, status := "input", http.StatusBadRequest
	if httpErr, ok := err.(HTTPError); ok {
		code, status = httpErr.Code(), httpErr.HTTPCode()
	}
	return &httpError{err: err, code: code, statusCode: status}
}

// NewInternalErr marks err as an internal error with HTTP status 500.
// It implements errors.StackTracer and reuses an existing stack when available.
func NewInternalErr(err error) error {
	if err == nil {
		return nil
	}
	tracer := GetStackTracer(err)
	if tracer == nil {
		tracer = errors.NewStack(1)
	}
	return &internalHTTPError{
		httpError:   &httpError{err: err, code: "internal", statusCode: http.StatusInternalServerError},
		StackTracer: tracer,
	}
}

// FindHTTPError finds the first HTTPError in a Cause or Unwrap error chain.
func FindHTTPError(err error) HTTPError {
	for err != nil {
		if httpErr, ok := err.(HTTPError); ok {
			return httpErr
		}
		err = unwrapError(err)
	}
	return nil
}

// GetStackTracer finds the first stack tracer in a Cause or Unwrap error chain.
func GetStackTracer(err error) errors.StackTracer {
	for err != nil {
		if tracer, ok := err.(errors.StackTracer); ok {
			return tracer
		}
		err = unwrapError(err)
	}
	return nil
}

func unwrapError(err error) error {
	if cause := errors.Unwrap(err); cause != nil {
		return cause
	}
	return stderrors.Unwrap(err)
}
