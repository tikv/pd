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
	if nested := AsHTTPError(e.err); nested != nil {
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
// It panics on nil.
func NewInvalidInputErr(err error) error {
	if err == nil {
		panic("NewCodedError error is nil")
	}
	wrapped := &httpError{err: err, code: "input", statusCode: http.StatusBadRequest}
	if httpErr := AsHTTPError(err); httpErr != nil {
		wrapped.code, wrapped.statusCode = httpErr.Code(), httpErr.HTTPCode()
	}
	return wrapped
}

// NewInternalErr marks err as an internal error with HTTP status 500.
// It implements errors.StackTracer and reuses an existing stack when available.
// Existing internal error codes retain their HTTP mapping. It panics on nil.
func NewInternalErr(err error) error {
	if err == nil {
		panic("makeInternalStackCode error is nil")
	}
	wrapped := &httpError{err: err, code: "internal", statusCode: http.StatusInternalServerError}
	if httpErr := AsHTTPError(err); httpErr != nil && IsInternalHTTPError(httpErr) {
		wrapped.code, wrapped.statusCode = httpErr.Code(), httpErr.HTTPCode()
	}
	tracer := GetStackTracer(err)
	if tracer == nil {
		tracer = errors.NewStack(1)
	}
	return &internalHTTPError{
		httpError:   wrapped,
		StackTracer: tracer,
	}
}

// FindHTTPError preserves the first HTTP error's code and any additional error
// codes in a Cause or Unwrap chain.
func FindHTTPError(err error) HTTPError {
	var code HTTPError
	current := err
	// Track traversal without comparing potentially non-comparable error values.
	hasContext := false
	appendCode := func(next HTTPError, withContext bool) {
		original := error(next)
		if withContext {
			context := &httpErrorContext{HTTPError: next, top: current}
			if group, ok := next.(errors.ErrorGroup); ok {
				next = &httpErrorGroupContext{httpErrorContext: context, ErrorGroup: group}
			} else {
				next = context
			}
			original = next
		}
		if code == nil {
			code = next
		} else {
			code = &httpErrorGroup{HTTPError: code, others: []error{code, next}}
		}
		current = original
	}
	for err != nil {
		if httpErr := AsHTTPError(err); httpErr != nil {
			if code == nil || code.Code() != httpErr.Code() {
				appendCode(httpErr, hasContext)
			}
		} else if group, ok := err.(errors.ErrorGroup); ok {
			var codes []HTTPError
			for _, item := range group.Errors() {
				if itemCode := FindHTTPError(item); itemCode != nil {
					codes = append(codes, itemCode)
				}
			}
			if len(codes) > 0 {
				next := codes[0]
				if len(codes) > 1 {
					var others []error
					if initial, ok := next.(errors.ErrorGroup); ok {
						others = initial.Errors()
					}
					for _, other := range codes[1:] {
						others = append(others, errors.Errors(other)...)
					}
					next = &httpErrorGroup{HTTPError: next, others: others}
				}
				appendCode(next, true)
			}
		}
		err = unwrapError(err)
		hasContext = true
	}
	return code
}

// GetStackTracer finds the first stack tracer in a Cause or Unwrap error chain.
func GetStackTracer(err error) errors.StackTracer {
	for current := err; current != nil; current = unwrapError(current) {
		if tracer, ok := current.(errors.StackTracer); ok {
			return tracer
		}
	}
	if group, ok := err.(errors.ErrorGroup); ok {
		for _, item := range group.Errors() {
			if tracer := GetStackTracer(item); tracer != nil {
				return tracer
			}
		}
	}
	return nil
}

// IsInternalHTTPError reports whether the code belongs to the internal family.
// Codes outside that family do not include a stack, even with HTTP 500.
func IsInternalHTTPError(err HTTPError) bool {
	return err.Code() == "internal" || strings.HasPrefix(err.Code(), "internal.")
}

// AsHTTPError returns err if it implements HTTPError, without traversing its chain.
func AsHTTPError(err error) HTTPError {
	httpErr, _ := err.(HTTPError)
	return httpErr
}

type httpErrorContext struct {
	HTTPError
	top error
}

func (e *httpErrorContext) Error() string { return e.top.Error() }

// Cause retains the original context while traversing the error chain.
func (e *httpErrorContext) Cause() error {
	if cause := unwrapError(e.top); cause != nil {
		return cause
	}
	return e.HTTPError
}

// httpErrorGroupContext retains group members when an error chain adds context.
type httpErrorGroupContext struct {
	*httpErrorContext
	errors.ErrorGroup
}

type httpErrorGroup struct {
	HTTPError
	others []error
}

// Error joins the primary and additional error messages with semicolons.
func (e *httpErrorGroup) Error() string {
	var message strings.Builder
	message.WriteString(e.HTTPError.Error())
	for _, item := range e.others {
		message.WriteString("; ")
		message.WriteString(item.Error())
	}
	return message.String()
}

// Cause returns the first error in the group.
func (e *httpErrorGroup) Cause() error { return e.HTTPError }

// Errors returns the primary and additional errors without changing their order.
func (e *httpErrorGroup) Errors() []error {
	return append([]error{e.HTTPError}, e.others...)
}

// unwrapError gives legacy Cause methods precedence over standard Unwrap methods.
func unwrapError(err error) error {
	if cause := errors.Unwrap(err); cause != nil {
		return cause
	}
	return stderrors.Unwrap(err)
}
