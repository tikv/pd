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

package apiutil

import (
	stderrors "errors"
	"net/http"

	"github.com/unrolled/render"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
)

type httpError struct {
	err        error
	code       string
	statusCode int
	stack      errors.StackTrace
}

func (e *httpError) Error() string { return e.err.Error() }
func (e *httpError) Unwrap() error { return e.err }

// Cause returns the underlying error for github.com/pingcap/errors.
func (e *httpError) Cause() error { return e.err }

// NewInvalidInputErr marks err as an invalid input error with HTTP status 400.
func NewInvalidInputErr(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := err.(*httpError); ok {
		return err
	}
	return &httpError{err: err, code: "input", statusCode: http.StatusBadRequest}
}

// NewInternalErr marks err as an internal error with HTTP status 500 and a stack trace.
func NewInternalErr(err error) error {
	if err == nil {
		return nil
	}
	var stack errors.StackTrace
	if tracer := errors.GetStackTracer(err); tracer != nil {
		stack = tracer.StackTrace()
	} else {
		stack = errors.NewStack(1).StackTrace()
	}
	return &httpError{err: err, code: "internal", statusCode: http.StatusInternalServerError, stack: stack}
}

// ErrorResp writes a structured response for errors marked by NewInvalidInputErr
// or NewInternalErr. Other errors produce HTTP status 500 and a JSON string.
// A nil error also produces HTTP status 500 and is logged at the error level.
func ErrorResp(rd *render.Render, w http.ResponseWriter, err error) {
	if err == nil {
		log.Error("nil is given to errorResp")
		rd.JSON(w, http.StatusInternalServerError, "nil error")
		return
	}
	var httpErr *httpError
	if !stderrors.As(err, &httpErr) {
		rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("TiDB-Error-Code", httpErr.code)
	rd.JSON(w, httpErr.statusCode, struct {
		Code  string            `json:"code"`
		Msg   string            `json:"msg"`
		Data  error             `json:"data"`
		Stack errors.StackTrace `json:"stack,omitempty"`
	}{
		Code:  httpErr.code,
		Msg:   err.Error(),
		Data:  httpErr.err,
		Stack: httpErr.stack,
	})
}
