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

// Package errcode facilitates standardized API error codes.
// The goal is that clients can reliably understand errors by checking against immutable error codes
// A RegisteredCode should never be modified once committed (and released for use by clients).
// Instead a new RegisteredCode should be created.
//
// Note that the error codes are strings.
// Integer error codes have extensibility issues (branches using the same numbers, etc).
//
// This package is designed to have few opinions and be a starting point for how you want to do errors.
// The only requirement is to satisfy the ErrorCode interface (see documentation of ErrCode).
//
// A few generic error codes are provided here.
// You are encouraged to create your own application customized error codes rather than just using generic errors.
//
// See JSONFormat for an opinion on how to send back error information to a client.
// Note that this includes a body of response data (the "data field") with more detailed and structured information.
// This package provides no help on defining conventions, versioning, etc for that data.
package errcode

import (
	"net/http"
	"reflect"
)

// RegisteredCode helps document that we are using a registered error code that must never change
type RegisteredCode string

const (
	// InternalCode means the operation placed the system is in an inconsistent or unrecoverable state.
	// Essentially a handled panic.
	// This is exactly the same as a HTTP 500, so it is not necessary to send this code over HTTP.
	InternalCode RegisteredCode = "internal"
	// InvalidInputCode is a validation failure of an input.
	// The response will indicate the exact input issue.
	InvalidInputCode RegisteredCode = "input"
	// NotFoundCode indicates a resource is missing.
	// The response can indicate what resource is missing.
	// However, when a single resource is requested, that may be implicit.
	NotFoundCode RegisteredCode = "missing"
)

/*
ErrorCode is the interface that ties an error and RegisteredCode together.

Note that there are two additional interfaces (HasHTTPCode and HasClientData) that can be defined by an ErrorCode
To further customize behavior
For example, internalError implements HasHTTPCode to change it to a 500.

ErrorCode allows error codes to be defined
without being forced to use a particular struct such as CodedError.
CodedError is convenient for generic errors that wrap many different code errors.
Please see the docs for CodedError.
For an application specific error with a 1:1 mapping between a go error structure and a RegisteredCode,
You probably want to use this interface directly. Example:

	const PathBlockedCode RegisteredCode = "path.state.blocked"

	type PathBlocked struct {
		start     uint64 `json:"start"`
		end       uint64 `json:"end"`
		obstacle  uint64 `json:"end"`
	}

	func (e PathBlocked) Error() string {
		return fmt.Sprintf("The path %d -> %d has obstacle %d", e.start, e.end, e.obstacle)
	}

	func (e PathBlocked) Code() RegisteredCode {
		return PathBlockedCode
	}
*/
type ErrorCode interface {
	Error() string // The Error interface
	Code() RegisteredCode
}

// HasHTTPCode is defined to override the default of 400 Bad Request
type HasHTTPCode interface {
	GetHTTPCode() int
}

// HTTPCode defaults to 400 BadRequest
// This is overidden by defining the HasHTTPCode interface with a GetHTTPCode function
func HTTPCode(errCode ErrorCode) int {
	httpCode := http.StatusBadRequest
	if hasData, ok := errCode.(HasHTTPCode); ok {
		httpCode = hasData.GetHTTPCode()
	}
	return httpCode
}

// HasClientData is used to defined how to retrieve the data portion of an ErrorCode to be returned to the client.
// Otherwise the struct itself will be assumed to be all the data.
// This is provided for exensibility, but may be unecessary for you.
type HasClientData interface {
	GetClientData() interface{}
}

// ClientData retrieves data from a structure that impilements HasClientData
// If HasClientData is defined, use GetClientData()
// Otherwise use the object itself with a call to ErrorData.
func ClientData(errCode ErrorCode) interface{} {
	var data interface{} = errCode
	if hasData, ok := errCode.(HasClientData); ok {
		data = hasData.GetClientData()
	} else {
		data = ErrorData(errCode)
	}
	return data
}

// JSONFormat is a standard way to serilalize an ErrorCode to JSON.
// Msg is the string from Error().
// The Data field is filled in by GetClientData
type JSONFormat struct {
	Data interface{}    `json:"data"`
	Msg  string         `json:"msg"`
	Code RegisteredCode `json:"code"`
}

// NewJSONFormat turns an ErrCode into a JSONFormat
func NewJSONFormat(errCode ErrorCode) JSONFormat {
	data := ClientData(errCode)
	return JSONFormat{Code: errCode.Code(), Data: data, Msg: errCode.Error()}
}

// CodedError is a convenience to attach a code to an error and already satisfy the ErrorCode interface.
// If the error is a struct, that struct will get preseneted as data to the client.
//
// To override the http code or the data representation or just for clearer documentation,
// you are encouraged to wrap CodeError with your own struct that inherits it.
// Look at the implementation of invalidInput, internalError, and notFound.
type CodedError struct {
	RegisteredCode RegisteredCode
	Err            error
}

var _ ErrorCode = (*CodedError)(nil)     // assert implements interface
var _ HasClientData = (*CodedError)(nil) // assert implements interface

func (e CodedError) Error() string {
	return e.Err.Error()
}

// Code returns the RegisteredCode field
func (e CodedError) Code() RegisteredCode {
	return e.RegisteredCode
}

// GetClientData returns the underlying Err field.
func (e CodedError) GetClientData() interface{} {
	return ErrorData(e.Err)
}

// invalidInput gives the code InvalidInputCode
type invalidInputErr struct{ CodedError }

// NewInvalidInputErr creates an invalidInput with error code of InvalidInputCode
func NewInvalidInputErr(err error) ErrorCode {
	return invalidInputErr{CodedError{RegisteredCode: InvalidInputCode, Err: err}}
}

var _ ErrorCode = (*invalidInputErr)(nil) // assert implements interface

// internalError gives the code InvalidInputCode
type internalErr struct{ CodedError }

// GetHTTPCode returns 500
func (e internalErr) GetHTTPCode() int {
	return http.StatusInternalServerError
}

// NewInternalErr creates an internalError with error code InternalCode
func NewInternalErr(err error) ErrorCode {
	return internalErr{CodedError{RegisteredCode: InternalCode, Err: err}}
}

var _ ErrorCode = (*internalErr)(nil)   // assert implements interface
var _ HasHTTPCode = (*internalErr)(nil) // assert implements interface

// notFound gives the code NotFoundCode
type notFoundErr struct{ CodedError }

// HTTPCode returns 404
func (e notFoundErr) GetHTTPCode() int {
	return http.StatusNotFound
}

// NewNotFoundErr creates a notFound with error code of InternalCode
func NewNotFoundErr(err error) ErrorCode {
	return notFoundErr{CodedError{RegisteredCode: NotFoundCode, Err: err}}
}

var _ ErrorCode = (*notFoundErr)(nil)   // assert implements interface
var _ HasHTTPCode = (*notFoundErr)(nil) // assert implements interface

// ErrorData avoids sending an error string twice when returning rich data to a client from an error.
// It checks if an error is just using a simple string or a struct wrapping a string (errors.New).
// In that case it will return an empty struct.
// Otherwise it will return the given data.
func ErrorData(err error) interface{} {
	justString := false
	v := reflect.ValueOf(err)
	vKind := v.Kind()
	if vKind == reflect.Ptr {
		v = v.Elem()
		vKind = v.Kind()
	}
	if vKind == reflect.String {
		justString = true
	} else if vKind == reflect.Struct {
		typ := v.Type()
		numFields := typ.NumField()
		if numFields == 0 {
			justString = true
		} else if numFields == 1 {
			fieldTyp := typ.Field(0)
			if fieldTyp.Type.Kind() == reflect.String {
				fieldV := v.Field(0)
				if fieldV.CanInterface() {
					if vstr, ok := fieldV.Interface().(string); ok {
						justString = vstr == err.Error()
					}
				} else {
					justString = true
				}
			}
		}
	}
	if justString {
		var empty struct{}
		return empty
	}
	return err
}
