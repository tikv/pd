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

// Package errcode is designed to create standardized API error codes.
// The goal is that clients can reliably check against immutable error codes
//
// Two approaches can be takend
// 1) centralized: define all errors in one module
// 2) modular: define errors where they occur
//
// The centralized approach helps organize information.
// All errors can be found in one file, and changes can be carefully reviewed.
// The downside of centralized is the potential need to import types from other packages.
//
// A RegisteredCode should never be modified once committed (and released)
package errcode

import (
	"net/http"
	"reflect"
)

// RegisteredCode helps document that we are using a registered error code that must never change
type RegisteredCode string

const (
	// InternalErrorCode means the operation placed the system is in an inconsistent or unrecoverable state
	// Essentially a handled panic.
	// This is exactly the same as a HTTP 500, so it is not necessary to send this code over HTTP.
	InternalErrorCode RegisteredCode = "internal"
	// InvalidInputCode is a validation failure of an input
	InvalidInputCode RegisteredCode = "input.invalid"
	// NotFoundCode indicates a resource is missing
	NotFoundCode RegisteredCode = "missing"
)

// ErrorCode is an interface that allows error codes to be defined without importing this package or
// being forced to use a particular struct such as CodedError.
//
// Note that there are two additional interfaces (HasHTTPCode and HasClientData) that can be defined by an ErrorCode
// To further customize behavior
// For example, InternalError implements HasHTTPCode to change it to a 500.
type ErrorCode interface {
	Error() string // The Error interface
	Code() RegisteredCode
}

// HasHTTPCode is defined to override the default of 400 Bad Request
type HasHTTPCode interface {
	getHTTPCode() int
}

// GetHTTPCode by default will give a 400 BadRequest
// This is overidden by defining the HasHTTPCode interface with a getHTTPCode function
func GetHTTPCode(errCode ErrorCode) int {
	httpCode := http.StatusBadRequest
	if hasData, ok := errCode.(HasHTTPCode); ok {
		httpCode = hasData.getHTTPCode()
	}
	return httpCode
}

// HasClientData is used to defined how to retrieve the data portion of an ErrorCode to be returned to the client.
// Otherwise the struct itself will be assumed to be all the data.
// This is provided for exensibility, but may be unecessary for you.
type HasClientData interface {
	getClientData() interface{}
}

// GetClientData retrieves data from a structure that impilements HasClientData
// If HasClientData is defined, use getClientData()
// Otherwise use the object itself
func GetClientData(errCode ErrorCode) interface{} {
	var data interface{} = errCode
	if hasData, ok := errCode.(HasClientData); ok {
		data = hasData.getClientData()
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
	data := GetClientData(errCode)
	return JSONFormat{Code: errCode.Code(), Data: data, Msg: errCode.Error()}
}

// CodedError is a convenience to attach a code to an error.
// If the error is a struct, that struct will get preseneted as data to the client.
//
// To override the http code or the data representation or just for clearer documentation,
// you are encouraged to wrap CodeError with your own struct that inherits it.
// Look at the implementation of InvalidInput and InternalError
type CodedError struct {
	RegisteredCode RegisteredCode
	Err            error
}

var _ ErrorCode = (*CodedError)(nil)     // assert implements interface
var _ HasClientData = (*CodedError)(nil) // assert implements interface

func (e CodedError) Error() string {
	return e.Err.Error()
}

// Code returns StoreTombstonedCode
func (e CodedError) Code() RegisteredCode {
	return e.RegisteredCode
}

func (e CodedError) getClientData() interface{} {
	return errorData(e.Err)
}

// errorData attempts to returns rich data to a client from an error.
// It checks if an error is just using a simple type or a sturct of one string (errors.New).
// In that case it will return an empty struct.
// Otherwise it will return the error struct.
func errorData(err error) interface{} {
	justString := true
	if reflect.ValueOf(err).Kind() == reflect.Struct {
		typ := reflect.TypeOf(err)
		numFields := typ.NumField()
		if numFields > 1 || (numFields == 1 && typ.Field(0).Type.Kind() == reflect.String) {
			justString = false
		}
	}
	if justString {
		var empty struct{}
		return empty
	}
	return err
}

// InvalidInput gives the code InvalidInputCode
type InvalidInput struct{ CodedError }

// NewInvalidInput creates an error code of InvalidInputCode
func NewInvalidInput(err error) InvalidInput {
	return InvalidInput{CodedError{RegisteredCode: InvalidInputCode, Err: err}}
}

var _ ErrorCode = (*InvalidInput)(nil) // assert implements interface

// InternalError gives the code InvalidInputCode
type InternalError struct{ CodedError }

// getHTTPCode returns 500
func (e InternalError) getHTTPCode() int {
	return http.StatusInternalServerError
}

// NewInternalError creates an error code of InternalErrorCode
func NewInternalError(err error) InternalError {
	return InternalError{CodedError{RegisteredCode: InternalErrorCode, Err: err}}
}

var _ ErrorCode = (*InternalError)(nil)   // assert implements interface
var _ HasHTTPCode = (*InternalError)(nil) // assert implements interface

// NotFound gives the code NotFoundCode
type NotFound struct{ CodedError }

// HTTPCode returns 404
func (e NotFound) getHTTPCode() int {
	return http.StatusNotFound
}

// NewNotFound creates an error code of InternalErrorCode
func NewNotFound(err error) NotFound {
	return NotFound{CodedError{RegisteredCode: NotFoundCode, Err: err}}
}

var _ ErrorCode = (*NotFound)(nil)   // assert implements interface
var _ HasHTTPCode = (*NotFound)(nil) // assert implements interface
