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
// A Code should never be modified once committed (and released for use by clients).
// Instead a new Code should be created.
//
// Error codes are represented as strings by CodeStr (see CodeStr documentation).
//
// This package is designed to have few opinions and be a starting point for how you want to do errors in your project.
// Interfaces are provided for extensibility.
// The main requirement is to satisfy the ErrorCode interface by attaching a Code
// See the documentation of ErrorCode.
//
// Hierarchies are supported: a Code can point to a parent.
// This is used in the HTTPCode implementation to inherit HTTP codes found with MetaDataFromAncestors.
// The hierarchy is present in the Code's string representation with a dot separation.
//
// A few generic top-level error codes are provided here.
// You are encouraged to create your own application customized error codes rather than just using generic errors.
//
// See JSONFormat for an opinion on how to send back error information to a client.
// Note that this includes a body of response data (the "data field") with more detailed and structured information.
// This package provides no help on defining conventions, versioning, etc for that data.
package errcode

import (
	"fmt"
	"net/http"
	"strings"
)

// CodeStr is a representation of the type of a particular error.
// The underlying type is string rather than int.
// This enhances both extensibility (avoids merge conflicts) and user-friendliness.
// A CodeStr can have dot separators indicating a hierarchy.
type CodeStr string

func (str CodeStr) String() string { return string(str) }

// A Code has a CodeStr representation.
// It is attached to a Parent to find metadata from it.
// The Meta field is provided for extensibility: e.g. attaching HTTP codes.
type Code struct {
	// codeStr does not include parent paths
	// The full code (with parent paths) is accessed with CodeStr
	codeStr CodeStr
	Parent  *Code
}

// CodeStr gives the full dot-separted path.
// This is what should be used for equality comparison.
func (code Code) CodeStr() CodeStr {
	if code.Parent == nil {
		return code.codeStr
	}
	return (*code.Parent).CodeStr() + "." + code.codeStr
}

// NewCode creates a new top-level code.
// A top-level code must not contain any dot separators: that will panic
// Most codes should be created from hierachry with the Child method.
func NewCode(codeRep CodeStr) Code {
	code := Code{codeStr: codeRep}
	if err := code.checkCodePath(); err != nil {
		panic(err)
	}
	return code
}

// Child creates a new code from a parent.
// For documentation purposes, a childStr may include the parent codes with dot-separation.
// An incorrect parent reference in the string panics.
func (code Code) Child(childStr CodeStr) Code {
	child := Code{codeStr: childStr, Parent: &code}
	if err := child.checkCodePath(); err != nil {
		panic(err)
	}
	// Don't store parent paths, those are re-constructed in CodeStr()
	paths := strings.Split(child.codeStr.String(), ".")
	child.codeStr = CodeStr(paths[len(paths)-1])
	return child
}

// FindAncestor looks for an ancestor satisfying the given test function.
func (code Code) findAncestor(test func(Code) bool) *Code {
	if test(code) {
		return &code
	}
	if code.Parent == nil {
		return nil
	}
	return (*code.Parent).findAncestor(test)
}

// IsAncestor looks for the given code in its ancestors.
func (code Code) IsAncestor(ancestorCode Code) bool {
	return nil != code.findAncestor(func(an Code) bool { return an == ancestorCode })
}

// MetaData is a pattern for attaching meta data to codes and inheriting it from a parent.
// See MetaDataFromAncestors.
// This is used to attach an HTTP code to a Code.
type MetaData map[CodeStr]interface{}

// MetaDataFromAncestors looks for meta data starting at the current code.
// If not found, it traverses up the hierarchy
// by looking for the first ancestor with the given metadata key.
// This is used in the HTTPCode implementation to inherit the HTTP Code from ancestors.
func (code Code) MetaDataFromAncestors(metaData MetaData) interface{} {
	if existing, ok := metaData[code.CodeStr()]; ok {
		return existing
	}
	if code.Parent == nil {
		return nil
	}
	return (*code.Parent).MetaDataFromAncestors(metaData)
}

var httpMetaData = make(MetaData)

// SetHTTP adds an HTTP code to the meta data
func (code Code) SetHTTP(httpCode int) Code {
	if existingCode, ok := httpMetaData[code.CodeStr()]; ok {
		panic(fmt.Sprintf("http already exists %v for %+v", existingCode, code))
	}
	httpMetaData[code.CodeStr()] = httpCode
	return code
}

// HTTPCode retrieves the HTTP code for a code or its first ancestor with an HTTP code.
// If none are specified, it defaults to 400 BadRequest
func (code Code) HTTPCode() int {
	httpCode := code.MetaDataFromAncestors(httpMetaData)
	if httpCode == nil {
		return http.StatusBadRequest
	}
	return httpCode.(int)
}

var (
	// InternalCode is equivalent to HTTP 500 Internal Server Error
	InternalCode = NewCode("internal").SetHTTP(http.StatusInternalServerError)
	// InvalidInputCode is equivalent to HTTP 400 Bad Request
	InvalidInputCode = NewCode("input").SetHTTP(http.StatusBadRequest)
	// NotFoundCode is equivalent to HTTP 404 Not Found
	NotFoundCode = NewCode("missing").SetHTTP(http.StatusNotFound)
	// StateCode is an error that is invalid due to the current object state
	// This is mapped to HTTP 400
	StateCode = NewCode("state").SetHTTP(http.StatusBadRequest)
)

/*
ErrorCode is the interface that ties an error and RegisteredCode together.

Note that there is an additional interface (HasClientData, please see the docs) that can be defined by an ErrorCode.
This customizes finding structured data for the client.

ErrorCode allows error codes to be defined
without being forced to use a particular struct such as CodedError.
CodedError is convenient for generic errors that wrap many different errors with similar codes.
Please see the docs for CodedError.
For an application specific error with a 1:1 mapping between a go error structure and a RegisteredCode,
You probably want to use this interface directly. Example:

	// First define a normal error type
	type PathBlocked struct {
		start     uint64 `json:"start"`
		end       uint64 `json:"end"`
		obstacle  uint64 `json:"end"`
	}

	func (e PathBlocked) Error() string {
		return fmt.Sprintf("The path %d -> %d has obstacle %d", e.start, e.end, e.obstacle)
	}

	// Now define the code
	var PathBlockedCode = errcode.StateCode.Child("state.blocked")

	// Now attach the code to the error type
	func (e PathBlocked) Code() Code {
		return PathBlockedCode
	}
*/
type ErrorCode interface {
	Error() string // The Error interface
	Code() Code
}

// HasClientData is used to defined how to retrieve the data portion of an ErrorCode to be returned to the client.
// Otherwise the struct itself will be assumed to be all the data by the ClientData method.
// This is provided for exensibility, but may be unnecessary for you.
// Data should be retrieved with the ClientData method.
type HasClientData interface {
	GetClientData() interface{}
}

// ClientData retrieves data from a structure that implements HasClientData
// If HasClientData is not defined it will use the given ErrorCode object.
// Normally this function is used rather than GetClientData.
func ClientData(errCode ErrorCode) interface{} {
	var data interface{} = errCode
	if hasData, ok := errCode.(HasClientData); ok {
		data = hasData.GetClientData()
	}
	return data
}

// JSONFormat is an opinion on how to serilalize an ErrorCode to JSON.
// Msg is the string from Error().
// The Data field is filled in by GetClientData
type JSONFormat struct {
	Data interface{} `json:"data"`
	Msg  string      `json:"msg"`
	Code CodeStr     `json:"code"`
}

// NewJSONFormat turns an ErrorCode into a JSONFormat
func NewJSONFormat(errCode ErrorCode) JSONFormat {
	data := ClientData(errCode)
	return JSONFormat{
		Data: data,
		Msg:  errCode.Error(),
		Code: errCode.Code().CodeStr(),
	}
}

// CodedError is a convenience to attach a code to an error and already satisfy the ErrorCode interface.
// If the error is a struct, that struct will get preseneted as data to the client.
//
// To override the http code or the data representation or just for clearer documentation,
// you are encouraged to wrap CodeError with your own struct that inherits it.
// Look at the implementation of invalidInput, internalError, and notFound.
type CodedError struct {
	GetCode Code
	Err     error
}

// NewCodedError is for constructing broad error kinds (e.g. those representing HTTP codes)
// Which could have many different underlying go errors.
// Eventually you may want to give your go errors more specific codes.
// The second argument is the broad code.
//
// If the error given is already an ErrorCode,
// that will be used as the code instead of the second argument.
func NewCodedError(err error, code Code) CodedError {
	if errcode, ok := err.(ErrorCode); ok {
		code = errcode.Code()
	}
	return CodedError{GetCode: code, Err: err}
}

var _ ErrorCode = (*CodedError)(nil)     // assert implements interface
var _ HasClientData = (*CodedError)(nil) // assert implements interface

func (e CodedError) Error() string {
	return e.Err.Error()
}

// Code returns the GetCode field
func (e CodedError) Code() Code {
	return e.GetCode
}

// GetClientData returns the underlying Err field.
func (e CodedError) GetClientData() interface{} {
	if errCode, ok := e.Err.(ErrorCode); ok {
		return ClientData(errCode)
	}
	return e.Err
}

// invalidInput gives the code InvalidInputCode
type invalidInputErr struct{ CodedError }

// NewInvalidInputErr creates an invalidInput from an err
// If the error is already an ErrorCode it will use that code
// Otherwise it will use InvalidInputCode which gives HTTP 400
func NewInvalidInputErr(err error) ErrorCode {
	return invalidInputErr{NewCodedError(err, InvalidInputCode)}
}

var _ ErrorCode = (*invalidInputErr)(nil)     // assert implements interface
var _ HasClientData = (*invalidInputErr)(nil) // assert implements interface

// internalError gives the code InvalidInputCode
type internalErr struct{ CodedError }

// NewInternalErr creates an internalError from an err
// If the given err is an ErrorCode that is a descendant of InternalCode,
// its code will be used.
// This ensures the intention of sending an HTTP 50x.
func NewInternalErr(err error) ErrorCode {
	code := InternalCode
	if errcode, ok := err.(ErrorCode); ok {
		errCode := errcode.Code()
		if errCode.IsAncestor(InternalCode) {
			code = errCode
		}
	}
	return internalErr{CodedError{GetCode: code, Err: err}}
}

var _ ErrorCode = (*internalErr)(nil)     // assert implements interface
var _ HasClientData = (*internalErr)(nil) // assert implements interface

// notFound gives the code NotFoundCode
type notFoundErr struct{ CodedError }

// NewNotFoundErr creates a notFound from an err
// If the error is already an ErrorCode it will use that code
// Otherwise it will use NotFoundCode which gives HTTP 404
func NewNotFoundErr(err error) ErrorCode {
	return internalErr{NewCodedError(err, NotFoundCode)}
}

var _ ErrorCode = (*notFoundErr)(nil)     // assert implements interface
var _ HasClientData = (*notFoundErr)(nil) // assert implements interface

// checkCodePath checks that the given code string either
// contains no dots or extends the parent code string
func (code Code) checkCodePath() error {
	paths := strings.Split(code.codeStr.String(), ".")
	if len(paths) == 1 {
		return nil
	}
	if code.Parent == nil {
		if len(paths) > 1 {
			return fmt.Errorf("expected no parent paths: %#v", code.codeStr)
		}
	} else {
		parent := *code.Parent
		parentPath := paths[len(paths)-2]
		if parentPath != parent.codeStr.String() {
			return fmt.Errorf("got %#v but expected a path to parent %#v for %#v", parentPath, parent.codeStr, code.codeStr)
		}
	}
	return nil
}
