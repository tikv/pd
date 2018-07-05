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
	"fmt"
	"net/http"
)

// RegisteredCode helps document that we are using a registered error code that must never change
type RegisteredCode string

const (
	// InternalErrorCode means the operation placed the system is in an inconsistent or unrecoverable state
	// Essentially a handled panic.
	// This is the same as a HTTP 500, so it is not necessary to send this code when using HTTP.k
	InternalErrorCode RegisteredCode = "internal"
)

// ErrorCode defines constant code functions Code() and HTTPCode().
// Code returns a RegisteredCode defined in this module.
// Most implementations of HTTPCode() will return the DefaultHTTPCode.
// The Error() function is not constant: it converts the underlying struct data into a detailed string message.
// The underlying struct data will also be returned as JSON, see ErrorCodeJSON.
type ErrorCode interface {
	Error() string // The Error interface
	HTTPCode() int
	Code() RegisteredCode
}

// JSONFormat is a standard way to serilalize an ErrorCode to JSON.
// Msg is the string from Error().
type JSONFormat struct {
	Data ErrorCode      `json:"data"`
	Msg  string         `json:"msg"`
	Code RegisteredCode `json:"code"`
}

// DefaultHTTPCode is the default used by an ErrorCode
const DefaultHTTPCode int = http.StatusBadRequest

// InternalError attaches additional data to InternalErrorCode.
type InternalError struct {
	Detail string `json:"detail"`
	Err    error  `json:"err"`
}

var _ ErrorCode = (*InternalError)(nil) // assert implements interface

func (e InternalError) Error() string {
	return fmt.Sprintf("An internal error occurred: %s %v", e.Detail, e.Err)
}

// HTTPCode returns 500
func (e InternalError) HTTPCode() int {
	return 500
}

// Code returns InternalErrorCode
func (e InternalError) Code() RegisteredCode {
	return InternalErrorCode
}
