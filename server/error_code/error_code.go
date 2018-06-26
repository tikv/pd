// Package errcode reserves error codes with specific names
package errcode

import "fmt"

// Code helps document that we want a registered error code and not any unit16
type Code uint16

const (
	// InternalError means the operation placed the system is in an inconsistent or unrecoverable state
	// Essentially a handled panic.
	// This is the same as a HTTP 500, so it is not necessary to send this code when using HTTP.k
	InternalError Code = 5
	// StoreTombstoned is an invalid operation was attempted on a store which is in a removed state
	StoreTombstoned Code = 100
)

// Every ErrorCode should be associated with a detailed error message
var errorMessages = map[Code]string{
	StoreTombstoned: "The store has been removed",
	InternalError:   "The system has encountered an unrecoverable error",
}

// An ErrorCode may be mappable to a HTTP error code other than 400
var httpCodes = map[Code]uint16{
	StoreTombstoned: 410,
	InternalError:   500,
}

// HTTPCode returns an appropriate HTTP error code for the given Code.
// If no HTTP error code is registered here, the default error code is 400
func HTTPCode(code Code) uint16 {
	httpCode, found := httpCodes[code]
	if !found {
		httpCode = 400
	}
	return httpCode
}

// ErrorCode is an Error that contains a Code.
type ErrorCode struct {
	code Code
	// Optionally add site-specific or dynamic information
	detail string
	// Optionally add an error
	err error
}

// Code makes the ErrorCode code field accessible
func (e ErrorCode) Code() Code {
	return e.code
}

func (e ErrorCode) Error() string {
	msg, found := errorMessages[e.code]
	if !found {
		msg = "error message not found for code"
	}
	return fmt.Sprintf("code %d: %s. %s %v", e.code, msg, e.detail, e.err)
}

// NewCode constructs an error code based error
// To give a site-specific error message, an optional string argument can be given
func NewCode(code Code, message ...string) ErrorCode {
	var detail string
	if message != nil && len(message) > 0 {
		detail = message[0]
	}
	return ErrorCode{code: code, detail: detail, err: nil}
}

// NewCodeError constructs an error code based error
// An additional site-specific error is attached
// To give a site-specific error message, an optional string argument can be given
func NewCodeError(code Code, err error, message ...string) ErrorCode {
	var detail string
	if message != nil && len(message) > 0 {
		detail = message[0]
	}
	return ErrorCode{code: code, detail: detail, err: err}
}
