package apiutil

import "github.com/joomcode/errorx"

var (
	apiUtilErrors = errorx.NewNamespace("apiutil")
	// ErrJSON is error info for json related error.
	ErrJSON = apiUtilErrors.NewType("json_error")
	// ErrIO is error info for IO related error.
	ErrIO = apiUtilErrors.NewType("io_error")
)
