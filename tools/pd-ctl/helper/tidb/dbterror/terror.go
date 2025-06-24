package dbterror

import (
	"github.com/tikv/pd/tools/pd-ctl/helper/tidb/errno"
	"github.com/tikv/pd/tools/pd-ctl/helper/tidb/terror"
)

// ErrClass represents a class of errors.
type ErrClass struct{ terror.ErrClass }

// Error classes.
var (
	ClassTypes      = ErrClass{terror.ClassTypes}
	ClassJSON       = ErrClass{terror.ClassJSON}
)

// NewStd calls New using the standard message for the error code
// Attention:
// this method is not goroutine-safe and
// usually be used in global variable initializer
func (ec ErrClass) NewStd(code terror.ErrCode) *terror.Error {
	return ec.NewStdErr(code, errno.MySQLErrName[uint16(code)])
}
