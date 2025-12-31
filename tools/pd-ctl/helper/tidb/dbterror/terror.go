// Copyright 2025 TiKV Project Authors.
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

// Notes: it's a copy from tidb

package dbterror

import (
	"github.com/tikv/pd/tools/pd-ctl/helper/tidb/errno"
	"github.com/tikv/pd/tools/pd-ctl/helper/tidb/terror"
)

// ErrClass represents a class of errors.
type ErrClass struct{ terror.ErrClass }

// Error classes.
var (
	ClassTypes = ErrClass{terror.ClassTypes}
	ClassJSON  = ErrClass{terror.ClassJSON}
)

// NewStd calls New using the standard message for the error code
// Attention:
// this method is not goroutine-safe and
// usually be used in global variable initializer
func (ec ErrClass) NewStd(code terror.ErrCode) *terror.Error {
	return ec.NewStdErr(code, errno.MySQLErrName[uint16(code)])
}
