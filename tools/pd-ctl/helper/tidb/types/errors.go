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

package types

import (
	"github.com/tikv/pd/tools/pd-ctl/helper/tidb/dbterror"
	mysql "github.com/tikv/pd/tools/pd-ctl/helper/tidb/errno"
)

// const strings for ErrWrongValue
const (
	DateTimeStr  = "datetime"
	DateStr      = "date"
	TimeStr      = "time"
	TimestampStr = "timestamp"
)

var (
	// ErrTruncated is returned when data has been truncated during conversion.
	ErrTruncated = dbterror.ClassTypes.NewStd(mysql.WarnDataTruncated)
	// ErrOverflow is returned when data is out of range for a field type.
	ErrOverflow = dbterror.ClassTypes.NewStd(mysql.ErrDataOutOfRange)
	// ErrDivByZero is return when do division by 0.
	ErrDivByZero = dbterror.ClassTypes.NewStd(mysql.ErrDivisionByZero)
	// ErrBadNumber is return when parsing an invalid binary decimal number.
	ErrBadNumber = dbterror.ClassTypes.NewStd(mysql.ErrBadNumber)
	// ErrDatetimeFunctionOverflow is returned when the calculation in datetime function cause overflow.
	ErrDatetimeFunctionOverflow = dbterror.ClassTypes.NewStd(mysql.ErrDatetimeFunctionOverflow)
	// ErrTruncatedWrongVal is returned when data has been truncated during conversion.
	ErrTruncatedWrongVal = dbterror.ClassTypes.NewStd(mysql.ErrTruncatedWrongValue)
	// ErrWrongValue is returned when the input value is in wrong format.
	ErrWrongValue = dbterror.ClassTypes.NewStdErr(mysql.ErrTruncatedWrongValue, mysql.MySQLErrName[mysql.ErrWrongValue])
)
