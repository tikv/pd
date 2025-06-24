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

package errno

import "github.com/tikv/pd/tools/pd-ctl/helper/tidb/mysql"

// MySQLErrName maps error code to MySQL error messages.
// Note: all ErrMessage to be added should be considered about the log redaction
// by setting the suitable configuration in the second argument of mysql.Message.
// See https://github.com/pingcap/tidb/blob/master/errno/logredaction.md
var MySQLErrName = map[uint16]*mysql.ErrMessage{
	ErrWrongFieldSpec:                   mysql.Message("Incorrect column specifier for column '%-.192s'", nil),
	ErrParse:                            mysql.Message("%s %s", nil),
	ErrJSONVacuousPath:                  mysql.Message("The path expression '$' is not allowed in this context.", nil),
	ErrJSONBadOneOrAllArg:               mysql.Message("The oneOrAll argument to %s may take these values: 'one' or 'all'.", nil),
	ErrTooBigFieldlength:                mysql.Message("Column length too big for column '%-.192s' (max = %d); use BLOB or TEXT instead", nil),
	ErrTooBigSet:                        mysql.Message("Too many strings for column %-.192s and SET", nil),
	ErrSyntax:                           mysql.Message("You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use", nil),
	ErrWarnDataOutOfRange:               mysql.Message("Out of range value for column '%s' at row %d", nil),
	WarnDataTruncated:                   mysql.Message("Data truncated for column '%s' at row %d", nil),
	ErrDuplicatedValueInType:            mysql.Message("Column '%-.100s' has duplicated value '%-.64s' in %s", []int{1}),
	ErrTruncatedWrongValue:              mysql.Message("Truncated incorrect %-.64s value: '%-.128s'", []int{1}),
	ErrDivisionByZero:                   mysql.Message("Division by 0", nil),
	ErrIllegalValueForType:              mysql.Message("Illegal %s '%-.192s' value found during parsing", []int{1}),
	ErrDataTooLong:                      mysql.Message("Data too long for column '%s' at row %d", nil),
	ErrWrongValueForType:                mysql.Message("Incorrect %-.32s value: '%-.128s' for function %-.32s", nil),
	ErrTooBigScale:                      mysql.Message("Too big scale %d specified for column '%-.192s'. Maximum is %d.", nil),
	ErrTooBigPrecision:                  mysql.Message("Too-big precision %d specified for '%-.192s'. Maximum is %d.", nil),
	ErrMBiggerThanD:                     mysql.Message("For float(M,D), double(M,D) or decimal(M,D), M must be >= D (column '%-.192s').", nil),
	ErrTooBigDisplaywidth:               mysql.Message("Display width out of range for column '%-.192s' (max = %d)", nil),
	ErrDatetimeFunctionOverflow:         mysql.Message("Datetime function: %-.32s field overflow", nil),
	ErrWrongValue:                       mysql.Message("Incorrect %-.32s value: '%-.128s'", []int{1}),
	WarnOptionIgnored:                   mysql.Message("<%-.64s> option ignored", nil),
	WarnPluginDeleteBuiltin:             mysql.Message("Built-in plugins cannot be deleted", nil),
	WarnPluginBusy:                      mysql.Message("Plugin is busy and will be uninstalled on shutdown", nil),
	WarnNonASCIISeparatorNotImplemented: mysql.Message("Non-ASCII separator arguments are not fully supported", nil),
	WarnCondItemTruncated:               mysql.Message("Data truncated for condition item '%s'", nil),
	ErrDataOutOfRange:                   mysql.Message("%s value is out of range in '%s'", []int{1}),
	WarnOptionBelowLimit:                mysql.Message("The value of '%s' should be no less than the value of '%s'", nil),
	ErUserAccessDeniedForUserAccountBlockedByPasswordLock: mysql.Message("Access denied for user '%s'@'%s'. Account is blocked for %s day(s) (%s day(s) remaining) due to %d consecutive failed logins.", nil),
	ErrInvalidFieldSize:    mysql.Message("Invalid size for column '%s'.", nil),
	ErrJSONDocumentTooDeep: mysql.Message("The JSON document exceeds the maximum depth.", nil),

	// TiDB errors.
	ErrBadNumber:              mysql.Message("Bad Number", nil),
	ErrCastAsSignedOverflow:   mysql.Message("Cast to signed converted positive out-of-range integer to its negative complement", nil),
	ErrCastNegIntAsUnsigned:   mysql.Message("Cast to unsigned converted negative integer to it's positive complement", nil),
	ErrInvalidYearFormat:      mysql.Message("invalid year format", nil),
	ErrInvalidYear:            mysql.Message("invalid year", nil),
	ErrIncorrectDatetimeValue: mysql.Message("Incorrect datetime value: '%s'", []int{0}),
	ErrInvalidWeekModeFormat:  mysql.Message("invalid week mode format: '%v'", nil),

	ErrJSONObjectKeyTooLong:        mysql.Message("TiDB does not yet support JSON objects with the key length >= 65536", nil),
	ErrPartitionStatsMissing:       mysql.Message("Build global-level stats failed due to missing partition-level stats: %s", nil),
	ErrPartitionColumnStatsMissing: mysql.Message("Build global-level stats failed due to missing partition-level column stats: %s, please run analyze table to refresh columns of all partitions", nil),
}
