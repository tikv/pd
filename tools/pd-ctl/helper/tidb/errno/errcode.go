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

// MySQL error code.
// This value is numeric. It is not portable to other database systems.
const (
	ErrWrongFieldSpec                                     = 1063
	ErrParse                                              = 1064
	ErrTooBigFieldlength                                  = 1074
	ErrTooBigSet                                          = 1097
	ErrSyntax                                             = 1149
	ErrWarnDataOutOfRange                                 = 1264
	WarnDataTruncated                                     = 1265
	ErrDuplicatedValueInType                              = 1291
	ErrTruncatedWrongValue                                = 1292
	ErrDivisionByZero                                     = 1365
	ErrIllegalValueForType                                = 1367
	ErrDataTooLong                                        = 1406
	ErrWrongValueForType                                  = 1411
	ErrTooBigScale                                        = 1425
	ErrTooBigPrecision                                    = 1426
	ErrMBiggerThanD                                       = 1427
	ErrTooBigDisplaywidth                                 = 1439
	ErrDatetimeFunctionOverflow                           = 1441
	ErrWrongValue                                         = 1525
	WarnOptionIgnored                                     = 1618
	WarnPluginDeleteBuiltin                               = 1619
	WarnPluginBusy                                        = 1620
	WarnNonASCIISeparatorNotImplemented                   = 1638
	WarnCondItemTruncated                                 = 1647
	ErrDataOutOfRange                                     = 1690
	WarnOptionBelowLimit                                  = 1708
	ErrInvalidFieldSize                                   = 3013
	ErrJSONVacuousPath                                    = 3153
	ErrJSONBadOneOrAllArg                                 = 3154
	ErrJSONDocumentTooDeep                                = 3157
	ErUserAccessDeniedForUserAccountBlockedByPasswordLock = 3955
	// MariaDB errors.
	// TiDB self-defined errors.
	ErrBadNumber              = 8029
	ErrCastAsSignedOverflow   = 8030
	ErrCastNegIntAsUnsigned   = 8031
	ErrInvalidYearFormat      = 8032
	ErrInvalidYear            = 8033
	ErrIncorrectDatetimeValue = 8034
	ErrInvalidWeekModeFormat  = 8037
	ErrJSONObjectKeyTooLong   = 8129
	ErrPartitionStatsMissing  = 8131

	// Error codes used by TiDB ddl package
	ErrPartitionColumnStatsMissing = 8244
)
