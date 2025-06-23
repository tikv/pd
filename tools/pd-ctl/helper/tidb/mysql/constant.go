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

package mysql

// MySQL type maximum length.
const (
	// NotFixedDec For arguments that have no fixed number of decimals, the decimals value is set to 31,
	// which is 1 more than the maximum number of decimals permitted for the DECIMAL, FLOAT, and DOUBLE data types.
	NotFixedDec = 31

	MaxIntWidth              = 20
	MaxRealWidth             = 23
	MaxFloatingTypeScale     = 30
	MaxFloatingTypeWidth     = 255
	MaxDecimalScale          = 30
	MaxDecimalWidth          = 65
	MaxDateWidth             = 10 // YYYY-MM-DD.
	MaxDatetimeWidthNoFsp    = 19 // YYYY-MM-DD HH:MM:SS
	MaxDatetimeWidthWithFsp  = 26 // YYYY-MM-DD HH:MM:SS[.fraction]
	MaxDatetimeFullWidth     = 29 // YYYY-MM-DD HH:MM:SS.###### AM
	MaxDurationWidthNoFsp    = 10 // HH:MM:SS
	MaxDurationWidthWithFsp  = 17 // HH:MM:SS[.fraction] -838:59:59.000000 to 838:59:59.000000
	MaxBlobWidth             = 16777216
	MaxLongBlobWidth         = 4294967295
	MaxBitDisplayWidth       = 64
	MaxFloatPrecisionLength  = 24
	MaxDoublePrecisionLength = 53
)
