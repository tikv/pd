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

// MySQL type information.
const (
	TypeUnspecified byte = 0
	TypeTiny        byte = 1 // TINYINT
	TypeShort       byte = 2 // SMALLINT
	TypeLong        byte = 3 // INT
	TypeFloat       byte = 4
	TypeDouble      byte = 5
	TypeNull        byte = 6
	TypeTimestamp   byte = 7
	TypeLonglong    byte = 8 // BIGINT
	TypeInt24       byte = 9 // MEDIUMINT
	TypeDate        byte = 10
	/* TypeDuration original name was TypeTime, renamed to TypeDuration to resolve the conflict with Go type Time.*/
	TypeDuration byte = 11
	TypeDatetime byte = 12
	TypeYear     byte = 13
	TypeNewDate  byte = 14
	TypeVarchar  byte = 15
	TypeBit      byte = 16

	TypeJSON       byte = 0xf5
	TypeNewDecimal byte = 0xf6
	TypeEnum       byte = 0xf7
	TypeSet        byte = 0xf8
	TypeTinyBlob   byte = 0xf9
	TypeMediumBlob byte = 0xfa
	TypeLongBlob   byte = 0xfb
	TypeBlob       byte = 0xfc
	TypeVarString  byte = 0xfd
	TypeString     byte = 0xfe /* TypeString is char type */
	TypeGeometry   byte = 0xff

	TypeTiDBVectorFloat32 byte = 0xe1
)
