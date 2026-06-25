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
	"encoding/binary"
	"unicode/utf8"

	"github.com/tikv/pd/tools/pd-ctl/helper/tidb/dbterror"
	mysql "github.com/tikv/pd/tools/pd-ctl/helper/tidb/errno"
)

// JSONTypeCode indicates JSON type.
type JSONTypeCode = byte

const (
	// JSONTypeCodeObject indicates the JSON is an object.
	JSONTypeCodeObject JSONTypeCode = 0x01
	// JSONTypeCodeArray indicates the JSON is an array.
	JSONTypeCodeArray JSONTypeCode = 0x03
	// JSONTypeCodeLiteral indicates the JSON is a literal.
	JSONTypeCodeLiteral JSONTypeCode = 0x04
	// JSONTypeCodeInt64 indicates the JSON is a signed integer.
	JSONTypeCodeInt64 JSONTypeCode = 0x09
	// JSONTypeCodeUint64 indicates the JSON is a unsigned integer.
	JSONTypeCodeUint64 JSONTypeCode = 0x0a
	// JSONTypeCodeFloat64 indicates the JSON is a double float number.
	JSONTypeCodeFloat64 JSONTypeCode = 0x0b
	// JSONTypeCodeString indicates the JSON is a string.
	JSONTypeCodeString JSONTypeCode = 0x0c
	// JSONTypeCodeOpaque indicates the JSON is a opaque
	JSONTypeCodeOpaque JSONTypeCode = 0x0d
	// JSONTypeCodeDate indicates the JSON is a opaque
	JSONTypeCodeDate JSONTypeCode = 0x0e
	// JSONTypeCodeDatetime indicates the JSON is a opaque
	JSONTypeCodeDatetime JSONTypeCode = 0x0f
	// JSONTypeCodeTimestamp indicates the JSON is a opaque
	JSONTypeCodeTimestamp JSONTypeCode = 0x10
	// JSONTypeCodeDuration indicates the JSON is a opaque
	JSONTypeCodeDuration JSONTypeCode = 0x11
)

const (
	// JSONLiteralNil represents JSON null.
	JSONLiteralNil byte = 0x00
	// JSONLiteralTrue represents JSON true.
	JSONLiteralTrue byte = 0x01
	// JSONLiteralFalse represents JSON false.
	JSONLiteralFalse byte = 0x02
)

const unknownTypeErrorMsg = "unknown type: %s"

// jsonSafeSet holds the value true if the ASCII character with the given array
// position can be represented inside a JSON string without any further
// escaping.
//
// All values are true except for the ASCII control characters (0-31), the
// double quote ("), and the backslash character ("\").
var jsonSafeSet = [utf8.RuneSelf]bool{
	' ':      true,
	'!':      true,
	'"':      false,
	'#':      true,
	'$':      true,
	'%':      true,
	'&':      true,
	'\'':     true,
	'(':      true,
	')':      true,
	'*':      true,
	'+':      true,
	',':      true,
	'-':      true,
	'.':      true,
	'/':      true,
	'0':      true,
	'1':      true,
	'2':      true,
	'3':      true,
	'4':      true,
	'5':      true,
	'6':      true,
	'7':      true,
	'8':      true,
	'9':      true,
	':':      true,
	';':      true,
	'<':      true,
	'=':      true,
	'>':      true,
	'?':      true,
	'@':      true,
	'A':      true,
	'B':      true,
	'C':      true,
	'D':      true,
	'E':      true,
	'F':      true,
	'G':      true,
	'H':      true,
	'I':      true,
	'J':      true,
	'K':      true,
	'L':      true,
	'M':      true,
	'N':      true,
	'O':      true,
	'P':      true,
	'Q':      true,
	'R':      true,
	'S':      true,
	'T':      true,
	'U':      true,
	'V':      true,
	'W':      true,
	'X':      true,
	'Y':      true,
	'Z':      true,
	'[':      true,
	'\\':     false,
	']':      true,
	'^':      true,
	'_':      true,
	'`':      true,
	'a':      true,
	'b':      true,
	'c':      true,
	'd':      true,
	'e':      true,
	'f':      true,
	'g':      true,
	'h':      true,
	'i':      true,
	'j':      true,
	'k':      true,
	'l':      true,
	'm':      true,
	'n':      true,
	'o':      true,
	'p':      true,
	'q':      true,
	'r':      true,
	's':      true,
	't':      true,
	'u':      true,
	'v':      true,
	'w':      true,
	'x':      true,
	'y':      true,
	'z':      true,
	'{':      true,
	'|':      true,
	'}':      true,
	'~':      true,
	'\u007f': true,
}

var (
	jsonHexChars = "0123456789abcdef"
	jsonEndian   = binary.LittleEndian
)

const (
	headerSize   = 8 // element size + data size.
	dataSizeOff  = 4
	keyEntrySize = 6 // keyOff +  keyLen
	keyLenOff    = 4
	valTypeSize  = 1
	valEntrySize = 5
)

// JSONModifyType is for modify a JSON. There are three valid values:
// JSONModifyInsert, JSONModifyReplace and JSONModifySet.
type JSONModifyType byte

const (
	// JSONModifyInsert is for insert a new element into a JSON.
	// If an old elemList exists, it would NOT replace it.
	JSONModifyInsert JSONModifyType = 0x01
	// JSONModifyReplace is for replace an old elemList from a JSON.
	// If no elemList exists, it would NOT insert it.
	JSONModifyReplace JSONModifyType = 0x02
	// JSONModifySet = JSONModifyInsert | JSONModifyReplace
	JSONModifySet JSONModifyType = 0x03
)

var (
	// ErrJSONDocumentTooDeep means that json's depth is too deep.
	ErrJSONDocumentTooDeep = dbterror.ClassJSON.NewStd(mysql.ErrJSONDocumentTooDeep)
	// ErrJSONObjectKeyTooLong means JSON object with key length >= 65536 which is not yet supported.
	ErrJSONObjectKeyTooLong = dbterror.ClassTypes.NewStdErr(mysql.ErrJSONObjectKeyTooLong, mysql.MySQLErrName[mysql.ErrJSONObjectKeyTooLong])
)

// json_contains_path function type choices
// See: https://dev.mysql.com/doc/refman/5.7/en/json-search-functions.html#function_json-contains-path
const (
	// 'all': 1 if all paths exist within the document, 0 otherwise.
	JSONContainsPathAll = "all"
	// 'one': 1 if at least one path exists within the document, 0 otherwise.
	JSONContainsPathOne = "one"
)
