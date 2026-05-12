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
	"bytes"
	"encoding/binary"
	"unicode"
	"unicode/utf8"

	"github.com/pingcap/errors"
)

// PeekBytesAsJSON trys to peek some bytes from b, until
// we can deserialize a JSON from those bytes.
func PeekBytesAsJSON(b []byte) (n int, err error) {
	if len(b) <= 0 {
		err = errors.New("Cant peek from empty bytes")
		return
	}
	switch c := b[0]; c {
	case JSONTypeCodeObject, JSONTypeCodeArray:
		if len(b) >= valTypeSize+headerSize {
			size := jsonEndian.Uint32(b[valTypeSize+dataSizeOff:])
			n = valTypeSize + int(size)
			return
		}
	case JSONTypeCodeString:
		strLen, lenLen := binary.Uvarint(b[valTypeSize:])
		return valTypeSize + int(strLen) + lenLen, nil
	case JSONTypeCodeInt64, JSONTypeCodeUint64, JSONTypeCodeFloat64, JSONTypeCodeDate, JSONTypeCodeDatetime, JSONTypeCodeTimestamp:
		n = valTypeSize + 8
		return
	case JSONTypeCodeLiteral:
		n = valTypeSize + 1
		return
	case JSONTypeCodeOpaque:
		bufLen, lenLen := binary.Uvarint(b[valTypeSize+1:])
		return valTypeSize + 1 + int(bufLen) + lenLen, nil
	case JSONTypeCodeDuration:
		n = valTypeSize + 12
		return
	}
	err = errors.New("Invalid JSON bytes")
	return
}

func buildBinaryJSONArray(elems []BinaryJSON) BinaryJSON {
	totalSize := headerSize + len(elems)*valEntrySize
	for _, elem := range elems {
		if elem.TypeCode != JSONTypeCodeLiteral {
			totalSize += len(elem.Value)
		}
	}
	buf := make([]byte, headerSize+len(elems)*valEntrySize, totalSize)
	jsonEndian.PutUint32(buf, uint32(len(elems)))
	jsonEndian.PutUint32(buf[dataSizeOff:], uint32(totalSize))
	buf = buildBinaryJSONElements(buf, headerSize, elems)
	return BinaryJSON{TypeCode: JSONTypeCodeArray, Value: buf}
}

func buildBinaryJSONElements(buf []byte, entryStart int, elems []BinaryJSON) []byte {
	for i, elem := range elems {
		buf[entryStart+i*valEntrySize] = elem.TypeCode
		if elem.TypeCode == JSONTypeCodeLiteral {
			buf[entryStart+i*valEntrySize+valTypeSize] = elem.Value[0]
		} else {
			jsonEndian.PutUint32(buf[entryStart+i*valEntrySize+valTypeSize:], uint32(len(buf)))
			buf = append(buf, elem.Value...)
		}
	}
	return buf
}

// quoteJSONString escapes interior quote and other characters for json functions.
//
// The implementation of `JSON_QUOTE` doesn't use this function. The `JSON_QUOTE` used `goJSON` to encode the string
// directly. Therefore, this function is not compatible with `JSON_QUOTE` function for the convience of internal usage.
//
// This function will add extra quotes to the string if it contains special characters which needs to escape, or it's not
// a valid ECMAScript identifier.
func quoteJSONString(s string) string {
	var escapeByteMap = map[byte]string{
		'\\': "\\\\",
		'"':  "\\\"",
		'\b': "\\b",
		'\f': "\\f",
		'\n': "\\n",
		'\r': "\\r",
		'\t': "\\t",
	}

	ret := new(bytes.Buffer)
	ret.WriteByte('"')

	start := 0
	hasEscaped := false

	for i := 0; i < len(s); {
		if b := s[i]; b < utf8.RuneSelf {
			escaped, ok := escapeByteMap[b]
			if ok {
				if start < i {
					ret.WriteString(s[start:i])
				}
				hasEscaped = true
				ret.WriteString(escaped)
				i++
				start = i
			} else {
				i++
			}
		} else {
			c, size := utf8.DecodeRuneInString(s[i:])
			if c == utf8.RuneError && size == 1 { // refer to codes of `binary.jsonMarshalStringTo`
				if start < i {
					ret.WriteString(s[start:i])
				}
				hasEscaped = true
				ret.WriteString(`\ufffd`)
				i += size
				start = i
				continue
			}
			i += size
		}
	}

	if start < len(s) {
		ret.WriteString(s[start:])
	}

	if hasEscaped || !isEcmascriptIdentifier(s) {
		ret.WriteByte('"')
		return ret.String()
	}
	return ret.String()[1:]
}

func isEcmascriptIdentifier(s string) bool {
	if s == "" {
		return false
	}

	for i := 0; i < len(s); i++ {
		c := rune(s[i])

		// accept Latin1 letter
		if c <= unicode.MaxLatin1 && unicode.IsLetter(c) {
			continue
		}
		// accept '$' and '_'
		if c == '$' || c == '_' {
			continue
		}

		// the first character must be a letter or '$' or '_'
		if i == 0 {
			return false
		}

		// accept unicode combining mark
		if unicode.Is(unicode.Mc, c) {
			continue
		}
		// accept digit
		if unicode.IsDigit(c) {
			continue
		}
		// accept unicode connector punctuation
		if unicode.Is(unicode.Pc, c) {
			continue
		}
		// accept ZWNJ and ZWJ
		if c == 0x200C || c == 0x200D {
			continue
		}

		return false
	}
	return true
}

// GetElemDepth for JSON_DEPTH
// Returns the maximum depth of a JSON document
// rules referenced by MySQL JSON_DEPTH function
// [https://dev.mysql.com/doc/refman/5.7/en/json-attribute-functions.html#function_json-depth]
// 1) An empty array, empty object, or scalar value has depth 1.
// 2) A nonempty array containing only elements of depth 1 or nonempty object containing only member values of depth 1 has depth 2.
// 3) Otherwise, a JSON document has depth greater than 2.
// e.g. depth of '{}', '[]', 'true': 1
// e.g. depth of '[10, 20]', '[[], {}]': 2
// e.g. depth of '[10, {"a": 20}]': 3
func (bj BinaryJSON) GetElemDepth() int {
	switch bj.TypeCode {
	case JSONTypeCodeObject:
		elemCount := bj.GetElemCount()
		maxDepth := 0
		for i := 0; i < elemCount; i++ {
			obj := bj.objectGetVal(i)
			depth := obj.GetElemDepth()
			if depth > maxDepth {
				maxDepth = depth
			}
		}
		return maxDepth + 1
	case JSONTypeCodeArray:
		elemCount := bj.GetElemCount()
		maxDepth := 0
		for i := 0; i < elemCount; i++ {
			obj := bj.ArrayGetElem(i)
			depth := obj.GetElemDepth()
			if depth > maxDepth {
				maxDepth = depth
			}
		}
		return maxDepth + 1
	default:
		return 1
	}
}
