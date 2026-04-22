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

// Notes: it's a copy from mok https://github.com/oh-my-tidb/mok

package mok

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"time"
)

func decodeKey(text string) (string, error) {
	var buf []byte
	r := bytes.NewBuffer([]byte(text))
	for {
		c, err := r.ReadByte()
		if err != nil {
			if err != io.EOF {
				return "", err
			}
			break
		}
		if c != '\\' {
			buf = append(buf, c)
			continue
		}
		n := r.Next(1)
		if len(n) == 0 {
			return "", io.EOF
		}
		// See: https://golang.org/ref/spec#Rune_literals
		if idx := strings.IndexByte(`abfnrtv\'"`, n[0]); idx != -1 {
			buf = append(buf, []byte("\a\b\f\n\r\t\v\\'\"")[idx])
			continue
		}

		switch n[0] {
		case 'x':
			fmt.Sscanf(string(r.Next(2)), "%02x", &c)
			buf = append(buf, c)
		default:
			n = append(n, r.Next(2)...)
			_, err := fmt.Sscanf(string(n), "%03o", &c)
			if err != nil {
				return "", err
			}
			buf = append(buf, c)
		}
	}
	return string(buf), nil
}

var indexTypeToString = map[byte]string{
	0:  "Null",
	1:  "Int64",
	2:  "Uint64",
	3:  "Float32",
	4:  "Float64",
	5:  "String",
	6:  "Bytes",
	7:  "BinaryLiteral",
	8:  "MysqlDecimal",
	9:  "MysqlDuration",
	10: "MysqlEnum",
	11: "MysqlBit",
	12: "MysqlSet",
	13: "MysqlTime",
	14: "Interface",
	15: "MinNotNull",
	16: "MaxValue",
	17: "Raw",
	18: "MysqlJSON",
}

// GetTimeFromTS extracts time.Time from a timestamp.
func GetTimeFromTS(ts uint64) time.Time {
	ms := int64(ts >> 18)
	return time.Unix(ms/1e3, (ms%1e3)*1e6)
}

type KeyMode byte

const (
	KeyModeTxn KeyMode = 'x'
	KeyModeRaw KeyMode = 'r'
)

func IsValidKeyMode(b byte) bool {
	return b == byte(KeyModeTxn) || b == byte(KeyModeRaw)
}

func IsRawKeyMode(b byte) bool {
	return b == byte(KeyModeRaw)
}

func (k KeyMode) String() string {
	switch k {
	case KeyModeTxn:
		return "txnkv"
	case KeyModeRaw:
		return "rawkv"
	default:
		return "other"
	}
}

func FromStringToKeyMode(s string) *KeyMode {
	var keyMode KeyMode
	switch s {
	case "txnkv":
		keyMode = KeyModeTxn
	case "rawkv":
		keyMode = KeyModeRaw
	default:
	}
	return &keyMode
}

func ParseRawKey(s string, format string) ([]byte, error) {
	switch format {
	case "hex":
		return hex.DecodeString(s)
	case "str": // for `s` with all characters printable.
		return []byte(s), nil
	default:
		return nil, fmt.Errorf("invalid raw key format: %s", format)
	}
}
