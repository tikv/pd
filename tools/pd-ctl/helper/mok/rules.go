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
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/tikv/pd/tools/pd-ctl/helper/tidb/codec"
)

type Rule func(*Node) *Variant

var rules []Rule

func init() {
	rules = []Rule{
		DecodeHex,
		DecodeComparableKey,
		DecodeRocksDBKey,
		DecodeKeyspace,
		DecodeTablePrefix,
		DecodeTableRow,
		DecodeTableIndex,
		DecodeIndexValues,
		DecodeLiteral,
		DecodeBase64,
		DecodeIntegerBytes,
		DecodeURLEscaped,
	}
}

func DecodeHex(n *Node) *Variant {
	if n.typ != "key" {
		return nil
	}
	if n.decodedBy == "hex" {
		return nil
	}

	// Use defer/recover to prevent panics
	var decoded []byte
	var err error

	func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("panic in DecodeHex: %v", r)
			}
		}()
		decoded, err = hex.DecodeString(string(n.val))
	}()

	if err != nil {
		return nil
	}
	newNode := N("key", decoded)
	newNode.decodedBy = "hex"
	return &Variant{
		method:   "decode hex key",
		children: []*Node{newNode},
	}
}

func DecodeComparableKey(n *Node) *Variant {
	if n.typ != "key" {
		return nil
	}
	if len(n.val) == 0 || n.decodedBy == "mvcc" {
		return nil
	}

	var decoded []byte
	var b []byte
	var err error

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in DecodeComparableKey: %v", r)
		}
	}()

	b, decoded, err = codec.DecodeBytes(n.val, nil)
	if err != nil {
		return nil
	}

	children := []*Node{N("key", decoded)}
	children[0].decodedBy = "mvcc"

	switch len(b) {
	case 0:
	case 8:
		children = append(children, N("ts", b))
	default:
		return nil
	}
	return &Variant{
		method:   "decode mvcc key",
		children: children,
	}
}

func DecodeRocksDBKey(n *Node) *Variant {
	if n.typ != "key" || len(n.val) == 0 {
		return nil
	}
	if n.val[0] == 'z' {
		return &Variant{
			method:   "decode rocksdb data key",
			children: []*Node{N("key", n.val[1:])},
		}
	}
	return nil
}

func DecodeKeyspace(n *Node) *Variant {
	if n.typ != "key" || len(n.val) == 0 {
		return nil
	}
	if len(n.val) >= 4 && IsValidKeyMode(n.val[0]) {
		keyType := "key"
		if IsRawKeyMode(n.val[0]) {
			keyType = "raw_key"
		}
		return &Variant{
			method:   "decode keyspace",
			children: []*Node{N("key_mode", n.val[0:1]), N("keyspace_id", n.val[1:4]), N(keyType, n.val[4:])},
		}
	}
	return nil
}

func DecodeTablePrefix(n *Node) *Variant {
	if n.typ != "key" || len(n.val) < 9 {
		return nil
	}
	if n.val[0] == 't' {
		return &Variant{
			method:   "table prefix",
			children: []*Node{N("table_id", n.val[1:])},
		}
	}
	return nil
}

func DecodeTableRow(n *Node) *Variant {
	if n.typ != "key" || len(n.val) < 19 {
		return nil
	}
	if n.val[0] == 't' && n.val[9] == '_' && n.val[10] == 'r' {
		handleTyp := "index_values"
		if remain, _, err := codec.DecodeInt(n.val[11:]); err == nil && len(remain) == 0 {
			handleTyp = "row_id"
		}
		return &Variant{
			method:   "table row key",
			children: []*Node{N("table_id", n.val[1:9]), N(handleTyp, n.val[11:])},
		}
	}
	return nil
}

func DecodeTableIndex(n *Node) *Variant {
	if n.typ != "key" || len(n.val) < 19 {
		return nil
	}
	if n.val[0] == 't' && n.val[9] == '_' && n.val[10] == 'i' {
		return &Variant{
			method:   "table index key",
			children: []*Node{N("table_id", n.val[1:9]), N("index_id", n.val[11:19]), N("index_values", n.val[19:])},
		}
	}
	return nil
}

func DecodeIndexValues(n *Node) *Variant {
	if n.typ != "index_values" {
		return nil
	}

	// Use defer/recover to prevent panics from propagating
	var children []*Node
	defer func() {
		if r := recover(); r != nil {
			// If we panic, just return the raw key
			children = append(children, N("key", n.val))
		}
	}()

	for key := n.val; len(key) > 0; {
		// Attempt to decode safely
		var remain []byte
		var e error

		// Use a nested recovery to handle potential panics in codec.DecodeOne
		func() {
			defer func() {
				if r := recover(); r != nil {
					e = fmt.Errorf("panic in DecodeOne: %v", r)
				}
			}()
			remain, _, e = codec.DecodeOne(key)
		}()

		if e != nil {
			children = append(children, N("key", key))
			break
		} else {
			// Safety check to prevent infinite loops
			if len(remain) >= len(key) {
				children = append(children, N("key", key))
				break
			}
			children = append(children, N("index_value", key[:len(key)-len(remain)]))
		}
		key = remain
	}

	return &Variant{
		method:   "decode index values",
		children: children,
	}
}

func DecodeLiteral(n *Node) *Variant {
	if n.typ != "key" {
		return nil
	}
	s, err := decodeKey(string(n.val))
	if err != nil {
		return nil
	}
	if s == string(n.val) {
		return nil
	}
	return &Variant{
		method:   "decode go literal key",
		children: []*Node{N("key", []byte(s))},
	}
}

func DecodeBase64(n *Node) *Variant {
	if n.typ != "key" {
		return nil
	}
	if n.decodedBy == "base64" {
		return nil
	}

	// Safety check for input length
	if len(n.val) > 10000 {
		return nil
	}

	// Use defer/recover to prevent panics
	var s []byte
	var err error

	func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("panic in DecodeBase64: %v", r)
			}
		}()
		s, err = base64.StdEncoding.DecodeString(string(n.val))
	}()

	if err != nil {
		return nil
	}
	child := N("key", []byte(s))
	child.decodedBy = "base64"

	return &Variant{
		method:   "decode base64 key",
		children: []*Node{child},
	}
}

func DecodeIntegerBytes(n *Node) *Variant {
	if n.typ != "key" {
		return nil
	}
	fields := strings.Fields(strings.ReplaceAll(strings.Trim(string(n.val), "[]"), ",", ""))
	var b []byte
	for _, f := range fields {
		c, err := strconv.ParseInt(f, 10, 9)
		if err != nil {
			return nil
		}
		b = append(b, byte(c))
	}
	return &Variant{
		method:   "decode integer bytes",
		children: []*Node{N("key", b)},
	}
}

func DecodeURLEscaped(n *Node) *Variant {
	if n.typ != "key" {
		return nil
	}
	s, err := url.PathUnescape(string(n.val))
	if err != nil || s == string(n.val) {
		return nil
	}
	return &Variant{
		method:   "decode url encoded",
		children: []*Node{N("key", []byte(s))},
	}
}
