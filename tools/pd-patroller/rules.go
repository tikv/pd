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

package main

import (
	"encoding/base64"
	"encoding/hex"
	"net/url"
	"strconv"
	"strings"

	"github.com/pingcap/tidb/util/codec"
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
	decoded, err := hex.DecodeString(string(n.val))
	if err != nil {
		return nil
	}
	return &Variant{
		method:   "decode hex key",
		children: []*Node{N("key", decoded)},
	}
}

func DecodeComparableKey(n *Node) *Variant {
	if n.typ != "key" {
		return nil
	}
	b, decoded, err := codec.DecodeBytes(n.val, nil)
	if err != nil {
		return nil
	}
	children := []*Node{N("key", decoded)}
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
	if n.typ != "key" {
		return nil
	}
	if len(n.val) > 0 && n.val[0] == 'z' {
		return &Variant{
			method:   "decode rocksdb data key",
			children: []*Node{N("key", n.val[1:])},
		}
	}
	return nil
}

func DecodeKeyspace(n *Node) *Variant {
	if n.typ == "key" && IsValidKeyMode(n.val[0]) && len(n.val) >= 4 {
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
	if n.typ == "key" && len(n.val) >= 9 && n.val[0] == 't' {
		return &Variant{
			method:   "table prefix",
			children: []*Node{N("table_id", n.val[1:])},
		}
	}
	return nil
}

func DecodeTableRow(n *Node) *Variant {
	if n.typ == "key" && len(n.val) >= 19 && n.val[0] == 't' && n.val[9] == '_' && n.val[10] == 'r' {
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
	if n.typ == "key" && len(n.val) >= 19 && n.val[0] == 't' && n.val[9] == '_' && n.val[10] == 'i' {
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
	var children []*Node
	for key := n.val; len(key) > 0; {
		remain, _, e := codec.DecodeOne(key)
		if e != nil {
			children = append(children, N("key", key))
			break
		} else {
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
	s, err := base64.StdEncoding.DecodeString(string(n.val))
	if err != nil {
		return nil
	}
	child := N("key", []byte(s))
	child.Expand()
	if len(child.variants) == 0 {
		return nil
	}
	return &Variant{
		method:   "decode base64 key",
		children: []*Node{N("key", []byte(s))},
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
