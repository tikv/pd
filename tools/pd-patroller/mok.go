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
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/pingcap/tidb/util/codec"
)

var keyFormat = "proto"

type Node struct {
	typ      string // "key", "table_id", "row_id", "index_id", "index_values", "index_value", "ts"
	val      []byte
	variants []*Variant
}

type Variant struct {
	method   string
	children []*Node
}

func N(t string, v []byte) *Node {
	return &Node{typ: t, val: v}
}

func (n *Node) String() string {
	switch n.typ {
	case "key", "raw_key", "index_values":
		switch keyFormat {
		case "hex":
			return `"` + strings.ToUpper(hex.EncodeToString(n.val)) + `"`
		case "base64":
			return `"` + base64.StdEncoding.EncodeToString(n.val) + `"`
		case "proto":
			return `"` + formatProto(string(n.val)) + `"`
		default:
			return fmt.Sprintf("%q", n.val)
		}
	case "key_mode":
		return fmt.Sprintf("key mode: %s", KeyMode(n.val[0]))
	case "keyspace_id":
		tmp := []byte{'\x00'}
		t := append(tmp, n.val...)
		id := binary.BigEndian.Uint32(t)
		return fmt.Sprintf("keyspace: %v", id)
	case "table_id":
		_, id, _ := codec.DecodeInt(n.val)
		return fmt.Sprintf("table: %v", id)
	case "row_id":
		_, id, _ := codec.DecodeInt(n.val)
		return fmt.Sprintf("row: %v", id)
	case "index_id":
		_, id, _ := codec.DecodeInt(n.val)
		return fmt.Sprintf("index: %v", id)
	case "index_value":
		_, d, _ := codec.DecodeOne(n.val)
		s, _ := d.ToString()
		return fmt.Sprintf("kind: %v, value: %v", indexTypeToString[d.Kind()], s)
	case "ts":
		_, ts, _ := codec.DecodeUintDesc(n.val)
		return fmt.Sprintf("ts: %v (%v)", ts, GetTimeFromTS(uint64(ts)))
	}
	return fmt.Sprintf("%v:%q", n.typ, n.val)
}

func (n *Node) Expand() *Node {
	for _, fn := range rules {
		if t := fn(n); t != nil {
			for _, child := range t.children {
				child.Expand()
			}
			n.variants = append(n.variants, t)
		}
	}
	return n
}

func (n *Node) Print() {
	fmt.Println(n.String())
	for i, t := range n.variants {
		t.PrintIndent("", i == len(n.variants)-1)
	}
}

func (n *Node) PrintIndent(indent string, last bool) {
	indent = printIndent(indent, last)
	fmt.Println(n.String())
	for i, t := range n.variants {
		t.PrintIndent(indent, i == len(n.variants)-1)
	}
}

func (v *Variant) PrintIndent(indent string, last bool) {
	indent = printIndent(indent, last)
	fmt.Printf("## %s\n", v.method)
	for i, c := range v.children {
		c.PrintIndent(indent, i == len(v.children)-1)
	}
}

func printIndent(indent string, last bool) string {
	if last {
		fmt.Print(indent + "└─")
		return indent + "  "
	}
	fmt.Print(indent + "├─")
	return indent + "│ "
}
