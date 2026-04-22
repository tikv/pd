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
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/tikv/pd/tools/pd-ctl/helper/tidb/codec"
)

var keyFormat = "proto"

type Node struct {
	typ       string // "key", "table_id", "row_id", "index_id", "index_values", "index_value", "ts"
	val       []byte
	variants  []*Variant
	decodedBy string // Tracks which rule decoded this node, prevents infinite recursion
	expanded  bool   // Marks whether the node has been expanded
}

type Variant struct {
	method   string
	children []*Node
}

func N(t string, v []byte) *Node {
	return &Node{typ: t, val: v, decodedBy: "", expanded: false}
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
	// Create a map to track visited nodes across the entire expansion
	visited := make(map[string]bool)
	// Start expansion with depth 0
	return n.expandWithDepth(0, visited)
}

// Track visited nodes to prevent cycles and duplicate processing
// Add a depth-limited expand method
func (n *Node) expandWithDepth(depth int, visited map[string]bool) *Node {
	// If already expanded, return immediately
	if n.expanded {
		return n
	}

	// Limit maximum recursion depth to prevent infinite recursion
	maxDepth := 20 // Set maximum recursion depth
	if depth > maxDepth {
		return n
	}

	// Create a unique identifier for this node based on type and value to detect cycles
	nodeKey := fmt.Sprintf("%s:%x", n.typ, n.val)
	if visited[nodeKey] {
		// Cycle detected, return immediately
		return n
	}

	// Mark current node as visited
	visited[nodeKey] = true

	// Mark as expanded
	n.expanded = true

	// Use defer/recover to prevent panics from crashing the program
	defer func() {
		if r := recover(); r != nil {
			// Log the panic but continue execution
			fmt.Printf("Recovered from panic in expandWithDepth: %v\n", r)
		}
	}()

	for _, fn := range rules {
		if t := fn(n); t != nil {
			// Add the variant before processing children to maintain structure
			n.variants = append(n.variants, t)

			for _, child := range t.children {
				// Recursively expand child nodes with incremented depth
				// Use the same visited map to track node visits across the entire tree
				child.expandWithDepth(depth+1, visited)
			}
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

func (n *Node) GetVariants() []*Variant {
	return n.variants
}

func (n *Node) GetValue() []byte {
	return n.val
}

func (n *Node) GetType() string {
	return n.typ
}

func (v *Variant) PrintIndent(indent string, last bool) {
	indent = printIndent(indent, last)
	fmt.Printf("## %s\n", v.method)
	for i, c := range v.children {
		c.PrintIndent(indent, i == len(v.children)-1)
	}
}

func (v *Variant) GetMethod() string {
	return v.method
}

func (v *Variant) GetChildren() []*Node {
	return v.children
}

func printIndent(indent string, last bool) string {
	if last {
		fmt.Print(indent + "└─")
		return indent + "  "
	}
	fmt.Print(indent + "├─")
	return indent + "│ "
}
