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

package command

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/util/codec"
)

// mok.go

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

// proto.go

var (
	backslashN  = []byte{'\\', 'n'}
	backslashR  = []byte{'\\', 'r'}
	backslashT  = []byte{'\\', 't'}
	backslashDQ = []byte{'\\', '"'}
	backslashBS = []byte{'\\', '\\'}
)

func formatProto(s string) string {
	var buf bytes.Buffer
	// Loop over the bytes, not the runes.
	for i := 0; i < len(s); i++ {
		// Divergence from C++: we don't escape apostrophes.
		// There's no need to escape them, and the C++ parser
		// copes with a naked apostrophe.
		switch c := s[i]; c {
		case '\n':
			buf.Write(backslashN)
		case '\r':
			buf.Write(backslashR)
		case '\t':
			buf.Write(backslashT)
		case '"':
			buf.Write(backslashDQ)
		case '\\':
			buf.Write(backslashBS)
		default:
			if isprint(c) {
				buf.WriteByte(c)
			} else {
				fmt.Fprintf(&buf, "\\%03o", c)
			}
		}
	}
	return buf.String()
}

// equivalent to C's isprint.
func isprint(c byte) bool {
	return c >= 0x20 && c < 0x7f
}

// rules.go

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

// util.go

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
