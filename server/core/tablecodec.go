// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"bytes"
	"encoding/binary"

	"github.com/juju/errors"
)

// TableIDDecoder defines method to extract tableID from key
type TableIDDecoder interface {
	DecodeTableID(key Key) int64
}

// DefaultTableIDDecoder is the default decoder.
// unit test will use other mocked decoder.
var DefaultTableIDDecoder defaultTableIDDecoder

type defaultTableIDDecoder struct{}

var tablePrefix = []byte{'t'}

const signMask uint64 = 0x8000000000000000

// Key represents high-level Key type.
type Key []byte

// HasPrefix tests whether the Key begins with prefix.
func (k Key) HasPrefix(prefix Key) bool {
	return bytes.HasPrefix(k, prefix)
}

// DecodeTableID decodes the table ID of the key, if the key is not table key, returns 0.
func (decoder defaultTableIDDecoder) DecodeTableID(key Key) int64 {
	if !key.HasPrefix(tablePrefix) {
		return 0
	}
	key = key[len(tablePrefix):]
	_, tableID, _ := DecodeInt(key)
	return tableID
}

// DecodeInt decodes value encoded by EncodeInt before.
// It returns the leftover un-decoded slice, decoded value if no error.
func DecodeInt(b []byte) ([]byte, int64, error) {
	if len(b) < 8 {
		return nil, 0, errors.New("insufficient bytes to decode value")
	}

	u := binary.BigEndian.Uint64(b[:8])
	v := decodeCmpUintToInt(u)
	b = b[8:]
	return b, v, nil
}

func decodeCmpUintToInt(u uint64) int64 {
	return int64(u ^ signMask)
}

// IsPureTableID return true iff b is consist of tablePrefix and 8-byte tableID
func IsPureTableID(b []byte) bool {
	return len(b) == len(tablePrefix) + 8
}
