// Copyright 2016 TiKV Project Authors.
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

package codec

import (
	"bytes"
	"encoding/binary"

	"github.com/pingcap/errors"
)

var (
	tablePrefix  = []byte{'t'}
	metaPrefix   = []byte{'m'}
	recordPrefix = []byte("_r")
	indexPrefix  = []byte("_i")
)

const (
	signMask uint64 = 0x8000000000000000

	encGroupSize = 8
	encMarker    = byte(0xFF)
	encPad       = byte(0x0)

	// RawKeyspaceModePrefix is the raw keyspace prefix mode byte.
	RawKeyspaceModePrefix = byte('r')
	// TxnKeyspaceModePrefix is the txn keyspace prefix mode byte.
	TxnKeyspaceModePrefix = byte('x')
	// KeyspacePrefixLen is the raw keyspace prefix length before memcomparable encoding.
	KeyspacePrefixLen = 4
)

// Key represents high-level Key type.
type Key []byte

// MakeKeyspacePrefix constructs the raw keyspace prefix for the given mode and keyspace ID.
// Keyspace keys encode the lower 24 bits of the keyspace ID after the mode byte.
func MakeKeyspacePrefix(mode byte, id uint32) []byte {
	prefix := make([]byte, KeyspacePrefixLen)
	binary.BigEndian.PutUint32(prefix, id)
	prefix[0] = mode
	return prefix
}

// ParseKeyspacePrefix parses a raw keyspace prefix from key.
// It returns false for keys that do not start with a known keyspace mode byte.
func ParseKeyspacePrefix(key []byte) (mode byte, id uint32, ok bool) {
	if len(key) < KeyspacePrefixLen {
		return 0, 0, false
	}
	mode = key[0]
	if mode != RawKeyspaceModePrefix && mode != TxnKeyspaceModePrefix {
		return 0, 0, false
	}
	idBytes := [KeyspacePrefixLen]byte{0, key[1], key[2], key[3]}
	id = binary.BigEndian.Uint32(idBytes[:])
	return mode, id, true
}

// unwrapKeyspace strips the API v2 keyspace prefix (mode byte + 24-bit id)
// when the remainder is a TiDB meta/table key. Classic keys and raw keys that
// only happen to start with 'x'/'r' are left unchanged with hasKeyspace false.
func unwrapKeyspace(key []byte) (payload []byte, keyspaceID uint32, hasKeyspace bool) {
	_, keyspaceID, ok := ParseKeyspacePrefix(key)
	if !ok {
		return key, 0, false
	}
	rest := key[KeyspacePrefixLen:]
	if !bytes.HasPrefix(rest, tablePrefix) && !bytes.HasPrefix(rest, metaPrefix) {
		return key, 0, false
	}
	return rest, keyspaceID, true
}

// TableIdentity identifies the logical table a key belongs to. HasKeyspace is
// false for classic TiDB keys, distinguishing them from keyspace 0. TableID is
// 0 when the key is not a table key (including meta keys), so all non-table
// keys of one keyspace share a single identity. Two table keys belong to the
// same logical table iff their TableIdentity values are equal.
type TableIdentity struct {
	KeyspaceID  uint32
	TableID     int64
	HasKeyspace bool
}

// TableIdentity returns the keyspace-qualified table identity of an encoded key.
func (k Key) TableIdentity() TableIdentity {
	_, key, err := DecodeBytes(k)
	if err != nil {
		// should never happen for region boundary keys produced by TiKV
		return TableIdentity{}
	}
	key, keyspaceID, hasKeyspace := unwrapKeyspace(key)
	identity := TableIdentity{KeyspaceID: keyspaceID, HasKeyspace: hasKeyspace}
	if bytes.HasPrefix(key, tablePrefix) {
		// A truncated table key fails to decode and keeps TableID 0, i.e. it
		// is treated as a non-table key, matching the historical semantics.
		_, identity.TableID, _ = DecodeInt(key[len(tablePrefix):])
	}
	return identity
}

// MetaOrTable checks if the key is a meta key or table key.
// If the key is a meta key, it returns true and 0.
// If the key is a table key, it returns false and table ID.
// Otherwise, it returns false and 0.
// It supports both classic TiDB keys and API v2 keyspace-prefixed keys.
func (k Key) MetaOrTable() (bool, int64) {
	_, key, err := DecodeBytes(k)
	if err != nil {
		return false, 0
	}
	key, _, _ = unwrapKeyspace(key)
	if bytes.HasPrefix(key, metaPrefix) {
		return true, 0
	}
	if bytes.HasPrefix(key, tablePrefix) {
		_, tableID, _ := DecodeInt(key[len(tablePrefix):])
		return false, tableID
	}
	return false, 0
}

var pads = make([]byte, encGroupSize)

// EncodeBytes guarantees the encoded value is in ascending order for comparison,
// encoding with the following rule:
//
//	[group1][marker1]...[groupN][markerN]
//	group is 8 bytes slice which is padding with 0.
//	marker is `0xFF - padding 0 count`
//
// For example:
//
//	[] -> [0, 0, 0, 0, 0, 0, 0, 0, 247]
//	[1, 2, 3] -> [1, 2, 3, 0, 0, 0, 0, 0, 250]
//	[1, 2, 3, 0] -> [1, 2, 3, 0, 0, 0, 0, 0, 251]
//	[1, 2, 3, 4, 5, 6, 7, 8] -> [1, 2, 3, 4, 5, 6, 7, 8, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247]
//
// Refer: https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format
func EncodeBytes(data []byte) Key {
	// Allocate more space to avoid unnecessary slice growing.
	// Assume that the byte slice size is about `(len(data) / encGroupSize + 1) * (encGroupSize + 1)` bytes,
	// that is `(len(data) / 8 + 1) * 9` in our implement.
	dLen := len(data)
	result := make([]byte, 0, (dLen/encGroupSize+1)*(encGroupSize+1))
	for idx := 0; idx <= dLen; idx += encGroupSize {
		remain := dLen - idx
		padCount := 0
		if remain >= encGroupSize {
			result = append(result, data[idx:idx+encGroupSize]...)
		} else {
			padCount = encGroupSize - remain
			result = append(result, data[idx:]...)
			result = append(result, pads[:padCount]...)
		}

		marker := encMarker - byte(padCount)
		result = append(result, marker)
	}
	return result
}

// EncodeInt appends the encoded value to slice b and returns the appended slice.
// EncodeInt guarantees that the encoded value is in ascending order for comparison.
func EncodeInt(b []byte, v int64) []byte {
	var data [8]byte
	u := encodeIntToCmpUint(v)
	binary.BigEndian.PutUint64(data[:], u)
	return append(b, data[:]...)
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

func encodeIntToCmpUint(v int64) uint64 {
	return uint64(v) ^ signMask
}

func decodeCmpUintToInt(u uint64) int64 {
	return int64(u ^ signMask)
}

// DecodeBytes decodes bytes which is encoded by EncodeBytes before,
// returns the leftover bytes and decoded value if no error.
func DecodeBytes(b []byte) (leftover, decoded []byte, err error) {
	decoded = make([]byte, 0, len(b))
	for {
		if len(b) < encGroupSize+1 {
			return nil, nil, errors.New("insufficient bytes to decode value")
		}

		groupBytes := b[:encGroupSize+1]

		group := groupBytes[:encGroupSize]
		marker := groupBytes[encGroupSize]

		padCount := encMarker - marker
		if padCount > encGroupSize {
			return nil, nil, errors.Errorf("invalid marker byte, group bytes %q", groupBytes)
		}

		realGroupSize := encGroupSize - padCount
		decoded = append(decoded, group[:realGroupSize]...)
		b = b[encGroupSize+1:]

		if padCount != 0 {
			var padByte = encPad
			// Check validity of padding bytes.
			for _, v := range group[realGroupSize:] {
				if v != padByte {
					return nil, nil, errors.Errorf("invalid padding byte, group bytes %q", groupBytes)
				}
			}
			break
		}
	}
	return b, decoded, nil
}

// GenerateTableKey generates a table split key.
func GenerateTableKey(tableID int64) []byte {
	buf := make([]byte, 0, len(tablePrefix)+8)
	buf = append(buf, tablePrefix...)
	buf = EncodeInt(buf, tableID)
	return buf
}

func appendRecordKeyPrefix(buf []byte, tableID int64) []byte {
	buf = append(buf, tablePrefix...)
	buf = EncodeInt(buf, tableID)
	return append(buf, recordPrefix...)
}

// GenerateRecordKeyPrefix generates a table record key prefix.
func GenerateRecordKeyPrefix(tableID int64) []byte {
	buf := make([]byte, 0, len(tablePrefix)+len(recordPrefix)+8)
	return appendRecordKeyPrefix(buf, tableID)
}

// GenerateRowKey generates a row key.
func GenerateRowKey(tableID, rowID int64) []byte {
	buf := make([]byte, 0, len(tablePrefix)+len(recordPrefix)+8*2)
	buf = appendRecordKeyPrefix(buf, tableID)
	buf = EncodeInt(buf, rowID)
	return buf
}

// GenerateIndexKey generates an index key prefix.
func GenerateIndexKey(tableID, indexID int64) []byte {
	buf := make([]byte, 0, len(tablePrefix)+len(indexPrefix)+8*2)
	buf = append(buf, tablePrefix...)
	buf = EncodeInt(buf, tableID)
	buf = append(buf, indexPrefix...)
	buf = EncodeInt(buf, indexID)
	return buf
}
