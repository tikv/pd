// Copyright 2017 TiKV Project Authors.
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
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestDecodeBytes(t *testing.T) {
	re := require.New(t)
	key := "abcdefghijklmnopqrstuvwxyz"
	for i := range key {
		_, k, err := DecodeBytes(EncodeBytes([]byte(key[:i])))
		re.NoError(err)
		re.Equal(key[:i], string(k))
	}
}

func TestTableID(t *testing.T) {
	re := require.New(t)
	key := EncodeBytes([]byte("t\x80\x00\x00\x00\x00\x00\x00\xff"))
	re.Equal(int64(0xff), key.TableIdentity().TableID)

	key = EncodeBytes([]byte("t\x80\x00\x00\x00\x00\x00\x00\xff_i\x01\x02"))
	re.Equal(int64(0xff), key.TableIdentity().TableID)

	key = []byte("t\x80\x00\x00\x00\x00\x00\x00\xff")
	re.Equal(int64(0), key.TableIdentity().TableID)

	key = EncodeBytes([]byte("T\x00\x00\x00\x00\x00\x00\x00\xff"))
	re.Equal(int64(0), key.TableIdentity().TableID)

	key = EncodeBytes([]byte("t\x80\x00\x00\x00\x00\x00\xff"))
	re.Equal(int64(0), key.TableIdentity().TableID)
}

func TestTableIDWithKeyspacePrefix(t *testing.T) {
	re := require.New(t)
	tableID := int64(100)
	otherTableID := int64(200)
	keyspaceID := uint32(42)

	classic := EncodeBytes(GenerateTableKey(tableID))
	re.Equal(TableIdentity{TableID: tableID}, classic.TableIdentity())

	for _, mode := range []byte{TxnKeyspaceModePrefix, RawKeyspaceModePrefix} {
		prefix := MakeKeyspacePrefix(mode, keyspaceID)
		identity := TableIdentity{KeyspaceID: keyspaceID, TableID: tableID, HasKeyspace: true}
		encoded := EncodeBytes(append(append([]byte{}, prefix...), GenerateTableKey(tableID)...))
		re.Equal(identity, encoded.TableIdentity(), "mode=%q", mode)

		other := EncodeBytes(append(append([]byte{}, prefix...), GenerateTableKey(otherTableID)...))
		re.Equal(otherTableID, other.TableIdentity().TableID, "mode=%q", mode)
		re.NotEqual(encoded.TableIdentity(), other.TableIdentity(), "mode=%q", mode)

		// Same table: record and index keys must still resolve to the same identity.
		record := EncodeBytes(append(append([]byte{}, prefix...), GenerateRowKey(tableID, 1)...))
		index := EncodeBytes(append(append([]byte{}, prefix...), GenerateIndexKey(tableID, 7)...))
		re.Equal(identity, record.TableIdentity(), "mode=%q", mode)
		re.Equal(identity, index.TableIdentity(), "mode=%q", mode)
	}

	// Same numeric table id under different keyspaces is a different identity.
	ks1 := EncodeBytes(append(MakeKeyspacePrefix(TxnKeyspaceModePrefix, 1), GenerateTableKey(tableID)...))
	ks2 := EncodeBytes(append(MakeKeyspacePrefix(TxnKeyspaceModePrefix, 2), GenerateTableKey(tableID)...))
	re.Equal(ks1.TableIdentity().TableID, ks2.TableIdentity().TableID)
	re.NotEqual(ks1.TableIdentity(), ks2.TableIdentity())

	// A raw key that only happens to start with the keyspace mode byte but is
	// not followed by a TiDB table/meta payload must not be treated as a table key.
	ambiguous := EncodeBytes([]byte{'x', 0x00, 0x00, 0x2a, 'u', 's', 'e', 'r'})
	re.Equal(TableIdentity{}, ambiguous.TableIdentity())
}

func TestMetaOrTableWithKeyspacePrefix(t *testing.T) {
	re := require.New(t)
	tableID := int64(55)
	keyspaceID := uint32(7)
	prefix := MakeKeyspacePrefix(TxnKeyspaceModePrefix, keyspaceID)

	isMeta, id := EncodeBytes(append(append([]byte{}, prefix...), metaPrefix...)).MetaOrTable()
	re.True(isMeta)
	re.Equal(int64(0), id)

	isMeta, id = EncodeBytes(append(append([]byte{}, prefix...), GenerateTableKey(tableID)...)).MetaOrTable()
	re.False(isMeta)
	re.Equal(tableID, id)

	isMeta, id = EncodeBytes([]byte("hello")).MetaOrTable()
	re.False(isMeta)
	re.Equal(int64(0), id)
}

func TestMakeKeyspacePrefix(t *testing.T) {
	re := require.New(t)
	re.Equal([]byte{'r', 0x01, 0x02, 0x03}, MakeKeyspacePrefix(RawKeyspaceModePrefix, 0x010203))
	// Only the lower 24 bits of the keyspace ID are encoded.
	re.Equal([]byte{'x', 0xff, 0xff, 0xff}, MakeKeyspacePrefix(TxnKeyspaceModePrefix, 0xffffff))
}

func TestParseKeyspacePrefix(t *testing.T) {
	re := require.New(t)

	mode, id, ok := ParseKeyspacePrefix([]byte{'r', 0x01, 0x02, 0x03})
	re.True(ok)
	re.Equal(RawKeyspaceModePrefix, mode)
	re.Equal(uint32(0x010203), id)

	mode, id, ok = ParseKeyspacePrefix([]byte{'x', 0xff, 0xff, 0xff, 't'})
	re.True(ok)
	re.Equal(TxnKeyspaceModePrefix, mode)
	re.Equal(uint32(0xffffff), id)

	// Too short.
	_, _, ok = ParseKeyspacePrefix([]byte{'x', 0x01, 0x02})
	re.False(ok)
	// Unknown mode byte.
	_, _, ok = ParseKeyspacePrefix([]byte{'t', 0x01, 0x02, 0x03})
	re.False(ok)
}

func TestGenerateRecordAndIndexKeys(t *testing.T) {
	re := require.New(t)
	tableID := int64(42)
	indexID := int64(7)

	rowKeyPrefix := GenerateRecordKeyPrefix(tableID)
	rowKey := GenerateRowKey(tableID, 1)
	re.Equal(rowKeyPrefix, rowKey[:len(rowKeyPrefix)])
	re.Equal(tableID, EncodeBytes(rowKey).TableIdentity().TableID)

	indexKey := GenerateIndexKey(tableID, indexID)
	_, decodedIndexKey, err := DecodeBytes(EncodeBytes(indexKey))
	re.NoError(err)
	re.Equal(indexKey, decodedIndexKey)
	re.Equal(tableID, EncodeBytes(indexKey).TableIdentity().TableID)
}
