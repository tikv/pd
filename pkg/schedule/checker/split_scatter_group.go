// Copyright 2026 TiKV Project Authors.
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

package checker

import (
	"bytes"
	"fmt"

	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/pkg/core"
)

var (
	splitScatterTablePrefix = []byte{'t'}
	splitScatterIndexPrefix = []byte("_i")
)

type splitScatterKeyspaceValidator func(uint32) bool

type splitScatterDecodedKey struct {
	rawKey         []byte
	keyspacePrefix []byte
	keyspaceID     uint32
}

func (key splitScatterDecodedKey) hasKeyspace() bool {
	return len(key.keyspacePrefix) > 0
}

func resolveSplitScatterRangeHintWithKeyspaceValidator(
	region *core.RegionInfo,
	validateKeyspace splitScatterKeyspaceValidator,
) splitScatterRangeHint {
	decodedKey, err := decodeSplitScatterRegionKey(region.GetStartKey(), validateKeyspace)
	if err != nil {
		return splitScatterRangeHint{}
	}
	rawKey := decodedKey.rawKey

	if !bytes.HasPrefix(rawKey, splitScatterTablePrefix) {
		return splitScatterRangeHint{}
	}
	rest := rawKey[len(splitScatterTablePrefix):]
	rest, tableID, err := codec.DecodeInt(rest)
	if err != nil {
		return splitScatterRangeHint{}
	}

	tablePrefix := append([]byte(nil), codec.GenerateTableKey(tableID)...)
	tableGroup := makeSplitScatterTableGroup(tableID)
	if decodedKey.hasKeyspace() {
		tableGroup = makeSplitScatterKeyspaceTableGroup(decodedKey.keyspaceID, tableID)
	}
	if !bytes.HasPrefix(rest, splitScatterIndexPrefix) {
		return splitScatterPrefixRangeWithKeyspaceGroup(decodedKey.keyspacePrefix, tablePrefix, tableGroup)
	}

	indexRest := rest[len(splitScatterIndexPrefix):]
	_, indexID, err := codec.DecodeInt(indexRest)
	if err != nil {
		return splitScatterRangeHint{}
	}

	indexPrefix := codec.GenerateIndexKey(tableID, indexID)
	indexGroup := makeSplitScatterIndexGroup(tableID, indexID)
	if decodedKey.hasKeyspace() {
		indexGroup = makeSplitScatterKeyspaceIndexGroup(decodedKey.keyspaceID, tableID, indexID)
	}
	indexRange := splitScatterPrefixRangeWithKeyspaceGroup(decodedKey.keyspacePrefix, indexPrefix, indexGroup)
	endKey := region.GetEndKey()

	// We intentionally over-approximate ambiguous table-key ranges. If PD can
	// no longer prove the region stays within a single index prefix, it falls
	// back to the table-scoped group instead of dropping back to the family
	// group, so table-boundary splits and merged ranges still participate in
	// the broader scatter continuity/baseline.
	// Both endKey and indexRange.endKey are MemComparable-encoded, so
	// bytes.Compare correctly reflects the key ordering.
	if len(endKey) == 0 || len(indexRange.startKey) == 0 || len(indexRange.endKey) == 0 || bytes.Compare(endKey, indexRange.endKey) > 0 {
		return splitScatterPrefixRangeWithKeyspaceGroup(decodedKey.keyspacePrefix, tablePrefix, tableGroup)
	}
	return indexRange
}

func decodeSplitScatterRegionKey(
	regionKey []byte,
	validateKeyspace splitScatterKeyspaceValidator,
) (splitScatterDecodedKey, error) {
	_, rawKey, err := codec.DecodeBytes(regionKey)
	if err != nil {
		return splitScatterDecodedKey{}, err
	}
	decodedKey := splitScatterDecodedKey{rawKey: rawKey}
	mode, keyspaceID, ok := codec.ParseKeyspacePrefix(rawKey)
	if !ok || mode != codec.TxnKeyspaceModePrefix {
		return decodedKey, nil
	}

	// Split-scatter range hints are table/index-scoped, so only TiDB txn
	// keyspace keys from a known keyspace range are decoded. With only a
	// region key, an API V2 txn prefix can be indistinguishable from a
	// classic/raw user key that starts with the same bytes.
	if validateKeyspace == nil || !validateKeyspace(keyspaceID) {
		return decodedKey, nil
	}
	decodedKey.rawKey = rawKey[codec.KeyspacePrefixLen:]
	decodedKey.keyspacePrefix = codec.MakeKeyspacePrefix(mode, keyspaceID)
	decodedKey.keyspaceID = keyspaceID
	return decodedKey, nil
}

func splitScatterPrefixRange(rawPrefix []byte) splitScatterRangeHint {
	return splitScatterPrefixRangeWithGroup(rawPrefix, "")
}

func splitScatterPrefixRangeWithGroup(rawPrefix []byte, scatterGroup string) splitScatterRangeHint {
	return splitScatterPrefixRangeWithKeyspaceGroup(nil, rawPrefix, scatterGroup)
}

func splitScatterPrefixRangeWithKeyspaceGroup(keyspacePrefix, rawPrefix []byte, scatterGroup string) splitScatterRangeHint {
	startKey := codec.EncodeBytes(appendKeyspacePrefix(keyspacePrefix, rawPrefix))
	endRawPrefix := splitScatterNextPrefix(rawPrefix)
	if len(endRawPrefix) == 0 {
		// Current callers use TiDB table/index prefixes, which always start
		// with 't' and therefore have a finite next prefix. Keep the fallback
		// here so this helper remains safe if it is ever used with an all-0xff
		// prefix.
		return splitScatterRangeHint{startKey: startKey, scatterGroup: scatterGroup}
	}
	return splitScatterRangeHint{
		startKey:     startKey,
		endKey:       codec.EncodeBytes(appendKeyspacePrefix(keyspacePrefix, endRawPrefix)),
		scatterGroup: scatterGroup,
	}
}

func appendKeyspacePrefix(keyspacePrefix, rawKey []byte) []byte {
	key := make([]byte, 0, len(keyspacePrefix)+len(rawKey))
	key = append(key, keyspacePrefix...)
	key = append(key, rawKey...)
	return key
}

func makeSplitScatterTableGroup(tableID int64) string {
	return fmt.Sprintf("split-scatter-table-%d", tableID)
}

func makeSplitScatterIndexGroup(tableID, indexID int64) string {
	return fmt.Sprintf("split-scatter-index-%d-%d", tableID, indexID)
}

func makeSplitScatterKeyspaceTableGroup(keyspaceID uint32, tableID int64) string {
	return fmt.Sprintf("split-scatter-keyspace-%d-table-%d", keyspaceID, tableID)
}

func makeSplitScatterKeyspaceIndexGroup(keyspaceID uint32, tableID, indexID int64) string {
	return fmt.Sprintf("split-scatter-keyspace-%d-index-%d-%d", keyspaceID, tableID, indexID)
}

func splitScatterNextPrefix(key []byte) []byte {
	next := append([]byte(nil), key...)
	for i := len(next) - 1; i >= 0; i-- {
		if next[i] == 0xFF {
			continue
		}
		next[i]++
		return next[:i+1]
	}
	return nil
}
