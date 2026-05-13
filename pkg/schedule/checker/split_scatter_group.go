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

func resolveSplitScatterRangeHint(region *core.RegionInfo) splitScatterRangeHint {
	_, decoded, err := codec.DecodeBytes(region.GetStartKey())
	if err != nil || !bytes.HasPrefix(decoded, splitScatterTablePrefix) {
		return splitScatterRangeHint{}
	}
	rest := decoded[len(splitScatterTablePrefix):]
	rest, tableID, err := codec.DecodeInt(rest)
	if err != nil {
		return splitScatterRangeHint{}
	}

	tablePrefix := append([]byte(nil), codec.GenerateTableKey(tableID)...)
	tableGroup := makeSplitScatterTableGroup(tableID)
	if !bytes.HasPrefix(rest, splitScatterIndexPrefix) {
		return splitScatterPrefixRangeWithGroup(tablePrefix, tableGroup)
	}

	indexRest := rest[len(splitScatterIndexPrefix):]
	_, indexID, err := codec.DecodeInt(indexRest)
	if err != nil {
		return splitScatterRangeHint{}
	}

	indexPrefix := codec.GenerateIndexKey(tableID, indexID)
	indexGroup := makeSplitScatterIndexGroup(tableID, indexID)
	indexRange := splitScatterPrefixRangeWithGroup(indexPrefix, indexGroup)
	endKey := region.GetEndKey()

	// We intentionally over-approximate ambiguous table-key ranges. If PD can
	// no longer prove the region stays within a single index prefix, it falls
	// back to the table-scoped group instead of dropping back to the family
	// group, so table-boundary splits and merged ranges still participate in
	// the broader scatter continuity/baseline.
	// Both endKey and indexRange.endKey are MemComparable-encoded, so
	// bytes.Compare correctly reflects the key ordering.
	if len(endKey) == 0 || len(indexRange.startKey) == 0 || len(indexRange.endKey) == 0 || bytes.Compare(endKey, indexRange.endKey) > 0 {
		return splitScatterPrefixRangeWithGroup(tablePrefix, tableGroup)
	}
	return indexRange
}

func splitScatterPrefixRange(rawPrefix []byte) splitScatterRangeHint {
	return splitScatterPrefixRangeWithGroup(rawPrefix, "")
}

func splitScatterPrefixRangeWithGroup(rawPrefix []byte, scatterGroup string) splitScatterRangeHint {
	startKey := append([]byte(nil), codec.EncodeBytes(rawPrefix)...)
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
		endKey:       append([]byte(nil), codec.EncodeBytes(endRawPrefix)...),
		scatterGroup: scatterGroup,
	}
}

func makeSplitScatterTableGroup(tableID int64) string {
	return fmt.Sprintf("split-scatter-table-%d", tableID)
}

func makeSplitScatterIndexGroup(tableID, indexID int64) string {
	return fmt.Sprintf("split-scatter-index-%d-%d", tableID, indexID)
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
