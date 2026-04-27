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
	if !bytes.HasPrefix(rest, splitScatterIndexPrefix) {
		return splitScatterPrefixRange(tablePrefix)
	}

	indexRest := rest[len(splitScatterIndexPrefix):]
	_, indexID, err := codec.DecodeInt(indexRest)
	if err != nil {
		return splitScatterRangeHint{}
	}

	indexPrefix := append(append([]byte(nil), tablePrefix...), splitScatterIndexPrefix...)
	indexPrefix = codec.EncodeInt(indexPrefix, indexID)
	indexRange := splitScatterPrefixRange(indexPrefix)
	endKey := region.GetEndKey()

	// We intentionally over-approximate ambiguous table-key ranges. If PD can
	// no longer prove the region stays within a single index prefix, it falls
	// back to the table-scoped group instead of dropping back to the family
	// group, so table-boundary splits and merged ranges still participate in
	// the broader scatter continuity/baseline.
	// Both endKey and indexRange.endKey are MemComparable-encoded, so
	// bytes.Compare correctly reflects the key ordering.
	if len(endKey) == 0 || len(indexRange.startKey) == 0 || len(indexRange.endKey) == 0 || bytes.Compare(endKey, indexRange.endKey) > 0 {
		return splitScatterPrefixRange(tablePrefix)
	}
	return indexRange
}

func splitScatterPrefixRange(rawPrefix []byte) splitScatterRangeHint {
	startKey := append([]byte(nil), codec.EncodeBytes(rawPrefix)...)
	endRawPrefix := splitScatterNextPrefix(rawPrefix)
	if len(endRawPrefix) == 0 {
		return splitScatterRangeHint{startKey: startKey}
	}
	return splitScatterRangeHint{
		startKey: startKey,
		endKey:   append([]byte(nil), codec.EncodeBytes(endRawPrefix)...),
	}
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
