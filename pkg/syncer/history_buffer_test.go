// Copyright 2018 TiKV Project Authors.
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

package syncer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/kv"
)

func TestNormalizeHistoryBufferCapacity(t *testing.T) {
	testCases := []struct {
		name     string
		size     int
		unit     int
		expected int
	}{
		{name: "below-unit", size: 1, unit: historyBufferCapacityUnit, expected: historyBufferCapacityUnit},
		{name: "one-unit", size: historyBufferCapacityUnit, unit: historyBufferCapacityUnit, expected: historyBufferCapacityUnit},
		{name: "round-to-two-units", size: historyBufferCapacityUnit + 1, unit: historyBufferCapacityUnit, expected: 2 * historyBufferCapacityUnit},
		{name: "round-to-four-units", size: 3 * historyBufferCapacityUnit, unit: historyBufferCapacityUnit, expected: 4 * historyBufferCapacityUnit},
		{name: "test-unit", size: 3, unit: 1, expected: 4},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.expected, normalizeHistoryBufferCapacity(testCase.size, testCase.unit))
		})
	}
}

func TestHistoryBufferKeepsRingBehaviorWithoutRetain(t *testing.T) {
	re := require.New(t)
	h := newTestHistoryBuffer(8)
	h.resetWithIndex(100)

	for i := 1; i <= 3; i++ {
		h.record(newHistoryBufferTestRegion(uint64(i)))
	}

	re.Equal(2, h.capacity())
	re.Nil(h.recordsFrom(100))
	records := h.recordsFrom(101)
	re.Len(records, 2)
	re.Equal(uint64(2), records[0].GetID())
	re.Equal(uint64(3), records[1].GetID())
}

func TestHistoryBufferRecordsAndFlushes(t *testing.T) {
	re := require.New(t)
	regions := make([]*core.RegionInfo, 0, 101)
	for i := range 101 {
		regions = append(regions, newHistoryBufferTestRegion(uint64(i)))
	}

	h := newHistoryBufferWithConfig(1, 1, 1, kv.NewMemoryKV())
	re.Equal(0, h.len())
	for _, r := range regions {
		h.record(r)
	}
	re.Equal(1, h.len())
	re.Equal(regions[h.nextIndex()-1], h.get(100))
	re.Nil(h.get(99))

	h = newHistoryBufferWithConfig(2, 2, 1, kv.NewMemoryKV())
	for _, r := range regions {
		h.record(r)
	}
	re.Equal(2, h.len())
	re.Equal(regions[h.nextIndex()-1], h.get(100))
	re.Equal(regions[h.nextIndex()-2], h.get(99))
	re.Nil(h.get(98))

	kvMem := kv.NewMemoryKV()
	h1 := newHistoryBufferWithConfig(100, 100, 100, kvMem)
	for i := range 6 {
		h1.record(regions[i])
	}
	re.Equal(6, h1.len())
	re.Equal(uint64(6), h1.nextIndex())
	h1.persist()

	h2 := newHistoryBufferWithConfig(100, 100, 100, kvMem)
	re.Equal(uint64(6), h2.nextIndex())
	re.Equal(uint64(6), h2.firstIndex())
	re.Nil(h2.get(5))
	re.Equal(0, h2.len())
	for _, r := range regions {
		index := h2.nextIndex()
		h2.record(r)
		re.Equal(r, h2.get(index))
	}

	re.Equal(uint64(107), h2.nextIndex())
	re.Nil(h2.get(h2.nextIndex()))
	s, err := h2.kv.Load(historyKey)
	re.NoError(err)
	re.Equal("106", s)

	histories := h2.recordsFrom(1)
	re.Empty(histories)
	histories = h2.recordsFrom(h2.firstIndex())
	re.Len(histories, 100)
	re.Equal(uint64(7), h2.firstIndex())
	re.Equal(regions[1:], histories)
}

func TestHistoryBufferPersistsNextIndexOnly(t *testing.T) {
	re := require.New(t)
	kvMem := kv.NewMemoryKV()
	h1 := newHistoryBufferWithConfig(4, 8, 1, kvMem)
	for i := 1; i <= 3; i++ {
		h1.record(newHistoryBufferTestRegion(uint64(i)))
	}
	re.Equal(3, h1.len())
	h1.persist()

	h2 := newHistoryBufferWithConfig(4, 8, 1, kvMem)
	re.Equal(uint64(3), h2.nextIndex())
	re.Equal(uint64(3), h2.firstIndex())
	re.Equal(0, h2.len())
	re.Nil(h2.get(2))
	s, err := h2.kv.Load(historyKey)
	re.NoError(err)
	re.Equal("3", s)
}

func TestHistoryBufferRetainKeepsCatchUpRecords(t *testing.T) {
	re := require.New(t)
	h := newHistoryBufferWithConfig(2, 8, 1, storage.NewStorageWithMemoryBackend())
	h.resetWithIndex(10)
	release := h.retainFrom(10)
	defer release()

	for i := range 5 {
		h.record(newHistoryBufferTestRegion(uint64(100 + i)))
	}

	records, nextIndex, ok := h.retainedRecordsFrom(10)
	re.True(ok)
	re.Equal(uint64(15), nextIndex)
	re.Equal(uint64(10), h.getFirstIndex())
	re.Len(records, 5)
	re.Equal(8, h.capacity())
}

func TestHistoryBufferRecordBatchFromGrowsBeforeOverwrite(t *testing.T) {
	re := require.New(t)
	h := newHistoryBufferWithConfig(2, 8, 1, storage.NewStorageWithMemoryBackend())
	h.resetWithIndex(10)
	records := make([]*core.RegionInfo, 0, 5)

	for i := range 5 {
		records = append(records, newHistoryBufferTestRegion(uint64(100+i)))
	}
	h.recordBatchFrom(10, records)

	re.Equal(uint64(10), h.getFirstIndex())
	re.Len(h.recordsFrom(10), 5)
	re.Equal(8, h.capacity())
}

func TestHistoryBufferObserveWindowFromDoesNotMoveFirstIndex(t *testing.T) {
	re := require.New(t)
	h := newHistoryBufferWithConfig(2, 8, 1, storage.NewStorageWithMemoryBackend())
	h.resetWithIndex(10)
	records := make([]*core.RegionInfo, 0, 6)
	for i := range 6 {
		records = append(records, newHistoryBufferTestRegion(uint64(100+i)))
	}
	h.recordBatchFrom(10, records)
	re.Equal(uint64(10), h.getFirstIndex())

	h.observeWindowFrom(16, 16)

	re.Equal(uint64(10), h.getFirstIndex())
	re.Len(h.recordsFrom(10), 6)
}

func TestHistoryBufferRecordBatchFromRespectsActiveFullSyncRetain(t *testing.T) {
	re := require.New(t)
	h := newHistoryBufferWithConfig(2, 8, 1, storage.NewStorageWithMemoryBackend())
	h.resetWithIndex(10)
	release := h.retainFrom(10)
	defer release()
	records := make([]*core.RegionInfo, 0, 6)

	for i := range 6 {
		records = append(records, newHistoryBufferTestRegion(uint64(100+i)))
	}
	h.recordBatchFrom(12, records)

	re.Equal(uint64(10), h.getFirstIndex())
	historyRecords := h.recordsFrom(10)
	re.Len(historyRecords, 6)
	re.Equal(uint64(100), historyRecords[0].GetID())
}

func TestHistoryBufferObserveRequiredWindowGrowsWithoutRetain(t *testing.T) {
	re := require.New(t)
	h := newTestHistoryBuffer(8)

	h.observeRequiredWindow(3)

	re.Equal(8, h.capacity())
}

func TestHistoryBufferShrinksOneStepAfterRequiredWindowStaysLow(t *testing.T) {
	re := require.New(t)
	h := newTestHistoryBuffer(8)
	h.observeRequiredWindow(3)
	re.Equal(8, h.capacity())
	h.maybeShrink()

	for range historyBufferShrinkRounds {
		h.observeRequiredWindow(1)
		h.maybeShrink()
	}

	re.Equal(4, h.capacity())
}

func newTestHistoryBuffer(maxCapacity int) *historyBuffer {
	return newHistoryBufferWithConfig(2, maxCapacity, 1, storage.NewStorageWithMemoryBackend())
}

func newHistoryBufferTestRegion(regionID uint64) *core.RegionInfo {
	return core.NewRegionInfo(&metapb.Region{Id: regionID}, nil)
}
