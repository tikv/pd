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

package pkdbforcemerge

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"path"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/storage"
)

func newTestManager(t *testing.T) (storage.Storage, *Manager) {
	t.Helper()
	re := require.New(t)
	store := storage.NewStorageWithMemoryBackend()
	manager, err := NewManager(store)
	re.NoError(err)
	return store, manager
}

func mustHexDecode(t *testing.T, key string) []byte {
	t.Helper()
	if key == "" {
		return nil
	}
	data, err := hex.DecodeString(key)
	require.NoError(t, err)
	return data
}

func newTestRegion(t *testing.T, startKeyHex, endKeyHex string) *core.RegionInfo {
	t.Helper()
	return core.NewRegionInfo(&metapb.Region{
		StartKey: mustHexDecode(t, startKeyHex),
		EndKey:   mustHexDecode(t, endKeyHex),
	}, nil)
}

func collectTreeRanges(m *Manager) []string {
	ranges := make([]string, 0, m.tree.Len())
	m.tree.Ascend(func(item *RangeItem) bool {
		ranges = append(ranges, item.StartKeyHex+"-"+item.EndKeyHex)
		return true
	})
	return ranges
}

func collectStoredRanges(t *testing.T, store storage.Storage) map[string]string {
	t.Helper()
	re := require.New(t)
	ranges := make(map[string]string)
	err := store.LoadForceMergeRanges(func(key, value string) {
		item := &RangeItem{}
		re.NoError(json.Unmarshal([]byte(value), item))
		re.NoError(adjustRangeItem(item))
		ranges[key] = item.StartKeyHex + "-" + item.EndKeyHex
	})
	re.NoError(err)
	return ranges
}

type failingForceMergeStorage struct {
	storage.Storage
	failSaveKey   string
	failDeleteKey string
	saveCalls     int
	deleteCalls   int
}

func (s *failingForceMergeStorage) SaveForceMergeRange(rangeKey string, data interface{}) error {
	s.saveCalls++
	if rangeKey == s.failSaveKey {
		return errors.New("save failed")
	}
	return s.Storage.SaveForceMergeRange(rangeKey, data)
}

func (s *failingForceMergeStorage) DeleteForceMergeRange(rangeKey string) error {
	s.deleteCalls++
	if rangeKey == s.failDeleteKey {
		return errors.New("delete failed")
	}
	return s.Storage.DeleteForceMergeRange(rangeKey)
}

func TestSearchRangeLocked(t *testing.T) {
	re := require.New(t)
	_, manager := newTestManager(t)
	re.NoError(manager.AddRanges([]string{"01", "04", "07"}, []string{"03", "06", "09"}))

	manager.Lock()
	left, right, overlaps := manager.searchRangeLocked(mustHexDecode(t, "02"), mustHexDecode(t, "08"))
	manager.Unlock()
	re.NotNil(left)
	re.Equal("01", left.StartKeyHex)
	re.Equal("03", left.EndKeyHex)
	re.NotNil(right)
	re.Equal("07", right.StartKeyHex)
	re.Equal("09", right.EndKeyHex)
	re.Len(overlaps, 1)
	re.Equal("04", overlaps[0].StartKeyHex)
	re.Equal("06", overlaps[0].EndKeyHex)

	manager.Lock()
	left, right, overlaps = manager.searchRangeLocked(mustHexDecode(t, "04"), mustHexDecode(t, "06"))
	manager.Unlock()
	re.Nil(left)
	re.Nil(right)
	re.Len(overlaps, 1)
	re.Equal("04", overlaps[0].StartKeyHex)
	re.Equal("06", overlaps[0].EndKeyHex)

	re.NoError(manager.AddRanges([]string{"0b", "0f"}, []string{"0d", "11"}))
	manager.Lock()
	left, right, overlaps = manager.searchRangeLocked(mustHexDecode(t, "0d"), mustHexDecode(t, "0f"))
	manager.Unlock()
	re.NotNil(left)
	re.Equal("0b", left.StartKeyHex)
	re.Equal("0d", left.EndKeyHex)
	re.NotNil(right)
	re.Equal("0f", right.StartKeyHex)
	re.Equal("11", right.EndKeyHex)
	re.Empty(overlaps)
}

func TestAddRangeLockedMergeAndPersist(t *testing.T) {
	re := require.New(t)
	store, manager := newTestManager(t)
	re.NoError(manager.AddRanges([]string{"01", "05"}, []string{"03", "07"}))

	item, err := newRangeItem("02", "06")
	re.NoError(err)

	manager.Lock()
	err = manager.addRangeLocked(item)
	manager.Unlock()
	re.NoError(err)

	re.Equal([]string{"01-07"}, collectTreeRanges(manager))
	re.Equal(map[string]string{"07": "01-07"}, collectStoredRanges(t, store))
}

func TestAddRangeLockedMergesAdjacentRanges(t *testing.T) {
	re := require.New(t)
	store, manager := newTestManager(t)
	re.NoError(manager.AddRanges([]string{"01", "05"}, []string{"03", "07"}))

	item, err := newRangeItem("03", "05")
	re.NoError(err)

	manager.Lock()
	err = manager.addRangeLocked(item)
	manager.Unlock()
	re.NoError(err)

	re.Equal([]string{"01-07"}, collectTreeRanges(manager))
	re.Equal(map[string]string{"07": "01-07"}, collectStoredRanges(t, store))
}

func TestAddRangeLockedKeepsContainingRange(t *testing.T) {
	re := require.New(t)
	baseStore, manager := newTestManager(t)
	re.NoError(manager.AddRanges([]string{"01"}, []string{"09"}))

	failStore := &failingForceMergeStorage{Storage: baseStore}
	manager.storage = failStore

	item, err := newRangeItem("03", "05")
	re.NoError(err)

	manager.Lock()
	err = manager.addRangeLocked(item)
	manager.Unlock()
	re.NoError(err)
	re.Equal(0, failStore.deleteCalls)
	re.Equal(0, failStore.saveCalls)

	re.Equal([]string{"01-09"}, collectTreeRanges(manager))
	re.Equal(map[string]string{"09": "01-09"}, collectStoredRanges(t, baseStore))
}

func TestSolveRegion(t *testing.T) {
	testCases := []struct {
		name           string
		startKeysHex   []string
		endKeysHex     []string
		regionStart    string
		regionEnd      string
		needForceMerge bool
		expectTree     []string
		expectStore    map[string]string
	}{
		{
			name:           "exact-overlap",
			startKeysHex:   []string{"01"},
			endKeysHex:     []string{"03"},
			regionStart:    "01",
			regionEnd:      "03",
			needForceMerge: true,
			expectTree:     []string{"01-03"},
			expectStore:    map[string]string{"03": "01-03"},
		},
		{
			name:           "merged-region-removes-previous-exact-overlap",
			startKeysHex:   []string{"01"},
			endKeysHex:     []string{"03"},
			regionStart:    "01",
			regionEnd:      "05",
			needForceMerge: false,
			expectTree:     []string{},
			expectStore:    map[string]string{},
		},
		{
			name:           "left-boundary-equal",
			startKeysHex:   []string{"01"},
			endKeysHex:     []string{"04"},
			regionStart:    "01",
			regionEnd:      "03",
			needForceMerge: true,
			expectTree:     []string{"01-04"},
			expectStore:    map[string]string{"04": "01-04"},
		},
		{
			name:           "right-boundary-equal",
			startKeysHex:   []string{"02"},
			endKeysHex:     []string{"05"},
			regionStart:    "03",
			regionEnd:      "05",
			needForceMerge: true,
			expectTree:     []string{"02-05"},
			expectStore:    map[string]string{"05": "02-05"},
		},
		{
			name:           "strict-cover",
			startKeysHex:   []string{"02"},
			endKeysHex:     []string{"06"},
			regionStart:    "03",
			regionEnd:      "05",
			needForceMerge: true,
			expectTree:     []string{"02-06"},
			expectStore:    map[string]string{"06": "02-06"},
		},
		{
			name:           "empty-end-key-region",
			startKeysHex:   []string{"02"},
			endKeysHex:     []string{"06"},
			regionStart:    "03",
			regionEnd:      "",
			needForceMerge: false,
			expectTree:     []string{"02-06"},
			expectStore:    map[string]string{"06": "02-06"},
		},
		{
			name:           "multi-range-no-cover-removes-overlaps",
			startKeysHex:   []string{"01", "04", "07"},
			endKeysHex:     []string{"03", "06", "09"},
			regionStart:    "02",
			regionEnd:      "08",
			needForceMerge: false,
			expectTree:     []string{"01-03", "07-09"},
			expectStore:    map[string]string{"03": "01-03", "09": "07-09"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			store, manager := newTestManager(t)
			re.NoError(manager.AddRanges(tc.startKeysHex, tc.endKeysHex))

			needForceMerge := manager.SolveRegion(newTestRegion(t, tc.regionStart, tc.regionEnd))
			re.Equal(tc.needForceMerge, needForceMerge)
			re.Equal(tc.expectTree, collectTreeRanges(manager))
			re.Equal(tc.expectStore, collectStoredRanges(t, store))
		})
	}
}

func TestLoadRangesRestoresCanonicalKeyAndDeletesInvalidData(t *testing.T) {
	re := require.New(t)
	store := storage.NewStorageWithMemoryBackend()

	re.NoError(store.SaveForceMergeRange("BB", &RangeItem{StartKeyHex: "AA", EndKeyHex: "BB"}))
	re.NoError(store.Save(path.Join("force_merge", "cc"), "{bad-json"))
	re.NoError(store.Save(path.Join("force_merge", "dd"), `{"start_key":"01","end_key":""}`))

	manager, err := NewManager(store)
	re.NoError(err)
	re.Equal([]string{"aa-bb"}, collectTreeRanges(manager))
	re.Equal(map[string]string{"bb": "aa-bb"}, collectStoredRanges(t, store))
}

func TestAddRangeLockedStopsAfterPersistError(t *testing.T) {
	re := require.New(t)
	baseStore, manager := newTestManager(t)
	re.NoError(manager.AddRanges([]string{"01", "05"}, []string{"03", "07"}))

	failStore := &failingForceMergeStorage{
		Storage:       baseStore,
		failDeleteKey: "03",
	}
	manager.storage = failStore

	item, err := newRangeItem("02", "06")
	re.NoError(err)

	manager.Lock()
	err = manager.addRangeLocked(item)
	manager.Unlock()
	re.Error(err)
	re.Equal(1, failStore.deleteCalls)
	re.Equal(0, failStore.saveCalls)
	re.Equal([]string{"01-03", "05-07"}, collectTreeRanges(manager))
	re.Equal(map[string]string{"03": "01-03", "07": "05-07"}, collectStoredRanges(t, baseStore))
}

func TestAddRangesValidation(t *testing.T) {
	re := require.New(t)
	_, manager := newTestManager(t)

	err := manager.AddRanges([]string{"01"}, []string{})
	re.ErrorContains(err, "start key count 1 does not match end key count 0")

	err = manager.AddRanges([]string{"02", "01"}, []string{"03", "04"})
	re.ErrorContains(err, "start keys should be sorted")

	err = manager.AddRanges([]string{"01", "02"}, []string{"04", "03"})
	re.ErrorContains(err, "end keys should be sorted")

	err = manager.AddRanges([]string{"01", "03"}, []string{"04", "05"})
	re.ErrorContains(err, "ranges should not overlap")

	re.Empty(collectTreeRanges(manager))
}

func TestAddRangesMergesAdjacentRangesInCheckPhase(t *testing.T) {
	re := require.New(t)
	store, manager := newTestManager(t)

	err := manager.AddRanges(
		[]string{"01", "03", "07", "09"},
		[]string{"03", "05", "09", "0b"},
	)
	re.NoError(err)

	re.Equal([]string{"01-05", "07-0b"}, collectTreeRanges(manager))
	re.Equal(map[string]string{
		"05": "01-05",
		"0b": "07-0b",
	}, collectStoredRanges(t, store))
}
