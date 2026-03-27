// Copyright 2022 TiKV Project Authors.
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

package keyspace

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/keyspacepb"

	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/versioninfo/kerneltype"
)

func TestValidateID(t *testing.T) {
	re := require.New(t)
	testCases := []struct {
		id     uint32
		hasErr bool
	}{
		{100, false},
		{constant.MaxValidKeyspaceID, false},
		{constant.MaxValidKeyspaceID + 1, true},
		{math.MaxUint32, true},
	}

	// Add kernel-specific test case for MaxValidKeyspaceID - 1
	if kerneltype.IsNextGen() {
		// In NextGen mode, MaxValidKeyspaceID - 1 is SystemKeyspaceID, which is protected
		testCases = append(testCases, struct {
			id     uint32
			hasErr bool
		}{constant.MaxValidKeyspaceID - 1, true})
	} else {
		// In Classic mode, MaxValidKeyspaceID - 1 is not protected
		testCases = append(testCases, struct {
			id     uint32
			hasErr bool
		}{constant.MaxValidKeyspaceID - 1, false})
	}

	for _, testCase := range testCases {
		re.Equal(testCase.hasErr, validateID(testCase.id) != nil)
	}
}

func TestValidateName(t *testing.T) {
	re := require.New(t)
	testCases := []struct {
		name   string
		hasErr bool
	}{
		{"keyspaceName1", false},
		{"keyspace_name_1", false},
		{"10", false},
		{"", true},
		{"keyspace/", true},
		{"keyspace:1", true},
		{"many many spaces", true},
		{"keyspace?limit=1", true},
		{"keyspace%1", true},
		{"789z-_", false},
		{"789z-_)", true},
		{"18446744073709551615", false}, // max uint64
		{"18446744073709551615a", true},
		{"78912345678982u7389217897238917389127893781278937128973812728397281378932179837", true},
		{"scope1", false},
		{"-----", false},
	}
	for _, testCase := range testCases {
		re.Equal(testCase.hasErr, validateName(testCase.name) != nil)
	}
}

func TestProtectedKeyspaceValidation(t *testing.T) {
	re := require.New(t)
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/versioninfo/kerneltype/mockNextGenBuildFlag"))
	}()
	const classic = `return(false)`
	const nextGen = `return(true)`

	cases := []struct {
		name          string
		nextGenFlag   string
		idToTest      uint32
		nameToTest    string
		expectErrID   bool
		expectErrName bool
	}{
		// classic
		{"classic_default_id", classic, constant.DefaultKeyspaceID, "", true, false},
		{"classic_default_name", classic, 1, constant.DefaultKeyspaceName, false, true},
		{"classic_system_id_allowed", classic, constant.SystemKeyspaceID, "", false, false},
		{"classic_system_name_allowed", classic, 1, constant.SystemKeyspaceName, false, false},
		{"classic_normal_case", classic, 100, "normal_keyspace", false, false},
		// next-gen
		{"nextgen_system_id", nextGen, constant.SystemKeyspaceID, "", true, false},
		{"nextgen_system_name", nextGen, 1, constant.SystemKeyspaceName, false, true},
		{"nextgen_default_id_allowed", nextGen, constant.DefaultKeyspaceID, "", false, false},
		{"nextgen_default_name_allowed", nextGen, 1, constant.DefaultKeyspaceName, false, false},
		{"nextgen_normal_case", nextGen, 100, "normal_keyspace", false, false},
	}

	for _, c := range cases {
		t.Run(c.name, func(_ *testing.T) {
			re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/versioninfo/kerneltype/mockNextGenBuildFlag", c.nextGenFlag))
			errID := validateID(c.idToTest)
			if c.expectErrID {
				re.Error(errID)
			} else {
				re.NoError(errID)
			}
			if c.nameToTest != "" {
				errName := validateName(c.nameToTest)
				if c.expectErrName {
					re.Error(errName)
				} else {
					re.NoError(errName)
				}
			}
		})
	}
}

func TestExtractKeyspaceID(t *testing.T) {
	re := require.New(t)
	testCases := []struct {
		name            string
		key             []byte
		expectedID      uint32
		expectedKeyType KeyType
	}{
		{
			name:            "empty key",
			key:             []byte{},
			expectedID:      constant.MaxValidKeyspaceID,
			expectedKeyType: KeyTypeTxn,
		},
		{
			name:            "keyspace 0 txn mode",
			key:             MakeRegionBound(0).TxnLeftBound,
			expectedID:      0,
			expectedKeyType: KeyTypeTxn,
		},
		{
			name:            "keyspace 100 txn mode",
			key:             MakeRegionBound(100).TxnLeftBound,
			expectedID:      100,
			expectedKeyType: KeyTypeTxn,
		},
		{
			name:            "keyspace 4242 txn mode",
			key:             MakeRegionBound(4242).TxnLeftBound,
			expectedID:      4242,
			expectedKeyType: KeyTypeTxn,
		},
		{
			name:            "keyspace 0 raw mode ",
			key:             MakeRegionBound(0).RawLeftBound,
			expectedID:      0,
			expectedKeyType: KeyTypeRaw,
		},
		{
			name:            "keyspace 100 raw mode (not supported)",
			key:             MakeRegionBound(100).RawLeftBound,
			expectedID:      100,
			expectedKeyType: KeyTypeRaw,
		},
		{
			name:            "non-keyspace key (table key)",
			key:             codec.EncodeBytes([]byte{'t', 1, 2, 3}),
			expectedID:      0,
			expectedKeyType: KeyTypeUnknown,
		},
		{
			name:            "short key",
			key:             codec.EncodeBytes([]byte{'x'}),
			expectedID:      0,
			expectedKeyType: KeyTypeUnknown,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(_ *testing.T) {
			id, kt := ExtractKeyspaceID(tc.key)
			re.Equal(tc.expectedKeyType, kt, "test case: %s", tc.name)
			if tc.expectedKeyType != KeyTypeUnknown {
				re.Equal(tc.expectedID, id, "test case: %s", tc.name)
			}
		})
	}
}

// mockKeyspaceChecker is a mock implementation of Checker for testing.
type mockKeyspaceChecker struct {
	existingKeyspaces map[uint32]bool
	allExist          bool
}

func (m *mockKeyspaceChecker) KeyspaceExist(id uint32) bool {
	if m.allExist {
		return true
	}
	if m.existingKeyspaces == nil {
		return true // Default: all keyspaces exist
	}
	return m.existingKeyspaces[id]
}

func (m *mockKeyspaceChecker) GetKeyspaceIDInRange(start, end uint32, limit int) ([]uint32, bool) {
	if start >= end {
		return nil, false
	}
	if m.allExist {
		return []uint32{start}, true
	}
	found := false
	ret := make([]uint32, 0)
	for id, exists := range m.existingKeyspaces {
		if exists && id >= start && id <= end {
			ret = append(ret, id)
			found = true
			if limit > 0 && len(ret) >= limit {
				break
			}
		}
	}
	return ret, found
}

func TestRegionSpansMultipleKeyspaces(t *testing.T) {
	re := require.New(t)

	// Mock checker where all keyspaces exist
	allExistChecker := &mockKeyspaceChecker{allExist: true}

	// Mock checker where only specific keyspaces exist
	specificChecker := &mockKeyspaceChecker{
		existingKeyspaces: map[uint32]bool{
			100: true,
			101: true,
			105: true,
		},
	}

	testCases := []struct {
		name           string
		startKey       []byte
		endKey         []byte
		checker        *mockKeyspaceChecker
		expectedResult bool
	}{
		{
			name:           "empty start and end key",
			startKey:       []byte{},
			endKey:         []byte{},
			checker:        allExistChecker,
			expectedResult: true,
		},
		{
			name:           "empty end key",
			startKey:       MakeRegionBound(1).TxnLeftBound,
			endKey:         []byte{},
			checker:        allExistChecker,
			expectedResult: true,
		},
		{
			name:           "same keyspace txn mode",
			startKey:       MakeRegionBound(100).TxnLeftBound,
			endKey:         MakeRegionBound(100).TxnRightBound,
			checker:        allExistChecker,
			expectedResult: false,
		},
		{
			name:           "adjacent keyspace boundary txn mode",
			startKey:       MakeRegionBound(100).TxnLeftBound,
			endKey:         MakeRegionBound(101).TxnLeftBound, // same as rightBound(100)
			checker:        allExistChecker,
			expectedResult: false, // [left100, right100) is within keyspace 100
		},
		{
			name:           "span two keyspaces txn mode",
			startKey:       MakeRegionBound(100).TxnLeftBound,
			endKey:         MakeRegionBound(101).TxnRightBound,
			checker:        allExistChecker,
			expectedResult: true,
		},
		{
			name:           "span multiple keyspaces txn mode",
			startKey:       MakeRegionBound(100).TxnLeftBound,
			endKey:         MakeRegionBound(105).TxnRightBound,
			checker:        allExistChecker,
			expectedResult: true,
		},
		{
			name:           "empty start key",
			startKey:       []byte{},
			endKey:         MakeRegionBound(100).TxnLeftBound,
			checker:        allExistChecker,
			expectedResult: true,
		},
		{
			name:           "non-keyspace keys",
			startKey:       codec.EncodeBytes([]byte{'t', 1, 2, 3}),
			endKey:         codec.EncodeBytes([]byte{'t', 1, 2, 4}),
			checker:        allExistChecker,
			expectedResult: false,
		},
		{
			name:           "span deleted keyspace but still cross two existing keyspaces - should span",
			startKey:       MakeRegionBound(100).TxnLeftBound,
			endKey:         MakeRegionBound(102).TxnRightBound,
			checker:        specificChecker, // keyspace 102 doesn't exist
			expectedResult: true,
		},
		{
			name:     "empty end key with sparse existing keyspaces should span when crossing two existing",
			startKey: MakeRegionBound(101).TxnLeftBound,
			endKey:   []byte{},
			checker: &mockKeyspaceChecker{
				existingKeyspaces: map[uint32]bool{
					101: true,
					102: true,
				},
			},
			expectedResult: true,
		},
		{
			name:           "span existing keyspaces - should span",
			startKey:       MakeRegionBound(100).TxnLeftBound,
			endKey:         MakeRegionBound(101).TxnRightBound,
			checker:        specificChecker, // both 100 and 101 exist
			expectedResult: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(_ *testing.T) {
			result := RegionSpansMultipleKeyspaces(tc.startKey, tc.endKey, tc.checker)
			re.Equal(tc.expectedResult, result, "test case: %s", tc.name)
		})
	}
}

func TestGetKeyspaceSplitKeys(t *testing.T) {
	re := require.New(t)

	allExistChecker := &mockKeyspaceChecker{allExist: true}
	oneExistChecker := &mockKeyspaceChecker{
		existingKeyspaces: map[uint32]bool{
			101: true,
		},
	}
	specificChecker := &mockKeyspaceChecker{
		existingKeyspaces: map[uint32]bool{
			100: true,
			101: true,
			102: true,
			// 103, 104, 105 don't exist
		},
	}

	testCases := []struct {
		name              string
		startKey          []byte
		endKey            []byte
		checker           *mockKeyspaceChecker
		expectedSplitKeys [][]byte
	}{
		{
			name:              "non-keyspace keys should not split 99",
			startKey:          []byte{'t', 1, 2, 4},
			endKey:            MakeRegionBound(99).TxnLeftBound,
			checker:           specificChecker,
			expectedSplitKeys: nil,
		},
		{
			name:     "non-keyspace keys should not split",
			startKey: MakeRegionBound(102).RawLeftBound,
			endKey:   []byte{'t', 1, 2, 4},
			checker:  specificChecker,
			expectedSplitKeys: [][]byte{
				MakeRegionBound(103).RawLeftBound,
			},
		},
		{
			name:     "non-keyspace keys should not split",
			startKey: []byte{'t', 1, 2, 4},
			endKey:   MakeRegionBound(102).TxnLeftBound,
			checker:  specificChecker,
			expectedSplitKeys: [][]byte{
				MakeRegionBound(100).TxnLeftBound,
				MakeRegionBound(101).TxnLeftBound,
			},
		},
		{
			name:     "split keys with sparse existing keyspaces",
			startKey: MakeRegionBound(99).RawLeftBound,
			endKey:   []byte{'t', 1, 2, 4},
			checker:  specificChecker,
			expectedSplitKeys: [][]byte{
				MakeRegionBound(100).RawLeftBound,
				MakeRegionBound(101).RawLeftBound,
				MakeRegionBound(102).RawLeftBound,
				MakeRegionBound(103).RawLeftBound,
			},
		},
		{
			name:     "span two keyspaces txn mode",
			startKey: MakeRegionBound(100).TxnLeftBound,
			endKey:   MakeRegionBound(101).TxnRightBound,
			checker:  allExistChecker,
			expectedSplitKeys: [][]byte{
				MakeRegionBound(100).TxnRightBound,
			},
		},
		{
			name:     "span two keyspaces raw mode",
			startKey: MakeRegionBound(100).RawLeftBound,
			endKey:   MakeRegionBound(101).RawRightBound,
			checker:  allExistChecker,
			expectedSplitKeys: [][]byte{
				MakeRegionBound(100).RawRightBound,
			},
		},
		{
			name:              "same keyspace txn mode",
			startKey:          MakeRegionBound(100).TxnLeftBound,
			endKey:            MakeRegionBound(100).TxnRightBound,
			checker:           allExistChecker,
			expectedSplitKeys: nil,
		},
		{
			name:              "same keyspace raw mode",
			startKey:          MakeRegionBound(100).RawLeftBound,
			endKey:            MakeRegionBound(100).RawRightBound,
			checker:           allExistChecker,
			expectedSplitKeys: nil,
		},
		{
			name:     "adjacent range with one keyspace",
			startKey: MakeRegionBound(101).TxnLeftBound,
			endKey:   MakeRegionBound(102).TxnRightBound,
			checker:  oneExistChecker,
			expectedSplitKeys: [][]byte{
				MakeRegionBound(101).TxnRightBound,
			},
		},
		{
			name:     "empty start and end key with one exist",
			startKey: []byte{},
			endKey:   []byte{},
			checker:  oneExistChecker,
			expectedSplitKeys: [][]byte{
				MakeRegionBound(101).RawLeftBound,
				MakeRegionBound(101).RawRightBound,
				MakeRegionBound(101).TxnLeftBound,
				MakeRegionBound(101).TxnRightBound,
			},
		},
		{
			name:     "empty start and end key with three exist",
			startKey: []byte{},
			endKey:   []byte{},
			checker:  specificChecker,
			expectedSplitKeys: [][]byte{
				MakeRegionBound(100).RawLeftBound,
				MakeRegionBound(101).RawLeftBound,
				MakeRegionBound(102).RawLeftBound,
				MakeRegionBound(103).RawLeftBound,
				MakeRegionBound(100).TxnLeftBound,
				MakeRegionBound(101).TxnLeftBound,
				MakeRegionBound(102).TxnLeftBound,
				MakeRegionBound(103).TxnLeftBound,
			},
		},
		{
			name:              "not keys with no keyspace key",
			startKey:          []byte{'t', 1, 2, 3},
			endKey:            []byte{'t', 1, 2, 4},
			checker:           specificChecker,
			expectedSplitKeys: nil,
		},
		{
			name:     "span two keyspaces mix mode",
			startKey: MakeRegionBound(102).RawLeftBound,
			endKey:   MakeRegionBound(102).TxnLeftBound,
			checker:  specificChecker,
			expectedSplitKeys: [][]byte{
				MakeRegionBound(103).RawLeftBound,
			},
		},
		{
			name:              "span two keyspaces mix mode with latest keyspace",
			startKey:          MakeRegionBound(103).RawLeftBound,
			endKey:            MakeRegionBound(103).TxnLeftBound,
			checker:           specificChecker,
			expectedSplitKeys: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(_ *testing.T) {
			splitKeys := GetKeyspaceSplitKeys(tc.startKey, tc.endKey, tc.checker)
			re.Equal(tc.expectedSplitKeys, splitKeys, "test case: %s", tc.name)
		})
	}
}

func TestKeyspaceCache(t *testing.T) {
	re := require.New(t)
	cache := NewCache()

	cache.Save(100, "ks-100", keyspacepb.KeyspaceState_ENABLED)
	cache.Save(101, "ks-101", keyspacepb.KeyspaceState_ARCHIVED)
	cache.Save(102, "ks-102", keyspacepb.KeyspaceState_DISABLED)
	cache.Save(103, "ks-102", keyspacepb.KeyspaceState_TOMBSTONE)

	item, ok := cache.getKeyspaceByID(101)
	re.True(ok)
	re.Equal(uint32(101), item.keyspaceID)
	re.Equal("ks-101", item.name)
	re.Equal(keyspacepb.KeyspaceState_ARCHIVED, item.state)

	ok = cache.KeyspaceExist(103)
	re.False(ok)

	all := func() []uint32 {
		var ret []uint32
		cache.scanAllKeyspaces(func(keyspaceID uint32, _ string) bool {
			ret = append(ret, keyspaceID)
			return true
		})
		return ret
	}
	re.Equal([]uint32{100, 101, 102, 103}, all())

	ids, ok := cache.GetKeyspaceIDInRange(100, 103, 1)
	re.True(ok)
	re.Equal([]uint32{102}, ids)

	ids, ok = cache.GetKeyspaceIDInRange(101, 102, 1)
	re.True(ok)
	re.Equal([]uint32{102}, ids)

	ids, ok = cache.GetKeyspaceIDInRange(103, 104, 1)
	re.False(ok)
	re.Empty(ids)

	cache.DeleteKeyspace(101)
	_, ok = cache.getKeyspaceByID(101)
	re.False(ok)
	re.Equal([]uint32{100, 102, 103}, all())

	ids, ok = cache.GetKeyspaceIDInRange(100, 103, 5)
	re.True(ok)
	re.Len(ids, 2)
	re.Equal([]uint32{102, 100}, ids)
}
