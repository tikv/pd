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

func TestDecodeKeyspace(t *testing.T) {
	re := require.New(t)
	bound := MakeRegionBound(100)
	k := codec.Key(bound.TxnLeftBound)
	ok, keyspaceID := k.DecodeKeyspaceTxnKey()
	re.True(ok)
	re.Equal(uint32(100), keyspaceID)
}

func TestExtractKeyspaceID(t *testing.T) {
	re := require.New(t)
	testCases := []struct {
		name            string
		key             []byte
		expectedID      uint32
		expectedSuccess bool
	}{
		{
			name:            "empty key",
			key:             []byte{},
			expectedID:      constant.MaxValidKeyspaceID,
			expectedSuccess: true,
		},
		{
			name:            "keyspace 0 txn mode",
			key:             MakeRegionBound(0).TxnLeftBound,
			expectedID:      0,
			expectedSuccess: true,
		},
		{
			name:            "keyspace 100 txn mode",
			key:             MakeRegionBound(100).TxnLeftBound,
			expectedID:      100,
			expectedSuccess: true,
		},
		{
			name:            "keyspace 4242 txn mode",
			key:             MakeRegionBound(4242).TxnLeftBound,
			expectedID:      4242,
			expectedSuccess: true,
		},
		{
			name:            "keyspace 0 raw mode (not supported)",
			key:             MakeRegionBound(0).RawLeftBound,
			expectedID:      0,
			expectedSuccess: false,
		},
		{
			name:            "keyspace 100 raw mode (not supported)",
			key:             MakeRegionBound(100).RawLeftBound,
			expectedID:      0,
			expectedSuccess: false,
		},
		{
			name:            "non-keyspace key (table key)",
			key:             codec.EncodeBytes([]byte{'t', 1, 2, 3}),
			expectedID:      0,
			expectedSuccess: false,
		},
		{
			name:            "short key",
			key:             codec.EncodeBytes([]byte{'x'}),
			expectedID:      0,
			expectedSuccess: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(_ *testing.T) {
			id, ok := ExtractKeyspaceID(tc.key)
			re.Equal(tc.expectedSuccess, ok, "test case: %s", tc.name)
			if tc.expectedSuccess {
				re.Equal(tc.expectedID, id, "test case: %s", tc.name)
			}
		})
	}
}

// mockKeyspaceChecker is a mock implementation of KeyspaceChecker for testing.
type mockKeyspaceChecker struct {
	existingKeyspaces map[uint32]bool
}

func (m *mockKeyspaceChecker) KeyspaceExists(id uint32) bool {
	if m.existingKeyspaces == nil {
		return true // Default: all keyspaces exist
	}
	return m.existingKeyspaces[id]
}

func TestRegionSpansMultipleKeyspaces(t *testing.T) {
	re := require.New(t)

	// Mock checker where all keyspaces exist
	allExistChecker := &mockKeyspaceChecker{}

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
			name:           "empty end key",
			startKey:       MakeRegionBound(1).TxnLeftBound,
			endKey:         []byte{},
			checker:        allExistChecker,
			expectedResult: false,
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
			name:           "span deleted keyspace - should not span",
			startKey:       MakeRegionBound(100).TxnLeftBound,
			endKey:         MakeRegionBound(102).TxnRightBound,
			checker:        specificChecker, // keyspace 102 doesn't exist
			expectedResult: false,
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
		t.Run(tc.name, func(t *testing.T) {
			result := RegionSpansMultipleKeyspaces(tc.startKey, tc.endKey, tc.checker)
			re.Equal(tc.expectedResult, result, "test case: %s", tc.name)
		})
	}
}

func TestGetKeyspaceSplitKeys(t *testing.T) {
	re := require.New(t)

	// Mock checker where all keyspaces exist
	allExistChecker := &mockKeyspaceChecker{}

	// Mock checker where only specific keyspaces exist
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
		expectedSplitKeys int
	}{
		{
			name:              "empty end key",
			startKey:          MakeRegionBound(1).TxnLeftBound,
			endKey:            []byte{},
			checker:           allExistChecker,
			expectedSplitKeys: 0,
		},
		{
			name:              "same keyspace txn mode",
			startKey:          MakeRegionBound(100).TxnLeftBound,
			endKey:            MakeRegionBound(100).TxnRightBound,
			checker:           allExistChecker,
			expectedSplitKeys: 0,
		},
		{
			name:              "span two keyspaces txn mode",
			startKey:          MakeRegionBound(100).TxnLeftBound,
			endKey:            MakeRegionBound(101).TxnRightBound,
			checker:           allExistChecker,
			expectedSplitKeys: 1,
		},
		{
			name:              "span 5 keyspaces txn mode",
			startKey:          MakeRegionBound(100).TxnLeftBound,
			endKey:            MakeRegionBound(105).TxnRightBound,
			checker:           allExistChecker,
			expectedSplitKeys: 5,
		},
		{
			name:              "non-keyspace keys",
			startKey:          codec.EncodeBytes([]byte{'t', 1, 2, 3}),
			endKey:            codec.EncodeBytes([]byte{'t', 1, 2, 4}),
			checker:           allExistChecker,
			expectedSplitKeys: 0,
		},
		{
			name:              "span keyspaces with deleted ones - only split at existing boundaries",
			startKey:          MakeRegionBound(100).TxnLeftBound,
			endKey:            MakeRegionBound(105).TxnRightBound,
			checker:           specificChecker, // only 100, 101, 102 exist
			expectedSplitKeys: 2,               // split at boundary of 100 and 101 only (102 has no next)
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			splitKeys := GetKeyspaceSplitKeys(tc.startKey, tc.endKey, tc.checker)
			re.Equal(tc.expectedSplitKeys, len(splitKeys), "test case: %s", tc.name)

			// Verify that the split keys are valid and properly ordered
			if len(splitKeys) > 0 {
				for i := 0; i < len(splitKeys)-1; i++ {
					re.True(string(splitKeys[i]) < string(splitKeys[i+1]),
						"split keys should be in ascending order for test case: %s", tc.name)
				}
			}
		})
	}
}
