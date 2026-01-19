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
	"encoding/hex"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/schedule/labeler"
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

func TestMakeLabelRule(t *testing.T) {
	re := require.New(t)
	testCases := []struct {
		id                uint32
		expectedLabelRule *labeler.LabelRule
	}{
		{
			id: 0,
			expectedLabelRule: &labeler.LabelRule{
				ID:    getRegionLabelID(0),
				Index: 0,
				Labels: []labeler.RegionLabel{
					{
						Key:   regionLabelKey,
						Value: "0",
					},
				},
				RuleType: labeler.KeyRange,
				Data: []any{
					map[string]any{
						"start_key": hex.EncodeToString(codec.EncodeBytes([]byte{'r', 0, 0, 0})),
						"end_key":   hex.EncodeToString(codec.EncodeBytes([]byte{'r', 0, 0, 1})),
					},
					map[string]any{
						"start_key": hex.EncodeToString(codec.EncodeBytes([]byte{'x', 0, 0, 0})),
						"end_key":   hex.EncodeToString(codec.EncodeBytes([]byte{'x', 0, 0, 1})),
					},
				},
			},
		},
		{
			id: 4242,
			expectedLabelRule: &labeler.LabelRule{
				ID:    getRegionLabelID(4242),
				Index: 0,
				Labels: []labeler.RegionLabel{
					{
						Key:   regionLabelKey,
						Value: "4242",
					},
				},
				RuleType: labeler.KeyRange,
				Data: []any{
					map[string]any{
						"start_key": hex.EncodeToString(codec.EncodeBytes([]byte{'r', 0, 0x10, 0x92})),
						"end_key":   hex.EncodeToString(codec.EncodeBytes([]byte{'r', 0, 0x10, 0x93})),
					},
					map[string]any{
						"start_key": hex.EncodeToString(codec.EncodeBytes([]byte{'x', 0, 0x10, 0x92})),
						"end_key":   hex.EncodeToString(codec.EncodeBytes([]byte{'x', 0, 0x10, 0x93})),
					},
				},
			},
		},
	}
	for _, testCase := range testCases {
		re.Equal(testCase.expectedLabelRule, MakeLabelRule(testCase.id))
	}
}

func TestParseKeyspaceIDFromLabelRule(t *testing.T) {
	re := require.New(t)
	testCases := []struct {
		labelRule  *labeler.LabelRule
		expectedID uint32
		expectedOK bool
	}{
		// Valid keyspace label rule.
		{
			labelRule:  MakeLabelRule(1),
			expectedID: 1,
			expectedOK: true,
		},
		// Invalid keyspace label ID - unmatched prefix.
		{
			labelRule: &labeler.LabelRule{
				ID:    "not-keyspaces/1",
				Index: 0,
				Labels: []labeler.RegionLabel{
					{
						Key:   regionLabelKey,
						Value: "1",
					},
				},
			},
			expectedID: 0,
			expectedOK: false,
		},
		// Invalid keyspace label ID - invalid keyspace ID.
		{
			labelRule: &labeler.LabelRule{
				ID:    "keyspaces/id1",
				Index: 0,
				Labels: []labeler.RegionLabel{
					{
						Key:   regionLabelKey,
						Value: "1",
					},
				},
			},
			expectedID: 0,
			expectedOK: false,
		},
		{
			labelRule: &labeler.LabelRule{
				ID:    "keyspaces/1id",
				Index: 0,
				Labels: []labeler.RegionLabel{
					{
						Key:   regionLabelKey,
						Value: "1",
					},
				},
			},
			expectedID: 0,
			expectedOK: false,
		},
		// Invalid keyspace label ID - invalid keyspace ID label rule.
		{
			labelRule: &labeler.LabelRule{
				ID:    getRegionLabelID(1),
				Index: 0,
				Labels: []labeler.RegionLabel{
					{
						Key:   "not-id",
						Value: "1",
					},
				},
			},
			expectedID: 0,
			expectedOK: false,
		},
		// Invalid keyspace label ID - unmatched keyspace ID with label rule.
		{
			labelRule: &labeler.LabelRule{
				ID:    getRegionLabelID(1),
				Index: 0,
				Labels: []labeler.RegionLabel{
					{
						Key:   "id",
						Value: "2",
					},
				},
			},
			expectedID: 0,
			expectedOK: false,
		},
	}
	for _, testCase := range testCases {
		id, ok := ParseKeyspaceIDFromLabelRule(testCase.labelRule)
		re.Equal(testCase.expectedID, id)
		re.Equal(testCase.expectedOK, ok)
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
			expectedID:      0,
			expectedSuccess: false,
		},
		{
			name:            "keyspace 0 raw mode",
			key:             MakeRegionBound(0).RawLeftBound,
			expectedID:      0,
			expectedSuccess: true,
		},
		{
			name:            "keyspace 0 txn mode",
			key:             MakeRegionBound(0).TxnLeftBound,
			expectedID:      0,
			expectedSuccess: true,
		},
		{
			name:            "keyspace 100 raw mode",
			key:             MakeRegionBound(100).RawLeftBound,
			expectedID:      100,
			expectedSuccess: true,
		},
		{
			name:            "keyspace 100 txn mode",
			key:             MakeRegionBound(100).TxnLeftBound,
			expectedID:      100,
			expectedSuccess: true,
		},
		{
			name:            "keyspace 4242 raw mode",
			key:             MakeRegionBound(4242).RawLeftBound,
			expectedID:      4242,
			expectedSuccess: true,
		},
		{
			name:            "keyspace 4242 txn mode",
			key:             MakeRegionBound(4242).TxnLeftBound,
			expectedID:      4242,
			expectedSuccess: true,
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
		t.Run(tc.name, func(t *testing.T) {
			id, ok := ExtractKeyspaceID(tc.key)
			re.Equal(tc.expectedSuccess, ok, "test case: %s", tc.name)
			if tc.expectedSuccess {
				re.Equal(tc.expectedID, id, "test case: %s", tc.name)
			}
		})
	}
}

func TestRegionSpansMultipleKeyspaces(t *testing.T) {
	re := require.New(t)
	testCases := []struct {
		name           string
		startKey       []byte
		endKey         []byte
		expectedResult bool
	}{
		{
			name:           "empty end key",
			startKey:       MakeRegionBound(1).RawLeftBound,
			endKey:         []byte{},
			expectedResult: false,
		},
		{
			name:           "same keyspace raw mode",
			startKey:       MakeRegionBound(100).RawLeftBound,
			endKey:         MakeRegionBound(100).RawRightBound,
			expectedResult: false,
		},
		{
			name:           "same keyspace txn mode",
			startKey:       MakeRegionBound(100).TxnLeftBound,
			endKey:         MakeRegionBound(100).TxnRightBound,
			expectedResult: false,
		},
		{
			name:           "adjacent keyspace boundary raw mode",
			startKey:       MakeRegionBound(100).RawLeftBound,
			endKey:         MakeRegionBound(101).RawLeftBound, // same as rightBound(100)
			expectedResult: false,                              // [left100, right100) is within keyspace 100
		},
		{
			name:           "span two keyspaces raw mode",
			startKey:       MakeRegionBound(100).RawLeftBound,
			endKey:         MakeRegionBound(101).RawRightBound, // beyond keyspace 100
			expectedResult: true,
		},
		{
			name:           "span two keyspaces txn mode",
			startKey:       MakeRegionBound(100).TxnLeftBound,
			endKey:         MakeRegionBound(101).TxnRightBound,
			expectedResult: true,
		},
		{
			name:           "span multiple keyspaces raw mode",
			startKey:       MakeRegionBound(100).RawLeftBound,
			endKey:         MakeRegionBound(105).RawRightBound,
			expectedResult: true,
		},
		{
			name:           "span multiple keyspaces txn mode",
			startKey:       MakeRegionBound(100).TxnLeftBound,
			endKey:         MakeRegionBound(105).TxnRightBound,
			expectedResult: true,
		},
		{
			name:           "empty start key",
			startKey:       []byte{},
			endKey:         MakeRegionBound(100).RawLeftBound,
			expectedResult: false,
		},
		{
			name:           "non-keyspace keys",
			startKey:       codec.EncodeBytes([]byte{'t', 1, 2, 3}),
			endKey:         codec.EncodeBytes([]byte{'t', 1, 2, 4}),
			expectedResult: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := RegionSpansMultipleKeyspaces(tc.startKey, tc.endKey)
			re.Equal(tc.expectedResult, result, "test case: %s", tc.name)
		})
	}
}

func TestGetKeyspaceSplitKeys(t *testing.T) {
	re := require.New(t)
	testCases := []struct {
		name              string
		startKey          []byte
		endKey            []byte
		expectedSplitKeys int
	}{
		{
			name:              "empty end key",
			startKey:          MakeRegionBound(1).RawLeftBound,
			endKey:            []byte{},
			expectedSplitKeys: 0,
		},
		{
			name:              "same keyspace raw mode",
			startKey:          MakeRegionBound(100).RawLeftBound,
			endKey:            MakeRegionBound(100).RawRightBound,
			expectedSplitKeys: 0, // [left100, right100) is within one keyspace
		},
		{
			name:              "same keyspace txn mode",
			startKey:          MakeRegionBound(100).TxnLeftBound,
			endKey:            MakeRegionBound(100).TxnRightBound,
			expectedSplitKeys: 0,
		},
		{
			name:              "span two keyspaces raw mode",
			startKey:          MakeRegionBound(100).RawLeftBound,
			endKey:            MakeRegionBound(101).RawRightBound, // spans keyspace 100 and 101
			expectedSplitKeys: 1,                                   // one boundary: rightBound(100)
		},
		{
			name:              "span two keyspaces txn mode",
			startKey:          MakeRegionBound(100).TxnLeftBound,
			endKey:            MakeRegionBound(101).TxnRightBound,
			expectedSplitKeys: 1,
		},
		{
			name:              "span 5 keyspaces raw mode",
			startKey:          MakeRegionBound(100).RawLeftBound,
			endKey:            MakeRegionBound(105).RawRightBound,
			expectedSplitKeys: 5, // boundaries at 100, 101, 102, 103, 104
		},
		{
			name:              "span 5 keyspaces txn mode",
			startKey:          MakeRegionBound(100).TxnLeftBound,
			endKey:            MakeRegionBound(105).TxnRightBound,
			expectedSplitKeys: 5,
		},
		{
			name:              "non-keyspace keys",
			startKey:          codec.EncodeBytes([]byte{'t', 1, 2, 3}),
			endKey:            codec.EncodeBytes([]byte{'t', 1, 2, 4}),
			expectedSplitKeys: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			splitKeys := GetKeyspaceSplitKeys(tc.startKey, tc.endKey)
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
