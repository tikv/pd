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
	"context"
	"encoding/hex"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/keyspacepb"

	"github.com/tikv/pd/pkg/codec"
	coreconstant "github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
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

func TestMakeRegionBound(t *testing.T) {
	re := require.New(t)
	encodeKey := func(key []byte) []byte {
		return []byte(codec.EncodeBytes(key))
	}

	regionBound := MakeRegionBound(0x010203)
	re.Equal(encodeKey([]byte{'r', 0x01, 0x02, 0x03}), regionBound.RawLeftBound)
	re.Equal(encodeKey([]byte{'r', 0x01, 0x02, 0x04}), regionBound.RawRightBound)
	re.Equal(encodeKey([]byte{'x', 0x01, 0x02, 0x03}), regionBound.TxnLeftBound)
	re.Equal(encodeKey([]byte{'x', 0x01, 0x02, 0x04}), regionBound.TxnRightBound)

	carryRegionBound := MakeRegionBound(0x0102ff)
	re.Equal(encodeKey([]byte{'r', 0x01, 0x03, 0x00}), carryRegionBound.RawRightBound)
	re.Equal(encodeKey([]byte{'x', 0x01, 0x03, 0x00}), carryRegionBound.TxnRightBound)

	maxRegionBound := MakeRegionBound(constant.MaxValidKeyspaceID)
	re.Equal(encodeKey([]byte{'r', 0xff, 0xff, 0xff}), maxRegionBound.RawLeftBound)
	re.Equal(encodeKey([]byte{'s', 0x00, 0x00, 0x00}), maxRegionBound.RawRightBound)
	re.Equal(encodeKey([]byte{'x', 0xff, 0xff, 0xff}), maxRegionBound.TxnLeftBound)
	re.Equal(encodeKey([]byte{'y', 0x00, 0x00, 0x00}), maxRegionBound.TxnRightBound)
}

func TestMaxKeyspaceLabelRuleSplitKeys(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	regionLabeler, err := labeler.NewRegionLabeler(ctx, endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil), time.Hour)
	re.NoError(err)

	re.NoError(regionLabeler.SetLabelRule(MakeTxnLabelRule(constant.MaxValidKeyspaceID)))
	encodeKey := func(key []byte) []byte {
		return []byte(codec.EncodeBytes(key))
	}
	re.Equal(
		[][]byte{
			encodeKey([]byte{'x', 0xff, 0xff, 0xff}),
			encodeKey([]byte{'y', 0x00, 0x00, 0x00}),
		},
		regionLabeler.GetSplitKeys(nil, nil),
	)
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
		boundType         regionBoundType
		expectedLabelRule *labeler.LabelRule
	}{
		{
			id:        0,
			boundType: txnRegionBound,
			expectedLabelRule: &labeler.LabelRule{
				ID:    "keyspaces/0",
				Index: 0,
				Labels: []labeler.RegionLabel{
					{
						Key:   "id",
						Value: "0",
					},
				},
				RuleType: "key-range",
				Data: []any{
					map[string]any{
						"start_key": hex.EncodeToString(codec.EncodeBytes([]byte{'x', 0, 0, 0})),
						"end_key":   hex.EncodeToString(codec.EncodeBytes([]byte{'x', 0, 0, 1})),
					},
				},
			},
		},
		{
			id:        4242,
			boundType: txnRegionBound,
			expectedLabelRule: &labeler.LabelRule{
				ID:    "keyspaces/4242",
				Index: 0,
				Labels: []labeler.RegionLabel{
					{
						Key:   "id",
						Value: "4242",
					},
				},
				RuleType: "key-range",
				Data: []any{
					map[string]any{
						"start_key": hex.EncodeToString(codec.EncodeBytes([]byte{'x', 0, 0x10, 0x92})),
						"end_key":   hex.EncodeToString(codec.EncodeBytes([]byte{'x', 0, 0x10, 0x93})),
					},
				},
			},
		},
		{
			id:        4242,
			boundType: rawRegionBound,
			expectedLabelRule: &labeler.LabelRule{
				ID:    "keyspaces/4242",
				Index: 0,
				Labels: []labeler.RegionLabel{
					{
						Key:   "id",
						Value: "4242",
					},
				},
				RuleType: "key-range",
				Data: []any{
					map[string]any{
						"start_key": hex.EncodeToString(codec.EncodeBytes([]byte{'r', 0, 0x10, 0x92})),
						"end_key":   hex.EncodeToString(codec.EncodeBytes([]byte{'r', 0, 0x10, 0x93})),
					},
				},
			},
		},
	}
	for _, testCase := range testCases {
		re.Equal(testCase.expectedLabelRule, buildLabelRule(testCase.id, testCase.boundType))
	}
}

func TestKeyTypeToRegionBoundType(t *testing.T) {
	re := require.New(t)
	re.Equal(rawRegionBound, keyTypeToRegionBoundType(coreconstant.Raw))
	re.Equal(txnRegionBound, keyTypeToRegionBoundType(coreconstant.Table))
	re.Equal(txnRegionBound, keyTypeToRegionBoundType(coreconstant.Txn))
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
			labelRule:  MakeTxnLabelRule(1),
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
						Key:   constant.RegionLabelKey,
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
						Key:   constant.RegionLabelKey,
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
						Key:   constant.RegionLabelKey,
						Value: "1",
					},
				},
			},
			expectedID: 0,
			expectedOK: false,
		},
		// Invalid keyspace label ID - non-canonical keyspace ID.
		{
			labelRule: &labeler.LabelRule{
				ID:     "keyspaces/01",
				Labels: []labeler.RegionLabel{{Key: constant.RegionLabelKey, Value: "01"}},
			},
			expectedID: 0,
			expectedOK: false,
		},
		// Invalid keyspace label ID - out of valid keyspace range.
		{
			labelRule: &labeler.LabelRule{
				ID: "keyspaces/" + strconv.FormatUint(uint64(constant.MaxValidKeyspaceID)+1, 10),
				Labels: []labeler.RegionLabel{
					{
						Key:   constant.RegionLabelKey,
						Value: strconv.FormatUint(uint64(constant.MaxValidKeyspaceID)+1, 10),
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
		// Invalid keyspace zero label rule - missing keyspace ID label.
		{
			labelRule: &labeler.LabelRule{
				ID: getRegionLabelID(0),
				Labels: []labeler.RegionLabel{
					{
						Key:   "not-id",
						Value: "0",
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

func TestExtractKeyspaceID(t *testing.T) {
	re := require.New(t)
	testCases := []struct {
		name            string
		key             []byte
		expectedID      uint32
		expectedKeyType regionBoundType
		expectedOK      bool
	}{
		{
			name:            "empty key",
			key:             []byte{},
			expectedID:      constant.MaxValidKeyspaceID,
			expectedKeyType: txnRegionBound,
			expectedOK:      true,
		},
		{
			name:            "keyspace 0 txn mode",
			key:             MakeRegionBound(0).TxnLeftBound,
			expectedID:      0,
			expectedKeyType: txnRegionBound,
			expectedOK:      true,
		},
		{
			name:            "keyspace 4242 txn mode",
			key:             MakeRegionBound(4242).TxnLeftBound,
			expectedID:      4242,
			expectedKeyType: txnRegionBound,
			expectedOK:      true,
		},
		{
			name:            "keyspace 0 raw mode ",
			key:             MakeRegionBound(0).RawLeftBound,
			expectedID:      0,
			expectedKeyType: rawRegionBound,
			expectedOK:      true,
		},
		{
			name:            "keyspace 100 raw mode (not supported)",
			key:             MakeRegionBound(100).RawLeftBound,
			expectedID:      100,
			expectedKeyType: rawRegionBound,
			expectedOK:      true,
		},
		{
			name:       "non-keyspace key (table key)",
			key:        codec.EncodeBytes([]byte{'t', 1, 2, 3}),
			expectedOK: false,
		},
		{
			name:       "short key",
			key:        codec.EncodeBytes([]byte{'x'}),
			expectedOK: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(_ *testing.T) {
			id, kt, ok := ExtractKeyspaceID(tc.key)
			re.Equal(tc.expectedOK, ok, "test case: %s", tc.name)
			if tc.expectedOK {
				re.Equal(tc.expectedKeyType, kt, "test case: %s", tc.name)
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
			name:           "start key is classical, end key has a valid keyspace id - ambiguous, should span",
			startKey:       codec.EncodeBytes([]byte{'t', 1, 2, 3}),
			endKey:         MakeRegionBound(100).TxnLeftBound,
			checker:        allExistChecker,
			expectedResult: true,
		},
		{
			name:           "start key has a valid keyspace id, end key is classical - ambiguous, should span",
			startKey:       MakeRegionBound(100).TxnLeftBound,
			endKey:         codec.EncodeBytes([]byte{'t', 1, 2, 3}),
			checker:        allExistChecker,
			expectedResult: true,
		},
		{
			name:           "span deleted keyspace but still cross two existing keyspaces - should span",
			startKey:       MakeRegionBound(100).TxnLeftBound,
			endKey:         MakeRegionBound(102).TxnRightBound,
			checker:        specificChecker, // keyspace 102 doesn't exist
			expectedResult: true,
		},
		{
			name:           "span existing keyspaces - should span",
			startKey:       MakeRegionBound(100).TxnLeftBound,
			endKey:         MakeRegionBound(101).TxnRightBound,
			checker:        specificChecker, // both 100 and 101 exist
			expectedResult: true,
		},
		{
			// endKey absent: spans iff the start keyspace still exists. Not
			// routed through KeyspaceExist(MaxValidKeyspaceID), so the result is
			// the same for every checker implementation.
			name:           "empty end key, start keyspace exists",
			startKey:       MakeRegionBound(100).TxnLeftBound,
			endKey:         []byte{},
			checker:        specificChecker,
			expectedResult: true,
		},
		{
			name:           "empty end key, start keyspace deleted",
			startKey:       MakeRegionBound(200).TxnLeftBound,
			endKey:         []byte{},
			checker:        specificChecker, // keyspace 200 does not exist
			expectedResult: false,
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
		keyType           coreconstant.KeyType
		checker           *mockKeyspaceChecker
		expectedSplitKeys [][]byte
	}{
		{
			name:              "classical start before first keyspace",
			startKey:          []byte{'t', 1, 2, 4},
			endKey:            MakeRegionBound(99).TxnLeftBound,
			keyType:           coreconstant.Txn,
			checker:           specificChecker,
			expectedSplitKeys: nil,
		},
		{
			name:     "raw range into classical tail",
			startKey: MakeRegionBound(102).RawLeftBound,
			endKey:   []byte{'t', 1, 2, 4},
			keyType:  coreconstant.Raw,
			checker:  specificChecker,
			expectedSplitKeys: [][]byte{
				MakeRegionBound(103).RawLeftBound,
			},
		},
		{
			name:     "classical start into txn keyspace",
			startKey: []byte{'t', 1, 2, 4},
			endKey:   MakeRegionBound(102).TxnLeftBound,
			keyType:  coreconstant.Txn,
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
			keyType:  coreconstant.Raw,
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
			keyType:  coreconstant.Txn,
			checker:  allExistChecker,
			expectedSplitKeys: [][]byte{
				MakeRegionBound(100).TxnRightBound,
			},
		},
		{
			name:     "span two keyspaces raw mode",
			startKey: MakeRegionBound(100).RawLeftBound,
			endKey:   MakeRegionBound(101).RawRightBound,
			keyType:  coreconstant.Raw,
			checker:  allExistChecker,
			expectedSplitKeys: [][]byte{
				MakeRegionBound(100).RawRightBound,
			},
		},
		{
			name:              "same keyspace txn mode",
			startKey:          MakeRegionBound(100).TxnLeftBound,
			endKey:            MakeRegionBound(100).TxnRightBound,
			keyType:           coreconstant.Txn,
			checker:           allExistChecker,
			expectedSplitKeys: nil,
		},
		{
			name:              "same keyspace raw mode",
			startKey:          MakeRegionBound(100).RawLeftBound,
			endKey:            MakeRegionBound(100).RawRightBound,
			keyType:           coreconstant.Raw,
			checker:           allExistChecker,
			expectedSplitKeys: nil,
		},
		{
			name:     "adjacent range with one keyspace",
			startKey: MakeRegionBound(101).TxnLeftBound,
			endKey:   MakeRegionBound(102).TxnRightBound,
			keyType:  coreconstant.Txn,
			checker:  oneExistChecker,
			expectedSplitKeys: [][]byte{
				MakeRegionBound(101).TxnRightBound,
			},
		},
		{
			name:     "empty start and end key, raw mode, three exist",
			startKey: []byte{},
			endKey:   []byte{},
			keyType:  coreconstant.Raw,
			checker:  specificChecker,
			expectedSplitKeys: [][]byte{
				MakeRegionBound(100).RawLeftBound,
				MakeRegionBound(101).RawLeftBound,
				MakeRegionBound(102).RawLeftBound,
				MakeRegionBound(103).RawLeftBound,
			},
		},
		{
			name:     "empty start and end key, txn mode, three exist",
			startKey: []byte{},
			endKey:   []byte{},
			keyType:  coreconstant.Txn,
			checker:  specificChecker,
			expectedSplitKeys: [][]byte{
				MakeRegionBound(100).TxnLeftBound,
				MakeRegionBound(101).TxnLeftBound,
				MakeRegionBound(102).TxnLeftBound,
				MakeRegionBound(103).TxnLeftBound,
			},
		},
		{
			name:              "no keyspace key on either side",
			startKey:          []byte{'t', 1, 2, 3},
			endKey:            []byte{'t', 1, 2, 4},
			keyType:           coreconstant.Txn,
			checker:           specificChecker,
			expectedSplitKeys: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(_ *testing.T) {
			splitKeys := GetKeyspaceSplitKeys(tc.startKey, tc.endKey, tc.keyType, tc.checker)
			re.Equal(tc.expectedSplitKeys, splitKeys, "test case: %s", tc.name)
		})
	}
}

// existOnlyChecker only answers KeyspaceExist; it panics if GetKeyspaceIDInRange
// is called, so it can assert a code path never falls back to a range scan.
type existOnlyChecker struct {
	existing map[uint32]bool
}

func (c *existOnlyChecker) KeyspaceExist(id uint32) bool {
	return c.existing[id]
}

func (*existOnlyChecker) GetKeyspaceIDInRange(uint32, uint32, int) ([]uint32, bool) {
	panic("GetKeyspaceIDInRange should not be called when the ID window can hold no keyspace of its own")
}

func TestGetKeyspaceSplitKeysForeignModeKeys(t *testing.T) {
	re := require.New(t)

	specificChecker := &mockKeyspaceChecker{
		existingKeyspaces: map[uint32]bool{
			100: true,
			101: true,
			102: true,
		},
	}

	testCases := []struct {
		name              string
		startKey          []byte
		endKey            []byte
		keyType           coreconstant.KeyType
		checker           Checker
		expectedSplitKeys [][]byte
	}{
		{
			// Both ends sort before the txn block ('r' < 'x'), so neither
			// carries any information about the txn keyspace space at all.
			name:              "raw start and classical end, txn mode - both before the txn block",
			startKey:          MakeRegionBound(100).RawLeftBound,
			endKey:            []byte{'t', 1, 2, 4},
			keyType:           coreconstant.Txn,
			checker:           specificChecker,
			expectedSplitKeys: nil,
		},
		{
			// Both ends sort at or after the raw block ('t' and 'x' both > 'r'),
			// so neither carries any information about the raw keyspace space.
			name:              "classical start and txn end, raw mode - both after the raw block",
			startKey:          []byte{'t', 1, 2, 4},
			endKey:            MakeRegionBound(100).TxnLeftBound,
			keyType:           coreconstant.Raw,
			checker:           specificChecker,
			expectedSplitKeys: nil,
		},
		{
			// startKey is a real raw keyspace key; under txn mode it is
			// classified as "before the txn block", i.e. startID=0 - the same
			// window a plain classical/empty start key would produce.
			name:     "raw start key under txn mode is treated like an empty start",
			startKey: MakeRegionBound(200).RawLeftBound,
			endKey:   MakeRegionBound(101).TxnLeftBound,
			keyType:  coreconstant.Txn,
			checker:  specificChecker,
			expectedSplitKeys: [][]byte{
				MakeRegionBound(100).TxnLeftBound,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(_ *testing.T) {
			splitKeys := GetKeyspaceSplitKeys(tc.startKey, tc.endKey, tc.keyType, tc.checker)
			re.Equal(tc.expectedSplitKeys, splitKeys, "test case: %s", tc.name)
		})
	}
}

// deeperTxnKey builds a real memcomparable txn-mode key that sits strictly
// inside keyspace id's key range (as opposed to MakeRegionBound's bare left
// bound, which sits exactly at the keyspace's first byte).
func deeperTxnKey(id uint32, suffix ...byte) []byte {
	payload := append(codec.MakeKeyspacePrefix(codec.TxnKeyspaceModePrefix, id), suffix...)
	return codec.EncodeBytes(payload)
}

func TestGetKeyspaceSplitKeysAdjacentIDsSkipsRangeScan(t *testing.T) {
	re := require.New(t)

	// startKey sits inside keyspace 100 and endKey sits inside keyspace 101,
	// so [startID, endID] = [100, 101] and the only possible split point is
	// the boundary they share.
	startKey := deeperTxnKey(100, 5, 5)
	endKey := deeperTxnKey(101, 5, 5)
	boundary := MakeRegionBound(100).TxnRightBound
	re.Equal(boundary, MakeRegionBound(101).TxnLeftBound, "sanity check: keyspace 100's right bound must equal keyspace 101's left bound")

	testCases := []struct {
		name              string
		startKey          []byte
		endKey            []byte
		checker           *existOnlyChecker
		expectedSplitKeys [][]byte
	}{
		{
			name:              "boundary between existing adjacent keyspaces is split",
			startKey:          startKey,
			endKey:            endKey,
			checker:           &existOnlyChecker{existing: map[uint32]bool{100: true, 101: true}},
			expectedSplitKeys: [][]byte{boundary},
		},
		{
			name:              "only the lower keyspace exists",
			startKey:          startKey,
			endKey:            endKey,
			checker:           &existOnlyChecker{existing: map[uint32]bool{100: true}},
			expectedSplitKeys: [][]byte{boundary},
		},
		{
			name:              "neither adjacent keyspace exists",
			startKey:          startKey,
			endKey:            endKey,
			checker:           &existOnlyChecker{},
			expectedSplitKeys: nil,
		},
		{
			name:              "boundary coincides with the region's own end key, so it is excluded",
			startKey:          MakeRegionBound(100).TxnLeftBound,
			endKey:            MakeRegionBound(100).TxnRightBound,
			checker:           &existOnlyChecker{existing: map[uint32]bool{100: true, 101: true}},
			expectedSplitKeys: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(_ *testing.T) {
			re.NotPanics(func() {
				splitKeys := GetKeyspaceSplitKeys(tc.startKey, tc.endKey, coreconstant.Txn, tc.checker)
				re.Equal(tc.expectedSplitKeys, splitKeys, "test case: %s", tc.name)
			})
		})
	}
}

// TestGetKeyspaceSplitKeysAdjacentIDsIgnoreMaxSentinelExistence verifies that
// the adjacent-ID shortcut's result does not depend on how a Checker
// implementation answers KeyspaceExist(MaxValidKeyspaceID). endID lands on
// that sentinel whenever the end key is empty or classified as sorting past
// the target mode's key block - it is never a real keyspace ID any Checker
// was asked to track - so a Checker (like the real MCS scheduling-server
// implementation) that answers false for it must not change the outcome
// compared to one (like the real Manager implementation) that hardcodes true.
func TestGetKeyspaceSplitKeysAdjacentIDsIgnoreMaxSentinelExistence(t *testing.T) {
	re := require.New(t)

	// startID resolves to MaxValidKeyspaceID-1 and, since endKey is empty,
	// endID defaults to the MaxValidKeyspaceID sentinel: adjacent IDs.
	startKey := deeperTxnKey(constant.MaxValidKeyspaceID-1, 5, 5)
	boundary := MakeRegionBound(constant.MaxValidKeyspaceID - 1).TxnRightBound

	// The checker does not know about the start keyspace and, like the real
	// MCS Checker, has no entry for the MaxValidKeyspaceID sentinel either.
	checker := &existOnlyChecker{}

	splitKeys := GetKeyspaceSplitKeys(startKey, []byte{}, coreconstant.Txn, checker)
	re.Equal([][]byte{boundary}, splitKeys,
		"the split point must not be dropped just because the checker doesn't recognize the MaxValidKeyspaceID sentinel as existing")
}

func TestKeyspaceExists(t *testing.T) {
	re := require.New(t)
	cache := NewCache()

	cache.Save(100, "ks-100", keyspacepb.KeyspaceState_ENABLED)
	cache.Save(101, "ks-101", keyspacepb.KeyspaceState_ARCHIVED)
	cache.Save(102, "ks-102", keyspacepb.KeyspaceState_DISABLED)
	cache.Save(103, "ks-103", keyspacepb.KeyspaceState_TOMBSTONE)

	re.True(cache.KeyspaceExist(100))
	re.True(cache.KeyspaceExist(101))
	re.True(cache.KeyspaceExist(102))
	re.False(cache.KeyspaceExist(103))
}

func TestGetKeyspaceIDInRange(t *testing.T) {
	re := require.New(t)
	cache := NewCache()

	cache.Save(100, "ks-100", keyspacepb.KeyspaceState_ENABLED)
	cache.Save(101, "ks-101", keyspacepb.KeyspaceState_ARCHIVED)
	cache.Save(102, "ks-102", keyspacepb.KeyspaceState_DISABLED)
	cache.Save(103, "ks-103", keyspacepb.KeyspaceState_TOMBSTONE)

	item, ok := cache.getKeyspaceByID(101)
	re.True(ok)
	re.Equal(uint32(101), item.keyspaceID)
	re.Equal("ks-101", item.name)
	re.Equal(keyspacepb.KeyspaceState_ARCHIVED, item.state)

	ok = cache.KeyspaceExist(103)
	re.False(ok)

	all := func() []uint32 {
		var ret []uint32
		cache.scanAllKeyspaces(func(keyspaceID uint32, _ string, _ keyspacepb.KeyspaceState) bool {
			ret = append(ret, keyspaceID)
			return true
		})
		return ret
	}
	re.Equal([]uint32{100, 101, 102, 103}, all())

	// 103 is tombstone, so it should not be returned.
	ids, ok := cache.GetKeyspaceIDInRange(100, 103, 1)
	re.True(ok)
	re.Equal([]uint32{102}, ids)

	ids, ok = cache.GetKeyspaceIDInRange(101, 102, 1)
	re.True(ok)
	re.Equal([]uint32{102}, ids)

	// 102 is tombstone, so it returns nothings.
	ids, ok = cache.GetKeyspaceIDInRange(103, 104, 1)
	re.False(ok)
	re.Empty(ids)

	// Delete 101 and check the cache again.
	cache.DeleteKeyspace(101)
	_, ok = cache.getKeyspaceByID(101)
	re.False(ok)
	re.Equal([]uint32{100, 102, 103}, all())

	// the order of returned keyspace IDs should be descending.
	ids, ok = cache.GetKeyspaceIDInRange(100, 103, 5)
	re.True(ok)
	re.Len(ids, 2)
	re.Equal([]uint32{102, 100}, ids)
}
