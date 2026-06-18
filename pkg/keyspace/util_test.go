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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"

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

func TestMakeKeyspacePrefix(t *testing.T) {
	re := require.New(t)

	testCases := []struct {
		name       string
		mode       byte
		id         uint32
		wantPrefix []byte
	}{
		{
			name:       "raw",
			mode:       RawKeyspaceModePrefix,
			id:         0x010203,
			wantPrefix: []byte{'r', 0x01, 0x02, 0x03},
		},
		{
			name:       "txn",
			mode:       TxnKeyspaceModePrefix,
			id:         constant.MaxValidKeyspaceID,
			wantPrefix: []byte{'x', 0xff, 0xff, 0xff},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(_ *testing.T) {
			re.Equal(testCase.wantPrefix, MakeKeyspacePrefix(testCase.mode, testCase.id))
		})
	}
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

func TestParseKeyspacePrefix(t *testing.T) {
	re := require.New(t)

	testCases := []struct {
		name string
		key  []byte
		mode byte
		id   uint32
	}{
		{
			name: "raw",
			key:  []byte{'r', 0x01, 0x02, 0x03},
			mode: RawKeyspaceModePrefix,
			id:   0x010203,
		},
		{
			name: "txn with suffix",
			key:  []byte{'x', 0xff, 0xff, 0xff, 't'},
			mode: TxnKeyspaceModePrefix,
			id:   constant.MaxValidKeyspaceID,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(_ *testing.T) {
			mode, id, ok := ParseKeyspacePrefix(testCase.key)
			re.True(ok)
			re.Equal(testCase.mode, mode)
			re.Equal(testCase.id, id)
		})
	}

	_, _, ok := ParseKeyspacePrefix([]byte{'x', 0x01, 0x02})
	re.False(ok)
	_, _, ok = ParseKeyspacePrefix([]byte{'t', 0x01, 0x02, 0x03})
	re.False(ok)
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
