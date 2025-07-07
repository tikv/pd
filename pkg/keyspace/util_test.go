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
)

func TestValidateID(t *testing.T) {
	re := require.New(t)
	testCases := []struct {
		id     uint32
		hasErr bool
	}{
		{100, false},
		{constant.MaxValidKeyspaceID - 1, false},
		{constant.MaxValidKeyspaceID, false},
		{constant.MaxValidKeyspaceID + 1, true},
		{math.MaxUint32, true},
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
		{"nextgen_reserved_range_id", nextGen, uint32(constant.ReservedKeyspaceIDStart + 1), "", true, false},
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
