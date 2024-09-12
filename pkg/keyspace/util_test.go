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
	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/schedule/labeler"
)

func TestValidateID(t *testing.T) {
	re := require.New(t)
	testCases := []struct {
		id     uint32
		hasErr bool
	}{
		{constant.DefaultKeyspaceID, true}, // Reserved id should result in error.
		{100, false},
		{spaceIDMax - 1, false},
		{spaceIDMax, false},
		{spaceIDMax + 1, true},
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
		{constant.DefaultKeyspaceName, true}, // Reserved name should result in error.
		{"keyspaceName1", false},
		{"keyspace_name_1", false},
		{"10", false},
		{"", true},
		{"keyspace/", true},
		{"keyspace:1", true},
		{"many many spaces", true},
		{"keyspace?limit=1", true},
		{"keyspace%1", true},
	}
	for _, testCase := range testCases {
		re.Equal(testCase.hasErr, validateName(testCase.name) != nil)
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
