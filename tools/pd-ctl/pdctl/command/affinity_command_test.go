// Copyright 2025 TiKV Project Authors.
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

package command

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"

	pd "github.com/tikv/pd/client/http"
)

func TestBuildAffinityGroupDefinitionsTable(t *testing.T) {
	re := require.New(t)
	info := tableAffinityInfo{
		DB:      "test",
		Table:   "t",
		TableID: 123,
	}
	defs, err := buildAffinityGroupDefinitions(info, "")
	re.NoError(err)
	re.Len(defs, 1)
	re.Equal("_tidb_c_t_123", defs[0].id)
	re.Len(defs[0].ranges, 1)
	start, end := tableKeyRange(123)
	re.Equal(start, defs[0].ranges[0].StartKey)
	re.Equal(end, defs[0].ranges[0].EndKey)

	_, err = buildAffinityGroupDefinitions(info, "p1")
	re.Error(err)
}

func TestBuildAffinityGroupDefinitionsPartition(t *testing.T) {
	re := require.New(t)
	info := tableAffinityInfo{
		DB:      "test",
		Table:   "pt",
		TableID: 10,
		Partitions: []partitionInfo{
			{ID: 101, Name: "p1"},
			{ID: 102, Name: "p2"},
		},
	}
	// all partitions
	defs, err := buildAffinityGroupDefinitions(info, "")
	re.NoError(err)
	re.Len(defs, 2)
	re.ElementsMatch([]string{"_tidb_p_t_10_p101", "_tidb_p_t_10_p102"}, collectGroupIDs(defs))

	// single partition by name
	defs, err = buildAffinityGroupDefinitions(info, "p2")
	re.NoError(err)
	re.Len(defs, 1)
	re.Equal("_tidb_p_t_10_p102", defs[0].id)

	// single partition by ID string
	defs, err = buildAffinityGroupDefinitions(info, "101")
	re.NoError(err)
	re.Len(defs, 1)
	re.Equal("_tidb_p_t_10_p101", defs[0].id)
}

func TestParseGroupIDs(t *testing.T) {
	re := require.New(t)
	groupStates := map[string]*pd.AffinityGroupState{
		"_tidb_c_t_5":     nil,
		"_tidb_p_t_6_p7":  nil,
		"_tidb_p_t_6_p8":  nil,
		"unrelated_group": nil,
	}
	parsed := parseGroupIDs(groupStates)
	re.ElementsMatch([]int64{5, 6}, parsed.tableIDs)
	re.ElementsMatch([]int64{7, 8}, parsed.partitionIDs)
	re.Contains(parsed.tableGroups, "_tidb_c_t_5")
	re.Contains(parsed.partitionGroups, "_tidb_p_t_6_p7")
	re.Contains(parsed.partitionGroups, "_tidb_p_t_6_p8")
}

func TestEncodeTablePrefix(t *testing.T) {
	re := require.New(t)

	// Test table ID 115
	// Expected key should use EncodeBytes format:
	// - table prefix 't' + EncodeInt(115) = "74 80 00 00 00 00 00 00 73"
	// - After EncodeBytes: "74 80 00 00 00 00 00 00 ff 73 00 00 00 00 00 00 00 f8"
	tableID := int64(115)
	key := encodeTablePrefix(tableID)
	expectedHex := "7480000000000000ff7300000000000000f8"
	actualHex := hex.EncodeToString(key)

	re.Equal(expectedHex, actualHex, "encodeTablePrefix should use EncodeBytes to encode the key")
}

func TestTableKeyRange(t *testing.T) {
	re := require.New(t)

	tableID := int64(115)
	start, end := tableKeyRange(tableID)

	// Start key should be encoded table prefix for table 115
	expectedStartHex := "7480000000000000ff7300000000000000f8"
	actualStartHex := hex.EncodeToString(start)
	re.Equal(expectedStartHex, actualStartHex, "start key should match expected encoding")

	// End key should be encoded table prefix for table 116
	expectedEndHex := "7480000000000000ff7400000000000000f8"
	actualEndHex := hex.EncodeToString(end)
	re.Equal(expectedEndHex, actualEndHex, "end key should match expected encoding")

	// Verify start < end
	re.Less(start, end)
}

func TestTableKeyRangeWithPartition(t *testing.T) {
	re := require.New(t)

	// Test partition ID encoding
	// In TiDB, each partition is like an independent table, using partition ID as table ID
	partitionID := int64(201)
	start, end := tableKeyRange(partitionID)

	// Partition key should use the same encoding as table key: t{partitionID}
	expectedStartHex := "7480000000000000ffc900000000000000f8"
	actualStartHex := hex.EncodeToString(start)
	re.Equal(expectedStartHex, actualStartHex, "partition start key should use same encoding as table key")

	// End key should be encoded table prefix for partition 202
	expectedEndHex := "7480000000000000ffca00000000000000f8"
	actualEndHex := hex.EncodeToString(end)
	re.Equal(expectedEndHex, actualEndHex, "partition end key should match expected encoding")

	// Verify start < end
	re.Less(start, end)
}

func TestBuildAffinityGroupDefinitionsWithPartitionKeyEncoding(t *testing.T) {
	re := require.New(t)

	// Test that partition table definitions use correct key encoding
	info := tableAffinityInfo{
		DB:      "test",
		Table:   "pt",
		TableID: 10,
		Partitions: []partitionInfo{
			{ID: 201, Name: "p1"},
		},
	}

	defs, err := buildAffinityGroupDefinitions(info, "p1")
	re.NoError(err)
	re.Len(defs, 1)

	// Verify the key range uses EncodeBytes for partition ID
	expectedStartHex := "7480000000000000ffc900000000000000f8" // t{201} with EncodeBytes
	actualStartHex := hex.EncodeToString(defs[0].ranges[0].StartKey)
	re.Equal(expectedStartHex, actualStartHex, "partition key range should use EncodeBytes encoding")
}
