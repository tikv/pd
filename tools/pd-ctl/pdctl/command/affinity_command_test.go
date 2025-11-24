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
