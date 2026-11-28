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

package command

import (
	"bytes"
	"context"
	"encoding/hex"
	"io"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	pd "github.com/tikv/pd/client/http"
	"github.com/tikv/pd/pkg/schedule/affinity"
)

func TestAffinityRangeAddDoesNotReadLabelRules(t *testing.T) {
	re := require.New(t)
	previous := PDCli
	t.Cleanup(func() { PDCli = previous })
	client := &affinityTestClient{}
	PDCli = client
	cmd := NewAffinityCommand()
	cmd.PersistentPreRunE = nil
	cmd.SetArgs([]string{"range", "add", "--group-id", "custom", "--range", "7800000174800000ff0000000064000000fc,7800000174800000ff0000000065000000fc"})
	re.NoError(cmd.Execute())
	re.Equal("custom", client.calledID)
	re.Zero(client.labelRuleReads)
}

func TestAffinityRangeShowMissingLabelRule(t *testing.T) {
	previous := PDCli
	t.Cleanup(func() { PDCli = previous })

	for _, tc := range []struct {
		name         string
		rangeCount   int
		missingReads int
		wantReads    int
		wantJSON     string
	}{
		{"empty group", 0, 1, 1, `{"id":"custom","ranges":[]}`},
		{"rule found on retry", 1, 1, 2, `{"id":"custom","ranges":[{"start_key":"0a","end_key":"0b"}]}`},
		{"rule still missing", 1, 2, 2, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &affinityTestClient{
				rangeCount:       tc.rangeCount,
				missingRuleReads: tc.missingReads,
				ruleData:         []any{map[string]any{"start_key": "0a", "end_key": "0b"}},
			}
			PDCli = client
			cmd := NewAffinityCommand()
			cmd.PersistentPreRunE = nil
			cmd.SilenceUsage = true
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(io.Discard)
			cmd.SetArgs([]string{"range", "show", "--group-id", "custom"})
			err := cmd.Execute()
			require.Equal(t, tc.wantReads, client.labelRuleReads)
			if tc.wantJSON == "" {
				require.ErrorContains(t, err, "affinity group custom reports key ranges but its label rule is missing, please retry")
				require.Empty(t, out.String())
				return
			}
			require.NoError(t, err)
			require.JSONEq(t, tc.wantJSON, out.String())
		})
	}
}

func TestAffinityCommandsRejectPositionalArgs(t *testing.T) {
	for _, args := range [][]string{{"show"}, {"create"}, {"delete"}, {"update"}, {"policy"}, {"range", "show"}, {"range", "add"}, {"range", "remove"}} {
		t.Run(args[len(args)-1], func(t *testing.T) {
			re := require.New(t)
			cmd := NewAffinityCommand()
			cmd.PersistentPreRunE = nil
			sub, _, err := cmd.Find(args)
			re.NoError(err)
			called := false
			sub.RunE = func(_ *cobra.Command, _ []string) error { called = true; return nil }
			cmd.SetArgs(append(args, "unexpected-group"))
			re.ErrorContains(cmd.Execute(), "unknown command")
			re.False(called)
		})
	}
}

func TestAffinityPolicyBatchSelectors(t *testing.T) {
	for _, selector := range []string{"group-id", "table-id", "partition-id", "keyspace-id"} {
		t.Run(selector, func(t *testing.T) {
			cmd := NewAffinityCommand()
			cmd.PersistentPreRunE = nil
			cmd.SetArgs([]string{"policy", "--group-ids", "a,b", "--" + selector, "1", "--balance-policy", "fixed"})
			require.ErrorContains(t, cmd.Execute(), "--group-ids cannot be used with --"+selector)
		})
	}
}

func TestTiDBGroupKeyRange(t *testing.T) {
	re := require.New(t)
	// The expected keys are TiDB table prefixes t{id} encoded like Region keys,
	// as TiDB builds them for affinity groups. A keyspace adds the transaction
	// keyspace prefix x{keyspace ID in 3 bytes} before the encoding.
	for _, tc := range []struct {
		groupID    string
		start, end string
	}{
		{"_tidb_t_100", "7480000000000000ff6400000000000000f8", "7480000000000000ff6500000000000000f8"},
		{"_tidb_pt_100_p101", "7480000000000000ff6500000000000000f8", "7480000000000000ff6600000000000000f8"},
		{"_tidb_ks1_t_100", "7800000174800000ff0000000064000000fc", "7800000174800000ff0000000065000000fc"},
		{"_tidb_ks0_pt_100_p101", "7800000074800000ff0000000065000000fc", "7800000074800000ff0000000066000000fc"},
	} {
		keyRange, err := tidbGroupKeyRange(tc.groupID)
		re.NoError(err, tc.groupID)
		re.Equal(tc.start, hex.EncodeToString(keyRange.StartKey), tc.groupID)
		re.Equal(tc.end, hex.EncodeToString(keyRange.EndKey), tc.groupID)
	}

	for _, tc := range []struct {
		groupID, err string
	}{
		{"_tidb_pg1", "the _tidb_ prefix is reserved for TiDB"},
		{"_tidb_x_1", "the _tidb_ prefix is reserved for TiDB"},
		{"_tidb_t_0", "not a TiDB table or partition group"},
		{"_tidb_pt_99999999999999999999999_p1", "invalid table ID"},
		{"_tidb_ks1_pt_99999999999999999999999_p1", "invalid table ID"},
		{"_tidb_pt_1_p99999999999999999999999", "invalid physical ID"},
		{"_tidb_ks16777216_t_1", "invalid keyspace ID"},
		{"_tidb_t_9223372036854775807", "invalid physical ID"},
	} {
		_, err := tidbGroupKeyRange(tc.groupID)
		re.ErrorContains(err, tc.err, tc.groupID)
	}
}

func TestParseAffinityKeyRanges(t *testing.T) {
	re := require.New(t)
	for _, tc := range []struct {
		input, expected string
	}{
		{"0a,0b", "0a,0b"},
		{" 7480 , 7481 ", "7480,7481"},
		{"61,", "61,"},
		{",61", ",61"},
		{",", ","},
	} {
		ranges, err := parseAffinityKeyRanges([]string{tc.input})
		re.NoError(err, tc.input)
		re.Len(ranges, 1)
		re.Equal(tc.expected, formatAffinityKeyRange(ranges[0]))
	}

	for _, raw := range []string{"0a", "0a,0b,0c", "zz,0b", "0a,zz", "0,1"} {
		_, err := parseAffinityKeyRanges([]string{raw})
		re.Error(err, raw)
	}
}

type affinityTestClient struct {
	pd.Client
	calledID         string
	labelRuleReads   int
	missingRuleReads int
	rangeCount       int
	ruleData         []any
}

func (c *affinityTestClient) GetAffinityGroup(_ context.Context, groupID string) (*pd.AffinityGroupState, error) {
	c.calledID = groupID
	return &pd.AffinityGroupState{ID: groupID, RangeCount: c.rangeCount}, nil
}

func (c *affinityTestClient) GetRegionLabelRulesByIDs(_ context.Context, ruleIDs []string) ([]*pd.LabelRule, error) {
	c.labelRuleReads++
	c.calledID = ruleIDs[0]
	if c.labelRuleReads <= c.missingRuleReads {
		return nil, nil
	}
	return []*pd.LabelRule{{ID: c.calledID, Data: c.ruleData}}, nil
}

func (c *affinityTestClient) DeleteAffinityGroup(_ context.Context, groupID string, _ bool) error {
	c.calledID = groupID
	return nil
}

func (c *affinityTestClient) BatchDeleteAffinityGroups(_ context.Context, groupIDs []string, _ bool) error {
	if len(groupIDs) == 1 {
		c.calledID = groupIDs[0]
	}
	return nil
}

func (c *affinityTestClient) RemoveAffinityGroupKeyRanges(_ context.Context, groups map[string][]pd.AffinityGroupKeyRange) (map[string]*pd.AffinityGroupState, error) {
	return c.recordGroupChange(groups)
}

func (c *affinityTestClient) CreateAffinityGroups(_ context.Context, groups map[string][]pd.AffinityGroupKeyRange, _ ...pd.CreateAffinityGroupsOption) (map[string]*pd.AffinityGroupState, error) {
	return c.recordGroupChange(groups)
}

func (c *affinityTestClient) AddAffinityGroupKeyRanges(_ context.Context, groups map[string][]pd.AffinityGroupKeyRange) (map[string]*pd.AffinityGroupState, error) {
	return c.recordGroupChange(groups)
}

func (c *affinityTestClient) recordGroupChange(groups map[string][]pd.AffinityGroupKeyRange) (map[string]*pd.AffinityGroupState, error) {
	for id := range groups {
		c.calledID = id
	}
	return map[string]*pd.AffinityGroupState{c.calledID: {ID: c.calledID}}, nil
}

func (c *affinityTestClient) UpdateAffinityGroupPeers(_ context.Context, groupID string, _ uint64, _ []uint64) (*pd.AffinityGroupState, error) {
	c.calledID = groupID
	return &pd.AffinityGroupState{ID: groupID}, nil
}

func (c *affinityTestClient) UpdateAffinityGroupPolicy(_ context.Context, groupID string, _ string) (*pd.AffinityGroupState, error) {
	c.calledID = groupID
	return &pd.AffinityGroupState{ID: groupID}, nil
}

func TestAffinityCreateTiDBTableGroupRepairGuidance(t *testing.T) {
	previous := PDCli
	t.Cleanup(func() { PDCli = previous })

	for _, groupID := range []string{"_tidb_ks1_tg_100", "_tidb_tg_100"} {
		t.Run(groupID, func(t *testing.T) {
			re := require.New(t)
			client := &affinityTestClient{}
			PDCli = client
			cmd := NewAffinityCommand()
			cmd.PersistentPreRunE = nil
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)
			cmd.SetArgs([]string{"create", "--group-id", groupID, "--range", "0a,0b"})
			err := cmd.Execute()
			re.ErrorContains(err, "repair it with ADMIN REPAIR AFFINITY TABLE GROUP")
			re.Empty(client.calledID)
		})
	}
}

func TestAffinityGroupSelectors(t *testing.T) {
	previous := PDCli
	t.Cleanup(func() { PDCli = previous })

	runCmd := func(args ...string) (string, error) {
		client := &affinityTestClient{}
		PDCli = client
		cmd := NewAffinityCommand()
		cmd.PersistentPreRunE = nil
		cmd.SetOut(io.Discard)
		cmd.SetErr(io.Discard)
		cmd.SetArgs(args)
		err := cmd.Execute()
		return client.calledID, err
	}

	const sampleRange = "7800000174800000ff0000000064000000fc,7800000174800000ff0000000065000000fc"

	// Both ID forms are sent to the target PD regardless of how pd-ctl was built.
	for _, tc := range []struct {
		name   string
		args   []string
		wantID string
	}{
		{"create_keyspace_table", []string{"create", "--table-id", "9001", "--keyspace-id", "1"}, "_tidb_ks1_t_9001"},
		{"policy_keyspace_table", []string{"policy", "--table-id", "9001", "--keyspace-id", "1", "--balance-policy", "fixed"}, "_tidb_ks1_t_9001"},
		{"range_add_keyspace_table", []string{"range", "add", "--table-id", "9001", "--keyspace-id", "1", "--force", "--range", sampleRange}, "_tidb_ks1_t_9001"},
		{"show_keyspace_group_id", []string{"show", "--group-id", "_tidb_ks1_t_9001"}, "_tidb_ks1_t_9001"},
		{"show_long_group_id", []string{"show", "--group-id", "a-0_" + strings.Repeat("z", 60)}, "a-0_" + strings.Repeat("z", 60)},
		{"show_table", []string{"show", "--table-id", "9001"}, "_tidb_t_9001"},
		{"show_partition", []string{"show", "--table-id", "9001", "--partition-id", "9002"}, "_tidb_pt_9001_p9002"},
		{"range_show_table", []string{"range", "show", "--table-id", "9001"}, affinity.GetLabelRuleID("_tidb_t_9001")},
		{"delete_table", []string{"delete", "--table-id", "9001"}, "_tidb_t_9001"},
		{"delete_force_table", []string{"delete", "--table-id", "9001", "--force"}, "_tidb_t_9001"},
		{"range_remove_table", []string{"range", "remove", "--table-id", "9001", "--force", "--range", sampleRange}, "_tidb_t_9001"},
		{"range_remove_partition", []string{"range", "remove", "--table-id", "9001", "--partition-id", "9002", "--force", "--range", sampleRange}, "_tidb_pt_9001_p9002"},
		{"create_unscoped_table", []string{"create", "--table-id", "9001"}, "_tidb_t_9001"},
		{"create_unscoped_partition", []string{"create", "--table-id", "9001", "--partition-id", "9002"}, "_tidb_pt_9001_p9002"},
		{"range_add_unscoped_table", []string{"range", "add", "--table-id", "9001", "--force", "--range", sampleRange}, "_tidb_t_9001"},
		{"update_unscoped_table", []string{"update", "--table-id", "9001", "--leader", "1", "--voters", "1,2,3"}, "_tidb_t_9001"},
		{"policy_unscoped_table", []string{"policy", "--table-id", "9001", "--balance-policy", "fixed"}, "_tidb_t_9001"},
		{"policy_unscoped_group_id", []string{"policy", "--group-id", "_tidb_t_9001", "--balance-policy", "fixed"}, "_tidb_t_9001"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			calledID, err := runCmd(tc.args...)
			re.NoError(err)
			re.Equal(tc.wantID, calledID)
		})
	}
	for _, groupID := range []string{"", "a/b", strings.Repeat("a", 65)} {
		calledID, err := runCmd("show", "--group-id", groupID)
		require.ErrorContains(t, err, "it must be 1 to 64 letters, digits, hyphens or underscores")
		require.Empty(t, calledID)
	}
}
