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
	"context"
	"encoding/hex"
	"io"
	"strconv"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/errors"

	"github.com/tikv/pd/client/constants"
	pd "github.com/tikv/pd/client/http"
	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/versioninfo/kerneltype"
)

type affinityRangeAddClient struct {
	pd.Client
	added bool
	read  bool
}

func (c *affinityRangeAddClient) GetRegionLabelRulesByIDs(context.Context, []string) ([]*pd.LabelRule, error) {
	c.read = true
	return nil, errors.New("label rules are not synchronized")
}

func (c *affinityRangeAddClient) AddAffinityGroupKeyRanges(_ context.Context, groups map[string][]pd.AffinityGroupKeyRange) (map[string]*pd.AffinityGroupState, error) {
	c.added = true
	return map[string]*pd.AffinityGroupState{"custom": {ID: "custom", RangeCount: len(groups["custom"])}}, nil
}

func TestAffinityRangeAddDoesNotReadLabelRules(t *testing.T) {
	re := require.New(t)
	previous := PDCli
	t.Cleanup(func() { PDCli = previous })
	client := &affinityRangeAddClient{}
	PDCli = client
	cmd := NewAffinityCommand()
	cmd.PersistentPreRunE = nil
	cmd.SetArgs([]string{"range", "add", "--group-id", "custom", "--range", "7800000174800000ff0000000064000000fc,7800000174800000ff0000000065000000fc"})
	re.NoError(cmd.Execute())
	re.True(client.added)
	re.False(client.read)
}

func TestAffinityCommandsRejectPositionalArgs(t *testing.T) {
	for _, args := range [][]string{{"show"}, {"create"}, {"delete"}, {"update"}, {"range", "show"}, {"range", "add"}, {"range", "remove"}} {
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
	ranges, err := parseAffinityKeyRanges([]string{"0a,0b", " 7480 , 7481 ", ","})
	re.NoError(err)
	re.Len(ranges, 3)
	re.Equal([]byte{0x0a}, ranges[0].StartKey)
	re.Equal([]byte{0x0b}, ranges[0].EndKey)
	re.Equal([]byte{0x74, 0x80}, ranges[1].StartKey)
	re.Empty(ranges[2].StartKey)
	re.Empty(ranges[2].EndKey)

	for _, raw := range []string{"0a", "0a,0b,0c", "zz,0b", "0a,zz", "0,1"} {
		_, err := parseAffinityKeyRanges([]string{raw})
		re.Error(err, raw)
	}
}

func TestCheckCustomGroupKeyspace(t *testing.T) {
	re := require.New(t)
	// key returns the Region key of a table in a transaction keyspace, or the
	// key where the keyspace starts for table 0.
	key := func(keyspaceID uint32, tableID int64) []byte {
		raw := codec.MakeKeyspacePrefix(codec.TxnKeyspaceModePrefix, keyspaceID)
		if tableID > 0 {
			raw = append(raw, codec.GenerateTableKey(tableID)...)
		}
		return codec.EncodeBytes(raw)
	}
	keyRange := func(start, end []byte) pd.AffinityGroupKeyRange {
		return pd.AffinityGroupKeyRange{StartKey: start, EndKey: end}
	}
	table := func(keyspaceID uint32, tableID int64) pd.AffinityGroupKeyRange {
		return keyRange(key(keyspaceID, tableID), key(keyspaceID, tableID+1))
	}
	ranges := func(r ...pd.AffinityGroupKeyRange) []pd.AffinityGroupKeyRange { return r }
	quote := func(r pd.AffinityGroupKeyRange) string { return strconv.Quote(formatAffinityKeyRange(r)) }
	afterLast := codec.EncodeKeyspaceBoundary(codec.TxnKeyspaceModePrefix, constants.MaxKeyspaceID+1)
	rawKeyspace := codec.EncodeBytes(codec.MakeKeyspacePrefix(codec.RawKeyspaceModePrefix, 1))
	for _, tc := range []struct {
		existing, added []pd.AffinityGroupKeyRange
		// problem is what the first range that breaks the rule does, or empty
		// if the ranges lie in one keyspace.
		problem string
	}{
		{added: ranges(table(1, 100), table(1, 200))},
		{existing: ranges(table(1, 100)), added: ranges(table(1, 200))},
		// A keyspace ends where the next one starts, and the last one at the
		// boundary after it.
		{added: ranges(keyRange(key(1, 0), key(2, 0)))},
		{added: ranges(keyRange(key(constants.MaxKeyspaceID, 0), afterLast[:]))},
		// A Region boundary must contain the complete encoded keyspace prefix.
		{added: ranges(keyRange([]byte{'x', 0, 0, 1, 't'}, []byte{'x', 0, 0, 1, 'u'})),
			problem: `key range "7800000174,7800000175" does not start in a transaction keyspace`},
		{added: ranges(keyRange(nil, nil)), problem: `key range "," covers the whole key space`},
		{
			added:   ranges(keyRange(nil, key(1, 100))),
			problem: `key range ",7800000174800000ff0000000064000000fc" has no start key`,
		},
		{
			added:   ranges(keyRange(key(1, 100), nil)),
			problem: `key range "7800000174800000ff0000000064000000fc," starts in keyspace 1 and has no end key`,
		},
		{added: ranges(keyRange([]byte{0x0a}, []byte{0x0b})), problem: `key range "0a,0b" does not start in a transaction keyspace`},
		{
			added:   ranges(keyRange(rawKeyspace, key(1, 0))),
			problem: "key range " + quote(keyRange(rawKeyspace, key(1, 0))) + " does not start in a transaction keyspace",
		},
		{
			added:   ranges(keyRange(afterLast[:], append(afterLast[:], 0))),
			problem: "key range " + quote(keyRange(afterLast[:], append(afterLast[:], 0))) + " does not start in a transaction keyspace",
		},
		{
			added:   ranges(keyRange(key(1, 100), key(3, 100))),
			problem: "key range " + quote(keyRange(key(1, 100), key(3, 100))) + " starts in keyspace 1 and ends after it",
		},
		{
			added:   ranges(keyRange(key(1, 0), append(key(2, 0), 0))),
			problem: "key range " + quote(keyRange(key(1, 0), append(key(2, 0), 0))) + " starts in keyspace 1 and ends after it",
		},
		{
			added:   ranges(table(1, 100), table(2, 100)),
			problem: "key range " + quote(table(2, 100)) + " lies in keyspace 2 and key range " + quote(table(1, 100)) + " in keyspace 1",
		},
		// The ranges that the group has are checked with the new ones.
		{
			existing: ranges(table(1, 100)),
			added:    ranges(table(2, 100)),
			problem:  "key range " + quote(table(2, 100)) + " lies in keyspace 2 and key range " + quote(table(1, 100)) + " in keyspace 1",
		},
		{existing: ranges(keyRange(nil, nil)), added: ranges(table(1, 100)), problem: `key range "," covers the whole key space`},
	} {
		err := checkCustomGroupKeyspace("poc", append(tc.existing, tc.added...))
		if tc.problem == "" {
			re.NoError(err, "%v %v", tc.existing, tc.added)
			continue
		}
		re.EqualError(err, "nextgen requires the key ranges of custom group poc to lie in one keyspace, but "+tc.problem)
	}
}

func TestKeyspaceOfKeyRejectsNonTxnPrefixes(t *testing.T) {
	for _, key := range [][]byte{codec.EncodeBytes([]byte("y")), codec.EncodeBytes([]byte("y\x00")), {'x', 0, 0, 1, 't'}} {
		_, ok := keyspaceOfKey(key)
		require.False(t, ok, "%x", key)
	}
}

type affinityStructuredSelectorClient struct {
	pd.Client
	calledID string
}

func (c *affinityStructuredSelectorClient) GetAffinityGroup(_ context.Context, groupID string) (*pd.AffinityGroupState, error) {
	c.calledID = groupID
	return &pd.AffinityGroupState{ID: groupID}, nil
}

func (c *affinityStructuredSelectorClient) GetRegionLabelRulesByIDs(_ context.Context, ruleIDs []string) ([]*pd.LabelRule, error) {
	if len(ruleIDs) == 1 {
		c.calledID = ruleIDs[0]
	}
	return []*pd.LabelRule{{ID: c.calledID}}, nil
}

func (c *affinityStructuredSelectorClient) DeleteAffinityGroup(_ context.Context, groupID string, _ bool) error {
	c.calledID = groupID
	return nil
}

func (c *affinityStructuredSelectorClient) BatchDeleteAffinityGroups(_ context.Context, groupIDs []string, _ bool) error {
	if len(groupIDs) == 1 {
		c.calledID = groupIDs[0]
	}
	return nil
}

func (c *affinityStructuredSelectorClient) RemoveAffinityGroupKeyRanges(_ context.Context, groups map[string][]pd.AffinityGroupKeyRange) (map[string]*pd.AffinityGroupState, error) {
	for id := range groups {
		c.calledID = id
	}
	return map[string]*pd.AffinityGroupState{c.calledID: {ID: c.calledID}}, nil
}

func (c *affinityStructuredSelectorClient) CreateAffinityGroups(_ context.Context, groups map[string][]pd.AffinityGroupKeyRange, _ ...pd.CreateAffinityGroupsOption) (map[string]*pd.AffinityGroupState, error) {
	for id := range groups {
		c.calledID = id
	}
	return map[string]*pd.AffinityGroupState{c.calledID: {ID: c.calledID}}, nil
}

func (c *affinityStructuredSelectorClient) AddAffinityGroupKeyRanges(_ context.Context, groups map[string][]pd.AffinityGroupKeyRange) (map[string]*pd.AffinityGroupState, error) {
	for id := range groups {
		c.calledID = id
	}
	return map[string]*pd.AffinityGroupState{c.calledID: {ID: c.calledID}}, nil
}

func (c *affinityStructuredSelectorClient) UpdateAffinityGroupPeers(_ context.Context, groupID string, _ uint64, _ []uint64) (*pd.AffinityGroupState, error) {
	c.calledID = groupID
	return &pd.AffinityGroupState{ID: groupID}, nil
}

func TestAffinityCreateTiDBTableGroupRepairGuidance(t *testing.T) {
	previous := PDCli
	t.Cleanup(func() { PDCli = previous })

	for _, groupID := range []string{"_tidb_ks1_tg_100", "_tidb_tg_100"} {
		t.Run(groupID, func(t *testing.T) {
			re := require.New(t)
			client := &affinityStructuredSelectorClient{}
			PDCli = client
			cmd := NewAffinityCommand()
			cmd.PersistentPreRunE = nil
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)
			cmd.SetArgs([]string{"create", "--group-id", groupID, "--range", "0a,0b"})
			re.ErrorContains(cmd.Execute(), "repair it with ADMIN REPAIR AFFINITY TABLE GROUP")
			re.Empty(client.calledID)
		})
	}
}

func TestAffinityStructuredSelectorValidation(t *testing.T) {
	previous := PDCli
	t.Cleanup(func() { PDCli = previous })

	runCmd := func(args ...string) (string, error) {
		client := &affinityStructuredSelectorClient{}
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

	// Lookups and removals accept unscoped --table-id / --partition-id in both Classic and NextGen.
	for _, tc := range []struct {
		name   string
		args   []string
		wantID string
	}{
		{"show_table", []string{"show", "--table-id", "9001"}, "_tidb_t_9001"},
		{"show_partition", []string{"show", "--table-id", "9001", "--partition-id", "9002"}, "_tidb_pt_9001_p9002"},
		{"range_show_table", []string{"range", "show", "--table-id", "9001"}, affinity.GetLabelRuleID("_tidb_t_9001")},
		{"delete_table", []string{"delete", "--table-id", "9001"}, "_tidb_t_9001"},
		{"delete_force_table", []string{"delete", "--table-id", "9001", "--force"}, "_tidb_t_9001"},
		{"range_remove_table", []string{"range", "remove", "--table-id", "9001", "--force", "--range", sampleRange}, "_tidb_t_9001"},
		{"range_remove_partition", []string{"range", "remove", "--table-id", "9001", "--partition-id", "9002", "--force", "--range", sampleRange}, "_tidb_pt_9001_p9002"},
		{"create_keyspace_table", []string{"create", "--table-id", "9001", "--keyspace-id", "1"}, "_tidb_ks1_t_9001"},
		{"range_add_keyspace_table", []string{"range", "add", "--table-id", "9001", "--keyspace-id", "1", "--force", "--range", sampleRange}, "_tidb_ks1_t_9001"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			calledID, err := runCmd(tc.args...)
			re.NoError(err)
			re.Equal(tc.wantID, calledID)
		})
	}

	// Create, update, and range add require --keyspace-id in NextGen.
	for _, tc := range []struct {
		name   string
		args   []string
		wantID string
	}{
		{"create_unscoped_table", []string{"create", "--table-id", "9001"}, "_tidb_t_9001"},
		{"create_unscoped_partition", []string{"create", "--table-id", "9001", "--partition-id", "9002"}, "_tidb_pt_9001_p9002"},
		{"range_add_unscoped_table", []string{"range", "add", "--table-id", "9001", "--force", "--range", sampleRange}, "_tidb_t_9001"},
		{"update_unscoped_table", []string{"update", "--table-id", "9001", "--leader", "1", "--voters", "1,2,3"}, "_tidb_t_9001"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			calledID, err := runCmd(tc.args...)
			if kerneltype.IsNextGen() {
				re.ErrorContains(err, "nextgen requires keyspace-aware TiDB affinity group IDs")
				re.Empty(calledID)
				return
			}
			re.NoError(err)
			re.Equal(tc.wantID, calledID)
		})
	}
}
