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

package affinity_test

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"

	pd "github.com/tikv/pd/client/http"
	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/utils/keyutil"
	"github.com/tikv/pd/pkg/versioninfo/kerneltype"
	pdTests "github.com/tikv/pd/tests"
	ctl "github.com/tikv/pd/tools/pd-ctl/pdctl"
	"github.com/tikv/pd/tools/pd-ctl/tests"
)

func TestAffinityCommands(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	cluster, err := pdTests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer cluster.Destroy()
	re.NoError(cluster.RunInitialServers())
	cluster.WaitLeader()
	leaderServer := cluster.GetLeaderServer()
	re.NoError(leaderServer.BootstrapCluster())

	// Prepare stores for peer updates.
	for id := uint64(1); id <= 2; id++ {
		pdTests.MustPutStore(re, cluster, &metapb.Store{Id: id, State: metapb.StoreState_Up})
	}

	manager, err := leaderServer.GetServer().GetAffinityManager()
	re.NoError(err)

	// Pre-create groups so CLI commands can operate on them.
	const tableID = uint64(1001)
	const partitionID = uint64(3)
	tableGroup := "_tidb_t_1001"
	partitionGroup := "_tidb_pt_1001_p3"
	targetFlags := []string{"--table-id", strconv.FormatUint(tableID, 10)}
	if kerneltype.IsNextGen() {
		tableGroup = "_tidb_ks1_t_1001"
		partitionGroup = "_tidb_ks1_pt_1001_p3"
		targetFlags = append(targetFlags, "--keyspace-id", "1")
	}
	re.NoError(manager.CreateAffinityGroups([]affinity.GroupKeyRanges{
		{GroupID: tableGroup},
		{GroupID: partitionGroup},
	}))

	pdAddr := cluster.GetConfig().GetClientURL()
	cmd := ctl.GetRootCmd()

	// show should return both groups with the expected IDs.
	groups := make(map[string]*pd.AffinityGroupState)
	tests.MustExec(re, cmd, []string{"-u", pdAddr, "config", "affinity", "show"}, &groups)
	re.Contains(groups, tableGroup)
	re.Contains(groups, partitionGroup)

	// Explicit zero retains the legacy list-all behavior without a scope selector.
	zeroGroups := make(map[string]*pd.AffinityGroupState)
	tests.MustExec(re, ctl.GetRootCmd(), []string{
		"-u", pdAddr, "config", "affinity", "show", "--table-id", "0",
	}, &zeroGroups)
	re.Equal(groups, zeroGroups)

	// show table group
	var state pd.AffinityGroupState
	tests.MustExec(re, cmd, append([]string{
		"-u", pdAddr, "config", "affinity", "show",
	}, targetFlags...), &state)
	re.Equal(tableGroup, state.ID)

	// show partitioned table group
	tests.MustExec(re, cmd, append([]string{
		"-u", pdAddr, "config", "affinity", "show",
		"--partition-id", strconv.FormatUint(partitionID, 10),
	}, targetFlags...), &state)
	re.Equal(partitionGroup, state.ID)

	// Keep groups with identical table/partition IDs in multiple keyspaces alive
	// together, so updating or deleting one must preserve every other group's peers.
	for _, keyspaceID := range []string{"0", "42", "43", "16777215"} {
		for _, suffix := range []string{"t_1001", "pt_1001_p3"} {
			groupID := "_tidb_ks" + keyspaceID + "_" + suffix
			re.NoError(manager.CreateAffinityGroups([]affinity.GroupKeyRanges{{GroupID: groupID}}))
			_, err := manager.UpdateAffinityGroupPeers(groupID, 2, []uint64{2})
			re.NoError(err)
		}
	}
	expectedGroups := make(map[string]*pd.AffinityGroupState)
	tests.MustExec(re, ctl.GetRootCmd(), []string{"-u", pdAddr, "config", "affinity", "show"}, &expectedGroups)
	zeroGroups = make(map[string]*pd.AffinityGroupState)
	tests.MustExec(re, ctl.GetRootCmd(), []string{
		"-u", pdAddr, "config", "affinity", "show", "--table-id", "0",
	}, &zeroGroups)
	re.Equal(expectedGroups, zeroGroups)

	// Keyspace selection must target only the requested group, including keyspace 0.
	for _, keyspaceID := range []string{"0", "42", "43", "16777215"} {
		for _, suffix := range []string{"t_1001", "pt_1001_p3"} {
			groupID := "_tidb_ks" + keyspaceID + "_" + suffix
			flags := []string{"--keyspace-id", keyspaceID, "--table-id", "1001"}
			if suffix == "pt_1001_p3" {
				flags = append(flags, "--partition-id", "3")
			}
			for _, action := range []string{"show", "update", "delete"} {
				args := append([]string{"-u", pdAddr, "config", "affinity", action}, flags...)
				if action == "update" {
					args = append(args, "--leader", "1", "--voters", "1,2")
				}
				if action == "delete" {
					out := tests.MustExec(re, ctl.GetRootCmd(), args, nil)
					re.Contains(out, groupID)
					delete(expectedGroups, groupID)
				} else {
					tests.MustExec(re, ctl.GetRootCmd(), args, &state)
					re.Equal(groupID, state.ID)
					if action == "update" {
						re.Equal(uint64(1), state.LeaderStoreID)
						re.ElementsMatch([]uint64{1, 2}, state.VoterStoreIDs)
					}
				}
				remaining := make(map[string]*pd.AffinityGroupState)
				tests.MustExec(re, ctl.GetRootCmd(), []string{"-u", pdAddr, "config", "affinity", "show"}, &remaining)
				re.Len(remaining, len(expectedGroups))
				if action == "delete" {
					re.NotContains(remaining, groupID)
				}
				for otherID, expected := range expectedGroups {
					re.Contains(remaining, otherID)
					if otherID == groupID {
						continue
					}
					re.Equal(expected.LeaderStoreID, remaining[otherID].LeaderStoreID, otherID)
					re.ElementsMatch(expected.VoterStoreIDs, remaining[otherID].VoterStoreIDs, otherID)
				}
			}
		}
	}
	for _, action := range []string{"show", "update", "delete"} {
		invalidTargets := [][]string{
			{"--keyspace-id", "0"},
			{"--keyspace-id", "42"},
			{"--partition-id", "0"},
			{"--partition-id", "3"},
		}
		if action != "show" {
			invalidTargets = append(invalidTargets, nil)
		}
		for _, flags := range invalidTargets {
			for _, tableFlags := range [][]string{nil, {"--table-id", "0"}} {
				args := append([]string{"-u", pdAddr, "config", "affinity", action}, flags...)
				args = append(args, tableFlags...)
				out, err := tests.ExecuteCommand(ctl.GetRootCmd(), args...)
				re.ErrorContains(err, "--table-id is required", "%v", args)
				re.NotContains(string(out), tableGroup)
			}
		}
		_, err := tests.ExecuteCommand(ctl.GetRootCmd(), []string{
			"-u", pdAddr, "config", "affinity", action, "--keyspace-id", "16777216", "--table-id", "1001",
		}...)
		re.ErrorContains(err, "--keyspace-id must be between 0 and 16777215")
	}
	for _, tc := range []struct {
		flags []string
		err   string
	}{
		{nil, "leader is required"},
		{[]string{"--leader", "1"}, "voters is required"},
		{[]string{"--leader", "1", "--voters", "invalid"}, "failed to parse voters"},
	} {
		args := append([]string{"-u", pdAddr, "config", "affinity", "update"}, targetFlags...)
		args = append(args, tc.flags...)
		_, err := tests.ExecuteCommand(ctl.GetRootCmd(), args...)
		re.ErrorContains(err, tc.err)
	}

	// update peers for the partitioned table
	tests.MustExec(re, cmd, append([]string{
		"-u", pdAddr, "config", "affinity", "update",
		"--partition-id", strconv.FormatUint(partitionID, 10),
		"--leader", "1",
		"--voters", "1,2",
	}, targetFlags...), &state)
	re.Equal(uint64(1), state.LeaderStoreID)
	re.ElementsMatch([]uint64{1, 2}, state.VoterStoreIDs)

	// delete the normal table group
	cmd = ctl.GetRootCmd() // reset cmd to clean args
	out := tests.MustExec(re, cmd, append([]string{
		"-u", pdAddr, "config", "affinity", "delete",
	}, targetFlags...), nil)
	re.Contains(out, tableGroup)

	// ensure only the partition group remains.
	groups = make(map[string]*pd.AffinityGroupState)
	cmd = ctl.GetRootCmd() // reset cmd to clean args
	tests.MustExec(re, cmd, []string{"-u", pdAddr, "config", "affinity", "show"}, &groups)
	re.NotContains(groups, tableGroup)
	re.Contains(groups, partitionGroup)
}

func TestAffinityRangeShow(t *testing.T) {
	re := require.New(t)
	cluster, err := pdTests.NewTestCluster(t.Context(), 1)
	re.NoError(err)
	defer cluster.Destroy()
	re.NoError(cluster.RunInitialServers())
	cluster.WaitLeader()
	leaderServer := cluster.GetLeaderServer()
	re.NoError(leaderServer.BootstrapCluster())
	pdAddr := cluster.GetConfig().GetClientURL()
	run := func(args ...string) ([]byte, error) {
		return tests.ExecuteCommand(ctl.GetRootCmd(), append([]string{"-u", pdAddr, "config", "affinity"}, args...)...)
	}
	manager, err := leaderServer.GetServer().GetAffinityManager()
	re.NoError(err)
	key := func(tableID int64) string {
		raw := append(codec.MakeKeyspacePrefix(codec.TxnKeyspaceModePrefix, 1), codec.GenerateTableKey(tableID)...)
		return hex.EncodeToString(codec.EncodeBytes(raw))
	}
	keyRange := key(100) + "," + key(101)

	t.Run("missing", func(t *testing.T) {
		re := require.New(t)
		_, err := run("range", "show", "--group-id", "nonexistent")
		re.ErrorContains(err, "failed to get the key ranges of affinity group nonexistent")
		re.ErrorContains(err, "404 Not Found")
		re.ErrorContains(err, "affinity group nonexistent not found")
		_, err = run("range", "add", "--group-id", "nonexistent", "--range", keyRange)
		re.ErrorContains(err, "404 Not Found")
		re.ErrorContains(err, "affinity group nonexistent not found")
		re.ErrorContains(err, "failed to change the key ranges of affinity group nonexistent")
	})
	t.Run("empty", func(t *testing.T) {
		re := require.New(t)
		re.NoError(manager.CreateAffinityGroups([]affinity.GroupKeyRanges{{GroupID: "empty"}}))
		out, err := run("range", "show", "--group-id", "empty")
		re.NoError(err)
		re.JSONEq(`{"id":"empty","ranges":[]}`, string(out))
		_, err = run("range", "add", "--group-id", "empty", "--range", keyRange)
		re.NoError(err)
		_, err = run("range", "remove", "--group-id", "empty", "--range", keyRange)
		re.NoError(err)
		out, err = run("range", "show", "--group-id", "empty")
		re.NoError(err)
		re.JSONEq(`{"id":"empty","ranges":[]}`, string(out))
	})
	t.Run("orphaned-rule", func(t *testing.T) {
		re := require.New(t)
		re.False(manager.IsGroupExist("orphan"))
		regionLabeler := leaderServer.GetRaftCluster().GetRegionLabeler()
		re.NoError(regionLabeler.SetLabelRule(affinity.MakeLabelRule(affinity.GroupKeyRanges{
			GroupID:   "orphan",
			KeyRanges: []keyutil.KeyRange{{StartKey: []byte{0x20}, EndKey: []byte{0x21}}},
		})))
		out, err := run("range", "show", "--group-id", "orphan")
		re.NoError(err)
		re.JSONEq(`{"id":"orphan","ranges":[{"start_key":"20","end_key":"21"}]}`, string(out))
	})
}

func TestAffinityRangeRemoveHalfUnbounded(t *testing.T) {
	re := require.New(t)
	cluster, err := pdTests.NewTestCluster(t.Context(), 1)
	re.NoError(err)
	defer cluster.Destroy()
	re.NoError(cluster.RunInitialServers())
	cluster.WaitLeader()
	server := cluster.GetLeaderServer()
	re.NoError(server.BootstrapCluster())
	manager, err := server.GetServer().GetAffinityManager()
	re.NoError(err)
	key := func(table int64) []byte {
		return codec.EncodeBytes(append(codec.MakeKeyspacePrefix(codec.TxnKeyspaceModePrefix, 1), codec.GenerateTableKey(table)...))
	}
	const modeFlag = "github.com/tikv/pd/pkg/versioninfo/kerneltype/mockNextGenBuildFlag"
	t.Cleanup(func() { re.NoError(failpoint.Disable(modeFlag)) })
	for i, legacy := range []keyutil.KeyRange{{EndKey: key(100)}, {StartKey: key(300)}} {
		id := fmt.Sprintf("legacy-half-%d", i)
		re.NoError(failpoint.Enable(modeFlag, "return(false)"))
		re.NoError(manager.CreateAffinityGroups([]affinity.GroupKeyRanges{{GroupID: id, KeyRanges: []keyutil.KeyRange{legacy, {StartKey: key(200), EndKey: key(201)}}}}))
		re.NoError(failpoint.Disable(modeFlag))
		before := manager.GetAffinityGroupState(id)
		groups := make(map[string]*pd.AffinityGroupState)
		tests.MustExec(re, ctl.GetRootCmd(), []string{
			"-u", cluster.GetConfig().GetClientURL(), "config", "affinity", "range", "remove", "--group-id", id,
			"--range", hex.EncodeToString(legacy.StartKey) + "," + hex.EncodeToString(legacy.EndKey),
		}, &groups)
		re.Equal(1, groups[id].RangeCount)
		re.Equal(before.CreateTimestamp, groups[id].CreateTimestamp)
		re.NoError(manager.DeleteAffinityGroups([]string{id}, true))
	}
}

func TestAffinityGroupAndRangeCommands(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	cluster, err := pdTests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer cluster.Destroy()
	re.NoError(cluster.RunInitialServers())
	cluster.WaitLeader()
	re.NoError(cluster.GetLeaderServer().BootstrapCluster())
	pdAddr := cluster.GetConfig().GetClientURL()
	run := func(args ...string) ([]byte, error) {
		return tests.ExecuteCommand(ctl.GetRootCmd(), append([]string{"-u", pdAddr, "config", "affinity"}, args...)...)
	}
	mustRun := func(out any, args ...string) {
		tests.MustExec(re, ctl.GetRootCmd(), append([]string{"-u", pdAddr, "config", "affinity"}, args...), out)
	}
	type rangesOutput struct {
		ID     string `json:"id"`
		Ranges []struct {
			StartKey string `json:"start_key"`
			EndKey   string `json:"end_key"`
		} `json:"ranges"`
	}
	showRanges := func(args ...string) rangesOutput {
		var out rangesOutput
		mustRun(&out, append([]string{"range", "show"}, args...)...)
		return out
	}

	// customKey returns a key for custom groups. Nextgen requires their key
	// ranges to lie in one keyspace, so the key is in keyspace 1 there.
	customKey := func(b byte) string {
		if kerneltype.IsNextGen() {
			return hex.EncodeToString(codec.EncodeBytes(append(codec.MakeKeyspacePrefix(codec.TxnKeyspaceModePrefix, 1), b)))
		}
		return hex.EncodeToString([]byte{b})
	}
	customRange := func(start, end byte) string { return customKey(start) + "," + customKey(end) }

	// A group with a custom ID needs its key ranges.
	groups := make(map[string]*pd.AffinityGroupState)
	mustRun(&groups, "create", "--group-id", "poc-a", "--range", customRange(0x0a, 0x0b), "--range", customRange(0x0c, 0x0d))
	re.Equal(2, groups["poc-a"].RangeCount)
	ranges := showRanges("--group-id", "poc-a")
	re.Equal("poc-a", ranges.ID)
	re.Len(ranges.Ranges, 2)
	_, err = run("create", "--group-id", "poc-b")
	re.ErrorContains(err, "--range is required")
	_, err = run("create", "--group-id", "poc-b", "--range", "0b")
	re.ErrorContains(err, "invalid --range")

	groups = make(map[string]*pd.AffinityGroupState)
	mustRun(&groups, "range", "add", "--group-id", "poc-a", "--range", customRange(0x0e, 0x0f))
	re.Equal(3, groups["poc-a"].RangeCount)
	groups = make(map[string]*pd.AffinityGroupState)
	mustRun(&groups, "range", "remove", "--group-id", "poc-a", "--range", customRange(0x0a, 0x0b))
	re.Equal(2, groups["poc-a"].RangeCount)
	ranges = showRanges("--group-id", "poc-a")
	re.Len(ranges.Ranges, 2)
	re.Equal(customKey(0x0c), ranges.Ranges[0].StartKey)

	// TiDB groups take the key range of their physical ID.
	selector := []string{"--table-id", "100", "--partition-id", "101"}
	tidbPrefix := "_tidb_pt_100_p"
	if kerneltype.IsNextGen() {
		selector = append(selector, "--keyspace-id", "1")
		tidbPrefix = "_tidb_ks1_pt_100_p"
	}
	groups = make(map[string]*pd.AffinityGroupState)
	mustRun(&groups, append([]string{"create"}, selector...)...)
	re.Contains(groups, tidbPrefix+"101")
	ranges = showRanges(selector...)
	re.Len(ranges.Ranges, 1)
	if kerneltype.IsNextGen() {
		re.Equal("7800000174800000ff0000000065000000fc", ranges.Ranges[0].StartKey)
	} else {
		re.Equal("7480000000000000ff6500000000000000f8", ranges.Ranges[0].StartKey)
		re.Equal("7480000000000000ff6600000000000000f8", ranges.Ranges[0].EndKey)
	}
	groups = make(map[string]*pd.AffinityGroupState)
	mustRun(&groups, "create", "--group-id", tidbPrefix+"102")
	re.Equal(1, groups[tidbPrefix+"102"].RangeCount)
	// Another key range of the same keyspace, as PD checks for keyspace groups.
	tableKey := func(tableID int64) string {
		key := codec.GenerateTableKey(tableID)
		if kerneltype.IsNextGen() {
			key = append(codec.MakeKeyspacePrefix(codec.TxnKeyspaceModePrefix, 1), key...)
		}
		return hex.EncodeToString(codec.EncodeBytes(key))
	}
	extraRange := tableKey(200) + "," + tableKey(201)
	for _, tc := range []struct {
		args []string
		err  string
	}{
		{[]string{"create", "--group-id", tidbPrefix + "103", "--range", "0a,0b"}, "derived from its ID"},
		{[]string{"create", "--group-id", "_tidb_tg_1_i0"}, "ADMIN REPAIR AFFINITY TABLE GROUP"},
		{[]string{"create", "--group-id", "_tidb_ks1_tg_1_i0"}, "ADMIN REPAIR AFFINITY TABLE GROUP"},
		{[]string{"create", "--group-id", "_tidb_pg1"}, "reserved for TiDB"},
		{[]string{"create", "--group-id", "_tidb_custom"}, "reserved for TiDB"},
		{append([]string{"range", "add", "--range", extraRange}, selector...), "managed by TiDB"},
		{append([]string{"range", "remove", "--range", extraRange}, selector...), "managed by TiDB"},
		{[]string{"show", "--group-id", "poc-a", "--table-id", "1"}, "--group-id cannot be used with --table-id"},
		{[]string{"show", "--prefix", "poc-", "--group-id", "poc-a"}, "--prefix cannot be used"},
		{[]string{"show", "--prefix", "poc a"}, "invalid group ID prefix"},
		{[]string{"show", "--group-id", "poc a"}, "invalid --group-id"},
	} {
		_, err = run(tc.args...)
		re.ErrorContains(err, tc.err, "%v", tc.args)
	}
	groups = make(map[string]*pd.AffinityGroupState)
	mustRun(&groups, append([]string{"range", "add", "--force", "--range", extraRange}, selector...)...)
	re.Equal(2, groups[tidbPrefix+"101"].RangeCount)

	// Prefixes select groups across ID formats.
	groups = make(map[string]*pd.AffinityGroupState)
	mustRun(&groups, "show", "--prefix", tidbPrefix)
	re.Len(groups, 2)
	re.Contains(groups, tidbPrefix+"101")
	re.Contains(groups, tidbPrefix+"102")
	groups = make(map[string]*pd.AffinityGroupState)
	mustRun(&groups, "show", "--prefix", "poc-")
	re.Len(groups, 1)
	re.Contains(groups, "poc-a")
	groups = make(map[string]*pd.AffinityGroupState)
	mustRun(&groups, "show", "--prefix", "")
	re.Len(groups, 3)
	var state pd.AffinityGroupState
	mustRun(&state, "show", "--group-id", "poc-a")
	re.Equal("poc-a", state.ID)

	// Deleting a group removes its key ranges, so regular scheduling resumes there.
	out, err := run("delete", "--group-id", "poc-a", "--force")
	re.NoError(err)
	re.Contains(string(out), "Affinity group poc-a and its label rule are deleted if they existed")
	_, err = run("range", "show", "--group-id", "poc-a")
	re.ErrorContains(err, "affinity group poc-a not found")
	groups = make(map[string]*pd.AffinityGroupState)
	mustRun(&groups, "show", "--prefix", "poc-")
	re.Empty(groups)

	// A forced delete also removes a label rule left without its group.
	regionLabeler := cluster.GetLeaderServer().GetRaftCluster().GetRegionLabeler()
	re.NoError(regionLabeler.SetLabelRule(affinity.MakeLabelRule(affinity.GroupKeyRanges{
		GroupID:   "poc-leftover",
		KeyRanges: []keyutil.KeyRange{{StartKey: []byte{0x20}, EndKey: []byte{0x21}}},
	})))
	re.Len(showRanges("--group-id", "poc-leftover").Ranges, 1)
	_, err = run("delete", "--group-id", "poc-leftover")
	re.ErrorContains(err, "ErrAffinityGroupNotFound")
	re.NotNil(regionLabeler.GetLabelRule(affinity.GetLabelRuleID("poc-leftover")))
	out, err = run("delete", "--group-id", "poc-leftover", "--force")
	re.NoError(err)
	re.Contains(string(out), "Affinity group poc-leftover and its label rule are deleted if they existed")
	re.Nil(regionLabeler.GetLabelRule(affinity.GetLabelRuleID("poc-leftover")))
	_, err = run("range", "show", "--group-id", "poc-leftover")
	re.ErrorContains(err, "affinity group poc-leftover not found")
}

// TestAffinityCustomGroupKeyspace checks that Classic accepts unbounded ranges
// and NextGen keeps all ranges of a custom group within one transaction keyspace.
func TestAffinityCustomGroupKeyspace(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	cluster, err := pdTests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer cluster.Destroy()
	re.NoError(cluster.RunInitialServers())
	cluster.WaitLeader()
	leaderServer := cluster.GetLeaderServer()
	re.NoError(leaderServer.BootstrapCluster())
	manager, err := leaderServer.GetServer().GetAffinityManager()
	re.NoError(err)
	pdAddr := cluster.GetConfig().GetClientURL()
	run := func(args ...string) ([]byte, error) {
		return tests.ExecuteCommand(ctl.GetRootCmd(), append([]string{"-u", pdAddr, "config", "affinity"}, args...)...)
	}
	mustRun := func(out any, args ...string) {
		tests.MustExec(re, ctl.GetRootCmd(), append([]string{"-u", pdAddr, "config", "affinity"}, args...), out)
	}

	if !kerneltype.IsNextGen() {
		groups := make(map[string]*pd.AffinityGroupState)
		const openGroup = "poc-open"
		// Exercise both open endpoints on both the create and add paths.
		for _, pair := range [][2]string{{"61,", ",61"}, {",61", "61,"}} {
			mustRun(&groups, "create", "--group-id", openGroup, "--range", pair[0])
			re.Equal(1, groups[openGroup].RangeCount)
			mustRun(&groups, "range", "add", "--group-id", openGroup, "--range", pair[1])
			re.Equal(2, groups[openGroup].RangeCount)
			mustRun(&groups, "range", "remove", "--group-id", openGroup, "--range", pair[0], "--range", pair[1])
			re.Zero(groups[openGroup].RangeCount)
			_, err := run("delete", "--group-id", openGroup)
			re.NoError(err)
		}

		// A range without either endpoint covers the whole key space.
		mustRun(&groups, "create", "--group-id", "poc-all", "--range", ",")
		re.Equal(1, groups["poc-all"].RangeCount)
		mustRun(&groups, "range", "remove", "--group-id", "poc-all", "--range", ",")
		re.Zero(groups["poc-all"].RangeCount)
		mustRun(&groups, "range", "add", "--group-id", "poc-all", "--range", ",")
		re.Equal(1, groups["poc-all"].RangeCount)

		for _, keyRange := range []string{"61,61", "62,61"} {
			_, err := run("create", "--group-id", "poc-invalid", "--range", keyRange)
			re.ErrorContains(err, "start_key must be less than end_key", keyRange)
			_, err = run("range", "add", "--group-id", "poc-all", "--range", keyRange)
			re.ErrorContains(err, "start_key must be less than end_key", keyRange)
		}
		return
	}

	// key returns the Region key of a table in a transaction keyspace, or the
	// key where the keyspace starts for table 0.
	key := func(keyspaceID uint32, tableID int64) []byte {
		raw := codec.MakeKeyspacePrefix(codec.TxnKeyspaceModePrefix, keyspaceID)
		if tableID > 0 {
			raw = append(raw, codec.GenerateTableKey(tableID)...)
		}
		return codec.EncodeBytes(raw)
	}
	keyRange := func(start, end []byte) string { return hex.EncodeToString(start) + "," + hex.EncodeToString(end) }
	table := func(keyspaceID uint32, tableID int64) string {
		return keyRange(key(keyspaceID, tableID), key(keyspaceID, tableID+1))
	}
	rule := func(groupID string) string {
		return fmt.Sprintf("nextgen requires the key ranges of custom group %s to lie in one keyspace, but ", groupID)
	}

	groups := make(map[string]*pd.AffinityGroupState)
	mustRun(&groups, "create", "--group-id", "poc-one", "--range", table(1, 100))
	re.Equal(1, groups["poc-one"].RangeCount)

	// pd-ctl rejects unbounded ranges and ranges outside a single transaction keyspace.
	for _, tc := range []struct {
		groupID, keyRange, problem string
	}{
		{"poc-cross", keyRange(key(1, 100), key(3, 100)), "starts in keyspace 1 and ends after it"},
		{"poc-keyspaces", keyRange(key(1, 0), key(3, 0)), "starts in keyspace 1 and ends after it"},
		{"poc-all", ",", "covers the whole key space"},
		{"poc-open-end", "61,", "does not start in a transaction keyspace"},
		{"poc-open-start", ",61", "has no start key"},
		{"poc-no-end", keyRange(key(1, 100), nil), "starts in keyspace 1 and has no end key"},
		{"poc-outside", "0a,0b", "does not start in a transaction keyspace"},
	} {
		_, err := run("create", "--group-id", tc.groupID, "--range", tc.keyRange)
		re.EqualError(err, fmt.Sprintf("%skey range %q %s", rule(tc.groupID), tc.keyRange, tc.problem), tc.groupID)
		re.False(manager.IsGroupExist(tc.groupID), tc.groupID)
		_, err = run("range", "add", "--group-id", "poc-one", "--range", tc.keyRange)
		re.EqualError(err, fmt.Sprintf("%skey range %q %s", rule("poc-one"), tc.keyRange, tc.problem))
	}
	_, err = run("create", "--group-id", "poc-two", "--range", table(1, 100), "--range", table(2, 100))
	re.EqualError(err, fmt.Sprintf("%skey range %q lies in keyspace 2 and key range %q in keyspace 1", rule("poc-two"), table(2, 100), table(1, 100)))
	re.False(manager.IsGroupExist("poc-two"))

	// PD checks the new ranges with the persisted ranges of the group.
	_, err = run("range", "add", "--group-id", "poc-one", "--range", table(2, 100))
	re.ErrorContains(err, "key ranges must belong to transaction keyspace 1")
	groups = make(map[string]*pd.AffinityGroupState)
	mustRun(&groups, "range", "add", "--group-id", "poc-one", "--range", table(1, 110))
	re.Equal(2, groups["poc-one"].RangeCount)

	// Seed a legacy range; PD rejects further additions until it is removed.
	legacy := keyRange(key(4, 100), key(6, 100))
	const modeFlag = "github.com/tikv/pd/pkg/versioninfo/kerneltype/mockNextGenBuildFlag"
	re.NoError(failpoint.Enable(modeFlag, "return(false)"))
	t.Cleanup(func() { re.NoError(failpoint.Disable(modeFlag)) })
	re.NoError(manager.CreateAffinityGroups([]affinity.GroupKeyRanges{{
		GroupID:   "poc-legacy",
		KeyRanges: []keyutil.KeyRange{{StartKey: key(4, 100), EndKey: key(6, 100)}},
	}}))
	re.NoError(failpoint.Disable(modeFlag))
	_, err = run("range", "add", "--group-id", "poc-legacy", "--range", table(4, 50))
	re.ErrorContains(err, "key ranges must belong to transaction keyspace 4")
	groups = make(map[string]*pd.AffinityGroupState)
	mustRun(&groups, "range", "remove", "--group-id", "poc-legacy", "--range", legacy)
	re.Zero(groups["poc-legacy"].RangeCount)
	groups = make(map[string]*pd.AffinityGroupState)
	mustRun(&groups, "range", "add", "--group-id", "poc-legacy", "--range", table(4, 200))
	re.Equal(1, groups["poc-legacy"].RangeCount)
}

func TestAffinityNextGenLegacyCleanup(t *testing.T) {
	re := require.New(t)
	cluster, err := pdTests.NewTestCluster(t.Context(), 1)
	re.NoError(err)
	defer cluster.Destroy()
	re.NoError(cluster.RunInitialServers())
	cluster.WaitLeader()
	server := cluster.GetLeaderServer()
	re.NoError(server.BootstrapCluster())
	manager, err := server.GetServer().GetAffinityManager()
	re.NoError(err)
	const modeFlag = "github.com/tikv/pd/pkg/versioninfo/kerneltype/mockNextGenBuildFlag"
	re.NoError(failpoint.Enable(modeFlag, "return(false)"))
	defer func() { re.NoError(failpoint.Disable(modeFlag)) }()
	const groupID, orphanID = "_tidb_t_9001", "_tidb_tg_9002"
	kr := keyutil.KeyRange{StartKey: []byte("a"), EndKey: []byte("b")}
	re.NoError(manager.CreateAffinityGroups([]affinity.GroupKeyRanges{{GroupID: groupID, KeyRanges: []keyutil.KeyRange{kr}}}))
	regionLabeler := server.GetRaftCluster().GetRegionLabeler()
	re.NoError(regionLabeler.SetLabelRule(affinity.MakeLabelRule(affinity.GroupKeyRanges{GroupID: orphanID, KeyRanges: []keyutil.KeyRange{kr}})))
	re.NoError(failpoint.Enable(modeFlag, "return(true)"))
	run := func(args ...string) ([]byte, error) {
		return tests.ExecuteCommand(ctl.GetRootCmd(), append([]string{"-u", server.GetAddr(), "config", "affinity"}, args...)...)
	}
	out, err := run("show", "--group-id", groupID)
	re.NoError(err)
	re.Contains(string(out), groupID)
	for _, id := range []string{groupID, orphanID} {
		out, err := run("range", "show", "--group-id", id)
		re.NoError(err)
		re.Contains(string(out), `"start_key": "61"`)
		if id == groupID {
			for _, args := range [][]string{
				{"create", "--group-id", id},
				{"update", "--group-id", id, "--leader", "1", "--voters", "1"},
				{"range", "add", "--group-id", id, "--range", "63,64", "--force"},
			} {
				_, err := run(args...)
				re.ErrorContains(err, "pd-ctl --keyspace-id")
			}
			_, err := run("range", "remove", "--group-id", id, "--range", "61,62", "--force")
			re.NoError(err)
			re.Zero(manager.GetAffinityGroupState(id).RangeCount)
		}
		_, err = run("delete", "--group-id", id, "--force")
		re.NoError(err)
		re.False(manager.IsGroupExist(id))
		re.Nil(regionLabeler.GetLabelRule(affinity.GetLabelRuleID(id)))
	}
}
