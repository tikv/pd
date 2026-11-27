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
	cluster, err := pdTests.NewTestCluster(t.Context(), 1)
	re.NoError(err)
	defer cluster.Destroy()
	re.NoError(cluster.RunInitialServers())
	cluster.WaitLeader()
	leaderServer := cluster.GetLeaderServer()
	re.NoError(leaderServer.BootstrapCluster())

	manager, err := leaderServer.GetServer().GetAffinityManager()
	re.NoError(err)
	regionLabeler := leaderServer.GetRaftCluster().GetRegionLabeler()
	pdAddr := cluster.GetConfig().GetClientURL()
	run := func(args ...string) ([]byte, error) {
		return tests.ExecuteCommand(ctl.GetRootCmd(), append([]string{"-u", pdAddr, "config", "affinity"}, args...)...)
	}
	mustRun := func(re *require.Assertions, out any, args ...string) {
		tests.MustExec(re, ctl.GetRootCmd(), append([]string{"-u", pdAddr, "config", "affinity"}, args...), out)
	}
	key := func(keyspaceID uint32, raw []byte) []byte {
		prefix := codec.MakeKeyspacePrefix(codec.TxnKeyspaceModePrefix, keyspaceID)
		return codec.EncodeBytes(append(prefix, raw...))
	}
	tableKey := func(keyspaceID uint32, tableID int64) []byte {
		return key(keyspaceID, codec.GenerateTableKey(tableID))
	}
	hexRange := func(start, end []byte) string {
		return hex.EncodeToString(start) + "," + hex.EncodeToString(end)
	}

	// Prepare stores for peer updates.
	for id := uint64(1); id <= 2; id++ {
		pdTests.MustPutStore(re, cluster, &metapb.Store{Id: id, State: metapb.StoreState_Up})
	}

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
	re.NoError(manager.DeleteAffinityGroups([]string{partitionGroup}, true))

	t.Run("range show", func(t *testing.T) {
		keyRange := hexRange(tableKey(1, 100), tableKey(1, 101))
		re := require.New(t)
		_, err := run("range", "show", "--group-id", "nonexistent")
		re.ErrorContains(err, "failed to get the key ranges of affinity group nonexistent")
		_, err = run("range", "add", "--group-id", "nonexistent", "--range", keyRange)
		re.ErrorContains(err, "404 Not Found")
		re.ErrorContains(err, "affinity group nonexistent not found")

		re.NoError(manager.CreateAffinityGroups([]affinity.GroupKeyRanges{{GroupID: "empty"}}))
		for _, action := range []string{"show", "add", "remove", "show"} {
			args := []string{"range", action, "--group-id", "empty"}
			if action != "show" {
				args = append(args, "--range", keyRange)
			}
			out, err := run(args...)
			re.NoError(err)
			if action == "show" {
				re.JSONEq(`{"id":"empty","ranges":[]}`, string(out))
			}
		}
		re.NoError(manager.DeleteAffinityGroups([]string{"empty"}, true))
	})
	t.Run("group and range commands", func(t *testing.T) {
		re := require.New(t)
		type rangesOutput struct {
			ID     string `json:"id"`
			Ranges []struct {
				StartKey string `json:"start_key"`
				EndKey   string `json:"end_key"`
			} `json:"ranges"`
		}
		showRanges := func(args ...string) rangesOutput {
			var out rangesOutput
			mustRun(re, &out, append([]string{"range", "show"}, args...)...)
			return out
		}

		// customKey returns a key for custom groups. Nextgen requires their key
		// ranges to lie in one keyspace, so the key is in keyspace 1 there.
		customKey := func(b byte) string {
			if kerneltype.IsNextGen() {
				return hex.EncodeToString(key(1, []byte{b}))
			}
			return hex.EncodeToString([]byte{b})
		}
		customRange := func(start, end byte) string { return customKey(start) + "," + customKey(end) }

		groupOp := func(id string, want int, args ...string) {
			var groups map[string]*pd.AffinityGroupState
			mustRun(re, &groups, append(args, "--group-id", id)...)
			re.Equal(want, groups[id].RangeCount)
		}
		groupOp("poc-a", 2, "create", "--range", customRange(0x0a, 0x0b), "--range", customRange(0x0c, 0x0d))
		ranges := showRanges("--group-id", "poc-a")
		re.Equal("poc-a", ranges.ID)
		re.Len(ranges.Ranges, 2)
		groupOp("poc-a", 3, "range", "add", "--range", customRange(0x0e, 0x0f))
		groupOp("poc-a", 2, "range", "remove", "--range", customRange(0x0a, 0x0b))
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
		var groups map[string]*pd.AffinityGroupState
		mustRun(re, &groups, append([]string{"create"}, selector...)...)
		re.Contains(groups, tidbPrefix+"101")
		ranges = showRanges(selector...)
		re.Len(ranges.Ranges, 1)
		groupOp(tidbPrefix+"102", 1, "create")
		// Another key range of the same keyspace, as PD checks for keyspace groups.
		start, end := codec.GenerateTableKey(200), codec.GenerateTableKey(201)
		if kerneltype.IsNextGen() {
			start, end = tableKey(1, 200), tableKey(1, 201)
		} else {
			start, end = codec.EncodeBytes(start), codec.EncodeBytes(end)
		}
		extraRange := hexRange(start, end)
		for _, tc := range []struct {
			args []string
			err  string
		}{
			{[]string{"create", "--group-id", "poc-b"}, "--range is required"},
			{[]string{"create", "--group-id", "poc-b", "--range", "0b"}, "invalid --range"},
			{[]string{"create", "--group-id", tidbPrefix + "103", "--range", "0a,0b"}, "derived from its ID"},
			{[]string{"create", "--group-id", "_tidb_pg1"}, "reserved for TiDB"},
			{[]string{"create", "--group-id", "_tidb_custom"}, "reserved for TiDB"},
			{append([]string{"range", "add", "--range", extraRange}, selector...), "managed by TiDB"},
			{append([]string{"range", "remove", "--range", extraRange}, selector...), "managed by TiDB"},
			{[]string{"show", "--group-id", "poc-a", "--table-id", "1"}, "--group-id cannot be used with --table-id"},
			{[]string{"show", "--prefix", "poc-", "--group-id", "poc-a"}, "--prefix cannot be used"},
			{[]string{"show", "--prefix", "poc a"}, "invalid group ID prefix"},
			{[]string{"show", "--group-id", "poc a"}, "invalid --group-id"},
		} {
			_, err := run(tc.args...)
			re.ErrorContains(err, tc.err, "%v", tc.args)
		}
		clear(groups)
		mustRun(re, &groups, append([]string{"range", "add", "--force", "--range", extraRange}, selector...)...)
		re.Equal(2, groups[tidbPrefix+"101"].RangeCount)

		// Prefixes select groups across ID formats.
		clear(groups)
		mustRun(re, &groups, "show", "--prefix", tidbPrefix)
		re.Len(groups, 2)
		re.Contains(groups, tidbPrefix+"101")
		re.Contains(groups, tidbPrefix+"102")
		clear(groups)
		mustRun(re, &groups, "show", "--prefix", "poc-")
		re.Len(groups, 1)
		re.Contains(groups, "poc-a")
		clear(groups)
		mustRun(re, &groups, "show", "--prefix", "")
		re.Len(groups, 3)
		var state pd.AffinityGroupState
		mustRun(re, &state, "show", "--group-id", "poc-a")
		re.Equal("poc-a", state.ID)

		// Deleting a group removes its key ranges, so regular scheduling resumes there.
		out, err := run("delete", "--group-id", "poc-a", "--force")
		re.NoError(err)
		re.Contains(string(out), "Affinity group poc-a and its label rule are deleted if they existed")
		_, err = run("range", "show", "--group-id", "poc-a")
		re.ErrorContains(err, "affinity group poc-a not found")
		clear(groups)
		mustRun(re, &groups, "show", "--prefix", "poc-")
		re.Empty(groups)

		re.NoError(manager.DeleteAffinityGroups([]string{tidbPrefix + "101", tidbPrefix + "102"}, true))
	})
	t.Run("custom group keyspace", func(t *testing.T) {
		re := require.New(t)
		if !kerneltype.IsNextGen() {
			const id = "poc-open"
			groups := make(map[string]*pd.AffinityGroupState)
			for _, pair := range [][2]string{{"61,", ",61"}, {",61", "61,"}} {
				mustRun(re, &groups, "create", "--group-id", id, "--range", pair[0])
				re.Equal(1, groups[id].RangeCount)
				mustRun(re, &groups, "range", "add", "--group-id", id, "--range", pair[1])
				re.Equal(2, groups[id].RangeCount)
				mustRun(re, &groups, "range", "remove", "--group-id", id, "--range", pair[0], "--range", pair[1])
				re.Zero(groups[id].RangeCount)
				_, err := run("delete", "--group-id", id)
				re.NoError(err)
			}
			mustRun(re, &groups, "create", "--group-id", id, "--range", ",")
			re.Equal(1, groups[id].RangeCount)
			mustRun(re, &groups, "range", "remove", "--group-id", id, "--range", ",")
			re.Zero(groups[id].RangeCount)
			mustRun(re, &groups, "range", "add", "--group-id", id, "--range", ",")
			re.Equal(1, groups[id].RangeCount)
			re.NoError(manager.DeleteAffinityGroups([]string{id}, true))
			return
		}

		table := func(ks uint32, id int64) string {
			return hexRange(tableKey(ks, id), tableKey(ks, id+1))
		}
		groups := make(map[string]*pd.AffinityGroupState)
		mustRun(re, &groups, "create", "--group-id", "poc-one", "--range", table(1, 100))
		t.Cleanup(func() { mustRun(re, nil, "delete", "--group-id", "poc-one", "--force") })
		re.Equal(1, groups["poc-one"].RangeCount)

		_, err := run("create", "--group-id", "poc-two", "--range", table(1, 100), "--range", table(2, 100))
		re.ErrorContains(err, "400 Bad Request")
		re.ErrorContains(err, "key ranges must belong to transaction keyspace 1")
		re.False(manager.IsGroupExist("poc-two"))

		_, err = run("range", "add", "--group-id", "poc-one", "--range", table(2, 100))
		re.ErrorContains(err, "400 Bad Request")
		re.ErrorContains(err, "key ranges must belong to transaction keyspace 1")
		mustRun(re, &groups, "range", "add", "--group-id", "poc-one", "--range", table(1, 110))
		re.Equal(2, groups["poc-one"].RangeCount)
	})
	t.Run("NextGen legacy cleanup", func(t *testing.T) {
		re := require.New(t)
		const modeFlag = "github.com/tikv/pd/pkg/versioninfo/kerneltype/mockNextGenBuildFlag"
		re.NoError(failpoint.Enable(modeFlag, "return(false)"))
		defer func() { re.NoError(failpoint.Disable(modeFlag)) }()
		const groupID, orphanID = "_tidb_t_9001", "_tidb_tg_9002"
		kr := keyutil.KeyRange{StartKey: []byte("a"), EndKey: []byte("b")}
		re.NoError(manager.CreateAffinityGroups([]affinity.GroupKeyRanges{{GroupID: groupID, KeyRanges: []keyutil.KeyRange{kr}}}))
		re.NoError(regionLabeler.SetLabelRule(affinity.MakeLabelRule(affinity.GroupKeyRanges{GroupID: orphanID, KeyRanges: []keyutil.KeyRange{kr}})))
		re.NoError(failpoint.Enable(modeFlag, "return(true)"))
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
			} else {
				_, err := run("delete", "--group-id", id)
				re.ErrorContains(err, "ErrAffinityGroupNotFound")
				re.NotNil(regionLabeler.GetLabelRule(affinity.GetLabelRuleID(id)))
			}
			_, err = run("delete", "--group-id", id, "--force")
			re.NoError(err)
			re.False(manager.IsGroupExist(id))
			re.Nil(regionLabeler.GetLabelRule(affinity.GetLabelRuleID(id)))
		}
	})
}
