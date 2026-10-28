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
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	pd "github.com/tikv/pd/client/http"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/versioninfo/kerneltype"
	pdTests "github.com/tikv/pd/tests"
	ctl "github.com/tikv/pd/tools/pd-ctl/pdctl"
	"github.com/tikv/pd/tools/pd-ctl/tests"
)

func TestAffinityCommands(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
