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
	pdTests "github.com/tikv/pd/tests"
	ctl "github.com/tikv/pd/tools/pd-ctl/pdctl"
	"github.com/tikv/pd/tools/pd-ctl/pdctl/command"
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
	tableGroup := command.FormatGroupID(tableID, 0)
	partitionGroup := command.FormatGroupID(tableID, partitionID)
	re.NoError(manager.CreateAffinityGroups([]affinity.GroupKeyRanges{
		{GroupID: tableGroup},
		{GroupID: partitionGroup},
	}))

	pdAddr := cluster.GetConfig().GetClientURL()
	cmd := ctl.GetRootCmd()

	// list should return both groups with the expected IDs.
	groups := make(map[string]*pd.AffinityGroupState)
	tests.MustExec(re, cmd, []string{"-u", pdAddr, "config", "affinity", "list"}, &groups)
	re.Contains(groups, tableGroup)
	re.Contains(groups, partitionGroup)

	// show table group
	var state pd.AffinityGroupState
	tests.MustExec(re, cmd, []string{
		"-u", pdAddr, "config", "affinity", "show",
		"--table-id", strconv.FormatUint(tableID, 10),
	}, &state)
	re.Equal(tableGroup, state.ID)

	// show partitioned table group
	tests.MustExec(re, cmd, []string{
		"-u", pdAddr, "config", "affinity", "show",
		"--table-id", strconv.FormatUint(tableID, 10),
		"--partition-id", strconv.FormatUint(partitionID, 10),
	}, &state)
	re.Equal(partitionGroup, state.ID)

	// update peers for the partitioned table
	tests.MustExec(re, cmd, []string{
		"-u", pdAddr, "config", "affinity", "update",
		"--table-id", strconv.FormatUint(tableID, 10),
		"--partition-id", strconv.FormatUint(partitionID, 10),
		"--leader", "1",
		"--voters", "1,2",
	}, &state)
	re.Equal(uint64(1), state.LeaderStoreID)
	re.ElementsMatch([]uint64{1, 2}, state.VoterStoreIDs)

	// delete the normal table group
	out := tests.MustExec(re, cmd, []string{
		"-u", pdAddr, "config", "affinity", "delete",
		"--table-id", strconv.FormatUint(tableID, 10),
		"--partition-id", "0",
	}, nil)
	re.Contains(out, tableGroup)

	// ensure only the partition group remains.
	groups = make(map[string]*pd.AffinityGroupState)
	tests.MustExec(re, cmd, []string{"-u", pdAddr, "config", "affinity", "list"}, &groups)
	re.NotContains(groups, tableGroup)
	re.Contains(groups, partitionGroup)
}
