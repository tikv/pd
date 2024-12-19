// Copyright 2023 TiKV Project Authors.
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

package keyspace_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/apiv2/handlers"
	"github.com/tikv/pd/server/config"
	pdTests "github.com/tikv/pd/tests"
	handlersutil "github.com/tikv/pd/tests/server/apiv2/handlers"
	ctl "github.com/tikv/pd/tools/pd-ctl/pdctl"
	"github.com/tikv/pd/tools/pd-ctl/tests"
)

func TestKeyspaceGroup(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tc, err := pdTests.NewTestAPICluster(ctx, 1)
	re.NoError(err)
	defer tc.Destroy()
	err = tc.RunInitialServers()
	re.NoError(err)
	tc.WaitLeader()
	leaderServer := tc.GetLeaderServer()
	re.NoError(leaderServer.BootstrapCluster())
	pdAddr := tc.GetConfig().GetClientURL()
	cmd := ctl.GetRootCmd()

	// Show keyspace group information.
	defaultKeyspaceGroupID := fmt.Sprintf("%d", constant.DefaultKeyspaceGroupID)
	args := []string{"-u", pdAddr, "keyspace-group"}
	output, err := tests.ExecuteCommand(cmd, append(args, defaultKeyspaceGroupID)...)
	re.NoError(err)
	var keyspaceGroup endpoint.KeyspaceGroup
	err = json.Unmarshal(output, &keyspaceGroup)
	re.NoError(err)
	re.Equal(constant.DefaultKeyspaceGroupID, keyspaceGroup.ID)
	re.Contains(keyspaceGroup.Keyspaces, constant.DefaultKeyspaceID)
	// Split keyspace group.
	handlersutil.MustCreateKeyspaceGroup(re, leaderServer, &handlers.CreateKeyspaceGroupParams{
		KeyspaceGroups: []*endpoint.KeyspaceGroup{
			{
				ID:        1,
				UserKind:  endpoint.Standard.String(),
				Members:   make([]endpoint.KeyspaceGroupMember, constant.DefaultKeyspaceGroupReplicaCount),
				Keyspaces: []uint32{111, 222, 333},
			},
		},
	})
	_, err = tests.ExecuteCommand(cmd, append(args, "split", "1", "2", "222", "333")...)
	re.NoError(err)
	output, err = tests.ExecuteCommand(cmd, append(args, "1")...)
	re.NoError(err)
	keyspaceGroup = endpoint.KeyspaceGroup{}
	err = json.Unmarshal(output, &keyspaceGroup)
	re.NoError(err)
	re.Equal(uint32(1), keyspaceGroup.ID)
	re.Equal([]uint32{111}, keyspaceGroup.Keyspaces)
	output, err = tests.ExecuteCommand(cmd, append(args, "2")...)
	re.NoError(err)
	keyspaceGroup = endpoint.KeyspaceGroup{}
	err = json.Unmarshal(output, &keyspaceGroup)
	re.NoError(err)
	re.Equal(uint32(2), keyspaceGroup.ID)
	re.Equal([]uint32{222, 333}, keyspaceGroup.Keyspaces)
}

func TestSplitKeyspaceGroup(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/acceleratedAllocNodes", `return(true)`))
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/delayStartServerLoop", `return(true)`))
	keyspaces := make([]string, 0)
	// we test the case which exceed the default max txn ops limit in etcd, which is 128.
	for i := range 129 {
		keyspaces = append(keyspaces, fmt.Sprintf("keyspace_%d", i))
	}
	tc, err := pdTests.NewTestAPICluster(ctx, 3, func(conf *config.Config, _ string) {
		conf.Keyspace.PreAlloc = keyspaces
	})
	re.NoError(err)
	defer tc.Destroy()
	err = tc.RunInitialServers()
	re.NoError(err)
	pdAddr := tc.GetConfig().GetClientURL()

	ttc, err := pdTests.NewTestTSOCluster(ctx, 2, pdAddr)
	re.NoError(err)
	defer ttc.Destroy()
	cmd := ctl.GetRootCmd()

	tc.WaitLeader()
	leaderServer := tc.GetLeaderServer()
	re.NoError(leaderServer.BootstrapCluster())

	// split keyspace group.
	testutil.Eventually(re, func() bool {
		args := []string{"-u", pdAddr, "keyspace-group", "split", "0", "1", "2"}
		output, err := tests.ExecuteCommand(cmd, args...)
		re.NoError(err)
		return strings.Contains(string(output), "Success")
	})

	// get all keyspaces
	args := []string{"-u", pdAddr, "keyspace-group"}
	output, err := tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	strings.Contains(string(output), "Success")
	var keyspaceGroups []*endpoint.KeyspaceGroup
	err = json.Unmarshal(output, &keyspaceGroups)
	re.NoError(err)
	re.Len(keyspaceGroups, 2)
	re.Zero(keyspaceGroups[0].ID)
	re.Equal(uint32(1), keyspaceGroups[1].ID)

	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/acceleratedAllocNodes"))
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/delayStartServerLoop"))
}

func TestExternalAllocNodeWhenStart(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// external alloc node for keyspace group, when keyspace manager update keyspace info to keyspace group
	// we hope the keyspace group can be updated correctly.
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/externalAllocNode", `return("127.0.0.1:2379,127.0.0.1:2380")`))
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/acceleratedAllocNodes", `return(true)`))
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/delayStartServerLoop", `return(true)`))
	keyspaces := make([]string, 0)
	for i := range 10 {
		keyspaces = append(keyspaces, fmt.Sprintf("keyspace_%d", i))
	}
	tc, err := pdTests.NewTestAPICluster(ctx, 1, func(conf *config.Config, _ string) {
		conf.Keyspace.PreAlloc = keyspaces
	})
	re.NoError(err)
	defer tc.Destroy()
	err = tc.RunInitialServers()
	re.NoError(err)
	pdAddr := tc.GetConfig().GetClientURL()

	cmd := ctl.GetRootCmd()

	tc.WaitLeader()
	leaderServer := tc.GetLeaderServer()
	re.NoError(leaderServer.BootstrapCluster())

	// check keyspace group information.
	defaultKeyspaceGroupID := fmt.Sprintf("%d", constant.DefaultKeyspaceGroupID)
	args := []string{"-u", pdAddr, "keyspace-group"}
	testutil.Eventually(re, func() bool {
		output, err := tests.ExecuteCommand(cmd, append(args, defaultKeyspaceGroupID)...)
		re.NoError(err)
		var keyspaceGroup endpoint.KeyspaceGroup
		err = json.Unmarshal(output, &keyspaceGroup)
		re.NoError(err)
		return len(keyspaceGroup.Keyspaces) == len(keyspaces)+1 && len(keyspaceGroup.Members) == 2
	})

	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/externalAllocNode"))
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/acceleratedAllocNodes"))
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/delayStartServerLoop"))
}

func TestSetNodeAndPriorityKeyspaceGroup(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	keyspaces := make([]string, 0)
	for i := range 10 {
		keyspaces = append(keyspaces, fmt.Sprintf("keyspace_%d", i))
	}
	tc, err := pdTests.NewTestAPICluster(ctx, 3, func(conf *config.Config, _ string) {
		conf.Keyspace.PreAlloc = keyspaces
	})
	re.NoError(err)
	defer tc.Destroy()
	err = tc.RunInitialServers()
	re.NoError(err)
	pdAddr := tc.GetConfig().GetClientURL()

	ttc, err := pdTests.NewTestTSOCluster(ctx, 2, pdAddr)
	re.NoError(err)
	defer ttc.Destroy()
	tsoAddrs := ttc.GetAddrs()
	cmd := ctl.GetRootCmd()

	tc.WaitLeader()
	leaderServer := tc.GetLeaderServer()
	re.NoError(leaderServer.BootstrapCluster())

	// set-node keyspace group.
	defaultKeyspaceGroupID := fmt.Sprintf("%d", constant.DefaultKeyspaceGroupID)
	testutil.Eventually(re, func() bool {
		args := []string{"-u", pdAddr, "keyspace-group", "set-node", defaultKeyspaceGroupID, tsoAddrs[0], tsoAddrs[1]}
		output, err := tests.ExecuteCommand(cmd, args...)
		re.NoError(err)
		return strings.Contains(string(output), "Success")
	})

	// set-priority keyspace group.
	checkPriority := func(p int) {
		testutil.Eventually(re, func() bool {
			args := []string{"-u", pdAddr, "keyspace-group", "set-priority", defaultKeyspaceGroupID, tsoAddrs[0]}
			if p >= 0 {
				args = append(args, strconv.Itoa(p))
			} else {
				args = append(args, "--", strconv.Itoa(p))
			}
			output, err := tests.ExecuteCommand(cmd, args...)
			re.NoError(err)
			return strings.Contains(string(output), "Success")
		})

		// check keyspace group information.
		args := []string{"-u", pdAddr, "keyspace-group"}
		output, err := tests.ExecuteCommand(cmd, append(args, defaultKeyspaceGroupID)...)
		re.NoError(err)
		var keyspaceGroup endpoint.KeyspaceGroup
		err = json.Unmarshal(output, &keyspaceGroup)
		re.NoError(err)
		re.Equal(constant.DefaultKeyspaceGroupID, keyspaceGroup.ID)
		re.Len(keyspaceGroup.Members, 2)
		for _, member := range keyspaceGroup.Members {
			re.Contains(tsoAddrs, member.Address)
			if member.Address == tsoAddrs[0] {
				re.Equal(p, member.Priority)
			} else {
				re.Equal(0, member.Priority)
			}
		}
	}

	checkPriority(200)
	checkPriority(-200)

	// params error for set-node.
	args := []string{"-u", pdAddr, "keyspace-group", "set-node", defaultKeyspaceGroupID, tsoAddrs[0]}
	output, err := tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Contains(string(output), "Success!")
	args = []string{"-u", pdAddr, "keyspace-group", "set-node", defaultKeyspaceGroupID, "", ""}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Contains(string(output), "Failed to parse the tso node address")
	args = []string{"-u", pdAddr, "keyspace-group", "set-node", defaultKeyspaceGroupID, tsoAddrs[0], "http://pingcap.com"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Contains(string(output), "node does not exist")

	// params error for set-priority.
	args = []string{"-u", pdAddr, "keyspace-group", "set-priority", defaultKeyspaceGroupID, "", "200"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Contains(string(output), "Failed to parse the tso node address")
	args = []string{"-u", pdAddr, "keyspace-group", "set-priority", defaultKeyspaceGroupID, "http://pingcap.com", "200"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Contains(string(output), "node does not exist")
	args = []string{"-u", pdAddr, "keyspace-group", "set-priority", defaultKeyspaceGroupID, tsoAddrs[0], "xxx"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Contains(string(output), "Failed to parse the priority")
}

func TestMergeKeyspaceGroup(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/acceleratedAllocNodes", `return(true)`))
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/delayStartServerLoop", `return(true)`))
	keyspaces := make([]string, 0)
	// we test the case which exceed the default max txn ops limit in etcd, which is 128.
	for i := range 129 {
		keyspaces = append(keyspaces, fmt.Sprintf("keyspace_%d", i))
	}
	tc, err := pdTests.NewTestAPICluster(ctx, 1, func(conf *config.Config, _ string) {
		conf.Keyspace.PreAlloc = keyspaces
	})
	re.NoError(err)
	defer tc.Destroy()
	err = tc.RunInitialServers()
	re.NoError(err)
	pdAddr := tc.GetConfig().GetClientURL()

	ttc, err := pdTests.NewTestTSOCluster(ctx, 2, pdAddr)
	re.NoError(err)
	defer ttc.Destroy()
	cmd := ctl.GetRootCmd()

	tc.WaitLeader()
	leaderServer := tc.GetLeaderServer()
	re.NoError(leaderServer.BootstrapCluster())

	// split keyspace group.
	testutil.Eventually(re, func() bool {
		args := []string{"-u", pdAddr, "keyspace-group", "split", "0", "1", "2"}
		output, err := tests.ExecuteCommand(cmd, args...)
		re.NoError(err)
		return strings.Contains(string(output), "Success")
	})

	args := []string{"-u", pdAddr, "keyspace-group", "finish-split", "1"}
	output, err := tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	strings.Contains(string(output), "Success")

	// merge keyspace group.
	testutil.Eventually(re, func() bool {
		args := []string{"-u", pdAddr, "keyspace-group", "merge", "0", "1"}
		output, err := tests.ExecuteCommand(cmd, args...)
		re.NoError(err)
		return strings.Contains(string(output), "Success")
	})

	args = []string{"-u", pdAddr, "keyspace-group", "finish-merge", "0"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	strings.Contains(string(output), "Success")
	args = []string{"-u", pdAddr, "keyspace-group", "0"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	var keyspaceGroup endpoint.KeyspaceGroup
	err = json.Unmarshal(output, &keyspaceGroup)
	re.NoError(err)
	re.Len(keyspaceGroup.Keyspaces, 130)
	re.Nil(keyspaceGroup.MergeState)

	// split keyspace group multiple times.
	for i := 1; i <= 10; i++ {
		splitTargetID := fmt.Sprintf("%d", i)
		testutil.Eventually(re, func() bool {
			args := []string{"-u", pdAddr, "keyspace-group", "split", "0", splitTargetID, splitTargetID}
			output, err := tests.ExecuteCommand(cmd, args...)
			re.NoError(err)
			return strings.Contains(string(output), "Success")
		})
		args := []string{"-u", pdAddr, "keyspace-group", "finish-split", splitTargetID}
		output, err := tests.ExecuteCommand(cmd, args...)
		re.NoError(err)
		strings.Contains(string(output), "Success")
	}

	// merge keyspace group with `all` flag.
	testutil.Eventually(re, func() bool {
		args := []string{"-u", pdAddr, "keyspace-group", "merge", "0", "--all"}
		output, err := tests.ExecuteCommand(cmd, args...)
		re.NoError(err)
		return strings.Contains(string(output), "Success")
	})

	args = []string{"-u", pdAddr, "keyspace-group", "finish-merge", "0"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	strings.Contains(string(output), "Success")
	args = []string{"-u", pdAddr, "keyspace-group", "0"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	err = json.Unmarshal(output, &keyspaceGroup)
	re.NoError(err)
	re.Len(keyspaceGroup.Keyspaces, 130)
	re.Nil(keyspaceGroup.MergeState)

	// merge keyspace group with wrong args.
	args = []string{"-u", pdAddr, "keyspace-group", "merge"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	strings.Contains(string(output), "Must specify the source keyspace group ID(s) or the merge all flag")
	args = []string{"-u", pdAddr, "keyspace-group", "merge", "0"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	strings.Contains(string(output), "Must specify the source keyspace group ID(s) or the merge all flag")
	args = []string{"-u", pdAddr, "keyspace-group", "merge", "0", "1", "--all"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	strings.Contains(string(output), "Must specify the source keyspace group ID(s) or the merge all flag")
	args = []string{"-u", pdAddr, "keyspace-group", "merge", "1", "--all"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	strings.Contains(string(output), "Unable to merge all keyspace groups into a non-default keyspace group")

	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/acceleratedAllocNodes"))
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/delayStartServerLoop"))
}

func TestKeyspaceGroupState(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/acceleratedAllocNodes", `return(true)`))
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/delayStartServerLoop", `return(true)`))
	keyspaces := make([]string, 0)
	for i := range 10 {
		keyspaces = append(keyspaces, fmt.Sprintf("keyspace_%d", i))
	}
	tc, err := pdTests.NewTestAPICluster(ctx, 1, func(conf *config.Config, _ string) {
		conf.Keyspace.PreAlloc = keyspaces
	})
	re.NoError(err)
	defer tc.Destroy()
	err = tc.RunInitialServers()
	re.NoError(err)
	pdAddr := tc.GetConfig().GetClientURL()

	ttc, err := pdTests.NewTestTSOCluster(ctx, 2, pdAddr)
	re.NoError(err)
	defer ttc.Destroy()
	cmd := ctl.GetRootCmd()

	tc.WaitLeader()
	leaderServer := tc.GetLeaderServer()
	re.NoError(leaderServer.BootstrapCluster())

	// split keyspace group.
	testutil.Eventually(re, func() bool {
		args := []string{"-u", pdAddr, "keyspace-group", "split", "0", "1", "2"}
		output, err := tests.ExecuteCommand(cmd, args...)
		re.NoError(err)
		return strings.Contains(string(output), "Success")
	})
	args := []string{"-u", pdAddr, "keyspace-group", "finish-split", "1"}
	output, err := tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	strings.Contains(string(output), "Success")
	args = []string{"-u", pdAddr, "keyspace-group", "--state", "split"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	strings.Contains(string(output), "Success")
	var keyspaceGroups []*endpoint.KeyspaceGroup
	err = json.Unmarshal(output, &keyspaceGroups)
	re.NoError(err)
	re.Empty(keyspaceGroups)
	testutil.Eventually(re, func() bool {
		args := []string{"-u", pdAddr, "keyspace-group", "split", "0", "2", "3"}
		output, err := tests.ExecuteCommand(cmd, args...)
		re.NoError(err)
		return strings.Contains(string(output), "Success")
	})
	args = []string{"-u", pdAddr, "keyspace-group", "--state", "split"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	strings.Contains(string(output), "Success")
	err = json.Unmarshal(output, &keyspaceGroups)
	re.NoError(err)
	re.Len(keyspaceGroups, 2)
	re.Equal(uint32(0), keyspaceGroups[0].ID)
	re.Equal(uint32(2), keyspaceGroups[1].ID)

	args = []string{"-u", pdAddr, "keyspace-group", "finish-split", "2"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	strings.Contains(string(output), "Success")
	// merge keyspace group.
	testutil.Eventually(re, func() bool {
		args := []string{"-u", pdAddr, "keyspace-group", "merge", "0", "1"}
		output, err := tests.ExecuteCommand(cmd, args...)
		re.NoError(err)
		return strings.Contains(string(output), "Success")
	})

	args = []string{"-u", pdAddr, "keyspace-group", "--state", "merge"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	strings.Contains(string(output), "Success")
	err = json.Unmarshal(output, &keyspaceGroups)
	re.NoError(err)
	err = json.Unmarshal(output, &keyspaceGroups)
	re.NoError(err)
	re.Len(keyspaceGroups, 1)
	re.Equal(uint32(0), keyspaceGroups[0].ID)

	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/acceleratedAllocNodes"))
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/delayStartServerLoop"))
}

func TestShowKeyspaceGroupPrimary(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/acceleratedAllocNodes", `return(true)`))
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/fastGroupSplitPatroller", `return(true)`))
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/delayStartServerLoop", `return(true)`))
	keyspaces := make([]string, 0)
	for i := range 10 {
		keyspaces = append(keyspaces, fmt.Sprintf("keyspace_%d", i))
	}
	tc, err := pdTests.NewTestAPICluster(ctx, 1, func(conf *config.Config, _ string) {
		conf.Keyspace.PreAlloc = keyspaces
	})
	re.NoError(err)
	defer tc.Destroy()
	err = tc.RunInitialServers()
	re.NoError(err)
	pdAddr := tc.GetConfig().GetClientURL()

	ttc, err := pdTests.NewTestTSOCluster(ctx, 2, pdAddr)
	re.NoError(err)
	defer ttc.Destroy()
	tsoAddrs := ttc.GetAddrs()
	cmd := ctl.GetRootCmd()

	tc.WaitLeader()
	leaderServer := tc.GetLeaderServer()
	re.NoError(leaderServer.BootstrapCluster())
	defaultKeyspaceGroupID := fmt.Sprintf("%d", constant.DefaultKeyspaceGroupID)

	// check keyspace group 0 information.
	var keyspaceGroup endpoint.KeyspaceGroup
	testutil.Eventually(re, func() bool {
		args := []string{"-u", pdAddr, "keyspace-group"}
		output, err := tests.ExecuteCommand(cmd, append(args, defaultKeyspaceGroupID)...)
		re.NoError(err)
		err = json.Unmarshal(output, &keyspaceGroup)
		re.NoError(err)
		re.Equal(constant.DefaultKeyspaceGroupID, keyspaceGroup.ID)
		return len(keyspaceGroup.Members) == 2
	})
	for _, member := range keyspaceGroup.Members {
		re.Contains(tsoAddrs, member.Address)
	}

	// get primary for keyspace group 0.
	testutil.Eventually(re, func() bool {
		args := []string{"-u", pdAddr, "keyspace-group", "primary", defaultKeyspaceGroupID}
		output, err := tests.ExecuteCommand(cmd, args...)
		re.NoError(err)
		var resp handlers.GetKeyspaceGroupPrimaryResponse
		json.Unmarshal(output, &resp)
		return tsoAddrs[0] == resp.Primary || tsoAddrs[1] == resp.Primary
	})

	// split keyspace group.
	testutil.Eventually(re, func() bool {
		args := []string{"-u", pdAddr, "keyspace-group", "split", "0", "1", "2"}
		output, err := tests.ExecuteCommand(cmd, args...)
		re.NoError(err)
		return strings.Contains(string(output), "Success")
	})

	// check keyspace group 1 information.
	testutil.Eventually(re, func() bool {
		args := []string{"-u", pdAddr, "keyspace-group"}
		output, err := tests.ExecuteCommand(cmd, append(args, "1")...)
		re.NoError(err)
		if strings.Contains(string(output), "Failed") {
			// It may be failed when meets error, such as [PD:etcd:ErrEtcdTxnConflict]etcd transaction failed, conflicted and rolled back
			re.Contains(string(output), "ErrEtcdTxnConflict", "output: %s", string(output))
			return false
		}
		err = json.Unmarshal(output, &keyspaceGroup)
		re.NoError(err)
		return len(keyspaceGroup.Members) == 2
	})
	for _, member := range keyspaceGroup.Members {
		re.Contains(tsoAddrs, member.Address)
	}

	// get primary for keyspace group 1.
	testutil.Eventually(re, func() bool {
		args := []string{"-u", pdAddr, "keyspace-group", "primary", "1"}
		output, err := tests.ExecuteCommand(cmd, args...)
		re.NoError(err)
		var resp handlers.GetKeyspaceGroupPrimaryResponse
		json.Unmarshal(output, &resp)
		return tsoAddrs[0] == resp.Primary || tsoAddrs[1] == resp.Primary
	})

	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/acceleratedAllocNodes"))
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/tso/fastGroupSplitPatroller"))
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/delayStartServerLoop"))
}

func TestInPDMode(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tc, err := pdTests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer tc.Destroy()
	err = tc.RunInitialServers()
	re.NoError(err)
	pdAddr := tc.GetConfig().GetClientURL()
	cmd := ctl.GetRootCmd()
	tc.WaitLeader()
	leaderServer := tc.GetLeaderServer()
	re.NoError(leaderServer.BootstrapCluster())

	argsList := [][]string{
		{"-u", pdAddr, "keyspace-group"},
		{"-u", pdAddr, "keyspace-group", "0"},
		{"-u", pdAddr, "keyspace-group", "split", "0", "1", "2"},
		{"-u", pdAddr, "keyspace-group", "split-range", "1", "2", "3", "4"},
		{"-u", pdAddr, "keyspace-group", "finish-split", "1"},
		{"-u", pdAddr, "keyspace-group", "merge", "1", "2"},
		{"-u", pdAddr, "keyspace-group", "merge", "0", "--all"},
		{"-u", pdAddr, "keyspace-group", "finish-merge", "1"},
		{"-u", pdAddr, "keyspace-group", "set-node", "0", "http://127.0.0.1:2379"},
		{"-u", pdAddr, "keyspace-group", "set-priority", "0", "http://127.0.0.1:2379", "200"},
		{"-u", pdAddr, "keyspace-group", "primary", "0"},
	}
	for _, args := range argsList {
		output, err := tests.ExecuteCommand(cmd, args...)
		re.NoError(err)
		re.Contains(string(output), "Failed",
			"args: %v, output: %v", args, string(output))
		re.Contains(string(output), "keyspace group manager is not initialized",
			"args: %v, output: %v", args, string(output))
	}

	leaderServer.SetKeyspaceManager(nil)
	args := []string{"-u", pdAddr, "keyspace-group", "split", "0", "1", "2"}
	output, err := tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Contains(string(output), "Failed",
		"args: %v, output: %v", args, string(output))
	re.Contains(string(output), "keyspace manager is not initialized",
		"args: %v, output: %v", args, string(output))
}
