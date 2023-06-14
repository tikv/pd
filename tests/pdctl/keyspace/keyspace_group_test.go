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
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/apiv2/handlers"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/pdctl"
	handlersutil "github.com/tikv/pd/tests/server/apiv2/handlers"
	pdctlCmd "github.com/tikv/pd/tools/pd-ctl/pdctl"
)

func TestKeyspaceGroup(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tc, err := tests.NewTestAPICluster(ctx, 1)
	re.NoError(err)
	err = tc.RunInitialServers()
	re.NoError(err)
	tc.WaitLeader()
	leaderServer := tc.GetServer(tc.GetLeader())
	re.NoError(leaderServer.BootstrapCluster())
	pdAddr := tc.GetConfig().GetClientURL()
	cmd := pdctlCmd.GetRootCmd()

	// Show keyspace group information.
	defaultKeyspaceGroupID := fmt.Sprintf("%d", utils.DefaultKeyspaceGroupID)
	args := []string{"-u", pdAddr, "keyspace-group"}
	output, err := pdctl.ExecuteCommand(cmd, append(args, defaultKeyspaceGroupID)...)
	re.NoError(err)
	var keyspaceGroup endpoint.KeyspaceGroup
	err = json.Unmarshal(output, &keyspaceGroup)
	re.NoError(err)
	re.Equal(utils.DefaultKeyspaceGroupID, keyspaceGroup.ID)
	re.Contains(keyspaceGroup.Keyspaces, utils.DefaultKeyspaceID)
	// Split keyspace group.
	handlersutil.MustCreateKeyspaceGroup(re, leaderServer, &handlers.CreateKeyspaceGroupParams{
		KeyspaceGroups: []*endpoint.KeyspaceGroup{
			{
				ID:        1,
				UserKind:  endpoint.Standard.String(),
				Members:   make([]endpoint.KeyspaceGroupMember, utils.KeyspaceGroupDefaultReplicaCount),
				Keyspaces: []uint32{111, 222, 333},
			},
		},
	})
	_, err = pdctl.ExecuteCommand(cmd, append(args, "split", "1", "2", "222", "333")...)
	re.NoError(err)
	output, err = pdctl.ExecuteCommand(cmd, append(args, "1")...)
	re.NoError(err)
	keyspaceGroup = endpoint.KeyspaceGroup{}
	err = json.Unmarshal(output, &keyspaceGroup)
	re.NoError(err)
	re.Equal(uint32(1), keyspaceGroup.ID)
	re.Equal(keyspaceGroup.Keyspaces, []uint32{111})
	output, err = pdctl.ExecuteCommand(cmd, append(args, "2")...)
	re.NoError(err)
	keyspaceGroup = endpoint.KeyspaceGroup{}
	err = json.Unmarshal(output, &keyspaceGroup)
	re.NoError(err)
	re.Equal(uint32(2), keyspaceGroup.ID)
	re.Equal(keyspaceGroup.Keyspaces, []uint32{222, 333})
}

func TestSplitKeyspaceGroup(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/acceleratedAllocNodes", `return(true)`))
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/delayStartServerLoop", `return(true)`))
	keyspaces := make([]string, 0)
	// we test the case which exceed the default max txn ops limit in etcd, which is 128.
	for i := 0; i < 129; i++ {
		keyspaces = append(keyspaces, fmt.Sprintf("keyspace_%d", i))
	}
	tc, err := tests.NewTestAPICluster(ctx, 3, func(conf *config.Config, serverName string) {
		conf.Keyspace.PreAlloc = keyspaces
	})
	re.NoError(err)
	err = tc.RunInitialServers()
	re.NoError(err)
	pdAddr := tc.GetConfig().GetClientURL()

	_, tsoServerCleanup1, err := tests.StartSingleTSOTestServer(ctx, re, pdAddr, tempurl.Alloc())
	defer tsoServerCleanup1()
	re.NoError(err)
	_, tsoServerCleanup2, err := tests.StartSingleTSOTestServer(ctx, re, pdAddr, tempurl.Alloc())
	defer tsoServerCleanup2()
	re.NoError(err)
	cmd := pdctlCmd.GetRootCmd()

	time.Sleep(2 * time.Second)
	tc.WaitLeader()
	leaderServer := tc.GetServer(tc.GetLeader())
	re.NoError(leaderServer.BootstrapCluster())

	// split keyspace group.
	testutil.Eventually(re, func() bool {
		args := []string{"-u", pdAddr, "keyspace-group", "split", "0", "1", "2"}
		output, err := pdctl.ExecuteCommand(cmd, args...)
		re.NoError(err)
		return strings.Contains(string(output), "Success")
	})

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
	for i := 0; i < 10; i++ {
		keyspaces = append(keyspaces, fmt.Sprintf("keyspace_%d", i))
	}
	tc, err := tests.NewTestAPICluster(ctx, 1, func(conf *config.Config, serverName string) {
		conf.Keyspace.PreAlloc = keyspaces
	})
	re.NoError(err)
	err = tc.RunInitialServers()
	re.NoError(err)
	pdAddr := tc.GetConfig().GetClientURL()

	cmd := pdctlCmd.GetRootCmd()

	time.Sleep(2 * time.Second)
	tc.WaitLeader()
	leaderServer := tc.GetServer(tc.GetLeader())
	re.NoError(leaderServer.BootstrapCluster())

	// check keyspace group information.
	defaultKeyspaceGroupID := fmt.Sprintf("%d", utils.DefaultKeyspaceGroupID)
	args := []string{"-u", pdAddr, "keyspace-group"}
	testutil.Eventually(re, func() bool {
		output, err := pdctl.ExecuteCommand(cmd, append(args, defaultKeyspaceGroupID)...)
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
	for i := 0; i < 10; i++ {
		keyspaces = append(keyspaces, fmt.Sprintf("keyspace_%d", i))
	}
	tc, err := tests.NewTestAPICluster(ctx, 3, func(conf *config.Config, serverName string) {
		conf.Keyspace.PreAlloc = keyspaces
	})
	re.NoError(err)
	err = tc.RunInitialServers()
	re.NoError(err)
	pdAddr := tc.GetConfig().GetClientURL()

	s1, tsoServerCleanup1, err := tests.StartSingleTSOTestServer(ctx, re, pdAddr, tempurl.Alloc())
	defer tsoServerCleanup1()
	re.NoError(err)
	s2, tsoServerCleanup2, err := tests.StartSingleTSOTestServer(ctx, re, pdAddr, tempurl.Alloc())
	defer tsoServerCleanup2()
	re.NoError(err)
	cmd := pdctlCmd.GetRootCmd()

	time.Sleep(2 * time.Second)
	tc.WaitLeader()
	leaderServer := tc.GetServer(tc.GetLeader())
	re.NoError(leaderServer.BootstrapCluster())

	// set-node keyspace group.
	defaultKeyspaceGroupID := fmt.Sprintf("%d", utils.DefaultKeyspaceGroupID)
	testutil.Eventually(re, func() bool {
		args := []string{"-u", pdAddr, "keyspace-group", "set-node", defaultKeyspaceGroupID, s1.GetAddr(), s2.GetAddr()}
		output, err := pdctl.ExecuteCommand(cmd, args...)
		re.NoError(err)
		return strings.Contains(string(output), "Success")
	})

	// set-priority keyspace group.
	testutil.Eventually(re, func() bool {
		args := []string{"-u", pdAddr, "keyspace-group", "set-priority", defaultKeyspaceGroupID, s1.GetAddr(), "200"}
		output, err := pdctl.ExecuteCommand(cmd, args...)
		re.NoError(err)
		fmt.Println(string(output))
		return strings.Contains(string(output), "Success")
	})

	// check keyspace group information.
	args := []string{"-u", pdAddr, "keyspace-group"}
	output, err := pdctl.ExecuteCommand(cmd, append(args, defaultKeyspaceGroupID)...)
	re.NoError(err)
	var keyspaceGroup endpoint.KeyspaceGroup
	err = json.Unmarshal(output, &keyspaceGroup)
	re.NoError(err)
	re.Equal(utils.DefaultKeyspaceGroupID, keyspaceGroup.ID)
	re.Len(keyspaceGroup.Members, 2)
	for _, member := range keyspaceGroup.Members {
		re.Contains([]string{s1.GetAddr(), s2.GetAddr()}, member.Address)
		if member.Address == s1.GetAddr() {
			re.Equal(200, member.Priority)
		} else {
			re.Equal(100, member.Priority)
		}
	}

	// params error for set-node.
	args = []string{"-u", pdAddr, "keyspace-group", "set-node", defaultKeyspaceGroupID, s1.GetAddr()}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Contains(string(output), "invalid num of nodes")
	args = []string{"-u", pdAddr, "keyspace-group", "set-node", defaultKeyspaceGroupID, "", ""}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Contains(string(output), "Failed to parse the tso node address")
	args = []string{"-u", pdAddr, "keyspace-group", "set-node", defaultKeyspaceGroupID, s1.GetAddr(), "http://pingcap.com"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Contains(string(output), "node does not exist")

	// params error for set-priority.
	args = []string{"-u", pdAddr, "keyspace-group", "set-priority", defaultKeyspaceGroupID, "", "200"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Contains(string(output), "Failed to parse the tso node address")
	args = []string{"-u", pdAddr, "keyspace-group", "set-priority", defaultKeyspaceGroupID, "http://pingcap.com", "200"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Contains(string(output), "node does not exist")
	args = []string{"-u", pdAddr, "keyspace-group", "set-priority", defaultKeyspaceGroupID, s1.GetAddr(), "xxx"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Contains(string(output), "Failed to parse the priority")
}
