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
	"github.com/stretchr/testify/suite"

	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/id"
	"github.com/tikv/pd/pkg/keyspace/constant"
	mcs "github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/apiv2/handlers"
	"github.com/tikv/pd/server/config"
	pdTests "github.com/tikv/pd/tests"
	handlersutil "github.com/tikv/pd/tests/server/apiv2/handlers"
	ctl "github.com/tikv/pd/tools/pd-ctl/pdctl"
	"github.com/tikv/pd/tools/pd-ctl/tests"
)

type keyspaceGroupTestSuite struct {
	suite.Suite
	ctx           context.Context
	cancel        context.CancelFunc
	cluster       *pdTests.TestCluster
	pdAddr        string
	tsoCluster    *pdTests.TestTSOCluster
	tsoAddrs      []string
	idAllocator   id.Allocator
	keyspaceCount int
}

func TestKeyspaceGroupTestsuite(t *testing.T) {
	suite.Run(t, new(keyspaceGroupTestSuite))
}

func (suite *keyspaceGroupTestSuite) SetupTest() {
	re := suite.Require()
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/acceleratedAllocNodes", `return(true)`))
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/delayStartServerLoop", `return(true)`))

	suite.idAllocator = mockid.NewIDAllocator()
	// we test the case which exceed the default max txn ops limit in etcd, which is 128.
	suite.keyspaceCount = 129
	keyspaces := make([]string, 0, suite.keyspaceCount)
	for i := range suite.keyspaceCount {
		keyspaces = append(keyspaces, fmt.Sprintf("keyspace_%d", i))
	}
	tc, err := pdTests.NewTestClusterWithKeyspaceGroup(suite.ctx, 3, func(conf *config.Config, _ string) {
		conf.Keyspace.PreAlloc = keyspaces
		conf.Keyspace.WaitRegionSplit = false
	})
	re.NoError(err)
	re.NoError(tc.RunInitialServers())
	tc.WaitLeader()
	leaderServer := tc.GetLeaderServer()
	re.NoError(leaderServer.BootstrapCluster())
	suite.cluster = tc
	suite.pdAddr = tc.GetConfig().GetClientURL()

	suite.tsoCluster, err = pdTests.NewTestTSOCluster(suite.ctx, 2, suite.pdAddr)
	re.NoError(err)
	suite.tsoAddrs = suite.tsoCluster.GetAddrs()

	suite.idAllocator.Alloc(1) // keyspace group 0 is reserved
}

func (suite *keyspaceGroupTestSuite) TearDownTest() {
	re := suite.Require()
	suite.cancel()
	suite.tsoCluster.Destroy()
	suite.cluster.Destroy()
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/acceleratedAllocNodes"))
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/delayStartServerLoop"))
}

func (suite *keyspaceGroupTestSuite) TestSplitKeyspaceGroup() {
	re := suite.Require()
	cmd := ctl.GetRootCmd()
	leaderServer := suite.cluster.GetLeaderServer()

	// Show keyspace group information.
	suite.checkKeyspaceContains(constant.DefaultKeyspaceGroupID, constant.DefaultKeyspaceID)
	args := []string{"-u", suite.pdAddr, "keyspace-group"}

	// Split keyspace group.
	keyspaceGroupID, _, err := suite.idAllocator.Alloc(1)
	re.NoError(err)
	keyspaceGroupIDStr := strconv.FormatUint(keyspaceGroupID, 10)
	handlersutil.MustCreateKeyspaceGroup(re, leaderServer, &handlers.CreateKeyspaceGroupParams{
		KeyspaceGroups: []*endpoint.KeyspaceGroup{
			{
				ID:        uint32(keyspaceGroupID),
				UserKind:  endpoint.Standard.String(),
				Members:   make([]endpoint.KeyspaceGroupMember, mcs.DefaultKeyspaceGroupReplicaCount),
				Keyspaces: []uint32{111, 222, 333},
			},
		},
	})
	newKeyspaceGroupID, _, err := suite.idAllocator.Alloc(1)
	re.NoError(err)
	newKeyspaceGroupIDStr := strconv.FormatUint(newKeyspaceGroupID, 10)
	_, err = tests.ExecuteCommand(cmd, append(args, "split", keyspaceGroupIDStr, newKeyspaceGroupIDStr, "222", "333")...)
	re.NoError(err)
	suite.checkKeyspaceContains(uint32(keyspaceGroupID), 111)
	suite.checkKeyspaceContains(uint32(newKeyspaceGroupID), 222, 333)
}

func (suite *keyspaceGroupTestSuite) TestExternalAllocNodeWhenStart() {
	re := suite.Require()
	// external alloc node for keyspace group, when keyspace manager update keyspace info to keyspace group
	// we hope the keyspace group can be updated correctly.
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/externalAllocNode", `return("127.0.0.1:2379,127.0.0.1:2380")`))
	cmd := ctl.GetRootCmd()
	// check keyspace group information.
	defaultKeyspaceGroupID := strconv.FormatUint(uint64(constant.DefaultKeyspaceGroupID), 10)
	args := []string{"-u", suite.pdAddr, "keyspace-group"}
	testutil.Eventually(re, func() bool {
		output, err := tests.ExecuteCommand(cmd, append(args, defaultKeyspaceGroupID)...)
		re.NoError(err)
		var keyspaceGroup endpoint.KeyspaceGroup
		err = json.Unmarshal(output, &keyspaceGroup)
		re.NoError(err)
		return len(keyspaceGroup.Keyspaces) == suite.keyspaceCount+1 && len(keyspaceGroup.Members) == 2
	})
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/externalAllocNode"))
}

func (suite *keyspaceGroupTestSuite) TestSetNodeAndPriorityKeyspaceGroup() {
	re := suite.Require()
	cmd := ctl.GetRootCmd()
	re.Len(suite.tsoAddrs, 2)

	// set-node keyspace group.
	defaultKeyspaceGroupID := strconv.FormatUint(uint64(constant.DefaultKeyspaceGroupID), 10)
	testutil.Eventually(re, func() bool {
		args := []string{"-u", suite.pdAddr, "keyspace-group", "set-node", defaultKeyspaceGroupID, suite.tsoAddrs[0], suite.tsoAddrs[1]}
		output, err := tests.ExecuteCommand(cmd, args...)
		re.NoError(err)
		return strings.Contains(string(output), "Success")
	})

	// set-priority keyspace group.
	checkPriority := func(p int) {
		testutil.Eventually(re, func() bool {
			args := []string{"-u", suite.pdAddr, "keyspace-group", "set-priority", defaultKeyspaceGroupID, suite.tsoAddrs[0]}
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
		args := []string{"-u", suite.pdAddr, "keyspace-group"}
		output, err := tests.ExecuteCommand(cmd, append(args, defaultKeyspaceGroupID)...)
		re.NoError(err)
		var keyspaceGroup endpoint.KeyspaceGroup
		err = json.Unmarshal(output, &keyspaceGroup)
		re.NoError(err)
		re.Equal(constant.DefaultKeyspaceGroupID, keyspaceGroup.ID)
		re.Len(keyspaceGroup.Members, 2)
		for _, member := range keyspaceGroup.Members {
			re.Contains(suite.tsoAddrs, member.Address)
			if member.Address == suite.tsoAddrs[0] {
				re.Equal(p, member.Priority)
			} else {
				re.Equal(0, member.Priority)
			}
		}
	}

	checkPriority(200)
	checkPriority(-200)

	// params error for set-node.
	args := []string{"-u", suite.pdAddr, "keyspace-group", "set-node", defaultKeyspaceGroupID, suite.tsoAddrs[0]}
	output, err := tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Contains(string(output), "Success!")
	args = []string{"-u", suite.pdAddr, "keyspace-group", "set-node", defaultKeyspaceGroupID, "", ""}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Contains(string(output), "Failed to parse the tso node address")
	args = []string{"-u", suite.pdAddr, "keyspace-group", "set-node", defaultKeyspaceGroupID, suite.tsoAddrs[0], "http://pingcap.com"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Contains(string(output), "node does not exist")

	// params error for set-priority.
	args = []string{"-u", suite.pdAddr, "keyspace-group", "set-priority", defaultKeyspaceGroupID, "", "200"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Contains(string(output), "Failed to parse the tso node address")
	args = []string{"-u", suite.pdAddr, "keyspace-group", "set-priority", defaultKeyspaceGroupID, "http://pingcap.com", "200"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Contains(string(output), "node does not exist")
	args = []string{"-u", suite.pdAddr, "keyspace-group", "set-priority", defaultKeyspaceGroupID, suite.tsoAddrs[0], "xxx"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Contains(string(output), "Failed to parse the priority")
}

func (suite *keyspaceGroupTestSuite) TestMergeKeyspaceGroup() {
	re := suite.Require()
	cmd := ctl.GetRootCmd()

	testKeyspaceID := uint32(42)
	testKeyspaceIDStr := strconv.FormatUint(uint64(testKeyspaceID), 10)
	suite.checkKeyspaceContains(constant.DefaultKeyspaceGroupID, testKeyspaceID)

	defaultKeyspaceGroupID := strconv.FormatUint(uint64(constant.DefaultKeyspaceGroupID), 10)
	newKeyspaceGroupID, _, err := suite.idAllocator.Alloc(1)
	re.NoError(err)
	newKeyspaceGroupIDStr := strconv.FormatUint(newKeyspaceGroupID, 10)

	// split keyspace group.
	testutil.Eventually(re, func() bool {
		args := []string{"-u", suite.pdAddr, "keyspace-group", "split", defaultKeyspaceGroupID, newKeyspaceGroupIDStr, testKeyspaceIDStr}
		output, err := tests.ExecuteCommand(cmd, args...)
		re.NoError(err)
		return strings.Contains(string(output), "Success")
	})

	args := []string{"-u", suite.pdAddr, "keyspace-group", "finish-split", newKeyspaceGroupIDStr}
	output, err := tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	strings.Contains(string(output), "Success")

	// merge keyspace group.
	testutil.Eventually(re, func() bool {
		args := []string{"-u", suite.pdAddr, "keyspace-group", "merge", defaultKeyspaceGroupID, newKeyspaceGroupIDStr}
		output, err := tests.ExecuteCommand(cmd, args...)
		re.NoError(err)
		return strings.Contains(string(output), "Success")
	})

	args = []string{"-u", suite.pdAddr, "keyspace-group", "finish-merge", defaultKeyspaceGroupID}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	strings.Contains(string(output), "Success")
	args = []string{"-u", suite.pdAddr, "keyspace-group", defaultKeyspaceGroupID}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	var keyspaceGroup endpoint.KeyspaceGroup
	err = json.Unmarshal(output, &keyspaceGroup)
	re.NoError(err, string(output))
	re.Len(keyspaceGroup.Keyspaces, suite.keyspaceCount+1)
	re.Nil(keyspaceGroup.MergeState)

	// split keyspace group multiple times.
	for i := 1; i <= 10; i++ {
		splitTargetID := strconv.Itoa(i)
		testutil.Eventually(re, func() bool {
			args := []string{"-u", suite.pdAddr, "keyspace-group", "split", defaultKeyspaceGroupID, splitTargetID, splitTargetID}
			output, err := tests.ExecuteCommand(cmd, args...)
			re.NoError(err)
			return strings.Contains(string(output), "Success")
		})
		args := []string{"-u", suite.pdAddr, "keyspace-group", "finish-split", splitTargetID}
		output, err := tests.ExecuteCommand(cmd, args...)
		re.NoError(err)
		strings.Contains(string(output), "Success")
	}

	// merge keyspace group with `all` flag.
	testutil.Eventually(re, func() bool {
		args := []string{"-u", suite.pdAddr, "keyspace-group", "merge", defaultKeyspaceGroupID, "--all"}
		output, err := tests.ExecuteCommand(cmd, args...)
		re.NoError(err)
		return strings.Contains(string(output), "Success")
	})

	args = []string{"-u", suite.pdAddr, "keyspace-group", "finish-merge", defaultKeyspaceGroupID}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	strings.Contains(string(output), "Success")
	args = []string{"-u", suite.pdAddr, "keyspace-group", defaultKeyspaceGroupID}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	err = json.Unmarshal(output, &keyspaceGroup)
	re.NoError(err)
	re.Len(keyspaceGroup.Keyspaces, suite.keyspaceCount+1)
	re.Nil(keyspaceGroup.MergeState)

	// merge keyspace group with wrong args.
	args = []string{"-u", suite.pdAddr, "keyspace-group", "merge"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	strings.Contains(string(output), "Must specify the source keyspace group ID(s) or the merge all flag")
	args = []string{"-u", suite.pdAddr, "keyspace-group", "merge", defaultKeyspaceGroupID}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	strings.Contains(string(output), "Must specify the source keyspace group ID(s) or the merge all flag")
	args = []string{"-u", suite.pdAddr, "keyspace-group", "merge", defaultKeyspaceGroupID, newKeyspaceGroupIDStr, "--all"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	strings.Contains(string(output), "Must specify the source keyspace group ID(s) or the merge all flag")
	args = []string{"-u", suite.pdAddr, "keyspace-group", "merge", newKeyspaceGroupIDStr, "--all"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	strings.Contains(string(output), "Unable to merge all keyspace groups into a non-default keyspace group")
}

func (suite *keyspaceGroupTestSuite) TestKeyspaceGroupState() {
	re := suite.Require()
	cmd := ctl.GetRootCmd()

	testKeyspaceID := uint32(54)
	testKeyspaceIDStr := strconv.FormatUint(uint64(testKeyspaceID), 10)
	suite.checkKeyspaceContains(constant.DefaultKeyspaceGroupID, testKeyspaceID)

	defaultKeyspaceGroupID := strconv.FormatUint(uint64(constant.DefaultKeyspaceGroupID), 10)
	newKeyspaceGroupID, _, err := suite.idAllocator.Alloc(1)
	re.NoError(err)
	newKeyspaceGroupIDStr := strconv.FormatUint(newKeyspaceGroupID, 10)

	// split keyspace group.
	testutil.Eventually(re, func() bool {
		args := []string{"-u", suite.pdAddr, "keyspace-group", "split", defaultKeyspaceGroupID, newKeyspaceGroupIDStr, testKeyspaceIDStr}
		output, err := tests.ExecuteCommand(cmd, args...)
		re.NoError(err)
		return strings.Contains(string(output), "Success")
	})
	args := []string{"-u", suite.pdAddr, "keyspace-group", "finish-split", newKeyspaceGroupIDStr}
	output, err := tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	strings.Contains(string(output), "Success")
	args = []string{"-u", suite.pdAddr, "keyspace-group", "--state", "split"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	strings.Contains(string(output), "Success")
	var keyspaceGroups []*endpoint.KeyspaceGroup
	err = json.Unmarshal(output, &keyspaceGroups)
	re.NoError(err)
	re.Empty(keyspaceGroups)

	testKeyspaceID = uint32(55)
	testKeyspaceIDStr = strconv.FormatUint(uint64(testKeyspaceID), 10)
	suite.checkKeyspaceContains(constant.DefaultKeyspaceGroupID, testKeyspaceID)

	newKeyspaceGroupID, _, err = suite.idAllocator.Alloc(1)
	re.NoError(err)
	newKeyspaceGroupIDStr = strconv.FormatUint(newKeyspaceGroupID, 10)

	testutil.Eventually(re, func() bool {
		args := []string{"-u", suite.pdAddr, "keyspace-group", "split", defaultKeyspaceGroupID, newKeyspaceGroupIDStr, testKeyspaceIDStr}
		output, err := tests.ExecuteCommand(cmd, args...)
		re.NoError(err)
		return strings.Contains(string(output), "Success")
	})
	args = []string{"-u", suite.pdAddr, "keyspace-group", "--state", "split"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	strings.Contains(string(output), "Success")
	err = json.Unmarshal(output, &keyspaceGroups)
	re.NoError(err)
	re.Len(keyspaceGroups, 2)
	re.Equal(uint32(0), keyspaceGroups[0].ID)
	re.Equal(uint32(newKeyspaceGroupID), keyspaceGroups[1].ID)

	args = []string{"-u", suite.pdAddr, "keyspace-group", "finish-split", newKeyspaceGroupIDStr}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	strings.Contains(string(output), "Success")
	// merge keyspace group.
	testutil.Eventually(re, func() bool {
		args := []string{"-u", suite.pdAddr, "keyspace-group", "merge", "0", newKeyspaceGroupIDStr}
		output, err := tests.ExecuteCommand(cmd, args...)
		re.NoError(err)
		return strings.Contains(string(output), "Success")
	})

	args = []string{"-u", suite.pdAddr, "keyspace-group", "--state", "merge"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	strings.Contains(string(output), "Success")
	err = json.Unmarshal(output, &keyspaceGroups)
	re.NoError(err)
	err = json.Unmarshal(output, &keyspaceGroups)
	re.NoError(err)
	re.Len(keyspaceGroups, 1)
	re.Equal(constant.DefaultKeyspaceGroupID, keyspaceGroups[0].ID)
}

func (suite *keyspaceGroupTestSuite) TestShowKeyspaceGroupPrimary() {
	re := suite.Require()
	cmd := ctl.GetRootCmd()

	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/fastGroupSplitPatroller", `return(true)`))

	defaultKeyspaceGroupID := strconv.FormatUint(uint64(constant.DefaultKeyspaceGroupID), 10)

	// check keyspace group 0 information.
	var keyspaceGroup endpoint.KeyspaceGroup
	testutil.Eventually(re, func() bool {
		args := []string{"-u", suite.pdAddr, "keyspace-group"}
		output, err := tests.ExecuteCommand(cmd, append(args, defaultKeyspaceGroupID)...)
		re.NoError(err)
		err = json.Unmarshal(output, &keyspaceGroup)
		re.NoError(err)
		re.Equal(constant.DefaultKeyspaceGroupID, keyspaceGroup.ID)
		return len(keyspaceGroup.Members) == 2
	})
	for _, member := range keyspaceGroup.Members {
		re.Contains(suite.tsoAddrs, member.Address)
	}

	// get primary for keyspace group 0.
	testutil.Eventually(re, func() bool {
		args := []string{"-u", suite.pdAddr, "keyspace-group", "primary", defaultKeyspaceGroupID}
		output, err := tests.ExecuteCommand(cmd, args...)
		re.NoError(err)
		var resp handlers.GetKeyspaceGroupPrimaryResponse
		json.Unmarshal(output, &resp)
		return suite.tsoAddrs[0] == resp.Primary || suite.tsoAddrs[1] == resp.Primary
	})

	// split keyspace group.
	testutil.Eventually(re, func() bool {
		args := []string{"-u", suite.pdAddr, "keyspace-group", "split", "0", "1", "2"}
		output, err := tests.ExecuteCommand(cmd, args...)
		re.NoError(err)
		return strings.Contains(string(output), "Success")
	})

	// check keyspace group 1 information.
	testutil.Eventually(re, func() bool {
		args := []string{"-u", suite.pdAddr, "keyspace-group"}
		output, err := tests.ExecuteCommand(cmd, append(args, "1")...)
		re.NoError(err)
		if strings.Contains(string(output), "Failed") {
			// If the error is ErrEtcdTxnConflict, it means there is a temporary failure.
			re.Contains(string(output), "ErrEtcdTxnConflict", "output: %s", string(output))
			return false
		}
		err = json.Unmarshal(output, &keyspaceGroup)
		re.NoError(err)
		return len(keyspaceGroup.Members) == 2
	})
	for _, member := range keyspaceGroup.Members {
		re.Contains(suite.tsoAddrs, member.Address)
	}

	// get primary for keyspace group 1.
	testutil.Eventually(re, func() bool {
		args := []string{"-u", suite.pdAddr, "keyspace-group", "primary", "1"}
		output, err := tests.ExecuteCommand(cmd, args...)
		re.NoError(err)
		var resp handlers.GetKeyspaceGroupPrimaryResponse
		json.Unmarshal(output, &resp)
		return suite.tsoAddrs[0] == resp.Primary || suite.tsoAddrs[1] == resp.Primary
	})

	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/tso/fastGroupSplitPatroller"))
}

func (suite *keyspaceGroupTestSuite) checkKeyspaceContains(keyspaceGroupID uint32, keyspaceIDs ...uint32) {
	re := suite.Require()
	cmd := ctl.GetRootCmd()
	keyspaceGroupIDStr := strconv.FormatUint(uint64(keyspaceGroupID), 10)
	args := []string{"-u", suite.pdAddr, "keyspace-group"}
	output, err := tests.ExecuteCommand(cmd, append(args, keyspaceGroupIDStr)...)
	re.NoError(err)
	var keyspaceGroup endpoint.KeyspaceGroup
	err = json.Unmarshal(output, &keyspaceGroup)
	re.NoError(err)
	re.Equal(keyspaceGroupID, keyspaceGroup.ID)
	for _, keyspaceID := range keyspaceIDs {
		re.Contains(keyspaceGroup.Keyspaces, keyspaceID)
	}
}

// TestCmdWithoutKeyspaceGroupInitialized tests without keyspace group initialized
// So we need another cluster to run this test.
func TestCmdWithoutKeyspaceGroupInitialized(t *testing.T) {
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
