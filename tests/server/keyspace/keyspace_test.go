// Copyright 2022 TiKV Project Authors.
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

package keyspace

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/keyspacepb"

	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

type keyspaceTestSuite struct {
	suite.Suite
	cancel  context.CancelFunc
	cluster *tests.TestCluster
	server  *tests.TestServer
	manager *keyspace.Manager
}

// preAllocKeyspace is used to test keyspace pre-allocation.
var preAllocKeyspace = []string{"pre-alloc0", "pre-alloc1", "pre-alloc2"}

func TestKeyspaceTestSuite(t *testing.T) {
	suite.Run(t, new(keyspaceTestSuite))
}

func (suite *keyspaceTestSuite) SetupTest() {
	re := suite.Require()
	ctx, cancel := context.WithCancel(context.Background())
	suite.cancel = cancel
	cluster, err := tests.NewTestCluster(ctx, 3, func(conf *config.Config, _ string) {
		conf.Keyspace.PreAlloc = preAllocKeyspace
		conf.Keyspace.WaitRegionSplit = false
	})
	suite.cluster = cluster
	re.NoError(err)
	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())
	suite.server = cluster.GetLeaderServer()
	suite.manager = suite.server.GetKeyspaceManager()
	re.NoError(suite.server.BootstrapCluster())
}

func (suite *keyspaceTestSuite) TearDownTest() {
	suite.cancel()
	suite.cluster.Destroy()
}

func makeMutations() []*keyspace.Mutation {
	return []*keyspace.Mutation{
		{
			Op:    keyspace.OpPut,
			Key:   "config_entry_1",
			Value: "new val",
		},
		{
			Op:    keyspace.OpPut,
			Key:   "new config",
			Value: "new val",
		},
		{
			Op:  keyspace.OpDel,
			Key: "config_entry_2",
		},
	}
}

func TestProtectedKeyspace(t *testing.T) {
	re := require.New(t)
	const classic = `return(false)`
	const nextGen = `return(true)`

	cases := []struct {
		name                  string
		nextGenFlag           string
		protectedKeyspaceID   uint32
		protectedKeyspaceName string
		gcConfig              string
	}{
		{
			name:                  "classic_default_keyspace",
			nextGenFlag:           classic,
			protectedKeyspaceID:   constant.DefaultKeyspaceID,
			protectedKeyspaceName: constant.DefaultKeyspaceName,
			gcConfig:              "",
		},
		{
			name:                  "nextgen_system_keyspace",
			nextGenFlag:           nextGen,
			protectedKeyspaceID:   constant.SystemKeyspaceID,
			protectedKeyspaceName: constant.SystemKeyspaceName,
			gcConfig:              keyspace.KeyspaceLevelGC,
		},
	}

	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/versioninfo/kerneltype/mockNextGenBuildFlag"))
	}()
	for _, c := range cases {
		t.Run(c.name, func(_ *testing.T) {
			re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/versioninfo/kerneltype/mockNextGenBuildFlag", c.nextGenFlag))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cluster, err := tests.NewTestCluster(ctx, 3, func(conf *config.Config, _ string) {
				conf.Keyspace.WaitRegionSplit = false
			})
			re.NoError(err)
			defer cluster.Destroy()
			re.NoError(cluster.RunInitialServers())
			re.NotEmpty(cluster.WaitLeader())
			server := cluster.GetLeaderServer()
			re.NoError(server.BootstrapCluster())
			manager := server.GetKeyspaceManager()
			// Load keyspace.
			meta, err := manager.LoadKeyspace(c.protectedKeyspaceName)
			re.NoError(err)
			re.Equal(c.protectedKeyspaceID, meta.GetId())
			// Check gc config.
			gcConfig := meta.Config[keyspace.GCManagementType]
			re.Equal(c.gcConfig, gcConfig)

			// Update keyspace.
			// Changing state of keyspace is not allowed.
			newTime := time.Now().Unix()
			_, err = manager.UpdateKeyspaceState(c.protectedKeyspaceName, keyspacepb.KeyspaceState_DISABLED, newTime)
			re.Error(err)
			// Changing config of keyspace is allowed.
			mutations := makeMutations()
			_, err = manager.UpdateKeyspaceConfig(c.protectedKeyspaceName, mutations)
			re.NoError(err)
		})
	}
}

// TestKeyspaceRegionSplit tests the full flow of keyspace boundary detection.
func (suite *keyspaceTestSuite) TestKeyspaceRegionSplit() {
	re := suite.Require()
	manager := suite.manager

	// Test ExtractKeyspaceID function
	testCases := []struct {
		name     string
		key      []byte
		expected uint32
		kt       keyspace.KeyType
	}{
		{"keyspace 1 txn", keyspace.MakeRegionBound(1).TxnLeftBound, 1, keyspace.KeyTypeTxn},
		{"keyspace 2 txn", keyspace.MakeRegionBound(2).TxnLeftBound, 2, keyspace.KeyTypeTxn},
		{"keyspace 100 txn", keyspace.MakeRegionBound(100).TxnLeftBound, 100, keyspace.KeyTypeTxn},
		{"empty key", []byte{}, constant.MaxValidKeyspaceID, keyspace.KeyTypeTxn},
		{"short key", []byte{'t', 0}, 0, keyspace.KeyTypeUnknown},
	}

	for _, tc := range testCases {
		id, kt := keyspace.ExtractKeyspaceID(tc.key)
		re.Equal(tc.kt, kt, "test case: %s", tc.name)
		if kt != keyspace.KeyTypeUnknown {
			re.Equal(tc.expected, id, "test case: %s", tc.name)
		}
	}

	// Test RegionSpansMultipleKeyspaces function
	spanTestCases := []struct {
		name       string
		startKey   []byte
		endKey     []byte
		shouldSpan bool
	}{
		{
			"same keyspace",
			keyspace.MakeRegionBound(1).TxnLeftBound,
			keyspace.MakeRegionBound(1).TxnRightBound,
			false,
		},
		{
			"at boundary (should not span)",
			keyspace.MakeRegionBound(1).TxnLeftBound,
			keyspace.MakeRegionBound(2).TxnLeftBound, // Exactly at the right bound
			false,
		},
		{
			"spans two keyspaces",
			keyspace.MakeRegionBound(1).TxnLeftBound,
			keyspace.MakeRegionBound(2).TxnRightBound,
			true,
		},
		{
			"spans multiple keyspaces",
			keyspace.MakeRegionBound(1).TxnLeftBound,
			keyspace.MakeRegionBound(5).TxnRightBound,
			true,
		},
	}

	for _, tc := range spanTestCases {
		spans := keyspace.RegionSpansMultipleKeyspaces(tc.startKey, tc.endKey, manager)
		re.Equal(tc.shouldSpan, spans, "test case: %s", tc.name)
	}

	// Test GetKeyspaceSplitKeys function
	// For a region spanning keyspaces 1-3, it should generate split keys at boundaries 2 and 3
	startKey := keyspace.MakeRegionBound(1).TxnLeftBound
	endKey := keyspace.MakeRegionBound(3).TxnRightBound
	splitKeys := keyspace.GetKeyspaceSplitKeys(startKey, endKey, manager)
	re.NotNil(splitKeys)
	re.GreaterOrEqual(len(splitKeys), 1, "Should generate at least one split key")

	// Verify first split key is at keyspace 2 boundary
	expectedBound2 := keyspace.MakeRegionBound(2)
	re.Equal(expectedBound2.TxnLeftBound, splitKeys[0], "First split key should be at keyspace 2 boundary")

	// If there are two split keys, the second should be at keyspace 3 boundary
	if len(splitKeys) >= 2 {
		expectedBound3 := keyspace.MakeRegionBound(3)
		re.Equal(expectedBound3.TxnLeftBound, splitKeys[1], "Second split key should be at keyspace 3 boundary")
	}
}
