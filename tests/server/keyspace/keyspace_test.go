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
	"encoding/hex"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/keyspacepb"

	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage/endpoint"
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

func (suite *keyspaceTestSuite) TestRegionLabeler() {
	re := suite.Require()
	regionLabeler := suite.server.GetRaftCluster().GetRegionLabeler()

	// Create test keyspaces.
	count := 20
	now := time.Now().Unix()
	keyspaces := make([]*keyspacepb.KeyspaceMeta, count)
	manager := suite.manager
	var err error
	for i := range count {
		keyspaces[i], err = manager.CreateKeyspace(&keyspace.CreateKeyspaceRequest{
			Name:       fmt.Sprintf("test_keyspace_%d", i),
			CreateTime: now,
		})
		re.NoError(err)
	}
	// Check for region labels.
	for _, meta := range keyspaces {
		checkLabelRule(re, meta.GetId(), regionLabeler)
	}
}

func checkLabelRule(re *require.Assertions, id uint32, regionLabeler *labeler.RegionLabeler) {
	labelID := "keyspaces/" + strconv.FormatUint(uint64(id), endpoint.SpaceIDBase)
	loadedLabel := regionLabeler.GetLabelRule(labelID)
	re.NotNil(loadedLabel)

	rangeRule, ok := loadedLabel.Data.([]*labeler.KeyRangeRule)
	re.True(ok)
	re.Len(rangeRule, 2)

	bound := keyspace.MakeRegionBound(id)

	re.Equal(hex.EncodeToString(bound.RawLeftBound), rangeRule[0].StartKeyHex)
	re.Equal(hex.EncodeToString(bound.RawRightBound), rangeRule[0].EndKeyHex)
	re.Equal(hex.EncodeToString(bound.TxnLeftBound), rangeRule[1].StartKeyHex)
	re.Equal(hex.EncodeToString(bound.TxnRightBound), rangeRule[1].EndKeyHex)
}

func (suite *keyspaceTestSuite) TestPreAlloc() {
	re := suite.Require()
	regionLabeler := suite.server.GetRaftCluster().GetRegionLabeler()
	for _, keyspaceName := range preAllocKeyspace {
		// Check pre-allocated keyspaces are correctly allocated.
		meta, err := suite.manager.LoadKeyspace(keyspaceName)
		re.NoError(err)
		// Check pre-allocated keyspaces also have the correct region label.
		checkLabelRule(re, meta.GetId(), regionLabeler)
	}
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
ok       bool
}{
{"keyspace 1 txn", []byte{'x', 0, 0, 1, 0}, 1, true},
{"keyspace 2 txn", []byte{'x', 0, 0, 2, 100}, 2, true},
{"keyspace 100 txn", []byte{'x', 0, 0, 100, 50}, 100, true},
{"empty key", []byte{}, 0, false},
{"short key", []byte{'x', 0}, 0, false},
}

for _, tc := range testCases {
id, ok := keyspace.ExtractKeyspaceID(tc.key)
re.Equal(tc.ok, ok, "test case: %s", tc.name)
if ok {
re.Equal(tc.expected, id, "test case: %s", tc.name)
}
}

// Test RegionSpansMultipleKeyspaces function
spanTestCases := []struct {
name      string
startKey  []byte
endKey    []byte
shouldSpan bool
}{
{
"same keyspace",
[]byte{'x', 0, 0, 1, 0},
[]byte{'x', 0, 0, 1, 100},
false,
},
{
"at boundary (should not span)",
[]byte{'x', 0, 0, 1, 0},
[]byte{'x', 0, 0, 2},  // Exactly at the right bound
false,
},
{
"spans two keyspaces",
[]byte{'x', 0, 0, 1, 0},
[]byte{'x', 0, 0, 2, 100},
true,
},
{
"spans multiple keyspaces",
[]byte{'x', 0, 0, 1, 0},
[]byte{'x', 0, 0, 5, 0},
true,
},
}

for _, tc := range spanTestCases {
spans := keyspace.RegionSpansMultipleKeyspaces(tc.startKey, tc.endKey, manager)
re.Equal(tc.shouldSpan, spans, "test case: %s", tc.name)
}

// Test GetKeyspaceSplitKeys function
// For a region spanning keyspaces 1-3, it should generate split keys at boundaries 2 and 3
startKey := []byte{'x', 0, 0, 1, 0}
endKey := []byte{'x', 0, 0, 3, 100}
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
