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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/keyspacepb"

	coreconstant "github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(testutil.WaitForEtcdConnections(m), testutil.LeakOptions...)
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

func TestKeyspaceCacheEpochAfterLeaderReturns(t *testing.T) {
	re := require.New(t)
	tc, err := tests.NewTestCluster(t.Context(), 2, func(cfg *config.Config, _ string) {
		cfg.Keyspace.WaitRegionSplit = false
	})
	re.NoError(err)
	t.Cleanup(tc.Destroy)
	re.NoError(tc.RunInitialServers())
	oldLeader := tc.WaitLeader()
	re.NotEmpty(oldLeader)
	oldServer := tc.GetLeaderServer()
	re.NoError(oldServer.BootstrapCluster())
	manager := oldServer.GetKeyspaceManager()
	id := uint32(101)
	_, err = manager.CreateKeyspaceByID(&keyspace.CreateKeyspaceByIDRequest{ID: &id, Name: "epoch-test", CreateTime: time.Now().Unix()})
	re.NoError(err)
	_, err = manager.UpdateKeyspaceStateByID(id, keyspacepb.KeyspaceState_DISABLED, time.Now().Unix())
	re.NoError(err)

	reached, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var paused atomic.Bool
	var releaseOnce sync.Once
	fp := "github.com/tikv/pd/pkg/keyspace/updateKeyspaceStateByIDBeforeCacheWrite"
	re.NoError(failpoint.EnableCall(fp, func() {
		// Only pause the old leader's request, not updates on its successor.
		if paused.CompareAndSwap(false, true) {
			close(reached)
			<-release
		}
	}))
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		<-done
		re.NoError(failpoint.Disable(fp))
	})
	var writeErr error
	go func() {
		defer close(done)
		_, writeErr = manager.UpdateKeyspaceStateByID(id, keyspacepb.KeyspaceState_ARCHIVED, time.Now().Unix())
	}()
	<-reached
	re.NoError(oldServer.ResignLeader())
	newLeader := tc.WaitLeaderChange(oldLeader)
	re.NotEmpty(newLeader)
	newServer := tc.GetLeaderServer()
	_, err = newServer.GetKeyspaceManager().UpdateKeyspaceStateByID(id, keyspacepb.KeyspaceState_TOMBSTONE, time.Now().Unix())
	re.NoError(err)
	re.NoError(newServer.ResignLeader())
	re.Equal(oldLeader, tc.WaitLeaderChange(newLeader))
	// Wait until the real Start hook has activated the new epoch and backfilled
	// its cache before allowing the old request to resume.
	testutil.Eventually(re, func() bool {
		ids, err := manager.GetKeyspaceIDInRange(id, id, 1)
		return err == nil && len(ids) == 0
	})
	releaseOnce.Do(func() { close(release) })
	<-done
	re.NoError(writeErr)
	re.False(manager.KeyspaceExist(id), "the old request must not resurrect the tombstoned keyspace")
	ids, err := manager.GetKeyspaceIDInRange(id, id, 1)
	re.NoError(err)
	re.Empty(ids)
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
		wantOK   bool
		wantMode string
	}{
		{"keyspace 1 txn", keyspace.MakeRegionBound(1).TxnLeftBound, 1, true, "txn"},
		{"keyspace 2 txn", keyspace.MakeRegionBound(2).TxnLeftBound, 2, true, "txn"},
		{"keyspace 100 txn", keyspace.MakeRegionBound(100).TxnLeftBound, 100, true, "txn"},
		{"empty key", []byte{}, constant.MaxValidKeyspaceID, true, "txn"},
		{"short key", []byte{'t', 0}, 0, false, ""},
	}

	for _, tc := range testCases {
		id, bound, ok := keyspace.ExtractKeyspaceID(tc.key)
		re.Equal(tc.wantOK, ok, "test case: %s", tc.name)
		if ok {
			re.Equal(tc.expected, id, "test case: %s", tc.name)
			re.Equal(tc.wantMode, bound.String(), "test case: %s", tc.name)
		}
	}

	// Test RegionSpansMultipleKeyspaces function
	// Bootstrap pre-allocation is asynchronous. Wait for every required ID and
	// a complete metadata lookup before asserting deterministic boundaries.
	testutil.Eventually(re, func() bool {
		ids, err := manager.GetKeyspaceIDInRange(1, 3, 3)
		return err == nil && len(ids) == 3
	})
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
		spans, err := keyspace.RegionSpansMultipleKeyspaces(tc.startKey, tc.endKey, manager)
		re.NoError(err)
		re.Equal(tc.shouldSpan, spans, "test case: %s", tc.name)
	}

	// Test GetKeyspaceSplitKeys function
	// For a region spanning keyspaces 1-3, it should generate split keys at boundaries 2 and 3
	testutil.Eventually(re, func() bool {
		ids, err := suite.server.GetKeyspaceManager().GetKeyspaceIDInRange(1, 3, 1)
		return err == nil && len(ids) > 0
	})

	startKey := keyspace.MakeRegionBound(1).TxnLeftBound
	endKey := keyspace.MakeRegionBound(3).TxnRightBound
	splitKeys, err := keyspace.GetKeyspaceSplitKeys(startKey, endKey, coreconstant.Txn, manager)
	re.NoError(err)
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
