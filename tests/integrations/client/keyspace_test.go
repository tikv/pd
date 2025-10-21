// Copyright 2021 TiKV Project Authors.
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

package client_test

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/keyspacepb"

	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/pkg/caller"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/pkg/versioninfo/kerneltype"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

const (
	testConfig1       = "config_entry_1"
	testConfig2       = "config_entry_2"
	testKeyspaceCount = 10
)

func mustMakeTestKeyspaces(re *require.Assertions, server *server.Server, start int) []*keyspacepb.KeyspaceMeta {
	now := time.Now().Unix()
	var err error
	keyspaces := make([]*keyspacepb.KeyspaceMeta, testKeyspaceCount)
	manager := server.GetKeyspaceManager()
	for i := range testKeyspaceCount {
		keyspaces[i], err = manager.CreateKeyspace(&keyspace.CreateKeyspaceRequest{
			Name: fmt.Sprintf("test_keyspace_%d", start+i),
			Config: map[string]string{
				testConfig1: "100",
				testConfig2: "200",
			},
			CreateTime: now,
		})
		re.NoError(err)
	}
	return keyspaces
}

func (suite *clientStatelessTestSuite) TestLoadKeyspace() {
	re := suite.Require()
	metas := mustMakeTestKeyspaces(re, suite.srv, 0)
	for _, expected := range metas {
		loaded, err := suite.client.LoadKeyspace(suite.ctx, expected.GetName())
		re.NoError(err)
		re.Equal(expected, loaded)
	}
	// Loading non-existing keyspace should result in error.
	_, err := suite.client.LoadKeyspace(suite.ctx, "non-existing keyspace")
	re.Error(err)
	// Loading bootstrap keyspace should be successful.
	bootstrapKeyspaceName := keyspace.GetBootstrapKeyspaceName()
	bootstrapKeyspaceID := keyspace.GetBootstrapKeyspaceID()
	keyspaceDefault, err := suite.client.LoadKeyspace(suite.ctx, bootstrapKeyspaceName)
	re.NoError(err)
	re.Equal(bootstrapKeyspaceID, keyspaceDefault.GetId())
	re.Equal(bootstrapKeyspaceName, keyspaceDefault.GetName())
}

func (suite *clientStatelessTestSuite) TestGetAllKeyspaces() {
	re := suite.Require()
	metas := mustMakeTestKeyspaces(re, suite.srv, 20)
	for _, expected := range metas {
		loaded, err := suite.client.LoadKeyspace(suite.ctx, expected.GetName())
		re.NoError(err)
		re.Equal(expected, loaded)
	}
	// Get all keyspaces.
	resKeyspaces, err := suite.client.GetAllKeyspaces(suite.ctx, 1, math.MaxUint32)
	re.NoError(err)
	if kerneltype.IsNextGen() {
		// In NextGen, the bootstrap keyspace is SYSTEM, not DEFAULT.
		// Should have test keyspaces + bootstrap keyspace
		re.Len(resKeyspaces, len(metas)+1)
	} else {
		// In Classic, the bootstrap keyspace is DEFAULT.
		// Should have test keyspaces
		re.Len(resKeyspaces, len(metas))
	}
	// Check expected keyspaces all in resKeyspaces.
	for _, expected := range metas {
		var isExists bool
		for _, resKeyspace := range resKeyspaces {
			if expected.GetName() == resKeyspace.GetName() {
				isExists = true
				continue
			}
		}
		if !isExists {
			re.Fail("not exists keyspace")
		}
	}
}

func mustCreateKeyspaceAtState(re *require.Assertions, server *server.Server, index int, state keyspacepb.KeyspaceState) *keyspacepb.KeyspaceMeta {
	manager := server.GetKeyspaceManager()
	meta, err := manager.CreateKeyspace(&keyspace.CreateKeyspaceRequest{
		Name:       fmt.Sprintf("test_keyspace_%d", index),
		Config:     nil,
		CreateTime: 0, // Use 0 to indicate unchanged keyspace.
	})
	re.NoError(err)
	switch state {
	case keyspacepb.KeyspaceState_ENABLED:
	case keyspacepb.KeyspaceState_DISABLED:
		meta, err = manager.UpdateKeyspaceStateByID(meta.GetId(), keyspacepb.KeyspaceState_DISABLED, 0)
		re.NoError(err)
	case keyspacepb.KeyspaceState_ARCHIVED:
		meta, err = manager.UpdateKeyspaceStateByID(meta.GetId(), keyspacepb.KeyspaceState_DISABLED, 0)
		re.NoError(err)
		meta, err = manager.UpdateKeyspaceStateByID(meta.GetId(), keyspacepb.KeyspaceState_ARCHIVED, 0)
		re.NoError(err)
	case keyspacepb.KeyspaceState_TOMBSTONE:
		meta, err = manager.UpdateKeyspaceStateByID(meta.GetId(), keyspacepb.KeyspaceState_DISABLED, 0)
		re.NoError(err)
		meta, err = manager.UpdateKeyspaceStateByID(meta.GetId(), keyspacepb.KeyspaceState_ARCHIVED, 0)
		re.NoError(err)
		meta, err = manager.UpdateKeyspaceStateByID(meta.GetId(), keyspacepb.KeyspaceState_TOMBSTONE, 0)
		re.NoError(err)
	default:
		re.Fail("unknown keyspace state")
	}
	return meta
}

func (suite *clientStatelessTestSuite) TestUpdateKeyspaceState() {
	re := suite.Require()
	allStates := []keyspacepb.KeyspaceState{
		keyspacepb.KeyspaceState_ENABLED,
		keyspacepb.KeyspaceState_DISABLED,
		keyspacepb.KeyspaceState_ARCHIVED,
		keyspacepb.KeyspaceState_TOMBSTONE,
	}
	allowedTransitions := map[keyspacepb.KeyspaceState][]keyspacepb.KeyspaceState{
		keyspacepb.KeyspaceState_ENABLED:   {keyspacepb.KeyspaceState_ENABLED, keyspacepb.KeyspaceState_DISABLED},
		keyspacepb.KeyspaceState_DISABLED:  {keyspacepb.KeyspaceState_DISABLED, keyspacepb.KeyspaceState_ENABLED, keyspacepb.KeyspaceState_ARCHIVED},
		keyspacepb.KeyspaceState_ARCHIVED:  {keyspacepb.KeyspaceState_ARCHIVED, keyspacepb.KeyspaceState_TOMBSTONE},
		keyspacepb.KeyspaceState_TOMBSTONE: {keyspacepb.KeyspaceState_TOMBSTONE},
	}
	// Use index to avoid collision with other tests.
	index := 1000
	for _, originState := range allStates {
		for _, targetState := range allStates {
			meta := mustCreateKeyspaceAtState(re, suite.srv, index, originState)
			updated, err := suite.client.UpdateKeyspaceState(suite.ctx, meta.GetId(), targetState)
			if slice.Contains(allowedTransitions[originState], targetState) {
				// If transition is allowed, then update must be successful.
				re.NoError(err)
				if originState != targetState {
					// If changing state, then must record stateChangedAt.
					re.NotEqual(updated.GetStateChangedAt(), meta.GetStateChangedAt())
				} else {
					// Otherwise the request should be idempotent.
					re.Equal(updated, meta)
				}
			} else {
				// If operation is not allowed, then update must fail, returned meta must be nil.
				re.Error(err)
				re.Nil(updated)
			}
			index++
		}
	}
}

func (s *clientStatefulTestSuite) TestIsKeyspaceUsingKeyspaceLevelGC() {
	re := s.Require()

	meta, err := s.srv.GetKeyspaceManager().CreateKeyspace(&keyspace.CreateKeyspaceRequest{
		Name:       "ks1",
		Config:     nil,
		CreateTime: time.Now().Unix(),
	})
	re.NoError(err)
	// By the time this test is writte, only for next-gen deployment we enable keyspace level GC by default.
	// Update this test when this
	re.Equal(kerneltype.IsNextGen(), pd.IsKeyspaceUsingKeyspaceLevelGC(meta))

	meta, err = s.srv.GetKeyspaceManager().CreateKeyspace(&keyspace.CreateKeyspaceRequest{
		Name: "ks2",
		Config: map[string]string{
			keyspace.GCManagementType: keyspace.KeyspaceLevelGC,
		},
		CreateTime: time.Now().Unix(),
	})
	re.NoError(err)
	re.True(pd.IsKeyspaceUsingKeyspaceLevelGC(meta))

	meta, err = s.srv.GetKeyspaceManager().CreateKeyspace(&keyspace.CreateKeyspaceRequest{
		Name: "ks3",
		Config: map[string]string{
			keyspace.GCManagementType: keyspace.UnifiedGC,
		},
		CreateTime: time.Now().Unix(),
	})
	re.NoError(err)
	re.False(pd.IsKeyspaceUsingKeyspaceLevelGC(meta))

	meta, err = s.srv.GetKeyspaceManager().CreateKeyspace(&keyspace.CreateKeyspaceRequest{
		Name: "ks4",
		Config: map[string]string{
			keyspace.GCManagementType: "",
		},
		CreateTime: time.Now().Unix(),
	})
	re.NoError(err)
	// In NextGen build, empty gc_management_type is automatically set to "keyspace_level"
	// In Classic build, empty gc_management_type remains empty and returns false
	re.Equal(kerneltype.IsNextGen(), pd.IsKeyspaceUsingKeyspaceLevelGC(meta))
}

func TestProtectedKeyspace(t *testing.T) {
	re := require.New(t)
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/versioninfo/kerneltype/mockNextGenBuildFlag"))
	}()
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
			name:                  "legacy_default_keyspace",
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

	for _, c := range cases {
		t.Run(c.name, func(_ *testing.T) {
			re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/versioninfo/kerneltype/mockNextGenBuildFlag", c.nextGenFlag))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			tc, err := tests.NewTestClusterWithKeyspaceGroup(ctx, 1, func(conf *config.Config, _ string) {
				conf.Keyspace.WaitRegionSplit = false
			})
			re.NoError(err)
			defer tc.Destroy()
			pdAddr := tc.GetConfig().GetClientURL()
			err = tc.RunInitialServers()
			re.NoError(err)
			tc.WaitLeader()
			leaderServer := tc.GetLeaderServer()
			re.NoError(leaderServer.BootstrapCluster())
			tsoCluster, err := tests.NewTestTSOCluster(ctx, 2, pdAddr)
			re.NoError(err)
			defer tsoCluster.Destroy()
			tsoCluster.WaitForDefaultPrimaryServing(re)

			// Test split keyspace
			kgm := leaderServer.GetServer().GetKeyspaceGroupManager()
			re.NotNil(kgm)
			err = kgm.SplitKeyspaceGroupByID(0, 1, []uint32{c.protectedKeyspaceID})
			re.Error(err)
			re.Contains(err.Error(), "cannot modify")

			// Test tso forward
			cli, err := pd.NewClientWithContext(ctx, caller.TestComponent,
				leaderServer.GetLeader().GetClientUrls(), pd.SecurityOption{})
			re.NoError(err)
			_, _, err = cli.GetTS(ctx)
			re.NoError(err)
		})
	}
}
