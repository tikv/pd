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

package tso

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/pkg/election"
	mcsutils "github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/member"
	"github.com/tikv/pd/pkg/storage/endpoint"
	tsopkg "github.com/tikv/pd/pkg/tso"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/tsoutil"
	"github.com/tikv/pd/server/apiv2/handlers"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/integrations/mcs"
	handlersutil "github.com/tikv/pd/tests/server/apiv2/handlers"
)

type tsoKeyspaceGroupManagerTestSuite struct {
	suite.Suite

	ctx    context.Context
	cancel context.CancelFunc

	// The PD cluster.
	cluster *tests.TestCluster
	// pdLeaderServer is the leader server of the PD cluster.
	pdLeaderServer *tests.TestServer
	// tsoCluster is the TSO service cluster.
	tsoCluster *mcs.TestTSOCluster
}

func TestTSOKeyspaceGroupManager(t *testing.T) {
	suite.Run(t, &tsoKeyspaceGroupManagerTestSuite{})
}

func (suite *tsoKeyspaceGroupManagerTestSuite) SetupSuite() {
	re := suite.Require()

	var err error
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.cluster, err = tests.NewTestAPICluster(suite.ctx, 1)
	re.NoError(err)
	err = suite.cluster.RunInitialServers()
	re.NoError(err)
	leaderName := suite.cluster.WaitLeader()
	suite.pdLeaderServer = suite.cluster.GetServer(leaderName)
	re.NoError(suite.pdLeaderServer.BootstrapCluster())
	suite.tsoCluster, err = mcs.NewTestTSOCluster(suite.ctx, 2, suite.pdLeaderServer.GetAddr())
	re.NoError(err)
}

func (suite *tsoKeyspaceGroupManagerTestSuite) TearDownSuite() {
	suite.cancel()
	suite.tsoCluster.Destroy()
	suite.cluster.Destroy()
}

func (suite *tsoKeyspaceGroupManagerTestSuite) TearDownTest() {
	cleanupKeyspaceGroups(suite.Require(), suite.pdLeaderServer)
}

func cleanupKeyspaceGroups(re *require.Assertions, server *tests.TestServer) {
	keyspaceGroups := handlersutil.MustLoadKeyspaceGroups(re, server, "0", "0")
	for _, group := range keyspaceGroups {
		// Do not delete default keyspace group.
		if group.ID == mcsutils.DefaultKeyspaceGroupID {
			continue
		}
		handlersutil.MustDeleteKeyspaceGroup(re, server, group.ID)
	}
}

func (suite *tsoKeyspaceGroupManagerTestSuite) TestKeyspacesServedByDefaultKeyspaceGroup() {
	// There is only default keyspace group. Any keyspace, which hasn't been assigned to
	// a keyspace group before, will be served by the default keyspace group.
	re := suite.Require()
	testutil.Eventually(re, func() bool {
		for _, keyspaceID := range []uint32{0, 1, 2} {
			served := false
			for _, server := range suite.tsoCluster.GetServers() {
				if server.IsKeyspaceServing(keyspaceID, mcsutils.DefaultKeyspaceGroupID) {
					tam, err := server.GetTSOAllocatorManager(mcsutils.DefaultKeyspaceGroupID)
					re.NoError(err)
					re.NotNil(tam)
					served = true
					break
				}
			}
			if !served {
				return false
			}
		}
		return true
	}, testutil.WithWaitFor(5*time.Second), testutil.WithTickInterval(50*time.Millisecond))

	// Any keyspace that was assigned to a keyspace group before, except default keyspace,
	// won't be served at this time. Default keyspace will be served by default keyspace group
	// all the time.
	for _, server := range suite.tsoCluster.GetServers() {
		server.IsKeyspaceServing(mcsutils.DefaultKeyspaceID, mcsutils.DefaultKeyspaceGroupID)
		for _, keyspaceGroupID := range []uint32{1, 2, 3} {
			server.IsKeyspaceServing(mcsutils.DefaultKeyspaceID, keyspaceGroupID)
			server.IsKeyspaceServing(mcsutils.DefaultKeyspaceID, keyspaceGroupID)
			for _, keyspaceID := range []uint32{1, 2, 3} {
				if server.IsKeyspaceServing(keyspaceID, keyspaceGroupID) {
					tam, err := server.GetTSOAllocatorManager(keyspaceGroupID)
					re.NoError(err)
					re.NotNil(tam)
				}
			}
		}
	}

	// Create a client for each keyspace and make sure they can successfully discover the service
	// provided by the default keyspace group.
	keyspaceIDs := []uint32{0, 1, 2, 3, 1000}
	clients := mcs.WaitForMultiKeyspacesTSOAvailable(
		suite.ctx, re, keyspaceIDs, []string{suite.pdLeaderServer.GetAddr()})
	re.Equal(len(keyspaceIDs), len(clients))
	mcs.CheckMultiKeyspacesTSO(suite.ctx, re, clients, func() {
		time.Sleep(3 * time.Second)
	})
}

func (suite *tsoKeyspaceGroupManagerTestSuite) TestKeyspacesServedByNonDefaultKeyspaceGroups() {
	// Create multiple keyspace groups, and every keyspace should be served by one of them
	// on a tso server.
	re := suite.Require()

	// Create keyspace groups.
	params := []struct {
		keyspaceGroupID uint32
		keyspaceIDs     []uint32
	}{
		{0, []uint32{0, 10}},
		{1, []uint32{1, 11}},
		{2, []uint32{2, 12}},
	}

	for _, param := range params {
		if param.keyspaceGroupID == 0 {
			// we have already created default keyspace group, so we can skip it.
			// keyspace 10 isn't assigned to any keyspace group, so they will be
			// served by default keyspace group.
			continue
		}
		handlersutil.MustCreateKeyspaceGroup(re, suite.pdLeaderServer, &handlers.CreateKeyspaceGroupParams{
			KeyspaceGroups: []*endpoint.KeyspaceGroup{
				{
					ID:        param.keyspaceGroupID,
					UserKind:  endpoint.Standard.String(),
					Members:   suite.tsoCluster.GetKeyspaceGroupMember(),
					Keyspaces: param.keyspaceIDs,
				},
			},
		})
	}

	// Wait until all keyspace groups are ready.
	testutil.Eventually(re, func() bool {
		for _, param := range params {
			for _, keyspaceID := range param.keyspaceIDs {
				served := false
				for _, server := range suite.tsoCluster.GetServers() {
					if server.IsKeyspaceServing(keyspaceID, param.keyspaceGroupID) {
						am, err := server.GetTSOAllocatorManager(param.keyspaceGroupID)
						re.NoError(err)
						re.NotNil(am)

						// Make sure every keyspace group is using the right timestamp path
						// for loading/saving timestamp from/to etcd and the right primary path
						// for primary election.
						var (
							timestampPath string
							primaryPath   string
						)
						clusterID := strconv.FormatUint(suite.pdLeaderServer.GetClusterID(), 10)
						if param.keyspaceGroupID == mcsutils.DefaultKeyspaceGroupID {
							timestampPath = fmt.Sprintf("/pd/%s/timestamp", clusterID)
							primaryPath = fmt.Sprintf("/ms/%s/tso/00000/primary", clusterID)
						} else {
							timestampPath = fmt.Sprintf("/ms/%s/tso/%05d/gta/timestamp",
								clusterID, param.keyspaceGroupID)
							primaryPath = fmt.Sprintf("/ms/%s/tso/%s/election/%05d/primary",
								clusterID, mcsutils.KeyspaceGroupsKey, param.keyspaceGroupID)
						}
						re.Equal(timestampPath, am.GetTimestampPath(tsopkg.GlobalDCLocation))
						re.Equal(primaryPath, am.GetMember().GetLeaderPath())

						served = true
					}
				}
				if !served {
					return false
				}
			}
		}
		return true
	}, testutil.WithWaitFor(5*time.Second), testutil.WithTickInterval(50*time.Millisecond))

	// Create a client for each keyspace and make sure they can successfully discover the service
	// provided by the corresponding keyspace group.
	keyspaceIDs := make([]uint32, 0)
	for _, param := range params {
		keyspaceIDs = append(keyspaceIDs, param.keyspaceIDs...)
	}

	clients := mcs.WaitForMultiKeyspacesTSOAvailable(
		suite.ctx, re, keyspaceIDs, []string{suite.pdLeaderServer.GetAddr()})
	re.Equal(len(keyspaceIDs), len(clients))
	mcs.CheckMultiKeyspacesTSO(suite.ctx, re, clients, func() {
		time.Sleep(3 * time.Second)
	})
}

func (suite *tsoKeyspaceGroupManagerTestSuite) TestTSOKeyspaceGroupSplit() {
	re := suite.Require()
	// Create the keyspace group 1 with keyspaces [111, 222, 333].
	handlersutil.MustCreateKeyspaceGroup(re, suite.pdLeaderServer, &handlers.CreateKeyspaceGroupParams{
		KeyspaceGroups: []*endpoint.KeyspaceGroup{
			{
				ID:        1,
				UserKind:  endpoint.Standard.String(),
				Members:   suite.tsoCluster.GetKeyspaceGroupMember(),
				Keyspaces: []uint32{111, 222, 333},
			},
		},
	})
	kg1 := handlersutil.MustLoadKeyspaceGroupByID(re, suite.pdLeaderServer, 1)
	re.Equal(uint32(1), kg1.ID)
	re.Equal([]uint32{111, 222, 333}, kg1.Keyspaces)
	re.False(kg1.IsSplitting())
	// Get a TSO from the keyspace group 1.
	var (
		ts  pdpb.Timestamp
		err error
	)
	testutil.Eventually(re, func() bool {
		ts, err = suite.requestTSO(re, 1, 222, 1)
		return err == nil && tsoutil.CompareTimestamp(&ts, &pdpb.Timestamp{}) > 0
	})
	ts.Physical += time.Hour.Milliseconds()
	// Set the TSO of the keyspace group 1 to a large value.
	err = suite.tsoCluster.GetPrimaryServer(222, 1).GetHandler().ResetTS(tsoutil.GenerateTS(&ts), false, true, 1)
	re.NoError(err)
	// Split the keyspace group 1 to 2.
	handlersutil.MustSplitKeyspaceGroup(re, suite.pdLeaderServer, 1, &handlers.SplitKeyspaceGroupByIDParams{
		NewID:     2,
		Keyspaces: []uint32{222, 333},
	})
	kg2 := handlersutil.MustLoadKeyspaceGroupByID(re, suite.pdLeaderServer, 2)
	re.Equal(uint32(2), kg2.ID)
	re.Equal([]uint32{222, 333}, kg2.Keyspaces)
	re.True(kg2.IsSplitTarget())
	// Check the split TSO from keyspace group 2.
	var splitTS pdpb.Timestamp
	testutil.Eventually(re, func() bool {
		splitTS, err = suite.requestTSO(re, 1, 222, 2)
		return err == nil && tsoutil.CompareTimestamp(&splitTS, &pdpb.Timestamp{}) > 0
	})
	splitTS, err = suite.requestTSO(re, 1, 222, 2)
	re.Greater(tsoutil.CompareTimestamp(&splitTS, &ts), 0)
}

func (suite *tsoKeyspaceGroupManagerTestSuite) requestTSO(
	re *require.Assertions,
	count, keyspaceID, keyspaceGroupID uint32,
) (pdpb.Timestamp, error) {
	primary := suite.tsoCluster.WaitForPrimaryServing(re, keyspaceID, keyspaceGroupID)
	kgm := primary.GetKeyspaceGroupManager()
	re.NotNil(kgm)
	ts, _, err := kgm.HandleTSORequest(keyspaceID, keyspaceGroupID, tsopkg.GlobalDCLocation, count)
	re.NoError(err)
	return ts, err
}

func (suite *tsoKeyspaceGroupManagerTestSuite) TestTSOKeyspaceGroupSplitElection() {
	re := suite.Require()
	// Create the keyspace group 1 with keyspaces [111, 222, 333].
	handlersutil.MustCreateKeyspaceGroup(re, suite.pdLeaderServer, &handlers.CreateKeyspaceGroupParams{
		KeyspaceGroups: []*endpoint.KeyspaceGroup{
			{
				ID:        1,
				UserKind:  endpoint.Standard.String(),
				Members:   suite.tsoCluster.GetKeyspaceGroupMember(),
				Keyspaces: []uint32{111, 222, 333},
			},
		},
	})
	kg1 := handlersutil.MustLoadKeyspaceGroupByID(re, suite.pdLeaderServer, 1)
	re.Equal(uint32(1), kg1.ID)
	re.Equal([]uint32{111, 222, 333}, kg1.Keyspaces)
	re.False(kg1.IsSplitting())
	// Split the keyspace group 1 to 2.
	handlersutil.MustSplitKeyspaceGroup(re, suite.pdLeaderServer, 1, &handlers.SplitKeyspaceGroupByIDParams{
		NewID:     2,
		Keyspaces: []uint32{222, 333},
	})
	kg2 := handlersutil.MustLoadKeyspaceGroupByID(re, suite.pdLeaderServer, 2)
	re.Equal(uint32(2), kg2.ID)
	re.Equal([]uint32{222, 333}, kg2.Keyspaces)
	re.True(kg2.IsSplitTarget())
	// Check the leadership.
	member1, err := suite.tsoCluster.WaitForPrimaryServing(re, 111, 1).GetMember(111, 1)
	re.NoError(err)
	re.NotNil(member1)
	member2, err := suite.tsoCluster.WaitForPrimaryServing(re, 222, 2).GetMember(222, 2)
	re.NoError(err)
	re.NotNil(member2)
	// Wait for the leader of the keyspace group 1 and 2 to be elected.
	testutil.Eventually(re, func() bool {
		return len(member1.GetLeaderListenUrls()) > 0 && len(member2.GetLeaderListenUrls()) > 0
	})
	// Check if the leader of the keyspace group 1 and 2 are the same.
	re.Equal(member1.GetLeaderListenUrls(), member2.GetLeaderListenUrls())
	// Resign and block the leader of the keyspace group 1 from being elected.
	member1.(*member.Participant).SetCampaignChecker(func(*election.Leadership) bool {
		return false
	})
	member1.ResetLeader()
	// The leader of the keyspace group 2 should be resigned also.
	testutil.Eventually(re, func() bool {
		return member2.IsLeader() == false
	})
	// Check if the leader of the keyspace group 1 and 2 are the same again.
	member1.(*member.Participant).SetCampaignChecker(nil)
	testutil.Eventually(re, func() bool {
		return len(member1.GetLeaderListenUrls()) > 0 && len(member2.GetLeaderListenUrls()) > 0
	})
	re.Equal(member1.GetLeaderListenUrls(), member2.GetLeaderListenUrls())
	// Finish the split.
	handlersutil.MustFinishSplitKeyspaceGroup(re, suite.pdLeaderServer, 2)
}

func (suite *tsoKeyspaceGroupManagerTestSuite) TestTSOKeyspaceGroupSplitClient() {
	re := suite.Require()
	// Create the keyspace group 1 with keyspaces [111, 222, 333].
	handlersutil.MustCreateKeyspaceGroup(re, suite.pdLeaderServer, &handlers.CreateKeyspaceGroupParams{
		KeyspaceGroups: []*endpoint.KeyspaceGroup{
			{
				ID:        1,
				UserKind:  endpoint.Standard.String(),
				Members:   suite.tsoCluster.GetKeyspaceGroupMember(),
				Keyspaces: []uint32{111, 222, 333},
			},
		},
	})
	kg1 := handlersutil.MustLoadKeyspaceGroupByID(re, suite.pdLeaderServer, 1)
	re.Equal(uint32(1), kg1.ID)
	re.Equal([]uint32{111, 222, 333}, kg1.Keyspaces)
	re.False(kg1.IsSplitting())
	// Prepare the client for keyspace 222.
	var tsoClient pd.TSOClient
	tsoClient, err := pd.NewClientWithKeyspace(suite.ctx, 222, []string{suite.pdLeaderServer.GetAddr()}, pd.SecurityOption{})
	re.NoError(err)
	re.NotNil(tsoClient)
	// Request the TSO for keyspace 222 concurrently.
	var (
		wg                        sync.WaitGroup
		ctx, cancel               = context.WithCancel(suite.ctx)
		lastPhysical, lastLogical int64
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				// Make sure at least one TSO request is successful.
				re.NotEmpty(lastPhysical)
				return
			default:
			}
			physical, logical, err := tsoClient.GetTS(ctx)
			if err != nil {
				errMsg := err.Error()
				// Ignore the errors caused by the split and context cancellation.
				if strings.Contains(errMsg, "context canceled") ||
					strings.Contains(errMsg, "not leader") ||
					strings.Contains(errMsg, "not served") ||
					strings.Contains(errMsg, "ErrKeyspaceNotAssigned") {
					continue
				}
				re.FailNow(errMsg)
			}
			if physical == lastPhysical {
				re.Greater(logical, lastLogical)
			} else {
				re.Greater(physical, lastPhysical)
			}
			lastPhysical, lastLogical = physical, logical
		}
	}()
	// Split the keyspace group 1 to 2.
	handlersutil.MustSplitKeyspaceGroup(re, suite.pdLeaderServer, 1, &handlers.SplitKeyspaceGroupByIDParams{
		NewID:     2,
		Keyspaces: []uint32{222, 333},
	})
	kg2 := handlersutil.MustLoadKeyspaceGroupByID(re, suite.pdLeaderServer, 2)
	re.Equal(uint32(2), kg2.ID)
	re.Equal([]uint32{222, 333}, kg2.Keyspaces)
	re.True(kg2.IsSplitTarget())
	// Finish the split.
	handlersutil.MustFinishSplitKeyspaceGroup(re, suite.pdLeaderServer, 2)
	// Wait for a while to make sure the client has received the new TSO.
	time.Sleep(time.Second)
	// Stop the client.
	cancel()
	wg.Wait()
}

func (suite *tsoKeyspaceGroupManagerTestSuite) TestTSOKeyspaceGroupMembers() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion", "return(true)"))
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/acceleratedAllocNodes", `return(true)`))
	kg := handlersutil.MustLoadKeyspaceGroupByID(re, suite.pdLeaderServer, 0)
	re.Equal(uint32(0), kg.ID)
	re.Equal([]uint32{0}, kg.Keyspaces)
	re.False(kg.IsSplitting())
	// wait for finishing alloc nodes
	testutil.Eventually(re, func() bool {
		kg = handlersutil.MustLoadKeyspaceGroupByID(re, suite.pdLeaderServer, 0)
		return len(kg.Members) == 2
	})
	testConfig := map[string]string{
		"config":                "1",
		"tso_keyspace_group_id": "0",
		"user_kind":             "basic",
	}
	handlersutil.MustCreateKeyspace(re, suite.pdLeaderServer, &handlers.CreateKeyspaceParams{
		Name:   "test_keyspace",
		Config: testConfig,
	})
	kg = handlersutil.MustLoadKeyspaceGroupByID(re, suite.pdLeaderServer, 0)
	testutil.Eventually(re, func() bool {
		kg = handlersutil.MustLoadKeyspaceGroupByID(re, suite.pdLeaderServer, 0)
		return len(kg.Members) == 2
	})
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion"))
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/acceleratedAllocNodes"))
}

func TestTwiceSplitKeyspaceGroup(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/acceleratedAllocNodes", `return(true)`))

	// Init api server config but not start.
	tc, err := tests.NewTestAPICluster(ctx, 1, func(conf *config.Config, serverName string) {
		conf.Keyspace.PreAlloc = []string{
			"keyspace_a", "keyspace_b",
		}
	})
	re.NoError(err)
	pdAddr := tc.GetConfig().GetClientURL()

	// Start pd client and wait pd server start.
	var clients sync.Map
	go func() {
		apiCtx := pd.NewAPIContextV2("keyspace_b") // its keyspace id is 2.
		cli, err := pd.NewClientWithAPIContext(ctx, apiCtx, []string{pdAddr}, pd.SecurityOption{})
		re.NoError(err)
		clients.Store("keyspace_b", cli)
	}()
	go func() {
		apiCtx := pd.NewAPIContextV2("keyspace_a") // its keyspace id is 1.
		cli, err := pd.NewClientWithAPIContext(ctx, apiCtx, []string{pdAddr}, pd.SecurityOption{})
		re.NoError(err)
		clients.Store("keyspace_a", cli)
	}()

	// Start api server and tso server.
	err = tc.RunInitialServers()
	re.NoError(err)
	defer tc.Destroy()
	tc.WaitLeader()
	leaderServer := tc.GetServer(tc.GetLeader())
	re.NoError(leaderServer.BootstrapCluster())

	tsoCluster, err := mcs.NewTestTSOCluster(ctx, 2, pdAddr)
	re.NoError(err)
	defer tsoCluster.Destroy()
	tsoCluster.WaitForDefaultPrimaryServing(re)

	// Wait pd clients are ready.
	testutil.Eventually(re, func() bool {
		count := 0
		clients.Range(func(key, value interface{}) bool {
			count++
			return true
		})
		return count == 2
	})
	clientA, ok := clients.Load("keyspace_a")
	re.True(ok)
	clientB, ok := clients.Load("keyspace_b")
	re.True(ok)

	// First split keyspace group 0 to 1 with keyspace 2.
	kgm := leaderServer.GetServer().GetKeyspaceGroupManager()
	re.NotNil(kgm)
	testutil.Eventually(re, func() bool {
		err = kgm.SplitKeyspaceGroupByID(0, 1, []uint32{2})
		return err == nil
	})

	// Trigger checkTSOSplit to ensure the split is finished.
	testutil.Eventually(re, func() bool {
		_, _, err = clientB.(pd.Client).GetTS(ctx)
		re.NoError(err)
		kg := handlersutil.MustLoadKeyspaceGroupByID(re, leaderServer, 0)
		return !kg.IsSplitting()
	})
	clientB.(pd.Client).Close()

	// Then split keyspace group 0 to 2 with keyspace 1.
	testutil.Eventually(re, func() bool {
		err = kgm.SplitKeyspaceGroupByID(0, 2, []uint32{1})
		return err == nil
	})

	// Trigger checkTSOSplit to ensure the split is finished.
	testutil.Eventually(re, func() bool {
		_, _, err = clientA.(pd.Client).GetTS(ctx)
		re.NoError(err)
		kg := handlersutil.MustLoadKeyspaceGroupByID(re, leaderServer, 0)
		return !kg.IsSplitting()
	})
	clientA.(pd.Client).Close()

	// Check the keyspace group 0 is split to 1 and 2.
	kg0 := handlersutil.MustLoadKeyspaceGroupByID(re, leaderServer, 0)
	kg1 := handlersutil.MustLoadKeyspaceGroupByID(re, leaderServer, 1)
	kg2 := handlersutil.MustLoadKeyspaceGroupByID(re, leaderServer, 2)
	re.Equal([]uint32{0}, kg0.Keyspaces)
	re.Equal([]uint32{2}, kg1.Keyspaces)
	re.Equal([]uint32{1}, kg2.Keyspaces)
	re.False(kg0.IsSplitting())
	re.False(kg1.IsSplitting())
	re.False(kg2.IsSplitting())

	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/acceleratedAllocNodes"))
}
