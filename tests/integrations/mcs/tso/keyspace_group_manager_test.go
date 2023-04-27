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
	"sync"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/testutil"
	"github.com/tikv/pd/pkg/election"
	mcsutils "github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/member"
	"github.com/tikv/pd/pkg/storage/endpoint"
	tsopkg "github.com/tikv/pd/pkg/tso"
	"github.com/tikv/pd/pkg/utils/tsoutil"
	"github.com/tikv/pd/server/apiv2/handlers"
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
	for _, group := range handlersutil.MustLoadKeyspaceGroups(re, server, "0", "0") {
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
		for _, server := range suite.tsoCluster.GetServers() {
			allServed := true
			for _, keyspaceID := range []uint32{0, 1, 2} {
				if server.IsKeyspaceServing(keyspaceID, mcsutils.DefaultKeyspaceGroupID) {
					tam, err := server.GetTSOAllocatorManager(mcsutils.DefaultKeyspaceGroupID)
					re.NoError(err)
					re.NotNil(tam)
				} else {
					allServed = false
				}
			}
			return allServed
		}
		return false
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
	err = suite.tsoCluster.GetPrimary(222, 1).GetHandler().ResetTS(tsoutil.GenerateTS(&ts), false, true, 1)
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
	re.Greater(tsoutil.CompareTimestamp(&splitTS, &ts), 0)
	// Finish the split.
	handlersutil.MustFinishSplitKeyspaceGroup(re, suite.pdLeaderServer, 2)
}

func (suite *tsoKeyspaceGroupManagerTestSuite) requestTSO(
	re *require.Assertions,
	count, keyspaceID, keyspaceGroupID uint32,
) (pdpb.Timestamp, error) {
	primary := suite.tsoCluster.WaitForPrimaryServing(re, keyspaceID, keyspaceGroupID)
	tam, err := primary.GetTSOAllocatorManager(keyspaceGroupID)
	re.NoError(err)
	re.NotNil(tam)
	return tam.HandleRequest(tsopkg.GlobalDCLocation, count)
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
	// TODO: remove the skip after the client is able to support multi-keyspace-group.
	suite.T().SkipNow()

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
				return
			default:
			}
			physical, logical, err := tsoClient.GetTS(ctx)
			re.NoError(err)
			re.Greater(physical, lastPhysical)
			re.Greater(logical, lastLogical)
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
	cancel()
	wg.Wait()
}
