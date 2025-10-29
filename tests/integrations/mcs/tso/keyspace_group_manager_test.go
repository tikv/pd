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
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"

	pd "github.com/tikv/pd/client"
	clierrs "github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/pkg/caller"
	"github.com/tikv/pd/pkg/election"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/keyspace/constant"
	mcs "github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/member"
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/tsoutil"
	"github.com/tikv/pd/server/apiv2/handlers"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/integrations/mcs/utils"
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
	tsoCluster *tests.TestTSOCluster

	allocator *mockid.IDAllocator
}

func (suite *tsoKeyspaceGroupManagerTestSuite) allocID() uint32 {
	re := suite.Require()
	id, _, err := suite.allocator.Alloc(1)
	re.NoError(err)
	return uint32(id)
}

func TestTSOKeyspaceGroupManagerSuite(t *testing.T) {
	suite.Run(t, &tsoKeyspaceGroupManagerTestSuite{})
}

func (suite *tsoKeyspaceGroupManagerTestSuite) SetupSuite() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/fastGroupSplitPatroller", `return(true)`))

	var err error
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.cluster, err = tests.NewTestClusterWithKeyspaceGroup(suite.ctx, 1)
	re.NoError(err)
	err = suite.cluster.RunInitialServers()
	re.NoError(err)
	leaderName := suite.cluster.WaitLeader()
	re.NotEmpty(leaderName)
	suite.pdLeaderServer = suite.cluster.GetServer(leaderName)
	re.NoError(suite.pdLeaderServer.BootstrapCluster())
	suite.tsoCluster, err = tests.NewTestTSOCluster(suite.ctx, 2, suite.pdLeaderServer.GetAddr())
	re.NoError(err)
	suite.allocator = mockid.NewIDAllocator()
	err = suite.allocator.SetBase(uint64(time.Now().Second()))
	re.NoError(err)
}

func (suite *tsoKeyspaceGroupManagerTestSuite) TearDownSuite() {
	suite.cancel()
	suite.tsoCluster.Destroy()
	suite.cluster.Destroy()
	suite.Require().NoError(failpoint.Disable("github.com/tikv/pd/pkg/tso/fastGroupSplitPatroller"))
}

func (suite *tsoKeyspaceGroupManagerTestSuite) TearDownTest() {
	cleanupKeyspaceGroups(suite.Require(), suite.pdLeaderServer)
}

func cleanupKeyspaceGroups(re *require.Assertions, server *tests.TestServer) {
	keyspaceGroups := handlersutil.MustLoadKeyspaceGroups(re, server, "0", "0")
	for _, group := range keyspaceGroups {
		// Do not delete default keyspace group.
		if group.ID == constant.DefaultKeyspaceGroupID {
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
				if server.IsKeyspaceServingByGroup(keyspaceID, constant.DefaultKeyspaceGroupID) {
					tam, err := server.GetTSOAllocator(constant.DefaultKeyspaceGroupID)
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

	runDefaultKeyspace := false
	for _, server := range suite.tsoCluster.GetServers() {
		if server.IsKeyspaceServingByGroup(constant.DefaultKeyspaceID, constant.DefaultKeyspaceGroupID) {
			runDefaultKeyspace = true
		}
		for _, keyspaceGroupID := range []uint32{1, 2, 3} {
			re.False(server.IsKeyspaceServingByGroup(constant.DefaultKeyspaceID, keyspaceGroupID))
			for _, keyspaceID := range []uint32{1, 2, 3} {
				if server.IsKeyspaceServingByGroup(keyspaceID, keyspaceGroupID) {
					tam, err := server.GetTSOAllocator(keyspaceGroupID)
					re.NoError(err)
					re.NotNil(tam)
				}
			}
		}
	}
	re.True(runDefaultKeyspace)

	// Create a client for each keyspace and make sure they can successfully discover the service
	// provided by the default keyspace group.
	keyspaceIDs := []uint32{0, 1, 2, 3, 1000}
	clients := utils.WaitForMultiKeyspacesTSOAvailable(
		suite.ctx, re, keyspaceIDs, []string{suite.pdLeaderServer.GetAddr()})
	re.Len(keyspaceIDs, len(clients))
	utils.CheckMultiKeyspacesTSO(suite.ctx, re, clients, func() {
		time.Sleep(3 * time.Second)
	})
	for _, client := range clients {
		client.Close()
	}
}

func (suite *tsoKeyspaceGroupManagerTestSuite) TestKeyspacesServedByNonDefaultKeyspaceGroups() {
	// Create multiple keyspace groups, and every keyspace should be served by one of them
	// on a tso server.
	re := suite.Require()

	// Create 3 keyspace groups with 2 keyspaces each.
	keyspaceIDs := make([]uint32, 0)
	keyspaceGroups := make(map[uint32][]uint32)
	for range 3 {
		keyspaceGroupID := suite.allocID()
		for range 2 {
			keyspaceID := suite.allocID()
			keyspaceIDs = append(keyspaceIDs, keyspaceID)
			keyspaceGroups[keyspaceGroupID] = append(keyspaceGroups[keyspaceGroupID], keyspaceID)
		}
	}

	for keyspaceGroupID, keyspaceIDs := range keyspaceGroups {
		handlersutil.MustCreateKeyspaceGroup(re, suite.pdLeaderServer, &handlers.CreateKeyspaceGroupParams{
			KeyspaceGroups: []*endpoint.KeyspaceGroup{
				{
					ID:        keyspaceGroupID,
					UserKind:  endpoint.Standard.String(),
					Members:   suite.tsoCluster.GetKeyspaceGroupMember(),
					Keyspaces: keyspaceIDs,
				},
			},
		})
	}

	// Wait until all keyspace groups are ready.
	testutil.Eventually(re, func() bool {
		for keyspaceGroupID, keyspaceIDs := range keyspaceGroups {
			for _, keyspaceID := range keyspaceIDs {
				served := false
				for _, server := range suite.tsoCluster.GetServers() {
					if server.IsKeyspaceServingByGroup(keyspaceID, keyspaceGroupID) {
						allocator, err := server.GetTSOAllocator(keyspaceGroupID)
						re.NoError(err)
						re.NotNil(allocator)

						// Make sure every keyspace group is using the right timestamp path
						// for loading/saving timestamp from/to etcd and the right primary path
						// for primary election.
						primaryPath := keypath.ElectionPath(&keypath.MsParam{
							ServiceName: mcs.TSOServiceName,
							GroupID:     keyspaceGroupID,
						})
						re.Equal(primaryPath, allocator.GetMember().GetElectionPath())

						served = true
					}
				}
				if !served {
					return false
				}
			}
		}
		return true
	})

	// Create a client for each keyspace and make sure they can successfully discover the service
	// provided by the corresponding keyspace group.
	clients := utils.WaitForMultiKeyspacesTSOAvailable(
		suite.ctx, re, keyspaceIDs, []string{suite.pdLeaderServer.GetAddr()})
	re.Len(keyspaceIDs, len(clients))
	utils.CheckMultiKeyspacesTSO(suite.ctx, re, clients, func() {
		time.Sleep(3 * time.Second)
	})
	for _, client := range clients {
		client.Close()
	}
}

func (suite *tsoKeyspaceGroupManagerTestSuite) TestTSOKeyspaceGroupSplit() {
	re := suite.Require()
	// Create the keyspace group `oldID` with keyspaces [111, 222, 333].
	oldID := suite.allocID()
	handlersutil.MustCreateKeyspaceGroup(re, suite.pdLeaderServer, &handlers.CreateKeyspaceGroupParams{
		KeyspaceGroups: []*endpoint.KeyspaceGroup{
			{
				ID:        oldID,
				UserKind:  endpoint.Standard.String(),
				Members:   suite.tsoCluster.GetKeyspaceGroupMember(),
				Keyspaces: []uint32{111, 222, 333},
			},
		},
	})
	kg1 := handlersutil.MustLoadKeyspaceGroupByID(re, suite.pdLeaderServer, oldID)
	re.Equal(oldID, kg1.ID)
	re.Equal([]uint32{111, 222, 333}, kg1.Keyspaces)
	re.False(kg1.IsSplitting())
	// Get a TSO from the keyspace group `oldID`.
	var (
		ts  pdpb.Timestamp
		err error
	)
	testutil.Eventually(re, func() bool {
		ts, err = suite.requestTSO(re, 222, oldID)
		return err == nil && tsoutil.CompareTimestamp(&ts, &pdpb.Timestamp{}) > 0
	})
	ts.Physical += time.Hour.Milliseconds()
	// Set the TSO of the keyspace group `oldID` to a large value.
	err = suite.tsoCluster.GetPrimaryServer(222, oldID).ResetTS(tsoutil.GenerateTS(&ts), false, true, oldID)
	re.NoError(err)
	// Split the keyspace group `oldID` to `newID`.
	newID := suite.allocID()
	handlersutil.MustSplitKeyspaceGroup(re, suite.pdLeaderServer, oldID, &handlers.SplitKeyspaceGroupByIDParams{
		NewID:     newID,
		Keyspaces: []uint32{222, 333},
	})
	// Wait for the split to complete automatically even there is no TSO request from the outside.
	testutil.Eventually(re, func() bool {
		kg2, code := handlersutil.TryLoadKeyspaceGroupByID(re, suite.pdLeaderServer, newID)
		if code != http.StatusOK {
			return false
		}
		re.Equal(newID, kg2.ID)
		re.Equal([]uint32{222, 333}, kg2.Keyspaces)
		return !kg2.IsSplitting()
	})
	// Check the split TSO from keyspace group `newID` now.
	splitTS, err := suite.requestTSO(re, 222, newID)
	re.NoError(err)
	re.Positive(tsoutil.CompareTimestamp(&splitTS, &ts))
}

func (suite *tsoKeyspaceGroupManagerTestSuite) requestTSO(
	re *require.Assertions,
	keyspaceID, keyspaceGroupID uint32,
) (pdpb.Timestamp, error) {
	primary := suite.tsoCluster.WaitForPrimaryServing(re, keyspaceID, keyspaceGroupID)
	kgm := primary.GetKeyspaceGroupManager()
	re.NotNil(kgm)
	ts, _, err := kgm.HandleTSORequest(suite.ctx, keyspaceID, keyspaceGroupID, 1)
	return ts, err
}

func (suite *tsoKeyspaceGroupManagerTestSuite) TestTSOKeyspaceGroupSplitElection() {
	re := suite.Require()
	// Create the keyspace group `oldID` with keyspaces [444, 555, 666].
	oldID := suite.allocID()
	handlersutil.MustCreateKeyspaceGroup(re, suite.pdLeaderServer, &handlers.CreateKeyspaceGroupParams{
		KeyspaceGroups: []*endpoint.KeyspaceGroup{
			{
				ID:        oldID,
				UserKind:  endpoint.Standard.String(),
				Members:   suite.tsoCluster.GetKeyspaceGroupMember(),
				Keyspaces: []uint32{444, 555, 666},
			},
		},
	})
	kg1 := handlersutil.MustLoadKeyspaceGroupByID(re, suite.pdLeaderServer, oldID)
	re.Equal(oldID, kg1.ID)
	re.Equal([]uint32{444, 555, 666}, kg1.Keyspaces)
	re.False(kg1.IsSplitting())
	// Split the keyspace group `oldID` to `newID`.
	newID := suite.allocID()
	handlersutil.MustSplitKeyspaceGroup(re, suite.pdLeaderServer, oldID, &handlers.SplitKeyspaceGroupByIDParams{
		NewID:     newID,
		Keyspaces: []uint32{555, 666},
	})
	kg2 := handlersutil.MustLoadKeyspaceGroupByID(re, suite.pdLeaderServer, newID)
	re.Equal(newID, kg2.ID)
	re.Equal([]uint32{555, 666}, kg2.Keyspaces)
	re.True(kg2.IsSplitTarget())
	// Check the leadership.
	primary1, err := suite.tsoCluster.WaitForPrimaryServing(re, 444, oldID).GetMember(444, oldID)
	re.NoError(err)
	re.NotNil(primary1)
	primary2, err := suite.tsoCluster.WaitForPrimaryServing(re, 555, newID).GetMember(555, newID)
	re.NoError(err)
	re.NotNil(primary2)
	// Wait for the primary of the keyspace group `oldID` and `newID` to be elected.
	testutil.Eventually(re, func() bool {
		return len(primary1.GetServingUrls()) > 0 && len(primary2.GetServingUrls()) > 0
	})
	// Check if the primary of the keyspace group `oldID` and `newID` are the same.
	re.Equal(primary1.GetServingUrls(), primary2.GetServingUrls())
	// Resign and block the primary of the keyspace group `oldID` from being elected.
	primary1.(*member.Participant).SetCampaignChecker(func(*election.Leadership) bool {
		return false
	})
	primary1.Resign()
	// The primary of the keyspace group `newID` should be resigned also.
	testutil.Eventually(re, func() bool {
		return primary2.IsServing() == false
	})
	// Check if the primary of the keyspace group `oldID` and `newID` are the same again.
	primary1.(*member.Participant).SetCampaignChecker(nil)
	testutil.Eventually(re, func() bool {
		return len(primary1.GetServingUrls()) > 0 && len(primary2.GetServingUrls()) > 0
	})
	re.Equal(primary1.GetServingUrls(), primary2.GetServingUrls())
	// Wait for the keyspace groups to finish the split.
	waitFinishSplit(re, suite.pdLeaderServer, oldID, newID, []uint32{444}, []uint32{555, 666})
}

func waitFinishSplit(
	re *require.Assertions,
	server *tests.TestServer,
	splitSourceID, splitTargetID uint32,
	splitSourceKeyspaces, splitTargetKeyspaces []uint32,
) {
	testutil.Eventually(re, func() bool {
		kg, code := handlersutil.TryLoadKeyspaceGroupByID(re, server, splitTargetID)
		if code != http.StatusOK {
			return false
		}
		re.Equal(splitTargetID, kg.ID)
		re.Equal(splitTargetKeyspaces, kg.Keyspaces)
		return !kg.IsSplitTarget()
	})
	testutil.Eventually(re, func() bool {
		kg, code := handlersutil.TryLoadKeyspaceGroupByID(re, server, splitSourceID)
		if code != http.StatusOK {
			return false
		}
		re.Equal(splitSourceID, kg.ID)
		re.Equal(splitSourceKeyspaces, kg.Keyspaces)
		return !kg.IsSplitSource()
	})
}

func (suite *tsoKeyspaceGroupManagerTestSuite) TestTSOKeyspaceGroupSplitClient() {
	re := suite.Require()
	// Enable the failpoint to slow down the system time to test whether the TSO is monotonic.
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/systemTimeSlow", `return(true)`))
	// Create the keyspace group `oldID` with keyspaces [777, 888, 999].
	oldID := suite.allocID()
	handlersutil.MustCreateKeyspaceGroup(re, suite.pdLeaderServer, &handlers.CreateKeyspaceGroupParams{
		KeyspaceGroups: []*endpoint.KeyspaceGroup{
			{
				ID:        oldID,
				UserKind:  endpoint.Standard.String(),
				Members:   suite.tsoCluster.GetKeyspaceGroupMember(),
				Keyspaces: []uint32{777, 888, 999},
			},
		},
	})
	kg1 := handlersutil.MustLoadKeyspaceGroupByID(re, suite.pdLeaderServer, oldID)
	re.Equal(oldID, kg1.ID)
	re.Equal([]uint32{777, 888, 999}, kg1.Keyspaces)
	re.False(kg1.IsSplitting())
	// Request the TSO for keyspace 555 concurrently via client.
	cancel := suite.dispatchClient(re, 888, oldID)
	// Split the keyspace group `oldID` to `newID`.
	newID := suite.allocID()
	handlersutil.MustSplitKeyspaceGroup(re, suite.pdLeaderServer, oldID, &handlers.SplitKeyspaceGroupByIDParams{
		NewID:     newID,
		Keyspaces: []uint32{888, 999},
	})
	// Wait for the keyspace groups to finish the split.
	waitFinishSplit(re, suite.pdLeaderServer, oldID, newID, []uint32{777}, []uint32{888, 999})
	// Stop the client.
	cancel()
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/tso/systemTimeSlow"))
}

func (suite *tsoKeyspaceGroupManagerTestSuite) dispatchClient(
	re *require.Assertions, keyspaceID, keyspaceGroupID uint32,
) context.CancelFunc {
	// Make sure the primary of the keyspace group is elected.
	primary, err := suite.tsoCluster.
		WaitForPrimaryServing(re, keyspaceID, keyspaceGroupID).
		GetMember(keyspaceID, keyspaceGroupID)
	re.NoError(err)
	re.NotNil(primary)
	// Prepare the client for keyspace.

	tsoClient := utils.SetupClientWithKeyspaceID(suite.ctx, re, keyspaceID, []string{suite.pdLeaderServer.GetAddr()})
	re.NoError(err)
	re.NotNil(tsoClient)
	var (
		wg                        sync.WaitGroup
		ctx, cancel               = context.WithCancel(suite.ctx)
		lastPhysical, lastLogical int64
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer tsoClient.Close()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			physical, logical, err := tsoClient.GetTS(ctx)
			if err != nil {
				errMsg := err.Error()
				// Ignore the errors caused by the split and context cancellation.
				if strings.Contains(errMsg, "context canceled") ||
					strings.Contains(errMsg, clierrs.NotLeaderErr) ||
					strings.Contains(errMsg, clierrs.NotServedErr) ||
					strings.Contains(errMsg, "ErrKeyspaceNotAssigned") ||
					strings.Contains(errMsg, "ErrKeyspaceGroupIsMerging") ||
					errors.Is(err, clierrs.ErrClientTSOStreamClosed) {
					continue
				}
				re.FailNow(fmt.Sprintf("%+v", err))
			}
			if physical == lastPhysical {
				re.Greater(logical, lastLogical)
			} else {
				re.Greater(physical, lastPhysical)
			}
			lastPhysical, lastLogical = physical, logical
		}
	}()
	return func() {
		// Wait for a while to make sure the client has sent more TSO requests.
		time.Sleep(time.Second)
		// Cancel the context to stop the client.
		cancel()
		// Wait for the client to stop.
		wg.Wait()
	}
}

func (suite *tsoKeyspaceGroupManagerTestSuite) TestTSOKeyspaceGroupMembers() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion", "return(true)"))
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/acceleratedAllocNodes", `return(true)`))
	// wait for finishing alloc nodes
	waitFinishAllocNodes(re, suite.pdLeaderServer, constant.DefaultKeyspaceGroupID)
	testConfig := map[string]string{
		"config":                "1",
		"tso_keyspace_group_id": "0",
		"user_kind":             "basic",
	}
	handlersutil.MustCreateKeyspace(re, suite.pdLeaderServer, &handlers.CreateKeyspaceParams{
		Name:   "test_keyspace",
		Config: testConfig,
	})
	waitFinishAllocNodes(re, suite.pdLeaderServer, constant.DefaultKeyspaceGroupID)
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion"))
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/acceleratedAllocNodes"))
}

func waitFinishAllocNodes(re *require.Assertions, server *tests.TestServer, groupID uint32) {
	testutil.Eventually(re, func() bool {
		kg := handlersutil.MustLoadKeyspaceGroupByID(re, server, groupID)
		re.Equal(groupID, kg.ID)
		return len(kg.Members) == mcs.DefaultKeyspaceGroupReplicaCount
	})
}

func TestTwiceSplitKeyspaceGroup(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/acceleratedAllocNodes", `return(true)`))
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/fastGroupSplitPatroller", `return(true)`))

	// Init PD config but not start.
	tc, err := tests.NewTestClusterWithKeyspaceGroup(ctx, 1, func(conf *config.Config, _ string) {
		conf.Keyspace.PreAlloc = []string{
			"keyspace_a", "keyspace_b",
		}
		conf.Keyspace.WaitRegionSplit = false
	})
	re.NoError(err)
	defer tc.Destroy()
	pdAddr := tc.GetConfig().GetClientURL()

	// Start PD and tso server.
	err = tc.RunInitialServers()
	re.NoError(err)
	tc.WaitLeader()
	leaderServer := tc.GetLeaderServer()
	re.NoError(leaderServer.BootstrapCluster())

	tsoCluster, err := tests.NewTestTSOCluster(ctx, 2, pdAddr)
	re.NoError(err)
	defer tsoCluster.Destroy()
	tsoCluster.WaitForDefaultPrimaryServing(re)

	// First split keyspace group 0 to 1 with keyspace 2.
	kgm := leaderServer.GetServer().GetKeyspaceGroupManager()
	re.NotNil(kgm)
	testutil.Eventually(re, func() bool {
		err = kgm.SplitKeyspaceGroupByID(0, 1, []uint32{2})
		return err == nil
	})

	bootstrapKeyspaceID := keyspace.GetBootstrapKeyspaceID()
	waitFinishSplit(re, leaderServer, 0, 1, []uint32{bootstrapKeyspaceID, 1}, []uint32{2})

	// Then split keyspace group 0 to 2 with keyspace 1.
	testutil.Eventually(re, func() bool {
		err = kgm.SplitKeyspaceGroupByID(0, 2, []uint32{1})
		return err == nil
	})

	waitFinishSplit(re, leaderServer, 0, 2, []uint32{bootstrapKeyspaceID}, []uint32{1})

	// Check the keyspace group 0 is split to 1 and 2.
	kg0 := handlersutil.MustLoadKeyspaceGroupByID(re, leaderServer, 0)
	kg1 := handlersutil.MustLoadKeyspaceGroupByID(re, leaderServer, 1)
	kg2 := handlersutil.MustLoadKeyspaceGroupByID(re, leaderServer, 2)
	re.Equal([]uint32{bootstrapKeyspaceID}, kg0.Keyspaces)
	re.Equal([]uint32{2}, kg1.Keyspaces)
	re.Equal([]uint32{1}, kg2.Keyspaces)
	re.False(kg0.IsSplitting())
	re.False(kg1.IsSplitting())
	re.False(kg2.IsSplitting())

	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/tso/fastGroupSplitPatroller"))
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/acceleratedAllocNodes"))
}

func (suite *tsoKeyspaceGroupManagerTestSuite) TestTSOKeyspaceGroupMerge() {
	re := suite.Require()
	// Create the keyspace group `firstID` and `secondID` with keyspaces [1111, 2222] and [3333].
	firstID, secondID := suite.allocID(), suite.allocID()
	handlersutil.MustCreateKeyspaceGroup(re, suite.pdLeaderServer, &handlers.CreateKeyspaceGroupParams{
		KeyspaceGroups: []*endpoint.KeyspaceGroup{
			{
				ID:        firstID,
				UserKind:  endpoint.Standard.String(),
				Members:   suite.tsoCluster.GetKeyspaceGroupMember(),
				Keyspaces: []uint32{1111, 2222},
			},
			{
				ID:        secondID,
				UserKind:  endpoint.Standard.String(),
				Members:   suite.tsoCluster.GetKeyspaceGroupMember(),
				Keyspaces: []uint32{3333},
			},
		},
	})
	// Get a TSO from the keyspace group `firstID`.
	var (
		ts  pdpb.Timestamp
		err error
	)
	testutil.Eventually(re, func() bool {
		ts, err = suite.requestTSO(re, 2222, firstID)
		return err == nil && tsoutil.CompareTimestamp(&ts, &pdpb.Timestamp{}) > 0
	})
	ts.Physical += time.Hour.Milliseconds()
	// Set the TSO of the keyspace group `firstID` to a large value.
	err = suite.tsoCluster.GetPrimaryServer(2222, firstID).ResetTS(tsoutil.GenerateTS(&ts), false, true, firstID)
	re.NoError(err)
	// Merge the keyspace group `firstID` and `secondID` to the default keyspace group.
	handlersutil.MustMergeKeyspaceGroup(re, suite.pdLeaderServer, constant.DefaultKeyspaceGroupID, &handlers.MergeKeyspaceGroupsParams{
		MergeList: []uint32{firstID, secondID},
	})
	// Check the keyspace group `firstID` and `secondID` are merged to the default keyspace group.
	kg := handlersutil.MustLoadKeyspaceGroupByID(re, suite.pdLeaderServer, constant.DefaultKeyspaceGroupID)
	re.Equal(constant.DefaultKeyspaceGroupID, kg.ID)
	for _, keyspaceID := range []uint32{1111, 2222, 3333} {
		re.Contains(kg.Keyspaces, keyspaceID)
	}
	re.True(kg.IsMergeTarget())
	// Check the merged TSO from the default keyspace group is greater than the TSO from the keyspace group`firstID`.
	var mergedTS pdpb.Timestamp
	testutil.Eventually(re, func() bool {
		mergedTS, err = suite.requestTSO(re, 3333, constant.DefaultKeyspaceGroupID)
		if err != nil {
			re.ErrorIs(err, errs.ErrKeyspaceGroupIsMerging)
		}
		return err == nil && tsoutil.CompareTimestamp(&mergedTS, &pdpb.Timestamp{}) > 0
	}, testutil.WithTickInterval(5*time.Second), testutil.WithWaitFor(time.Minute))
	re.Positive(tsoutil.CompareTimestamp(&mergedTS, &ts))
}

func (suite *tsoKeyspaceGroupManagerTestSuite) TestTSOKeyspaceGroupMergeClient() {
	re := suite.Require()
	// Create the keyspace group `id` with keyspaces [4444, 5555, 6666].
	id := suite.allocID()
	handlersutil.MustCreateKeyspaceGroup(re, suite.pdLeaderServer, &handlers.CreateKeyspaceGroupParams{
		KeyspaceGroups: []*endpoint.KeyspaceGroup{
			{
				ID:        id,
				UserKind:  endpoint.Standard.String(),
				Members:   suite.tsoCluster.GetKeyspaceGroupMember(),
				Keyspaces: []uint32{4444, 5555, 6666},
			},
		},
	})
	kg1 := handlersutil.MustLoadKeyspaceGroupByID(re, suite.pdLeaderServer, id)
	re.Equal(id, kg1.ID)
	re.Equal([]uint32{4444, 5555, 6666}, kg1.Keyspaces)
	re.False(kg1.IsMerging())
	// Request the TSO for keyspace 5555 concurrently via client.
	cancel := suite.dispatchClient(re, 5555, id)
	// Merge the keyspace group 1 to the default keyspace group.
	handlersutil.MustMergeKeyspaceGroup(re, suite.pdLeaderServer, constant.DefaultKeyspaceGroupID, &handlers.MergeKeyspaceGroupsParams{
		MergeList: []uint32{id},
	})
	// Wait for the default keyspace group to finish the merge.
	waitFinishMerge(re, suite.pdLeaderServer, constant.DefaultKeyspaceGroupID, []uint32{4444, 5555, 6666})
	// Stop the client.
	cancel()
}

func waitFinishMerge(
	re *require.Assertions,
	server *tests.TestServer,
	mergeTargetID uint32,
	keyspaces []uint32,
) {
	var kg *endpoint.KeyspaceGroup
	testutil.Eventually(re, func() bool {
		kg = handlersutil.MustLoadKeyspaceGroupByID(re, server, mergeTargetID)
		re.Equal(mergeTargetID, kg.ID)
		return !kg.IsMergeTarget()
	})
	// If the merge is finished, the target keyspace group should contain all the keyspaces.
	for _, keyspaceID := range keyspaces {
		re.Contains(kg.Keyspaces, keyspaceID)
	}
}

func (suite *tsoKeyspaceGroupManagerTestSuite) TestTSOKeyspaceGroupMergeBeforeInitTSO() {
	re := suite.Require()
	// Make sure the TSO of keyspace group `id` won't be initialized before it's merged.
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/failedToSaveTimestamp", `return(true)`))
	// Request the TSO for the default keyspace concurrently via client.
	id := suite.allocID()
	cancel := suite.dispatchClient(re, constant.DefaultKeyspaceID, constant.DefaultKeyspaceGroupID)
	// Create the keyspace group 1 with keyspaces [7777, 8888, 9999].
	handlersutil.MustCreateKeyspaceGroup(re, suite.pdLeaderServer, &handlers.CreateKeyspaceGroupParams{
		KeyspaceGroups: []*endpoint.KeyspaceGroup{
			{
				ID:        id,
				UserKind:  endpoint.Standard.String(),
				Members:   suite.tsoCluster.GetKeyspaceGroupMember(),
				Keyspaces: []uint32{7777, 8888, 9999},
			},
		},
	})
	// Merge the keyspace group `id` to the default keyspace group.
	handlersutil.MustMergeKeyspaceGroup(re, suite.pdLeaderServer, constant.DefaultKeyspaceGroupID, &handlers.MergeKeyspaceGroupsParams{
		MergeList: []uint32{id},
	})
	// Wait for the default keyspace group to finish the merge.
	waitFinishMerge(re, suite.pdLeaderServer, constant.DefaultKeyspaceGroupID, []uint32{7777, 8888, 9999})
	// Stop the client.
	cancel()
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/tso/failedToSaveTimestamp"))
}

// See https://github.com/tikv/pd/issues/6748
func TestGetTSOImmediately(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/fastPrimaryPriorityCheck", `return(true)`))
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/acceleratedAllocNodes", `return(true)`))
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/fastGroupSplitPatroller", `return(true)`))
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/skipKeyspaceRegionCheck", "return"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/tso/fastPrimaryPriorityCheck"))
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/acceleratedAllocNodes"))
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/tso/fastGroupSplitPatroller"))
		re.NoError(failpoint.Disable("github.com/tikv/pd/server/skipKeyspaceRegionCheck"))
	}()

	// Init PD config but not start.
	keyspaces := []string{
		"keyspace_a", "keyspace_b",
	}
	tc, err := tests.NewTestClusterWithKeyspaceGroup(ctx, 1, func(conf *config.Config, _ string) {
		conf.Keyspace.PreAlloc = keyspaces
		conf.Keyspace.WaitRegionSplit = false
	})
	re.NoError(err)
	defer tc.Destroy()
	pdAddr := tc.GetConfig().GetClientURL()

	// Start PD and tso server.
	err = tc.RunInitialServers()
	re.NoError(err)
	tc.WaitLeader()
	leaderServer := tc.GetLeaderServer()
	re.NoError(leaderServer.BootstrapCluster())

	tsoCluster, err := tests.NewTestTSOCluster(ctx, 2, pdAddr)
	re.NoError(err)
	defer tsoCluster.Destroy()
	tsoCluster.WaitForDefaultPrimaryServing(re)

	// First split keyspace group 0 to 1 with keyspace 2.
	kgm := leaderServer.GetServer().GetKeyspaceGroupManager()
	re.NotNil(kgm)
	testutil.Eventually(re, func() bool {
		err = kgm.SplitKeyspaceGroupByID(0, 1, []uint32{2})
		return err == nil
	})

	// Get the correct bootstrap keyspace ID for the current mode
	bootstrapKeyspaceID := keyspace.GetBootstrapKeyspaceID()
	waitFinishSplit(re, leaderServer, 0, 1, []uint32{bootstrapKeyspaceID, 1}, []uint32{2})

	kg0 := handlersutil.MustLoadKeyspaceGroupByID(re, leaderServer, 0)
	kg1 := handlersutil.MustLoadKeyspaceGroupByID(re, leaderServer, 1)
	re.Equal([]uint32{bootstrapKeyspaceID, 1}, kg0.Keyspaces)
	re.Equal([]uint32{2}, kg1.Keyspaces)
	re.False(kg0.IsSplitting())
	re.False(kg1.IsSplitting())

	// Let group 0 and group 1 have different primary node.
	err = kgm.SetPriorityForKeyspaceGroup(0, kg0.Members[0].Address, 100)
	re.NoError(err)
	err = kgm.SetPriorityForKeyspaceGroup(1, kg1.Members[1].Address, 100)
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		p0, err0 := kgm.GetKeyspaceGroupPrimaryByID(0)
		p1, err1 := kgm.GetKeyspaceGroupPrimaryByID(1)
		return err0 == nil && p0 == kg0.Members[0].Address &&
			err1 == nil && p1 == kg1.Members[1].Address
	})

	for _, name := range keyspaces {
		apiCtx := pd.NewAPIContextV2(name)
		cli, err := pd.NewClientWithAPIContext(ctx, apiCtx,
			caller.TestComponent,
			[]string{pdAddr}, pd.SecurityOption{})
		re.NoError(err)
		_, _, err = cli.GetTS(ctx)
		re.NoError(err)
		cli.Close()
	}
}

func (suite *tsoKeyspaceGroupManagerTestSuite) TestKeyspaceGroupMergeIntoDefault() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/acceleratedAllocNodes", `return(true)`))

	var (
		keyspaceGroupNum = etcdutil.MaxEtcdTxnOps
		keyspaceGroups   = make([]*endpoint.KeyspaceGroup, 0, keyspaceGroupNum)
		keyspaces        = make([]uint32, 0, keyspaceGroupNum)
	)
	for i := 1; i <= keyspaceGroupNum; i++ {
		id := suite.allocID()
		keyspaceGroups = append(keyspaceGroups, &endpoint.KeyspaceGroup{
			ID:        id,
			UserKind:  endpoint.UserKind(rand.Intn(int(endpoint.UserKindCount))).String(),
			Keyspaces: []uint32{id},
		})
		keyspaces = append(keyspaces, id)
		if i != keyspaceGroupNum {
			continue
		}
		handlersutil.MustCreateKeyspaceGroup(re, suite.pdLeaderServer, &handlers.CreateKeyspaceGroupParams{
			KeyspaceGroups: keyspaceGroups,
		})
	}
	// Check if all the keyspace groups are created.
	groups := handlersutil.MustLoadKeyspaceGroups(re, suite.pdLeaderServer, "0", "0")
	re.Len(groups, keyspaceGroupNum+1)
	// Wait for all the keyspace groups to be served.
	// Check if the first keyspace group is served.
	svr := suite.tsoCluster.WaitForDefaultPrimaryServing(re)
	re.NotNil(svr)
	for i := 1; i < keyspaceGroupNum; i++ {
		// Check if the keyspace group is served.
		svr = suite.tsoCluster.WaitForPrimaryServing(re, keyspaces[i], keyspaceGroups[i].ID)
		re.NotNil(svr)
		re.True(svr.IsKeyspaceServingByGroup(keyspaces[i], keyspaceGroups[i].ID))
	}
	// Merge all the keyspace groups into the default keyspace group.
	handlersutil.MustMergeKeyspaceGroup(re, suite.pdLeaderServer, constant.DefaultKeyspaceGroupID, &handlers.MergeKeyspaceGroupsParams{
		MergeAllIntoDefault: true,
	})
	// Wait for all the keyspace groups to be merged.
	waitFinishMerge(re, suite.pdLeaderServer, constant.DefaultKeyspaceGroupID, keyspaces)
	// Check if all the keyspace groups are merged.
	groups = handlersutil.MustLoadKeyspaceGroups(re, suite.pdLeaderServer, "0", "0")
	re.Len(groups, 1)

	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/acceleratedAllocNodes"))
}
