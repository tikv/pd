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
	"math"
	"math/rand/v2"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"

	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/clients/tso"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/caller"
	sd "github.com/tikv/pd/client/servicediscovery"
	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/keyspace/constant"
	mcsconst "github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/tsoutil"
	"github.com/tikv/pd/server/apiv2/handlers"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/integrations/mcs/utils"
	handlersutil "github.com/tikv/pd/tests/server/apiv2/handlers"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

type tsoClientTestSuite struct {
	suite.Suite
	legacy bool

	ctx    context.Context
	cancel context.CancelFunc
	// The PD cluster.
	cluster *tests.TestCluster
	// pdLeaderServer is the leader server of the PD cluster.
	pdLeaderServer *tests.TestServer
	// The TSO service in microservice mode.
	tsoCluster *tests.TestTSOCluster

	keyspaceGroups []struct {
		keyspaceGroupID uint32
		keyspaceIDs     []uint32
	}

	backendEndpoints string
	keyspaceIDs      []uint32
	clients          []pd.Client
}

func (suite *tsoClientTestSuite) getBackendEndpoints() []string {
	return strings.Split(suite.backendEndpoints, ",")
}

func TestLegacyTSOClientSuite(t *testing.T) {
	suite.Run(t, &tsoClientTestSuite{
		legacy: true,
	})
}

func TestMicroserviceTSOClientSuite(t *testing.T) {
	suite.Run(t, &tsoClientTestSuite{
		legacy: false,
	})
}

func (suite *tsoClientTestSuite) SetupSuite() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/fastUpdatePhysicalInterval", "return(true)"))

	var err error
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	if suite.legacy {
		suite.cluster, err = tests.NewTestCluster(suite.ctx, serverCount)
	} else {
		suite.cluster, err = tests.NewTestClusterWithKeyspaceGroup(suite.ctx, serverCount, func(conf *config.Config, _ string) {
			conf.Microservice.EnableTSODynamicSwitching = false
		})
	}
	re.NoError(err)
	err = suite.cluster.RunInitialServers()
	re.NoError(err)
	leaderName := suite.cluster.WaitLeader()
	re.NotEmpty(leaderName)
	suite.pdLeaderServer = suite.cluster.GetServer(leaderName)
	re.NoError(suite.pdLeaderServer.BootstrapCluster())
	suite.backendEndpoints = suite.pdLeaderServer.GetAddr()
	suite.keyspaceIDs = make([]uint32, 0)

	if !suite.legacy {
		suite.tsoCluster, err = tests.NewTestTSOCluster(suite.ctx, 3, suite.backendEndpoints)
		re.NoError(err)

		suite.keyspaceGroups = []struct {
			keyspaceGroupID uint32
			keyspaceIDs     []uint32
		}{
			{0, []uint32{constant.DefaultKeyspaceID, 10}},
			{1, []uint32{1, 11}},
			{2, []uint32{2}},
		}

		for _, keyspaceGroup := range suite.keyspaceGroups {
			suite.keyspaceIDs = append(suite.keyspaceIDs, keyspaceGroup.keyspaceIDs...)
		}

		for _, param := range suite.keyspaceGroups {
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
	}
}

// Create independent clients to prevent interfering with other tests.
func (suite *tsoClientTestSuite) SetupTest() {
	re := suite.Require()
	if suite.legacy {
		client, err := pd.NewClientWithContext(suite.ctx,
			caller.TestComponent,
			suite.getBackendEndpoints(), pd.SecurityOption{}, opt.WithForwardingOption(true))
		re.NoError(err)
		innerClient, ok := client.(interface{ GetServiceDiscovery() sd.ServiceDiscovery })
		re.True(ok)
		re.Equal(constant.NullKeyspaceID, innerClient.GetServiceDiscovery().GetKeyspaceID())
		re.Equal(constant.DefaultKeyspaceGroupID, innerClient.GetServiceDiscovery().GetKeyspaceGroupID())
		utils.WaitForTSOServiceAvailable(suite.ctx, re, client)
		suite.clients = make([]pd.Client, 0)
		suite.clients = append(suite.clients, client)
	} else {
		suite.waitForAllKeyspaceGroupsInServing(re)
	}
}

func (suite *tsoClientTestSuite) waitForAllKeyspaceGroupsInServing(re *require.Assertions) {
	// The tso servers are loading keyspace groups asynchronously. Make sure all keyspace groups
	// are available for serving tso requests from corresponding keyspaces by querying
	// IsKeyspaceServing(keyspaceID, the Desired KeyspaceGroupID). if use default keyspace group id
	// in the query, it will always return true as the keyspace will be served by default keyspace
	// group before the keyspace groups are loaded.
	testutil.Eventually(re, func() bool {
		for _, keyspaceGroup := range suite.keyspaceGroups {
			for _, keyspaceID := range keyspaceGroup.keyspaceIDs {
				served := false
				for _, server := range suite.tsoCluster.GetServers() {
					if server.IsKeyspaceServingByGroup(keyspaceID, keyspaceGroup.keyspaceGroupID) {
						served = true
						break
					}
				}
				if !served {
					return false
				}
			}
		}
		return true
	}, testutil.WithWaitFor(5*time.Second), testutil.WithTickInterval(50*time.Millisecond))

	// Create clients and make sure they all have discovered the tso service.
	suite.clients = utils.WaitForMultiKeyspacesTSOAvailable(
		suite.ctx, re, suite.keyspaceIDs, suite.getBackendEndpoints())
	re.Len(suite.keyspaceIDs, len(suite.clients))
}

func (suite *tsoClientTestSuite) TearDownTest() {
	for _, client := range suite.clients {
		client.Close()
	}
}

func (suite *tsoClientTestSuite) TearDownSuite() {
	re := suite.Require()
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/tso/fastUpdatePhysicalInterval"))
	suite.cancel()
	if !suite.legacy {
		suite.tsoCluster.Destroy()
	}
	suite.cluster.Destroy()
}

func (suite *tsoClientTestSuite) TestGetTS() {
	re := suite.Require()
	var wg sync.WaitGroup
	wg.Add(tsoRequestConcurrencyNumber * len(suite.clients))
	for range tsoRequestConcurrencyNumber {
		for _, client := range suite.clients {
			go func(client pd.Client) {
				defer wg.Done()
				var lastTS uint64
				for range tsoRequestRound {
					physical, logical, err := client.GetTS(suite.ctx)
					re.NoError(err)
					ts := tsoutil.ComposeTS(physical, logical)
					re.Less(lastTS, ts)
					lastTS = ts
				}
			}(client)
		}
	}
	wg.Wait()
}

func (suite *tsoClientTestSuite) TestGetTSAsync() {
	re := suite.Require()
	var wg sync.WaitGroup
	wg.Add(tsoRequestConcurrencyNumber * len(suite.clients))
	for range tsoRequestConcurrencyNumber {
		for _, client := range suite.clients {
			go func(client pd.Client) {
				defer wg.Done()
				tsFutures := make([]tso.TSFuture, tsoRequestRound)
				for j := range tsFutures {
					tsFutures[j] = client.GetTSAsync(suite.ctx)
				}
				var lastTS uint64 = math.MaxUint64
				for j := len(tsFutures) - 1; j >= 0; j-- {
					physical, logical, err := tsFutures[j].Wait()
					re.NoError(err)
					ts := tsoutil.ComposeTS(physical, logical)
					re.Greater(lastTS, ts)
					lastTS = ts
				}
			}(client)
		}
	}
	wg.Wait()
}

func (suite *tsoClientTestSuite) TestDiscoverTSOServiceWithLegacyPath() {
	re := suite.Require()
	keyspaceID := uint32(1000000)
	// Make sure this keyspace ID is not in use somewhere.
	re.False(slice.Contains(suite.keyspaceIDs, keyspaceID))
	failpointValue := fmt.Sprintf(`return(%d)`, keyspaceID)
	// Simulate the case that the server has lower version than the client and returns no tso addrs
	// in the GetClusterInfo RPC.
	re.NoError(failpoint.Enable("github.com/tikv/pd/client/servicediscovery/serverReturnsNoTSOAddrs", `return(true)`))
	re.NoError(failpoint.Enable("github.com/tikv/pd/client/servicediscovery/unexpectedCallOfFindGroupByKeyspaceID", failpointValue))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/client/servicediscovery/serverReturnsNoTSOAddrs"))
		re.NoError(failpoint.Disable("github.com/tikv/pd/client/servicediscovery/unexpectedCallOfFindGroupByKeyspaceID"))
	}()

	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()
	client := utils.SetupClientWithKeyspaceID(
		ctx, re, keyspaceID, suite.getBackendEndpoints())
	defer client.Close()
	var lastTS uint64
	for range tsoRequestRound {
		physical, logical, err := client.GetTS(ctx)
		re.NoError(err)
		ts := tsoutil.ComposeTS(physical, logical)
		re.Less(lastTS, ts)
		lastTS = ts
	}
}

// TestGetMinTS tests the correctness of GetMinTS.
func (suite *tsoClientTestSuite) TestGetMinTS() {
	re := suite.Require()
	var wg sync.WaitGroup
	wg.Add(tsoRequestConcurrencyNumber * len(suite.clients))
	for range tsoRequestConcurrencyNumber {
		for _, client := range suite.clients {
			go func(client pd.Client) {
				defer wg.Done()
				var lastMinTS uint64
				for range tsoRequestRound {
					physical, logical, err := client.GetMinTS(suite.ctx)
					re.NoError(err)
					minTS := tsoutil.ComposeTS(physical, logical)
					re.Less(lastMinTS, minTS)
					lastMinTS = minTS

					// Now we check whether the returned ts is the minimum one
					// among all keyspace groups, i.e., the returned ts is
					// less than the new timestamps of all keyspace groups.
					for _, client := range suite.clients {
						physical, logical, err := client.GetTS(suite.ctx)
						re.NoError(err)
						ts := tsoutil.ComposeTS(physical, logical)
						re.Less(minTS, ts)
					}
				}
			}(client)
		}
	}
	wg.Wait()

	re.NoError(failpoint.Enable("github.com/tikv/pd/client/servicediscovery/unreachableNetwork1", "return(true)"))
	time.Sleep(time.Second)
	testutil.Eventually(re, func() bool {
		var err error
		_, _, err = suite.clients[0].GetMinTS(suite.ctx)
		return err == nil
	})
	re.NoError(failpoint.Disable("github.com/tikv/pd/client/servicediscovery/unreachableNetwork1"))
}

// More details can be found in this issue: https://github.com/tikv/pd/issues/4884
func (suite *tsoClientTestSuite) TestUpdateAfterResetTSO() {
	re := suite.Require()
	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/member/skipCampaignLeaderCheck", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/member/skipCampaignLeaderCheck"))
	}()
	for i := range suite.clients {
		client := suite.clients[i]
		testutil.Eventually(re, func() bool {
			_, _, err := client.GetTS(ctx)
			return err == nil
		})
		// Resign leader to trigger the TSO resetting.
		re.NoError(failpoint.Enable("github.com/tikv/pd/server/cluster/updateAfterResetTSO", "return(true)"))
		oldLeaderName := suite.cluster.WaitLeader()
		re.NotEmpty(oldLeaderName)
		err := suite.cluster.GetServer(oldLeaderName).ResignLeader()
		re.NoError(err)
		re.NoError(failpoint.Disable("github.com/tikv/pd/server/cluster/updateAfterResetTSO"))
		newLeaderName := suite.cluster.WaitLeader()
		re.NotEmpty(newLeaderName)
		re.NotEqual(oldLeaderName, newLeaderName)
		// Request a new TSO.
		testutil.Eventually(re, func() bool {
			_, _, err := client.GetTS(ctx)
			return err == nil
		})
		// Transfer leader back.
		re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/delaySyncTimestamp", `return(true)`))
		err = suite.cluster.GetServer(newLeaderName).ResignLeader()
		re.NoError(err)
		// Should NOT panic here.
		testutil.Eventually(re, func() bool {
			_, _, err := client.GetTS(ctx)
			return err == nil
		})
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/tso/delaySyncTimestamp"))
	}
}

func (suite *tsoClientTestSuite) TestRandomResignLeader() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/member/skipCampaignLeaderCheck", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/member/skipCampaignLeaderCheck"))
	}()

	parallelAct := func() {
		// After https://github.com/tikv/pd/issues/6376 is fixed, we can use a smaller number here.
		// currently, the time to discover tso service is usually a little longer than 1s, compared
		// to the previous time taken < 1s.
		n := rand.IntN(2) + 3
		time.Sleep(time.Duration(n) * time.Second)
		if !suite.legacy {
			wg := sync.WaitGroup{}
			// Select the first keyspace from all keyspace groups. We need to make sure the selected
			// keyspaces are from different keyspace groups, otherwise multiple goroutines below could
			// try to resign the primary of the same keyspace group and cause race condition.
			keyspaceIDs := make([]uint32, 0)
			keyspaceGroups := make(map[uint32]uint32, 0)
			for _, keyspaceGroup := range suite.keyspaceGroups {
				if len(keyspaceGroup.keyspaceIDs) > 0 {
					keyspaceID := keyspaceGroup.keyspaceIDs[0]
					keyspaceIDs = append(keyspaceIDs, keyspaceID)
					keyspaceGroups[keyspaceID] = keyspaceGroup.keyspaceGroupID
				}
			}
			wg.Add(len(keyspaceIDs))
			for _, keyspaceID := range keyspaceIDs {
				go func(keyspaceID uint32) {
					defer wg.Done()
					keyspaceGroupID := keyspaceGroups[keyspaceID]
					suite.tsoCluster.WaitForPrimaryServing(re, keyspaceID, keyspaceGroupID)
					err := suite.tsoCluster.ResignPrimary(keyspaceID, keyspaceGroupID)
					re.NoError(err)
					suite.tsoCluster.WaitForPrimaryServing(re, keyspaceID, keyspaceGroupID)
				}(keyspaceID)
			}
			wg.Wait()
		} else {
			err := suite.cluster.ResignLeader()
			re.NoError(err)
			suite.cluster.WaitLeader()
		}
		time.Sleep(time.Duration(n) * time.Second)
	}

	utils.CheckMultiKeyspacesTSO(suite.ctx, re, suite.clients, parallelAct)
}

func (suite *tsoClientTestSuite) TestRandomShutdown() {
	re := suite.Require()
	var closedTSOAddr string
	if !suite.legacy {
		defer func() {
			if closedTSOAddr == "" {
				return
			}
			re.NoError(suite.tsoCluster.AddServer(closedTSOAddr))
		}()
	}

	parallelAct := func() {
		if !suite.legacy {
			primary := suite.tsoCluster.WaitForDefaultPrimaryServing(re)
			closedTSOAddr = primary.GetAddr()
			primary.Close()
			suite.tsoCluster.WaitForDefaultPrimaryServing(re)
			utils.WaitForAllTSOServiceAvailable(suite.ctx, re, suite.clients)
		} else {
			// After https://github.com/tikv/pd/issues/6376 is fixed, we can use a smaller number here.
			// currently, the time to discover tso service is usually a little longer than 1s, compared
			// to the previous time taken < 1s.
			n := rand.IntN(2) + 3
			time.Sleep(time.Duration(n) * time.Second)
			suite.cluster.GetLeaderServer().GetServer().Close()
			time.Sleep(time.Duration(n) * time.Second)
		}
	}

	utils.CheckMultiKeyspacesTSO(suite.ctx, re, suite.clients, parallelAct)
	if !suite.legacy {
		re.NotEmpty(closedTSOAddr)
		return
	}
	suite.TearDownSuite()
	suite.SetupSuite()
}

func (suite *tsoClientTestSuite) TestGetTSWhileResettingTSOClient() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/client/clients/tso/delayDispatchTSORequest", "return(true)"))
	var (
		stopSignal atomic.Bool
		wg         sync.WaitGroup
	)

	wg.Add(tsoRequestConcurrencyNumber * len(suite.clients))
	for range tsoRequestConcurrencyNumber {
		for _, client := range suite.clients {
			go func(client pd.Client) {
				defer wg.Done()
				var lastTS uint64
				for !stopSignal.Load() {
					physical, logical, err := client.GetTS(suite.ctx)
					if err != nil {
						re.ErrorContains(err, context.Canceled.Error())
					} else {
						ts := tsoutil.ComposeTS(physical, logical)
						re.Less(lastTS, ts)
						lastTS = ts
					}
				}
			}(client)
		}
	}
	// Reset the TSO clients while requesting TSO concurrently.
	for range tsoRequestConcurrencyNumber {
		for _, client := range suite.clients {
			client.(interface{ ResetTSOClient() }).ResetTSOClient()
		}
	}
	stopSignal.Store(true)
	wg.Wait()
	re.NoError(failpoint.Disable("github.com/tikv/pd/client/clients/tso/delayDispatchTSORequest"))
}

func (suite *tsoClientTestSuite) TestTSONotLeaderWhenRebaseErr() {
	// This test checks the behavior of PD leader resign when rebase fails.
	if !suite.legacy {
		suite.T().Skip("skipping test in microservice mode")
	}
	re := suite.Require()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 2)
	re.NoError(err)
	defer cluster.Destroy()
	err = cluster.RunInitialServers()
	re.NoError(err)
	leaderName := cluster.WaitLeader()
	re.NotEmpty(leaderName)
	pdLeader := cluster.GetServer(leaderName)
	re.NoError(pdLeader.BootstrapCluster())
	memberID := pdLeader.GetLeader().GetMemberId()
	pdClient, err := pd.NewClientWithContext(ctx,
		caller.TestComponent,
		[]string{pdLeader.GetAddr()}, pd.SecurityOption{})
	re.NoError(err)
	defer pdClient.Close()
	memberIDs := make([]string, 0, len(cluster.GetServers()))
	for _, server := range cluster.GetServers() {
		memberIDs = append(memberIDs, strconv.FormatUint(server.GetServerID(), 10))
	}
	memberIDList := strings.Join(memberIDs, ",")

	re.NoError(failpoint.Enable("github.com/tikv/pd/server/rebaseErr", fmt.Sprintf("return(\"%s\")", memberIDList)))
	re.NoError(failpoint.Enable("github.com/tikv/pd/client/skipRetry", "return(true)"))
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/leaderLoopCheckAgain", fmt.Sprintf("return(\"%d\")", memberID)))
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/exitCampaignLeader", fmt.Sprintf("return(\"%d\")", memberID)))
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/timeoutWaitPDLeader", fmt.Sprintf("return(\"%s\")", memberIDList)))
	leaderFailpointsDisabled := false
	disableLeaderFailpoints := func() {
		if leaderFailpointsDisabled {
			return
		}
		leaderFailpointsDisabled = true
		re.NoError(failpoint.Disable("github.com/tikv/pd/server/leaderLoopCheckAgain"))
		re.NoError(failpoint.Disable("github.com/tikv/pd/server/exitCampaignLeader"))
		re.NoError(failpoint.Disable("github.com/tikv/pd/server/timeoutWaitPDLeader"))
	}
	failpointsDisabled := false
	disableFailpoints := func() {
		if failpointsDisabled {
			return
		}
		failpointsDisabled = true
		re.NoError(failpoint.Disable("github.com/tikv/pd/client/skipRetry"))
		re.NoError(failpoint.Disable("github.com/tikv/pd/server/rebaseErr"))
		disableLeaderFailpoints()
	}
	defer disableFailpoints()
	// Exit the PD leader loop to trigger the rebase error without directly transferring etcd leadership.
	// Trying to get TSO should fail with "not leader" error.
	getTSError := func() error {
		_, _, err := pdClient.GetTS(ctx)
		return err
	}
	testutil.Eventually(re, func() bool {
		err := getTSError()
		return err != nil && strings.Contains(err.Error(), "not leader")
	}, testutil.WithWaitFor(3*time.Second), testutil.WithTickInterval(20*time.Millisecond))
	disableLeaderFailpoints()
	for range 10 {
		re.ErrorContains(getTSError(), "not leader")
	}
	disableFailpoints()
	// The TSO should be eventually available.
	testutil.Eventually(re, func() bool {
		_, _, err := pdClient.GetTS(ctx)
		return err == nil
	}, testutil.WithWaitFor(30*time.Second), testutil.WithTickInterval(100*time.Millisecond))
}

func (suite *tsoClientTestSuite) TestRetryGetTSNotLeader() {
	re := suite.Require()
	pdClient := suite.clients[0]
	re.NoError(failpoint.Enable("github.com/tikv/pd/client/mockMaxTSORetryTimes", "return(2000)"))
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/member/skipCampaignLeaderCheck", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/client/mockMaxTSORetryTimes"))
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/member/skipCampaignLeaderCheck"))
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	ctx1, cancel1 := context.WithCancel(suite.ctx)
	var lastTS uint64
	go func(client pd.Client) {
		defer wg.Done()
		for {
			select {
			case <-ctx1.Done():
				return
			default:
			}
			physical, logical, err := client.GetTS(ctx1)
			if err != nil {
				re.ErrorContains(err, context.Canceled.Error())
				continue
			}
			ts := tsoutil.ComposeTS(physical, logical)
			re.Less(lastTS, ts)
			lastTS = ts
		}
	}(pdClient)

	for range 5 {
		time.Sleep(time.Second)
		err := suite.pdLeaderServer.ResignLeaderWithRetry()
		re.NoError(err)
		leaderName := suite.cluster.WaitLeader()
		re.NotEmpty(leaderName)
		suite.pdLeaderServer = suite.cluster.GetServer(leaderName)
	}

	cancel1()
	wg.Wait()
	// Make sure the lastTS is not empty
	re.NotZero(lastTS)
}

func (suite *tsoClientTestSuite) TestGetTSRetry() {
	re := suite.Require()
	pdClient := suite.clients[0]

	re.NoError(failpoint.Enable("github.com/tikv/pd/client/checkRetry", "return(1)"))
	_, _, err := pdClient.GetTS(suite.ctx)
	re.NoError(err)
	re.NoError(failpoint.Disable("github.com/tikv/pd/client/checkRetry"))
}

func (suite *tsoClientTestSuite) TestTSOServiceDiscovery() {
	// This test checks the behavior of PD service discovery.
	if suite.legacy {
		suite.T().Skip("skipping test in legacy mode")
	}
	re := suite.Require()
	pdClient := suite.clients[0]

	physical, logical, err := pdClient.GetTS(suite.ctx)
	re.NoError(err)
	ts := tsoutil.ComposeTS(physical, logical)
	re.NotEmpty(ts)
	checkServiceDiscovery(re, pdClient, 3)

	_, cleanup2 := tests.StartSingleTSOTestServer(suite.ctx, re, suite.pdLeaderServer.GetAddr(), tempurl.Alloc())
	checkServiceDiscovery(re, pdClient, 4)
	cleanup2()
	checkServiceDiscovery(re, pdClient, 3)
}

// When we upgrade the PD cluster, there may be a period of time that the old and new PDs are running at the same time.
// So we need another cluster to run this test.
func TestMixedTSODeployment(t *testing.T) {
	re := require.New(t)

	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/fastUpdatePhysicalInterval", "return(true)"))
	re.NoError(failpoint.Enable("github.com/tikv/pd/client/servicediscovery/skipUpdateServiceMode", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/tso/fastUpdatePhysicalInterval"))
		re.NoError(failpoint.Disable("github.com/tikv/pd/client/servicediscovery/skipUpdateServiceMode"))
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	re.NoError(err)

	leaderServer := cluster.GetServer(cluster.WaitLeader())
	re.NotNil(leaderServer)
	backendEndpoints := leaderServer.GetAddr()

	pdSvr, err := cluster.Join(ctx)
	re.NoError(err)
	err = pdSvr.Run()
	re.NoError(err)

	s, cleanup := tests.StartSingleTSOTestServer(ctx, re, backendEndpoints, tempurl.Alloc())
	defer cleanup()
	tests.WaitForPrimaryServing(re, map[string]bs.Server{s.GetAddr(): s})

	ctx1, cancel1 := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	checkTSO(ctx1, re, &wg, backendEndpoints)
	for range 2 {
		n := rand.IntN(2) + 1
		time.Sleep(time.Duration(n) * time.Second)
		err = leaderServer.ResignLeaderWithRetry()
		re.NoError(err)
		leaderServer = cluster.GetServer(cluster.WaitLeader())
		re.NotNil(leaderServer)
	}
	cancel1()
	wg.Wait()
}

// TestUpgradingPDAndTSOClusters tests the scenario that after we restart the PD cluster
// then restart the TSO cluster, the TSO service can still serve TSO requests normally.
// So we need another cluster to run this test.
func TestUpgradingPDAndTSOClusters(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create an PD cluster which has 3 servers
	pdCluster, err := tests.NewTestClusterWithKeyspaceGroup(ctx, 3)
	re.NoError(err)
	defer pdCluster.Destroy()
	err = pdCluster.RunInitialServers()
	re.NoError(err)
	leaderName := pdCluster.WaitLeader()
	re.NotEmpty(leaderName)
	pdLeader := pdCluster.GetServer(leaderName)
	backendEndpoints := pdLeader.GetAddr()

	// Create a PD client in microservice env to let the PD leader to forward requests to the TSO cluster.
	re.NoError(failpoint.Enable("github.com/tikv/pd/client/servicediscovery/usePDServiceMode", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/client/servicediscovery/usePDServiceMode"))
	}()
	pdClient, err := pd.NewClientWithContext(ctx,
		caller.TestComponent,
		[]string{backendEndpoints}, pd.SecurityOption{}, opt.WithMaxErrorRetry(1))
	re.NoError(err)
	defer pdClient.Close()

	// Create a TSO cluster which has 2 servers
	tsoCluster, err := tests.NewTestTSOCluster(ctx, 2, backendEndpoints)
	re.NoError(err)
	defer func() {
		if tsoCluster != nil {
			tsoCluster.Destroy()
		}
	}()
	tsoCluster.WaitForDefaultPrimaryServing(re)
	// The TSO service should be eventually healthy
	utils.WaitForTSOServiceAvailable(ctx, re, pdClient)

	// Restart the API cluster
	_, err = tests.RestartTestPDCluster(ctx, pdCluster)
	re.NoError(err)
	// The TSO service should be eventually healthy
	utils.WaitForTSOServiceAvailable(ctx, re, pdClient)

	// Restart the TSO cluster
	tsoCluster, err = tests.RestartTestTSOCluster(ctx, tsoCluster)
	re.NoError(err)
	tsoCluster.WaitForDefaultPrimaryServing(re)
	// The TSO service should be eventually healthy
	utils.WaitForTSOServiceAvailable(ctx, re, pdClient)
}

// TestDynamicSwitchingPDToTSO tests that when dynamic switching is enabled and a TSO
// microservice starts, PD stops serving TSO locally, sets ServiceIndependent,
// and the TSO microservice serves timestamps successfully.
//
// NOTE: This test uses the usePDServiceMode failpoint to pin the client to PD_SVC_MODE,
// so it only validates server-side dynamic switching behavior. Client-side service-mode
// discovery is covered by TestTSOServiceSwitch in tests/integrations/mcs/tso/server_test.go.
func TestDynamicSwitchingPDToTSO(t *testing.T) {
	re := require.New(t)
	enableDynamicSwitchingTestFailpoints(t, re)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create PD cluster with dynamic switching enabled.
	pdCluster, err := tests.NewTestClusterWithKeyspaceGroup(ctx, 1, func(conf *config.Config, _ string) {
		conf.Microservice.EnableTSODynamicSwitching = true
	})
	re.NoError(err)
	defer pdCluster.Destroy()
	re.NoError(pdCluster.RunInitialServers())
	leaderName := pdCluster.WaitLeader()
	re.NotEmpty(leaderName)
	pdLeader := pdCluster.GetServer(leaderName)
	re.NoError(pdLeader.BootstrapCluster())
	backendEndpoints := pdLeader.GetAddr()

	// Create a PD client.
	pdClient, err := pd.NewClientWithContext(ctx,
		caller.TestComponent,
		[]string{backendEndpoints}, pd.SecurityOption{}, opt.WithMaxErrorRetry(1))
	re.NoError(err)
	defer pdClient.Close()

	// Without TSO microservice, PD should serve TSO locally.
	var globalLastTS uint64
	waitAndCheckTSOMonotonic(ctx, re, pdClient, &globalLastTS)
	re.False(pdLeader.GetServer().IsServiceIndependent(mcsconst.TSOServiceName))

	// Start TSO microservice.
	tsoCluster, err := tests.NewTestTSOCluster(ctx, 1, backendEndpoints)
	re.NoError(err)
	defer tsoCluster.Destroy()
	tsoCluster.WaitForDefaultPrimaryServing(re)

	// ServiceIndependent should be set and TSO should remain monotonically increasing.
	testutil.Eventually(re, func() bool {
		return pdLeader.GetServer().IsServiceIndependent(mcsconst.TSOServiceName)
	})
	waitAndCheckTSOMonotonic(ctx, re, pdClient, &globalLastTS)
}

// TestDynamicSwitchingRoundTrip tests that PD can repeatedly switch between serving
// TSO locally and forwarding to a TSO microservice while timestamps remain monotonically
// increasing across every transition.
//
// NOTE: This test uses the usePDServiceMode failpoint to pin the client to PD_SVC_MODE,
// so it only validates server-side dynamic switching behavior. Client-side service-mode
// discovery is covered by TestTSOServiceSwitch in tests/integrations/mcs/tso/server_test.go.
func TestDynamicSwitchingRoundTrip(t *testing.T) {
	re := require.New(t)
	enableDynamicSwitchingTestFailpoints(t, re)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create PD cluster with dynamic switching enabled.
	pdCluster, err := tests.NewTestClusterWithKeyspaceGroup(ctx, 1, func(conf *config.Config, _ string) {
		conf.Microservice.EnableTSODynamicSwitching = true
	})
	re.NoError(err)
	defer pdCluster.Destroy()
	re.NoError(pdCluster.RunInitialServers())
	leaderName := pdCluster.WaitLeader()
	re.NotEmpty(leaderName)
	pdLeader := pdCluster.GetServer(leaderName)
	re.NoError(pdLeader.BootstrapCluster())
	backendEndpoints := pdLeader.GetAddr()

	// Create a PD client.
	pdClient, err := pd.NewClientWithContext(ctx,
		caller.TestComponent,
		[]string{backendEndpoints}, pd.SecurityOption{}, opt.WithMaxErrorRetry(1))
	re.NoError(err)
	defer pdClient.Close()

	// Initially, PD should serve TSO locally.
	var globalLastTS uint64
	waitAndCheckTSOMonotonic(ctx, re, pdClient, &globalLastTS)
	re.False(pdLeader.GetServer().IsServiceIndependent(mcsconst.TSOServiceName))

	// Start the first TSO microservice and wait for transition.
	tsoCluster, err := tests.NewTestTSOCluster(ctx, 1, backendEndpoints)
	re.NoError(err)
	defer tsoCluster.Destroy()
	tsoCluster.WaitForDefaultPrimaryServing(re)
	testutil.Eventually(re, func() bool {
		return pdLeader.GetServer().IsServiceIndependent(mcsconst.TSOServiceName)
	})
	waitAndCheckTSOMonotonic(ctx, re, pdClient, &globalLastTS)

	// Destroy TSO microservice.
	tsoCluster.Destroy()

	// PD should resume serving TSO locally with monotonically increasing timestamps.
	testutil.Eventually(re, func() bool {
		return !pdLeader.GetServer().IsServiceIndependent(mcsconst.TSOServiceName)
	})
	waitAndCheckTSOMonotonic(ctx, re, pdClient, &globalLastTS)

	// Start a fresh TSO microservice and verify that PD can switch away from its
	// fallback allocator again without breaking timestamp monotonicity.
	tsoCluster, err = tests.NewTestTSOCluster(ctx, 1, backendEndpoints)
	re.NoError(err)
	defer tsoCluster.Destroy()
	tsoCluster.WaitForDefaultPrimaryServing(re)
	testutil.Eventually(re, func() bool {
		return pdLeader.GetServer().IsServiceIndependent(mcsconst.TSOServiceName)
	})
	waitAndCheckTSOMonotonic(ctx, re, pdClient, &globalLastTS)
}

// TestDynamicSwitchingWithLeaderTransfer tests that TSO requests recover after
// PD leader transfers when a TSO microservice is active, and timestamps remain
// monotonically increasing across transfers.
//
// NOTE: This test uses the usePDServiceMode failpoint to pin the client to PD_SVC_MODE,
// so it only validates server-side dynamic switching behavior. Client-side service-mode
// discovery is covered by TestTSOServiceSwitch in tests/integrations/mcs/tso/server_test.go.
func TestDynamicSwitchingWithLeaderTransfer(t *testing.T) {
	re := require.New(t)
	enableDynamicSwitchingTestFailpoints(t, re)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create a 3-node PD cluster with dynamic switching enabled.
	pdCluster, err := tests.NewTestClusterWithKeyspaceGroup(ctx, 3, func(conf *config.Config, _ string) {
		conf.Microservice.EnableTSODynamicSwitching = true
	})
	re.NoError(err)
	defer pdCluster.Destroy()
	re.NoError(pdCluster.RunInitialServers())
	leaderName := pdCluster.WaitLeader()
	re.NotEmpty(leaderName)
	pdLeader := pdCluster.GetServer(leaderName)
	re.NoError(pdLeader.BootstrapCluster())
	backendEndpoints := pdLeader.GetAddr()

	// PD client.
	pdClient, err := pd.NewClientWithContext(ctx,
		caller.TestComponent,
		[]string{backendEndpoints}, pd.SecurityOption{}, opt.WithMaxErrorRetry(1))
	re.NoError(err)
	defer pdClient.Close()

	// PD should start serving TSO.
	var globalLastTS uint64
	waitAndCheckTSOMonotonic(ctx, re, pdClient, &globalLastTS)

	// Start TSO microservice and wait for ServiceIndependent.
	tsoCluster, err := tests.NewTestTSOCluster(ctx, 1, backendEndpoints)
	re.NoError(err)
	defer tsoCluster.Destroy()
	tsoCluster.WaitForDefaultPrimaryServing(re)
	testutil.Eventually(re, func() bool {
		return pdLeader.GetServer().IsServiceIndependent(mcsconst.TSOServiceName)
	})

	// Transfer the leader twice and verify TSO requests recover with monotonic timestamps.
	for range 2 {
		oldLeaderName := pdCluster.WaitLeader()
		re.NotEmpty(oldLeaderName)
		newLeaderName := transferPDLeaderToNextServer(t, re, pdCluster, oldLeaderName)
		re.NotEqual(oldLeaderName, newLeaderName)

		// ServiceIndependent must remain set after leader resignation.
		newLeader := pdCluster.GetServer(newLeaderName)
		testutil.Eventually(re, func() bool {
			return newLeader.GetServer().IsServiceIndependent(mcsconst.TSOServiceName)
		})
		// The existing client should recover and remain monotonic after each leader transfer.
		waitAndCheckTSOMonotonic(ctx, re, pdClient, &globalLastTS)

		// A client initialized from the new leader should also use a working TSO
		// forwarding path and preserve monotonicity with the existing client.
		newLeaderClient, err := pd.NewClientWithContext(ctx,
			caller.TestComponent,
			[]string{newLeader.GetAddr()}, pd.SecurityOption{}, opt.WithMaxErrorRetry(1))
		re.NoError(err)
		waitAndCheckTSOMonotonic(ctx, re, newLeaderClient, &globalLastTS)
		newLeaderClient.Close()
	}
}

// TestDynamicSwitchingWithLeaderFailover tests that TSO requests recover after
// the current PD leader stops while a TSO microservice is active, and timestamps
// remain monotonically increasing across the failover.
//
// NOTE: This test uses the usePDServiceMode failpoint to pin the client to PD_SVC_MODE,
// so it only validates server-side dynamic switching behavior. Client-side service-mode
// discovery is covered by TestTSOServiceSwitch in tests/integrations/mcs/tso/server_test.go.
func TestDynamicSwitchingWithLeaderFailover(t *testing.T) {
	re := require.New(t)
	enableDynamicSwitchingTestFailpoints(t, re)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create a 3-node PD cluster with dynamic switching enabled.
	pdCluster, err := tests.NewTestClusterWithKeyspaceGroup(ctx, 3, func(conf *config.Config, _ string) {
		conf.Microservice.EnableTSODynamicSwitching = true
	})
	re.NoError(err)
	defer pdCluster.Destroy()
	re.NoError(pdCluster.RunInitialServers())
	leaderName := pdCluster.WaitLeader()
	re.NotEmpty(leaderName)
	pdLeader := pdCluster.GetServer(leaderName)
	re.NoError(pdLeader.BootstrapCluster())

	// Use all PD endpoints so the failover test focuses on server-side recovery
	// instead of depending on a single bootstrap endpoint remaining available.
	pdEndpoints := make([]string, 0, len(pdCluster.GetServers()))
	for _, server := range pdCluster.GetServers() {
		pdEndpoints = append(pdEndpoints, server.GetAddr())
	}
	sort.Strings(pdEndpoints)
	backendEndpoints := strings.Join(pdEndpoints, ",")

	pdClient, err := pd.NewClientWithContext(ctx,
		caller.TestComponent,
		pdEndpoints, pd.SecurityOption{}, opt.WithMaxErrorRetry(1))
	re.NoError(err)
	defer pdClient.Close()

	// Start the TSO microservice and establish the pre-failover timestamp baseline.
	tsoCluster, err := tests.NewTestTSOCluster(ctx, 1, backendEndpoints)
	re.NoError(err)
	defer tsoCluster.Destroy()
	tsoCluster.WaitForDefaultPrimaryServing(re)

	oldLeaderName := pdCluster.WaitLeader()
	re.NotEmpty(oldLeaderName)
	oldLeader := pdCluster.GetServer(oldLeaderName)
	testutil.Eventually(re, func() bool {
		return oldLeader.GetServer().IsServiceIndependent(mcsconst.TSOServiceName)
	})
	var globalLastTS uint64
	waitAndCheckTSOMonotonic(ctx, re, pdClient, &globalLastTS)

	// Follow the repository's established full-stack failover pattern: stop the
	// current PD leader without choosing its successor, then wait for a new leader.
	re.NoError(oldLeader.Stop())
	newLeaderName := pdCluster.WaitLeader()
	re.NotEmpty(newLeaderName)
	re.NotEqual(oldLeaderName, newLeaderName)
	newLeader := pdCluster.GetServer(newLeaderName)

	// The new PD leader must retain independent TSO mode before serving through
	// the existing client. The first successful timestamp after failover is also
	// checked against the pre-failover global timestamp.
	testutil.Eventually(re, func() bool {
		return newLeader.GetServer().IsServiceIndependent(mcsconst.TSOServiceName)
	})
	tsoCluster.WaitForDefaultPrimaryServing(re)
	waitAndCheckTSOMonotonic(ctx, re, pdClient, &globalLastTS)

	// A client initialized after the failover must use the new PD leader's
	// forwarding path and share the same global monotonicity baseline.
	newLeaderClient, err := pd.NewClientWithContext(ctx,
		caller.TestComponent,
		[]string{newLeader.GetAddr()}, pd.SecurityOption{}, opt.WithMaxErrorRetry(1))
	re.NoError(err)
	defer newLeaderClient.Close()
	waitAndCheckTSOMonotonic(ctx, re, newLeaderClient, &globalLastTS)
}

func transferPDLeaderToNextServer(
	t *testing.T, re *require.Assertions, pdCluster *tests.TestCluster, oldLeaderName string,
) string {
	t.Helper()

	oldLeader := pdCluster.GetServer(oldLeaderName)
	serverNames := make([]string, 0, len(pdCluster.GetServers()))
	for name := range pdCluster.GetServers() {
		serverNames = append(serverNames, name)
	}
	sort.Strings(serverNames)
	re.Greater(len(serverNames), 1)
	oldLeaderIndex := sort.SearchStrings(serverNames, oldLeaderName)
	re.Less(oldLeaderIndex, len(serverNames))
	re.Equal(oldLeaderName, serverNames[oldLeaderIndex])
	newLeaderName := serverNames[(oldLeaderIndex+1)%len(serverNames)]
	newLeader := pdCluster.GetServer(newLeaderName)

	oldLeaderID := oldLeader.GetServerID()
	newLeaderID := newLeader.GetServerID()
	testutil.Eventually(re, func() bool {
		etcdLeaderID, err := oldLeader.GetEtcdLeaderID()
		if err != nil {
			return false
		}
		if etcdLeaderID == newLeaderID {
			return true
		}
		if etcdLeaderID != oldLeaderID {
			return false
		}
		return oldLeader.MoveEtcdLeader(oldLeaderID, newLeaderID) == nil
	})

	testutil.Eventually(re, newLeader.IsLeader)
	re.Equal(newLeaderName, pdCluster.WaitLeader())
	testutil.Eventually(re, func() bool {
		etcdLeaderID, err := newLeader.GetEtcdLeaderID()
		return err == nil && etcdLeaderID == newLeaderID
	})
	return newLeaderName
}

func enableDynamicSwitchingTestFailpoints(t *testing.T, re *require.Assertions) {
	t.Helper()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/fastUpdatePhysicalInterval", "return(true)"))
	t.Cleanup(func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/tso/fastUpdatePhysicalInterval"))
	})
	re.NoError(failpoint.Enable("github.com/tikv/pd/client/servicediscovery/usePDServiceMode", "return(true)"))
	t.Cleanup(func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/client/servicediscovery/usePDServiceMode"))
	})
}

const tsoMonotonicitySuccessCount = 10

// waitAndCheckTSOMonotonic waits for TSO to become available and then verifies that
// consecutive successful TSO requests return globally increasing timestamps relative to
// *globalLastTS. Unlike calling WaitForTSOServiceAvailable + a separate
// monotonicity check, this function validates every successful GetTS (including the first
// one after a switchover) against globalLastTS, so a regression at the exact transition
// boundary cannot slip through.
func waitAndCheckTSOMonotonic(
	ctx context.Context, re *require.Assertions, pdClient pd.Client, globalLastTS *uint64,
) {
	var (
		successes       int
		monotonicityErr error
	)
	testutil.Eventually(re, func() bool {
		physical, logical, err := pdClient.GetTS(ctx)
		if err != nil {
			successes = 0
			return false
		}
		ts := tsoutil.ComposeTS(physical, logical)
		if ts <= *globalLastTS {
			monotonicityErr = fmt.Errorf(
				"TSO is not globally increasing: last %d, current %d", *globalLastTS, ts)
			return true
		}
		*globalLastTS = ts
		successes++
		return successes >= tsoMonotonicitySuccessCount
	})
	re.NoError(monotonicityErr)
}

func checkTSO(
	ctx context.Context, re *require.Assertions, wg *sync.WaitGroup, backendEndpoints string,
) {
	wg.Add(tsoRequestConcurrencyNumber)
	for range tsoRequestConcurrencyNumber {
		go func() {
			defer wg.Done()
			cli := utils.SetupClientWithAPIContext(ctx, re, pd.NewAPIContextV1(), strings.Split(backendEndpoints, ","))
			defer cli.Close()
			var ts, lastTS uint64
			for {
				select {
				case <-ctx.Done():
					// Make sure the lastTS is not empty
					re.NotEmpty(lastTS)
					return
				default:
				}
				physical, logical, err := cli.GetTS(ctx)
				// omit the error check since there are many kinds of errors
				if err != nil {
					continue
				}
				ts = tsoutil.ComposeTS(physical, logical)
				re.Less(lastTS, ts)
				lastTS = ts
			}
		}()
	}
}

func checkServiceDiscovery(re *require.Assertions, client pd.Client, urlsLen int) {
	inner, ok := client.(interface{ GetTSOServiceDiscovery() sd.ServiceDiscovery })
	if ok {
		tsoDiscovery := inner.GetTSOServiceDiscovery()
		err := tsoDiscovery.CheckMemberChanged()
		re.NoError(err)
		if tsoDiscovery != nil {
			urls := tsoDiscovery.(interface{ GetURLs() []string }).GetURLs()
			re.Len(urls, urlsLen)
		}
	}
}

// Race condition test between TSO request dispatcher and background connection updater

// Connection updater view:
// 1.1. Builds a stream A for TSO primary upon initialization.
// 1.2. Store the stream A into connection context manager.

// Request dispatcher view:
// 2.1. Upon no stream ready, builds a stream B for TSO primary.
// 2.2. Process the requests via stream B.

// Race timeline:
// 1.1. Creates stream A but haven't registered it.
// 2.1. Creates stream B and registers it to the connection context manager.
// 1.2. Registered stream A and cancelled the context of stream B.
// 2.2. Observes canceled context of stream B.
func (suite *tsoClientTestSuite) TestTSOStreamSetupRace() {
	if !suite.legacy {
		suite.T().Skip("race is in tryConnectToTSO, which is the non-proxy path")
	}
	re := suite.Require()

	const tsoFailpointPrefix = "github.com/tikv/pd/client/clients/tso/"

	backgroundBeforeStore := make(chan struct{})
	releaseBackgroundStore := make(chan struct{})

	re.NoError(failpoint.EnableCall(tsoFailpointPrefix+"pauseBeforeBackgroundStoreTSOLeaderStream", func() {
		log.Info("[tso race] 1.1.1 pause background goroutine before CleanAllAndStore")
		close(backgroundBeforeStore)
		<-releaseBackgroundStore
		log.Info("[tso race] 1.2.1 released pause for CleanAllAndStore")
	}))
	defer func() {
		re.NoError(failpoint.Disable(tsoFailpointPrefix + "pauseBeforeBackgroundStoreTSOLeaderStream"))
	}()

	ctx, cancel := context.WithCancel(suite.ctx)
	pdClient, err := pd.NewClientWithContext(ctx, caller.TestComponent, suite.getBackendEndpoints(), pd.SecurityOption{})
	re.NoError(err)

	safeClose := func(ch chan struct{}) {
		select {
		case <-ch:
		default:
			close(ch)
		}
	}
	defer func() {
		safeClose(releaseBackgroundStore)
		pdClient.Close()
		cancel()
	}()

	waitFor := func(ch <-chan struct{}, desc string) {
		select {
		case <-ch:
			log.Info("[tso race] " + desc)
		case <-time.After(30 * time.Second):
			re.Failf("timed out", "timed out waiting for: %s", desc)
		}
	}
	waitFor(backgroundBeforeStore, "1.1.2 background goroutine reaching CleanAllAndStore")

	re.NoError(failpoint.Disable(tsoFailpointPrefix + "pauseBeforeBackgroundStoreTSOLeaderStream"))

	requestAttachedToStream := make(chan struct{})
	releaseRequest := make(chan struct{})
	defer func() {
		safeClose(releaseRequest)
	}()

	re.NoError(failpoint.EnableCall(tsoFailpointPrefix+"pauseAfterTSORequestAttachedToStream", func() {
		log.Info("[tso race] 2.1.1 pausing tso request after attached to stream")
		close(requestAttachedToStream)
		<-releaseRequest
		log.Info("[tso race] 2.2.2 tso request released")
	}))
	defer func() {
		re.NoError(failpoint.Disable(tsoFailpointPrefix + "pauseAfterTSORequestAttachedToStream"))
	}()

	errCh := make(chan error, 1)
	go func() {
		_, _, err := pdClient.GetTS(context.Background())
		errCh <- err
	}()

	waitFor(requestAttachedToStream, "2.1.2 request attached to dispatcher's stream")

	re.NoError(failpoint.EnableCall(tsoFailpointPrefix+"notifyAfterBackgroundStoreTSOLeaderStream", func() {
		log.Info("[tso race] 1.2.2 background goroutine finished CleanAllAndStore")
		close(releaseRequest)
		log.Info("[tso race] 2.2.1 releasing pause for TSO request")
	}))
	defer func() {
		re.NoError(failpoint.Disable(tsoFailpointPrefix + "notifyAfterBackgroundStoreTSOLeaderStream"))
	}()

	close(releaseBackgroundStore)
	select {
	case err := <-errCh:
		re.NoError(err)
	case <-time.After(30 * time.Second):
		re.Failf("timed out", "GetTS has not returned")
	}
}
