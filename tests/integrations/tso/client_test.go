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
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"

	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/clients/tso"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/caller"
	sd "github.com/tikv/pd/client/servicediscovery"
	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/keyspace/constant"
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
	goleak.VerifyTestMain(testutil.WaitForEtcdConnections(m), testutil.LeakOptions...)
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
	as := assert.New(suite.T())
	var wg sync.WaitGroup
	wg.Add(tsoRequestConcurrencyNumber * len(suite.clients))
	for range tsoRequestConcurrencyNumber {
		for _, client := range suite.clients {
			go func(client pd.Client) {
				defer wg.Done()
				var lastTS uint64
				for range tsoRequestRound {
					physical, logical, err := client.GetTS(suite.ctx)
					if !as.NoError(err) {
						return
					}
					ts := tsoutil.ComposeTS(physical, logical)
					if !as.Less(lastTS, ts) {
						return
					}
					lastTS = ts
				}
			}(client)
		}
	}
	wg.Wait()
}

func (suite *tsoClientTestSuite) TestGetTSAsync() {
	as := assert.New(suite.T())
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
				for _, tsFuture := range slices.Backward(tsFutures) {
					physical, logical, err := tsFuture.Wait()
					if !as.NoError(err) {
						return
					}
					ts := tsoutil.ComposeTS(physical, logical)
					if !as.Greater(lastTS, ts) {
						return
					}
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
	re.False(slices.Contains(suite.keyspaceIDs, keyspaceID))
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
	as := assert.New(suite.T())
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
					if !as.NoError(err) {
						return
					}
					minTS := tsoutil.ComposeTS(physical, logical)
					if !as.Less(lastMinTS, minTS) {
						return
					}
					lastMinTS = minTS

					// Now we check whether the returned ts is the minimum one
					// among all keyspace groups, i.e., the returned ts is
					// less than the new timestamps of all keyspace groups.
					for _, client := range suite.clients {
						physical, logical, err := client.GetTS(suite.ctx)
						if !as.NoError(err) {
							return
						}
						ts := tsoutil.ComposeTS(physical, logical)
						if !as.Less(minTS, ts) {
							return
						}
					}
				}
			}(client)
		}
	}
	wg.Wait()

	re.NoError(failpoint.Enable("github.com/tikv/pd/client/servicediscovery/unreachableNetwork1", "return(true)"))
	testutil.Eventually(re, func() bool {
		clients := suite.clients[0].GetServiceDiscovery().GetAllServiceClients()
		if len(clients) == 0 {
			return false
		}
		for _, client := range clients {
			if client.Available() {
				return false
			}
		}
		return true
	})
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
	as := assert.New(suite.T())
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
			wg := &sync.WaitGroup{}
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
					if !testutil.EventuallyWithAssert(as, func() bool {
						return suite.tsoCluster.GetPrimaryServer(keyspaceID, keyspaceGroupID) != nil
					}, testutil.WithWaitFor(30*time.Second), testutil.WithTickInterval(100*time.Millisecond)) {
						return
					}
					err := suite.tsoCluster.ResignPrimary(keyspaceID, keyspaceGroupID)
					if !as.NoError(err) {
						return
					}
					if !testutil.EventuallyWithAssert(as, func() bool {
						return suite.tsoCluster.GetPrimaryServer(keyspaceID, keyspaceGroupID) != nil
					}, testutil.WithWaitFor(30*time.Second), testutil.WithTickInterval(100*time.Millisecond)) {
						return
					}
				}(keyspaceID)
			}
			wg.Wait()
		} else {
			err := suite.cluster.ResignLeader()
			if !as.NoError(err) { //nolint:testifylint // parallelAct runs in a worker goroutine, where require is unsafe.
				return
			}
			suite.cluster.WaitLeader()
		}
		time.Sleep(time.Duration(n) * time.Second)
	}

	utils.CheckMultiKeyspacesTSO(suite.ctx, as, suite.clients, parallelAct)
}

func (suite *tsoClientTestSuite) TestRandomShutdown() {
	as := assert.New(suite.T())
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
			keyspaceID := keyspace.GetBootstrapKeyspaceID()
			primary := suite.tsoCluster.GetPrimaryServer(keyspaceID, constant.DefaultKeyspaceGroupID)
			if !testutil.EventuallyWithAssert(as, func() bool {
				primary = suite.tsoCluster.GetPrimaryServer(keyspaceID, constant.DefaultKeyspaceGroupID)
				return primary != nil
			}, testutil.WithWaitFor(30*time.Second), testutil.WithTickInterval(100*time.Millisecond)) {
				return
			}
			closedTSOAddr = primary.GetAddr()
			primary.Close()
			if !testutil.EventuallyWithAssert(as, func() bool {
				return suite.tsoCluster.GetPrimaryServer(keyspaceID, constant.DefaultKeyspaceGroupID) != nil
			}, testutil.WithWaitFor(30*time.Second), testutil.WithTickInterval(100*time.Millisecond)) {
				return
			}
			for _, client := range suite.clients {
				if !testutil.EventuallyWithAssert(as, func() bool {
					_, _, err := client.GetTS(suite.ctx)
					return err == nil
				}) {
					return
				}
			}
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

	utils.CheckMultiKeyspacesTSO(suite.ctx, as, suite.clients, parallelAct)
	if !suite.legacy {
		re.NotEmpty(closedTSOAddr)
		return
	}
	suite.TearDownSuite()
	suite.SetupSuite()
}

func (suite *tsoClientTestSuite) TestGetTSWhileResettingTSOClient() {
	as := assert.New(suite.T())
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
						if !as.ErrorContains(err, context.Canceled.Error()) {
							return
						}
					} else {
						ts := tsoutil.ComposeTS(physical, logical)
						if !as.Less(lastTS, ts) {
							return
						}
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
	servers := cluster.GetServers()
	memberIDs := make([]string, 0, len(servers))
	for _, server := range servers {
		memberIDs = append(memberIDs, strconv.FormatUint(server.GetServerID(), 10))
	}
	memberIDList := strings.Join(memberIDs, ",")

	const initializationHook = "github.com/tikv/pd/server/cluster/afterTSOInitialized"
	initialized := make(chan *tests.TestServer, 1)
	release := make(chan struct{})
	var initializedOnce sync.Once
	resume := sync.OnceFunc(func() { close(release) })
	re.NoError(failpoint.EnableCall(initializationHook, func(id uint64) {
		for _, server := range servers {
			if server.GetServerID() == id {
				initializedOnce.Do(func() { initialized <- server })
				<-release
				return
			}
		}
	}))
	defer func() {
		resume()
		re.NoError(failpoint.Disable(initializationHook))
	}()
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/rebaseErr", fmt.Sprintf("return(\"%s\")", memberIDList)))
	re.NoError(failpoint.Enable("github.com/tikv/pd/client/skipRetry", "return(true)"))
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/exitCampaignLeader", fmt.Sprintf("return(\"%d\")", memberID)))
	disableFailpoints := sync.OnceFunc(func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/client/skipRetry"))
		re.NoError(failpoint.Disable("github.com/tikv/pd/server/rebaseErr"))
		re.NoError(failpoint.Disable("github.com/tikv/pd/server/exitCampaignLeader"))
	})
	defer disableFailpoints()
	var initializingServer *tests.TestServer
	select {
	case initializingServer = <-initialized:
	case <-time.After(30 * time.Second):
		suite.T().Fatal("leader initialization did not reach the TSO checkpoint")
	}
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/exitCampaignLeader"))
	svr := initializingServer.GetServer()
	re.NotNil(svr.GetMember().GetLeadership().GetLease())
	re.True(svr.GetTSOAllocator().IsInitialize())
	re.False(svr.IsServing())
	// The existing client may use the new term before ID rebasing completes.
	getTSError := func() error {
		_, _, err := pdClient.GetTS(ctx)
		return err
	}
	testutil.Eventually(re, func() bool {
		return getTSError() == nil
	}, testutil.WithWaitFor(5*time.Second))
	// Let this term fail rebasing, but prevent another term from serving TSO
	// during the rejection assertions below.
	const campaignHook = "github.com/tikv/pd/server/leaderLoopCheckAgain"
	re.NoError(failpoint.Enable(campaignHook, fmt.Sprintf("return(\"%s\")", memberIDList)))
	defer func() { re.NoError(failpoint.Disable(campaignHook)) }()
	resume()
	testutil.Eventually(re, func() bool {
		return svr.GetMember().GetLeadership().GetLease() == nil &&
			!svr.GetTSOAllocator().IsInitialize()
	})
	for range 10 {
		re.ErrorContains(getTSError(), "not leader")
	}
	disableFailpoints()
	re.NoError(failpoint.Disable(campaignHook))
	// The TSO should be eventually available.
	testutil.Eventually(re, func() bool {
		_, _, err := pdClient.GetTS(ctx)
		return err == nil
	}, testutil.WithWaitFor(30*time.Second), testutil.WithTickInterval(100*time.Millisecond))
}

func (suite *tsoClientTestSuite) TestRetryGetTSNotLeader() {
	as := assert.New(suite.T())
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
				if !as.ErrorContains(err, context.Canceled.Error()) {
					return
				}
				continue
			}
			ts := tsoutil.ComposeTS(physical, logical)
			if !as.Less(lastTS, ts) {
				return
			}
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

	ctx := t.Context()
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
	ctx := t.Context()

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

// TestTSOStreamSetupRace verifies that requests wait for the background updater
// to publish its initial stream instead of building a competing stream.
func (suite *tsoClientTestSuite) TestTSOStreamSetupRace() {
	if !suite.legacy {
		suite.T().Skip("race is in tryConnectToTSO, which is the non-proxy path")
	}
	re := suite.Require()
	const tsoFailpointPrefix = "github.com/tikv/pd/client/clients/tso/"

	backgroundBeforeStore := make(chan struct{})
	releaseBackgroundStore := make(chan struct{})
	releaseBackground := sync.OnceFunc(func() { close(releaseBackgroundStore) })
	defer releaseBackground()
	var builds atomic.Int32
	re.NoError(failpoint.EnableCall(tsoFailpointPrefix+"pauseBeforeBackgroundStoreTSOLeaderStream", func() {
		if builds.Add(1) == 1 {
			close(backgroundBeforeStore)
			<-releaseBackgroundStore
		}
	}))
	defer func() {
		re.NoError(failpoint.Disable(tsoFailpointPrefix + "pauseBeforeBackgroundStoreTSOLeaderStream"))
	}()

	backgroundStored := make(chan struct{})
	re.NoError(failpoint.EnableCall(tsoFailpointPrefix+"notifyAfterBackgroundStoreTSOLeaderStream",
		sync.OnceFunc(func() { close(backgroundStored) })))
	defer func() {
		re.NoError(failpoint.Disable(tsoFailpointPrefix + "notifyAfterBackgroundStoreTSOLeaderStream"))
	}()

	dispatcherWaiting := make(chan struct{})
	var emptyLookups atomic.Int32
	re.NoError(failpoint.EnableCall(tsoFailpointPrefix+"beforeSubscribeTSOStreamUpdates", func() {
		// A second empty lookup proves the dispatcher has scheduled recovery and
		// retried while the background stream remains unpublished.
		if emptyLookups.Add(1) == 2 {
			close(dispatcherWaiting)
		}
	}))
	defer func() {
		re.NoError(failpoint.Disable(tsoFailpointPrefix + "beforeSubscribeTSOStreamUpdates"))
	}()

	requestAttached := make(chan struct{})
	re.NoError(failpoint.EnableCall(tsoFailpointPrefix+"pauseAfterTSORequestAttachedToStream",
		sync.OnceFunc(func() { close(requestAttached) })))
	defer func() {
		re.NoError(failpoint.Disable(tsoFailpointPrefix + "pauseAfterTSORequestAttachedToStream"))
	}()

	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()
	pdClient, err := pd.NewClientWithContext(ctx, caller.TestComponent, suite.getBackendEndpoints(), pd.SecurityOption{})
	re.NoError(err)
	var wg sync.WaitGroup
	defer func() {
		releaseBackground()
		cancel()
		pdClient.Close()
		wg.Wait()
	}()

	waitFor := func(ch <-chan struct{}, desc string) {
		select {
		case <-ch:
		case <-time.After(30 * time.Second):
			re.FailNowf("timed out", "timed out waiting for: %s", desc)
		}
	}
	waitFor(backgroundBeforeStore, "background stream awaiting publication")

	type result struct {
		ts  uint64
		err error
	}
	resultCh := make(chan result, 1)
	future := pdClient.GetTSAsync(ctx)
	wg.Go(func() {
		physical, logical, err := future.Wait()
		resultCh <- result{ts: tsoutil.ComposeTS(physical, logical), err: err}
	})

	select {
	case <-dispatcherWaiting:
	case res := <-resultCh:
		re.FailNowf("request completed before background publication", "result: %+v", res)
	case <-time.After(30 * time.Second):
		re.FailNow("dispatcher did not wait for the background stream")
	}
	re.Equal(int32(1), builds.Load(), "only the updater should build the initial stream")
	select {
	case <-requestAttached:
		re.FailNow("request attached before the background stream was published")
	default:
	}
	select {
	case res := <-resultCh:
		re.FailNowf("request completed before background publication", "result: %+v", res)
	default:
	}

	releaseBackground()
	waitFor(backgroundStored, "background stream published")
	var firstTS uint64
	select {
	case res := <-resultCh:
		re.NoError(res.err)
		re.Positive(res.ts)
		firstTS = res.ts
	case <-time.After(30 * time.Second):
		re.FailNow("request did not finish after background stream publication")
	}
	waitFor(requestAttached, "request attached to the published stream")
	physical, logical, err := pdClient.GetTSAsync(ctx).Wait()
	re.NoError(err)
	re.Greater(tsoutil.ComposeTS(physical, logical), firstTS)
	re.Equal(int32(1), builds.Load(), "subsequent requests should reuse the published stream")
}

func TestPDClientUsesTSODuringPDInitialization(t *testing.T) {
	re := require.New(t)
	cluster, err := tests.NewTestCluster(t.Context(), 3)
	re.NoError(err)
	t.Cleanup(cluster.Destroy)
	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())
	leader := cluster.GetLeaderServer()
	re.NoError(leader.BootstrapCluster())
	target := cluster.GetServer(cluster.GetFollower())
	re.NotNil(target)

	// Keep the default client alive across the handoff, as existing TiDB and
	// TiKV clients do during a PD upgrade. Constructing a new client also
	// initializes router metadata, which still requires full PD readiness.
	clientCtx, clientCancel := context.WithTimeout(t.Context(), time.Minute)
	t.Cleanup(clientCancel)
	client, err := pd.NewClientWithContext(clientCtx, caller.TestComponent,
		[]string{target.GetAddr()}, pd.SecurityOption{},
		opt.WithCustomTimeoutOption(time.Second), opt.WithMaxErrorRetry(1))
	re.NoError(err)
	t.Cleanup(client.Close)
	re.Equal(leader.GetAddr(), client.GetLeaderURL())
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	physical, logical, err := client.GetTS(ctx)
	cancel()
	re.NoError(err)
	previous := &pdpb.Timestamp{Physical: physical, Logical: logical}

	const hook = "github.com/tikv/pd/server/cluster/afterTSOInitialized"
	reached := make(chan struct{})
	release := make(chan struct{})
	var reachedOnce sync.Once
	resume := sync.OnceFunc(func() { close(release) })
	memberID := target.GetServerID()
	re.NoError(failpoint.EnableCall(hook, func(id uint64) {
		if id == memberID {
			reachedOnce.Do(func() { close(reached) })
			<-release
		}
	}))
	t.Cleanup(func() {
		resume()
		re.NoError(failpoint.Disable(hook))
	})
	re.NoError(leader.MoveEtcdLeader(leader.GetServerID(), target.GetServerID()))
	select {
	case <-reached:
	case <-time.After(30 * time.Second):
		t.Fatal("leader initialization did not reach the TSO checkpoint")
	}
	re.False(target.GetServer().IsServing())

	// Recovery includes discovering the new owner and reconnecting its TSO
	// stream while that owner's metadata initialization remains blocked.
	testutil.Eventually(re, func() bool {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		physical, logical, err = client.GetTS(ctx)
		return err == nil
	}, testutil.WithWaitFor(5*time.Second))
	re.Equal(target.GetAddr(), client.GetLeaderURL())
	re.Positive(physical)
	re.Positive(logical)
	next := &pdpb.Timestamp{Physical: physical, Logical: logical}
	re.Negative(tsoutil.CompareTimestamp(previous, next))
	re.False(target.GetServer().IsServing())

	resume()
	testutil.Eventually(re, target.GetServer().IsServing)
}
