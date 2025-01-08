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
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/pingcap/failpoint"

	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/clients/tso"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/caller"
	"github.com/tikv/pd/client/pkg/utils/testutil"
	sd "github.com/tikv/pd/client/servicediscovery"
	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"github.com/tikv/pd/pkg/utils/tsoutil"
	"github.com/tikv/pd/server/apiv2/handlers"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/integrations/mcs"
	handlersutil "github.com/tikv/pd/tests/server/apiv2/handlers"
)

var r = rand.New(rand.NewSource(time.Now().UnixNano()))

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

	var err error
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	if suite.legacy {
		suite.cluster, err = tests.NewTestCluster(suite.ctx, serverCount)
	} else {
		suite.cluster, err = tests.NewTestPDServiceCluster(suite.ctx, serverCount, func(conf *config.Config, _ string) {
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
		mcs.WaitForTSOServiceAvailable(suite.ctx, re, client)
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
					if server.IsKeyspaceServing(keyspaceID, keyspaceGroup.keyspaceGroupID) {
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
	suite.clients = mcs.WaitForMultiKeyspacesTSOAvailable(
		suite.ctx, re, suite.keyspaceIDs, suite.getBackendEndpoints())
	re.Equal(len(suite.keyspaceIDs), len(suite.clients))
}

func (suite *tsoClientTestSuite) TearDownTest() {
	for _, client := range suite.clients {
		client.Close()
	}
}

func (suite *tsoClientTestSuite) TearDownSuite() {
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
	client := mcs.SetupClientWithKeyspaceID(
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
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/fastUpdatePhysicalInterval", "return(true)"))
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/member/skipCampaignLeaderCheck", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/tso/fastUpdatePhysicalInterval"))
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/member/skipCampaignLeaderCheck"))
	}()

	parallelAct := func() {
		// After https://github.com/tikv/pd/issues/6376 is fixed, we can use a smaller number here.
		// currently, the time to discover tso service is usually a little longer than 1s, compared
		// to the previous time taken < 1s.
		n := r.Intn(2) + 3
		time.Sleep(time.Duration(n) * time.Second)
		if !suite.legacy {
			wg := sync.WaitGroup{}
			// Select the first keyspace from all keyspace groups. We need to make sure the selected
			// keyspaces are from different keyspace groups, otherwise multiple goroutines below could
			// try to resign the primary of the same keyspace group and cause race condition.
			keyspaceIDs := make([]uint32, 0)
			for _, keyspaceGroup := range suite.keyspaceGroups {
				if len(keyspaceGroup.keyspaceIDs) > 0 {
					keyspaceIDs = append(keyspaceIDs, keyspaceGroup.keyspaceIDs[0])
				}
			}
			wg.Add(len(keyspaceIDs))
			for _, keyspaceID := range keyspaceIDs {
				go func(keyspaceID uint32) {
					defer wg.Done()
					err := suite.tsoCluster.ResignPrimary(keyspaceID, constant.DefaultKeyspaceGroupID)
					re.NoError(err)
					suite.tsoCluster.WaitForPrimaryServing(re, keyspaceID, 0)
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

	mcs.CheckMultiKeyspacesTSO(suite.ctx, re, suite.clients, parallelAct)
}

func (suite *tsoClientTestSuite) TestRandomShutdown() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/fastUpdatePhysicalInterval", "return(true)"))

	parallelAct := func() {
		// After https://github.com/tikv/pd/issues/6376 is fixed, we can use a smaller number here.
		// currently, the time to discover tso service is usually a little longer than 1s, compared
		// to the previous time taken < 1s.
		n := r.Intn(2) + 3
		time.Sleep(time.Duration(n) * time.Second)
		if !suite.legacy {
			suite.tsoCluster.WaitForDefaultPrimaryServing(re).Close()
		} else {
			suite.cluster.GetLeaderServer().GetServer().Close()
		}
		time.Sleep(time.Duration(n) * time.Second)
	}

	mcs.CheckMultiKeyspacesTSO(suite.ctx, re, suite.clients, parallelAct)
	suite.TearDownSuite()
	suite.SetupSuite()
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/tso/fastUpdatePhysicalInterval"))
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

// When we upgrade the PD cluster, there may be a period of time that the old and new PDs are running at the same time.
func TestMixedTSODeployment(t *testing.T) {
	re := require.New(t)

	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/fastUpdatePhysicalInterval", "return(true)"))
	re.NoError(failpoint.Enable("github.com/tikv/pd/client/servicediscovery/skipUpdateServiceMode", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/tso/fastUpdatePhysicalInterval"))
		re.NoError(failpoint.Disable("github.com/tikv/pd/client/servicediscovery/skipUpdateServiceMode"))
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cluster, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer cancel()
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	re.NoError(err)

	leaderServer := cluster.GetServer(cluster.WaitLeader())
	re.NotNil(leaderServer)
	backendEndpoints := leaderServer.GetAddr()

	apiSvr, err := cluster.JoinPDServer(ctx)
	re.NoError(err)
	err = apiSvr.Run()
	re.NoError(err)

	s, cleanup := tests.StartSingleTSOTestServer(ctx, re, backendEndpoints, tempurl.Alloc())
	defer cleanup()
	tests.WaitForPrimaryServing(re, map[string]bs.Server{s.GetAddr(): s})

	ctx1, cancel1 := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	checkTSO(ctx1, re, &wg, backendEndpoints)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 2 {
			n := r.Intn(2) + 1
			time.Sleep(time.Duration(n) * time.Second)
			leaderServer.ResignLeader()
			leaderServer = cluster.GetServer(cluster.WaitLeader())
			re.NotNil(leaderServer)
		}
		cancel1()
	}()
	wg.Wait()
}

// TestUpgradingAPIandTSOClusters tests the scenario that after we restart the API cluster
// then restart the TSO cluster, the TSO service can still serve TSO requests normally.
func TestUpgradingAPIandTSOClusters(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())

	// Create an API cluster which has 3 servers
	apiCluster, err := tests.NewTestPDServiceCluster(ctx, 3)
	re.NoError(err)
	err = apiCluster.RunInitialServers()
	re.NoError(err)
	leaderName := apiCluster.WaitLeader()
	re.NotEmpty(leaderName)
	pdLeader := apiCluster.GetServer(leaderName)
	backendEndpoints := pdLeader.GetAddr()

	// Create a pd client in PD mode to let the API leader to forward requests to the TSO cluster.
	re.NoError(failpoint.Enable("github.com/tikv/pd/client/servicediscovery/usePDServiceMode", "return(true)"))
	pdClient, err := pd.NewClientWithContext(context.Background(),
		caller.TestComponent,
		[]string{backendEndpoints}, pd.SecurityOption{}, opt.WithMaxErrorRetry(1))
	re.NoError(err)
	defer pdClient.Close()

	// Create a TSO cluster which has 2 servers
	tsoCluster, err := tests.NewTestTSOCluster(ctx, 2, backendEndpoints)
	re.NoError(err)
	tsoCluster.WaitForDefaultPrimaryServing(re)
	// The TSO service should be eventually healthy
	mcs.WaitForTSOServiceAvailable(ctx, re, pdClient)

	// Restart the API cluster
	apiCluster, err = tests.RestartTestAPICluster(ctx, apiCluster)
	re.NoError(err)
	// The TSO service should be eventually healthy
	mcs.WaitForTSOServiceAvailable(ctx, re, pdClient)

	// Restart the TSO cluster
	tsoCluster, err = tests.RestartTestTSOCluster(ctx, tsoCluster)
	re.NoError(err)
	// The TSO service should be eventually healthy
	mcs.WaitForTSOServiceAvailable(ctx, re, pdClient)

	tsoCluster.Destroy()
	apiCluster.Destroy()
	cancel()
	re.NoError(failpoint.Disable("github.com/tikv/pd/client/servicediscovery/usePDServiceMode"))
}

func checkTSO(
	ctx context.Context, re *require.Assertions, wg *sync.WaitGroup, backendEndpoints string,
) {
	wg.Add(tsoRequestConcurrencyNumber)
	for range tsoRequestConcurrencyNumber {
		go func() {
			defer wg.Done()
			cli := mcs.SetupClientWithAPIContext(ctx, re, pd.NewAPIContextV1(), strings.Split(backendEndpoints, ","))
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
