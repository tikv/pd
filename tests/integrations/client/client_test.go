// Copyright 2018 TiKV Project Authors.
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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"path"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/docker/go-units"
	"github.com/opentracing/basictracer-go"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/meta_storagepb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	pd "github.com/tikv/pd/client"
	clierrs "github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/retry"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/tso"
	"github.com/tikv/pd/pkg/utils/assertutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/tsoutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/integrations/mcs"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/goleak"
)

const (
	tsoRequestConcurrencyNumber = 5
	tsoRequestRound             = 30
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func TestClientClusterIDCheck(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Create the cluster #1.
	cluster1, err := tests.NewTestCluster(ctx, 3)
	re.NoError(err)
	defer cluster1.Destroy()
	endpoints1 := runServer(re, cluster1)
	// Create the cluster #2.
	cluster2, err := tests.NewTestCluster(ctx, 3)
	re.NoError(err)
	defer cluster2.Destroy()
	endpoints2 := runServer(re, cluster2)
	// Try to create a client with the mixed endpoints.
	_, err = pd.NewClientWithContext(
		ctx, append(endpoints1, endpoints2...),
		pd.SecurityOption{}, pd.WithMaxErrorRetry(1),
	)
	re.Error(err)
	re.Contains(err.Error(), "unmatched cluster id")
	// updateMember should fail due to unmatched cluster ID found.
	re.NoError(failpoint.Enable("github.com/tikv/pd/client/skipClusterIDCheck", `return(true)`))
	re.NoError(failpoint.Enable("github.com/tikv/pd/client/skipFirstUpdateMember", `return(true)`))
	_, err = pd.NewClientWithContext(ctx, []string{endpoints1[0], endpoints2[0]},
		pd.SecurityOption{}, pd.WithMaxErrorRetry(1),
	)
	re.Error(err)
	re.Contains(err.Error(), "ErrClientGetMember")
	re.NoError(failpoint.Disable("github.com/tikv/pd/client/skipFirstUpdateMember"))
	re.NoError(failpoint.Disable("github.com/tikv/pd/client/skipClusterIDCheck"))
}

func TestClientLeaderChange(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 3)
	re.NoError(err)
	defer cluster.Destroy()

	endpoints := runServer(re, cluster)
	endpointsWithWrongURL := append([]string{}, endpoints...)
	// inject wrong http scheme
	for i := range endpointsWithWrongURL {
		endpointsWithWrongURL[i] = "https://" + strings.TrimPrefix(endpointsWithWrongURL[i], "http://")
	}
	cli := setupCli(ctx, re, endpointsWithWrongURL)
	defer cli.Close()
	innerCli, ok := cli.(interface{ GetServiceDiscovery() pd.ServiceDiscovery })
	re.True(ok)

	var ts1, ts2 uint64
	testutil.Eventually(re, func() bool {
		p1, l1, err := cli.GetTS(context.TODO())
		if err == nil {
			ts1 = tsoutil.ComposeTS(p1, l1)
			return true
		}
		t.Log(err)
		return false
	})
	re.True(cluster.CheckTSOUnique(ts1))

	leader := cluster.GetLeader()
	waitLeader(re, innerCli.GetServiceDiscovery(), cluster.GetServer(leader))

	err = cluster.GetServer(leader).Stop()
	re.NoError(err)
	leader = cluster.WaitLeader()
	re.NotEmpty(leader)

	waitLeader(re, innerCli.GetServiceDiscovery(), cluster.GetServer(leader))

	// Check TS won't fall back after leader changed.
	testutil.Eventually(re, func() bool {
		p2, l2, err := cli.GetTS(context.TODO())
		if err == nil {
			ts2 = tsoutil.ComposeTS(p2, l2)
			return true
		}
		t.Log(err)
		return false
	})
	re.True(cluster.CheckTSOUnique(ts2))
	re.Less(ts1, ts2)

	// Check URL list.
	cli.Close()
	urls := innerCli.GetServiceDiscovery().GetServiceURLs()
	sort.Strings(urls)
	sort.Strings(endpoints)
	re.Equal(endpoints, urls)
}

func TestLeaderTransferAndMoveCluster(t *testing.T) {
	re := require.New(t)
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/member/changeFrequencyTimes", "return(10)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/member/changeFrequencyTimes"))
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 3)
	re.NoError(err)
	defer cluster.Destroy()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/member/skipCampaignLeaderCheck", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/member/skipCampaignLeaderCheck"))
	}()

	endpoints := runServer(re, cluster)
	cli := setupCli(ctx, re, endpoints)
	defer cli.Close()

	var lastTS uint64
	testutil.Eventually(re, func() bool {
		physical, logical, err := cli.GetTS(context.TODO())
		if err == nil {
			lastTS = tsoutil.ComposeTS(physical, logical)
			return true
		}
		t.Log(err)
		return false
	})
	re.True(cluster.CheckTSOUnique(lastTS))

	// Start a goroutine the make sure TS won't fall back.
	quit := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-quit:
				return
			default:
			}

			physical, logical, err := cli.GetTS(context.TODO())
			if err == nil {
				ts := tsoutil.ComposeTS(physical, logical)
				re.True(cluster.CheckTSOUnique(ts))
				re.Less(lastTS, ts)
				lastTS = ts
			}
			time.Sleep(time.Millisecond)
		}
	}()

	// Transfer leader.
	for i := 0; i < 3; i++ {
		oldLeaderName := cluster.WaitLeader()
		err := cluster.GetServer(oldLeaderName).ResignLeader()
		re.NoError(err)
		newLeaderName := cluster.WaitLeader()
		re.NotEqual(oldLeaderName, newLeaderName)
	}

	// ABC->ABCDEF
	oldServers := cluster.GetServers()
	oldLeaderName := cluster.WaitLeader()
	for i := 0; i < 3; i++ {
		newPD, err := cluster.Join(ctx)
		re.NoError(err)
		re.NoError(newPD.Run())
		oldLeaderName = cluster.WaitLeader()
		time.Sleep(5 * time.Second)
	}

	// ABCDEF->DEF
	oldNames := make([]string, 0)
	for _, s := range oldServers {
		oldNames = append(oldNames, s.GetServer().GetMemberInfo().GetName())
		s.Stop()
	}
	newLeaderName := cluster.WaitLeader()
	re.NotEqual(oldLeaderName, newLeaderName)
	re.NotContains(oldNames, newLeaderName)

	close(quit)
	wg.Wait()
}

func TestGetTSAfterTransferLeader(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 2)
	re.NoError(err)
	endpoints := runServer(re, cluster)
	leader := cluster.WaitLeader()
	re.NotEmpty(leader)
	defer cluster.Destroy()

	cli := setupCli(ctx, re, endpoints, pd.WithCustomTimeoutOption(10*time.Second))
	defer cli.Close()

	var leaderSwitched atomic.Bool
	cli.GetServiceDiscovery().AddServingURLSwitchedCallback(func() {
		leaderSwitched.Store(true)
	})
	err = cluster.GetServer(leader).ResignLeader()
	re.NoError(err)
	newLeader := cluster.WaitLeader()
	re.NotEmpty(newLeader)
	re.NotEqual(leader, newLeader)
	leader = cluster.WaitLeader()
	re.NotEmpty(leader)
	err = cli.GetServiceDiscovery().CheckMemberChanged()
	re.NoError(err)

	testutil.Eventually(re, leaderSwitched.Load)
	// The leader stream must be updated after the leader switch is sensed by the client.
	_, _, err = cli.GetTS(context.TODO())
	re.NoError(err)
}

func TestTSOAllocatorLeader(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dcLocationConfig := map[string]string{
		"pd1": "dc-1",
		"pd2": "dc-2",
		"pd3": "dc-3",
	}
	dcLocationNum := len(dcLocationConfig)
	cluster, err := tests.NewTestCluster(ctx, dcLocationNum, func(conf *config.Config, serverName string) {
		conf.EnableLocalTSO = true
		conf.Labels[config.ZoneLabel] = dcLocationConfig[serverName]
	})
	re.NoError(err)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	re.NoError(err)
	cluster.WaitAllLeaders(re, dcLocationConfig)

	var (
		testServers  = cluster.GetServers()
		endpoints    = make([]string, 0, len(testServers))
		endpointsMap = make(map[string]string)
	)
	for _, s := range testServers {
		endpoints = append(endpoints, s.GetConfig().AdvertiseClientUrls)
		endpointsMap[s.GetServer().GetMemberInfo().GetName()] = s.GetConfig().AdvertiseClientUrls
	}
	var allocatorLeaderMap = make(map[string]string)
	for _, dcLocation := range dcLocationConfig {
		var pdName string
		testutil.Eventually(re, func() bool {
			pdName = cluster.WaitAllocatorLeader(dcLocation)
			return len(pdName) > 0
		})
		allocatorLeaderMap[dcLocation] = pdName
	}
	cli := setupCli(ctx, re, endpoints)
	defer cli.Close()
	innerCli, ok := cli.(interface{ GetServiceDiscovery() pd.ServiceDiscovery })
	re.True(ok)

	// Check allocator leaders URL map.
	cli.Close()
	for dcLocation, url := range getTSOAllocatorServingEndpointURLs(cli.(TSOAllocatorsGetter)) {
		if dcLocation == tso.GlobalDCLocation {
			urls := innerCli.GetServiceDiscovery().GetServiceURLs()
			sort.Strings(urls)
			sort.Strings(endpoints)
			re.Equal(endpoints, urls)
			continue
		}
		pdName, exist := allocatorLeaderMap[dcLocation]
		re.True(exist)
		re.NotEmpty(pdName)
		pdURL, exist := endpointsMap[pdName]
		re.True(exist)
		re.NotEmpty(pdURL)
		re.Equal(pdURL, url)
	}
}

func TestTSOFollowerProxy(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 3)
	re.NoError(err)
	defer cluster.Destroy()

	endpoints := runServer(re, cluster)
	cli1 := setupCli(ctx, re, endpoints)
	defer cli1.Close()
	cli2 := setupCli(ctx, re, endpoints)
	defer cli2.Close()
	cli2.UpdateOption(pd.EnableTSOFollowerProxy, true)

	var wg sync.WaitGroup
	wg.Add(tsoRequestConcurrencyNumber)
	for i := 0; i < tsoRequestConcurrencyNumber; i++ {
		go func() {
			defer wg.Done()
			var lastTS uint64
			for i := 0; i < tsoRequestRound; i++ {
				physical, logical, err := cli2.GetTS(context.Background())
				re.NoError(err)
				ts := tsoutil.ComposeTS(physical, logical)
				re.Less(lastTS, ts)
				lastTS = ts
				// After requesting with the follower proxy, request with the leader directly.
				physical, logical, err = cli1.GetTS(context.Background())
				re.NoError(err)
				ts = tsoutil.ComposeTS(physical, logical)
				re.Less(lastTS, ts)
				lastTS = ts
			}
		}()
	}
	wg.Wait()
}

func TestTSOFollowerProxyWithTSOService(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestAPICluster(ctx, 1)
	re.NoError(err)
	defer cluster.Destroy()
	err = cluster.RunInitialServers()
	re.NoError(err)
	leaderName := cluster.WaitLeader()
	pdLeaderServer := cluster.GetServer(leaderName)
	re.NoError(pdLeaderServer.BootstrapCluster())
	backendEndpoints := pdLeaderServer.GetAddr()
	tsoCluster, err := tests.NewTestTSOCluster(ctx, 2, backendEndpoints)
	re.NoError(err)
	defer tsoCluster.Destroy()
	cli := mcs.SetupClientWithKeyspaceID(ctx, re, utils.DefaultKeyspaceID, strings.Split(backendEndpoints, ","))
	re.NotNil(cli)
	defer cli.Close()
	// TSO service does not support the follower proxy, so enabling it should fail.
	err = cli.UpdateOption(pd.EnableTSOFollowerProxy, true)
	re.Error(err)
}

// TestUnavailableTimeAfterLeaderIsReady is used to test https://github.com/tikv/pd/issues/5207
func TestUnavailableTimeAfterLeaderIsReady(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 3)
	re.NoError(err)
	defer cluster.Destroy()

	endpoints := runServer(re, cluster)
	cli := setupCli(ctx, re, endpoints)
	defer cli.Close()

	var wg sync.WaitGroup
	var maxUnavailableTime, leaderReadyTime time.Time
	getTsoFunc := func() {
		defer wg.Done()
		var lastTS uint64
		for i := 0; i < tsoRequestRound; i++ {
			var physical, logical int64
			var ts uint64
			physical, logical, err = cli.GetTS(context.Background())
			ts = tsoutil.ComposeTS(physical, logical)
			if err != nil {
				maxUnavailableTime = time.Now()
				continue
			}
			re.NoError(err)
			re.Less(lastTS, ts)
			lastTS = ts
		}
	}

	// test resign pd leader or stop pd leader
	wg.Add(1 + 1)
	go getTsoFunc()
	go func() {
		defer wg.Done()
		leader := cluster.GetLeaderServer()
		leader.Stop()
		re.NotEmpty(cluster.WaitLeader())
		leaderReadyTime = time.Now()
		tests.RunServers([]*tests.TestServer{leader})
	}()
	wg.Wait()
	re.Less(maxUnavailableTime.UnixMilli(), leaderReadyTime.Add(1*time.Second).UnixMilli())

	// test kill pd leader pod or network of leader is unreachable
	wg.Add(1 + 1)
	maxUnavailableTime, leaderReadyTime = time.Time{}, time.Time{}
	go getTsoFunc()
	go func() {
		defer wg.Done()
		leader := cluster.GetLeaderServer()
		re.NoError(failpoint.Enable("github.com/tikv/pd/client/unreachableNetwork", "return(true)"))
		leader.Stop()
		re.NotEmpty(cluster.WaitLeader())
		re.NoError(failpoint.Disable("github.com/tikv/pd/client/unreachableNetwork"))
		leaderReadyTime = time.Now()
	}()
	wg.Wait()
	re.Less(maxUnavailableTime.UnixMilli(), leaderReadyTime.Add(1*time.Second).UnixMilli())
}

// TODO: migrate the Local/Global TSO tests to TSO integration test folder.
func TestGlobalAndLocalTSO(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dcLocationConfig := map[string]string{
		"pd1": "dc-1",
		"pd2": "dc-2",
		"pd3": "dc-3",
	}
	dcLocationNum := len(dcLocationConfig)
	cluster, err := tests.NewTestCluster(ctx, dcLocationNum, func(conf *config.Config, serverName string) {
		conf.EnableLocalTSO = true
		conf.Labels[config.ZoneLabel] = dcLocationConfig[serverName]
	})
	re.NoError(err)
	defer cluster.Destroy()

	endpoints := runServer(re, cluster)
	cli := setupCli(ctx, re, endpoints)
	defer cli.Close()

	// Wait for all nodes becoming healthy.
	time.Sleep(time.Second * 5)

	// Join a new dc-location
	pd4, err := cluster.Join(ctx, func(conf *config.Config, _ string) {
		conf.EnableLocalTSO = true
		conf.Labels[config.ZoneLabel] = "dc-4"
	})
	re.NoError(err)
	err = pd4.Run()
	re.NoError(err)
	dcLocationConfig["pd4"] = "dc-4"
	cluster.CheckClusterDCLocation()
	cluster.WaitAllLeaders(re, dcLocationConfig)

	// Test a nonexistent dc-location for Local TSO
	p, l, err := cli.GetLocalTS(context.TODO(), "nonexistent-dc")
	re.Equal(int64(0), p)
	re.Equal(int64(0), l, int64(0))
	re.Error(err)
	re.Contains(err.Error(), "unknown dc-location")

	wg := &sync.WaitGroup{}
	requestGlobalAndLocalTSO(re, wg, dcLocationConfig, cli)

	// assert global tso after resign leader
	re.NoError(failpoint.Enable("github.com/tikv/pd/client/skipUpdateMember", `return(true)`))
	err = cluster.ResignLeader()
	re.NoError(err)
	re.NotEmpty(cluster.WaitLeader())
	_, _, err = cli.GetTS(ctx)
	re.Error(err)
	re.True(clierrs.IsLeaderChange(err))
	_, _, err = cli.GetTS(ctx)
	re.NoError(err)
	re.NoError(failpoint.Disable("github.com/tikv/pd/client/skipUpdateMember"))

	recorder := basictracer.NewInMemoryRecorder()
	tracer := basictracer.New(recorder)
	span := tracer.StartSpan("trace")
	ctx = opentracing.ContextWithSpan(ctx, span)
	future := cli.GetLocalTSAsync(ctx, "error-dc")
	spans := recorder.GetSpans()
	re.Len(spans, 1)
	_, _, err = future.Wait()
	re.Error(err)
	spans = recorder.GetSpans()
	re.Len(spans, 1)
	_, _, err = cli.GetTS(ctx)
	re.NoError(err)
	spans = recorder.GetSpans()
	re.Len(spans, 3)

	// Test the TSO follower proxy while enabling the Local TSO.
	cli.UpdateOption(pd.EnableTSOFollowerProxy, true)
	// Sleep a while here to prevent from canceling the ongoing TSO request.
	time.Sleep(time.Millisecond * 50)
	requestGlobalAndLocalTSO(re, wg, dcLocationConfig, cli)
	cli.UpdateOption(pd.EnableTSOFollowerProxy, false)
	time.Sleep(time.Millisecond * 50)
	requestGlobalAndLocalTSO(re, wg, dcLocationConfig, cli)
}

func requestGlobalAndLocalTSO(
	re *require.Assertions,
	wg *sync.WaitGroup,
	dcLocationConfig map[string]string,
	cli pd.Client,
) {
	for _, dcLocation := range dcLocationConfig {
		wg.Add(tsoRequestConcurrencyNumber)
		for i := 0; i < tsoRequestConcurrencyNumber; i++ {
			go func(dc string) {
				defer wg.Done()
				var lastTS uint64
				for i := 0; i < tsoRequestRound; i++ {
					globalPhysical1, globalLogical1, err := cli.GetTS(context.TODO())
					// The allocator leader may be changed due to the environment issue.
					if err != nil {
						re.ErrorContains(err, errs.NotLeaderErr)
					}
					globalTS1 := tsoutil.ComposeTS(globalPhysical1, globalLogical1)
					localPhysical, localLogical, err := cli.GetLocalTS(context.TODO(), dc)
					if err != nil {
						re.ErrorContains(err, errs.NotLeaderErr)
					}
					localTS := tsoutil.ComposeTS(localPhysical, localLogical)
					globalPhysical2, globalLogical2, err := cli.GetTS(context.TODO())
					if err != nil {
						re.ErrorContains(err, errs.NotLeaderErr)
					}
					globalTS2 := tsoutil.ComposeTS(globalPhysical2, globalLogical2)
					re.Less(lastTS, globalTS1)
					re.Less(globalTS1, localTS)
					re.Less(localTS, globalTS2)
					lastTS = globalTS2
				}
				re.Greater(lastTS, uint64(0))
			}(dcLocation)
		}
	}
	wg.Wait()
}

// GetTSOAllocators defines the TSO allocators getter.
type TSOAllocatorsGetter interface{ GetTSOAllocators() *sync.Map }

func getTSOAllocatorServingEndpointURLs(c TSOAllocatorsGetter) map[string]string {
	allocatorLeaders := make(map[string]string)
	c.GetTSOAllocators().Range(func(dcLocation, url any) bool {
		allocatorLeaders[dcLocation.(string)] = url.(string)
		return true
	})
	return allocatorLeaders
}

func TestCustomTimeout(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer cluster.Destroy()

	endpoints := runServer(re, cluster)
	cli := setupCli(ctx, re, endpoints, pd.WithCustomTimeoutOption(time.Second))
	defer cli.Close()

	start := time.Now()
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/customTimeout", "return(true)"))
	_, err = cli.GetAllStores(context.TODO())
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/customTimeout"))
	re.Error(err)
	re.GreaterOrEqual(time.Since(start), time.Second)
	re.Less(time.Since(start), 2*time.Second)
}

type followerForwardAndHandleTestSuite struct {
	suite.Suite
	ctx   context.Context
	clean context.CancelFunc

	cluster   *tests.TestCluster
	endpoints []string
	regionID  uint64
}

func TestFollowerForwardAndHandleTestSuite(t *testing.T) {
	suite.Run(t, new(followerForwardAndHandleTestSuite))
}

func (suite *followerForwardAndHandleTestSuite) SetupSuite() {
	re := suite.Require()
	suite.ctx, suite.clean = context.WithCancel(context.Background())
	pd.MemberHealthCheckInterval = 100 * time.Millisecond
	cluster, err := tests.NewTestCluster(suite.ctx, 3)
	re.NoError(err)
	suite.cluster = cluster
	suite.endpoints = runServer(re, cluster)
	re.NotEmpty(cluster.WaitLeader())
	leader := cluster.GetLeaderServer()
	grpcPDClient := testutil.MustNewGrpcClient(re, leader.GetAddr())
	suite.regionID = regionIDAllocator.alloc()
	testutil.Eventually(re, func() bool {
		regionHeartbeat, err := grpcPDClient.RegionHeartbeat(suite.ctx)
		re.NoError(err)
		region := &metapb.Region{
			Id: suite.regionID,
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: 1,
				Version: 1,
			},
			Peers: peers,
		}
		req := &pdpb.RegionHeartbeatRequest{
			Header: newHeader(leader.GetServer()),
			Region: region,
			Leader: peers[0],
		}
		err = regionHeartbeat.Send(req)
		re.NoError(err)
		_, err = regionHeartbeat.Recv()
		return err == nil
	})
}

func (*followerForwardAndHandleTestSuite) TearDownTest() {}

func (suite *followerForwardAndHandleTestSuite) TearDownSuite() {
	suite.cluster.Destroy()
	suite.clean()
}

func (suite *followerForwardAndHandleTestSuite) TestGetRegionByFollowerForwarding() {
	re := suite.Require()
	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()

	cli := setupCli(ctx, re, suite.endpoints, pd.WithForwardingOption(true))
	defer cli.Close()
	re.NoError(failpoint.Enable("github.com/tikv/pd/client/unreachableNetwork1", "return(true)"))
	time.Sleep(200 * time.Millisecond)
	r, err := cli.GetRegion(context.Background(), []byte("a"))
	re.NoError(err)
	re.NotNil(r)

	re.NoError(failpoint.Disable("github.com/tikv/pd/client/unreachableNetwork1"))
	time.Sleep(200 * time.Millisecond)
	r, err = cli.GetRegion(context.Background(), []byte("a"))
	re.NoError(err)
	re.NotNil(r)
}

// case 1: unreachable -> normal
func (suite *followerForwardAndHandleTestSuite) TestGetTsoByFollowerForwarding1() {
	re := suite.Require()
	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()
	cli := setupCli(ctx, re, suite.endpoints, pd.WithForwardingOption(true))
	defer cli.Close()

	re.NoError(failpoint.Enable("github.com/tikv/pd/client/unreachableNetwork", "return(true)"))
	var lastTS uint64
	testutil.Eventually(re, func() bool {
		physical, logical, err := cli.GetTS(context.TODO())
		if err == nil {
			lastTS = tsoutil.ComposeTS(physical, logical)
			return true
		}
		suite.T().Log(err)
		return false
	})

	lastTS = checkTS(re, cli, lastTS)
	re.NoError(failpoint.Disable("github.com/tikv/pd/client/unreachableNetwork"))
	time.Sleep(2 * time.Second)
	checkTS(re, cli, lastTS)

	re.NoError(failpoint.Enable("github.com/tikv/pd/client/responseNil", "return(true)"))
	regions, err := cli.BatchScanRegions(ctx, []pd.KeyRange{{StartKey: []byte(""), EndKey: []byte("")}}, 100)
	re.NoError(err)
	re.Empty(regions)
	re.NoError(failpoint.Disable("github.com/tikv/pd/client/responseNil"))
	regions, err = cli.BatchScanRegions(ctx, []pd.KeyRange{{StartKey: []byte(""), EndKey: []byte("")}}, 100)
	re.NoError(err)
	re.Len(regions, 1)
}

// case 2: unreachable -> leader transfer -> normal
func (suite *followerForwardAndHandleTestSuite) TestGetTsoByFollowerForwarding2() {
	re := suite.Require()
	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()
	cli := setupCli(ctx, re, suite.endpoints, pd.WithForwardingOption(true))
	defer cli.Close()

	re.NoError(failpoint.Enable("github.com/tikv/pd/client/unreachableNetwork", "return(true)"))
	var lastTS uint64
	testutil.Eventually(re, func() bool {
		physical, logical, err := cli.GetTS(context.TODO())
		if err == nil {
			lastTS = tsoutil.ComposeTS(physical, logical)
			return true
		}
		suite.T().Log(err)
		return false
	})

	lastTS = checkTS(re, cli, lastTS)
	re.NoError(suite.cluster.GetLeaderServer().ResignLeader())
	re.NotEmpty(suite.cluster.WaitLeader())
	lastTS = checkTS(re, cli, lastTS)

	re.NoError(failpoint.Disable("github.com/tikv/pd/client/unreachableNetwork"))
	time.Sleep(5 * time.Second)
	checkTS(re, cli, lastTS)
}

// case 3: network partition between client and follower A -> transfer leader to follower A -> normal
func (suite *followerForwardAndHandleTestSuite) TestGetTsoAndRegionByFollowerForwarding() {
	re := suite.Require()
	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()

	cluster := suite.cluster
	leader := cluster.GetLeaderServer()

	follower := cluster.GetServer(cluster.GetFollower())
	re.NoError(failpoint.Enable("github.com/tikv/pd/client/grpcutil/unreachableNetwork2", fmt.Sprintf("return(\"%s\")", follower.GetAddr())))

	cli := setupCli(ctx, re, suite.endpoints, pd.WithForwardingOption(true))
	defer cli.Close()
	var lastTS uint64
	testutil.Eventually(re, func() bool {
		physical, logical, err := cli.GetTS(context.TODO())
		if err == nil {
			lastTS = tsoutil.ComposeTS(physical, logical)
			return true
		}
		suite.T().Log(err)
		return false
	})
	lastTS = checkTS(re, cli, lastTS)
	r, err := cli.GetRegion(context.Background(), []byte("a"))
	re.NoError(err)
	re.NotNil(r)
	leader.GetServer().GetMember().ResignEtcdLeader(leader.GetServer().Context(),
		leader.GetServer().Name(), follower.GetServer().Name())
	re.NotEmpty(cluster.WaitLeader())
	testutil.Eventually(re, func() bool {
		physical, logical, err := cli.GetTS(context.TODO())
		if err == nil {
			lastTS = tsoutil.ComposeTS(physical, logical)
			return true
		}
		suite.T().Log(err)
		return false
	})
	lastTS = checkTS(re, cli, lastTS)
	testutil.Eventually(re, func() bool {
		r, err = cli.GetRegion(context.Background(), []byte("a"))
		if err == nil && r != nil {
			return true
		}
		return false
	})

	re.NoError(failpoint.Disable("github.com/tikv/pd/client/grpcutil/unreachableNetwork2"))
	testutil.Eventually(re, func() bool {
		physical, logical, err := cli.GetTS(context.TODO())
		if err == nil {
			lastTS = tsoutil.ComposeTS(physical, logical)
			return true
		}
		suite.T().Log(err)
		return false
	})
	lastTS = checkTS(re, cli, lastTS)
	testutil.Eventually(re, func() bool {
		r, err = cli.GetRegion(context.Background(), []byte("a"))
		if err == nil && r != nil {
			return true
		}
		return false
	})
}

func (suite *followerForwardAndHandleTestSuite) TestGetRegionFromLeaderWhenNetworkErr() {
	re := suite.Require()
	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()

	cluster := suite.cluster
	re.NotEmpty(cluster.WaitLeader())
	leader := cluster.GetLeaderServer()

	follower := cluster.GetServer(cluster.GetFollower())
	re.NoError(failpoint.Enable("github.com/tikv/pd/client/grpcutil/unreachableNetwork2", fmt.Sprintf("return(\"%s\")", follower.GetAddr())))

	cli := setupCli(ctx, re, suite.endpoints)
	defer cli.Close()

	cluster.GetLeaderServer().GetServer().GetMember().ResignEtcdLeader(ctx, leader.GetServer().Name(), follower.GetServer().Name())
	re.NotEmpty(cluster.WaitLeader())

	// here is just for trigger the leader change.
	cli.GetRegion(context.Background(), []byte("a"))

	testutil.Eventually(re, func() bool {
		return cli.GetLeaderURL() == follower.GetAddr()
	})
	r, err := cli.GetRegion(context.Background(), []byte("a"))
	re.Error(err)
	re.Nil(r)

	re.NoError(failpoint.Disable("github.com/tikv/pd/client/grpcutil/unreachableNetwork2"))
	cli.GetServiceDiscovery().CheckMemberChanged()
	testutil.Eventually(re, func() bool {
		r, err = cli.GetRegion(context.Background(), []byte("a"))
		if err == nil && r != nil {
			return true
		}
		return false
	})
}

func (suite *followerForwardAndHandleTestSuite) TestGetRegionFromFollower() {
	re := suite.Require()
	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()

	cluster := suite.cluster
	cli := setupCli(ctx, re, suite.endpoints)
	defer cli.Close()
	cli.UpdateOption(pd.EnableFollowerHandle, true)
	re.NotEmpty(cluster.WaitLeader())
	leader := cluster.GetLeaderServer()
	testutil.Eventually(re, func() bool {
		ret := true
		for _, s := range cluster.GetServers() {
			if s.IsLeader() {
				continue
			}
			if !s.GetServer().DirectlyGetRaftCluster().GetRegionSyncer().IsRunning() {
				ret = false
			}
		}
		return ret
	})
	// follower have no region
	cnt := 0
	for i := 0; i < 100; i++ {
		resp, err := cli.GetRegion(ctx, []byte("a"), pd.WithAllowFollowerHandle())
		if err == nil && resp != nil {
			cnt++
		}
		re.Equal(resp.Meta.Id, suite.regionID)
	}
	re.Equal(100, cnt)

	// because we can't check whether this request is processed by followers from response,
	// we can disable forward and make network problem for leader.
	re.NoError(failpoint.Enable("github.com/tikv/pd/client/unreachableNetwork1", fmt.Sprintf("return(\"%s\")", leader.GetAddr())))
	time.Sleep(150 * time.Millisecond)
	cnt = 0
	for i := 0; i < 100; i++ {
		resp, err := cli.GetRegion(ctx, []byte("a"), pd.WithAllowFollowerHandle())
		if err == nil && resp != nil {
			cnt++
		}
		re.Equal(resp.Meta.Id, suite.regionID)
	}
	re.Equal(100, cnt)
	re.NoError(failpoint.Disable("github.com/tikv/pd/client/unreachableNetwork1"))

	// make network problem for follower.
	follower := cluster.GetServer(cluster.GetFollower())
	re.NoError(failpoint.Enable("github.com/tikv/pd/client/unreachableNetwork1", fmt.Sprintf("return(\"%s\")", follower.GetAddr())))
	time.Sleep(100 * time.Millisecond)
	cnt = 0
	for i := 0; i < 100; i++ {
		resp, err := cli.GetRegion(ctx, []byte("a"), pd.WithAllowFollowerHandle())
		if err == nil && resp != nil {
			cnt++
		}
		re.Equal(resp.Meta.Id, suite.regionID)
	}
	re.Equal(100, cnt)
	re.NoError(failpoint.Disable("github.com/tikv/pd/client/unreachableNetwork1"))

	// follower client failed will retry by leader service client.
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/followerHandleError", "return(true)"))
	cnt = 0
	for i := 0; i < 100; i++ {
		resp, err := cli.GetRegion(ctx, []byte("a"), pd.WithAllowFollowerHandle())
		if err == nil && resp != nil {
			cnt++
		}
		re.Equal(resp.Meta.Id, suite.regionID)
	}
	re.Equal(100, cnt)
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/followerHandleError"))

	// test after being healthy
	re.NoError(failpoint.Enable("github.com/tikv/pd/client/unreachableNetwork1", fmt.Sprintf("return(\"%s\")", leader.GetAddr())))
	re.NoError(failpoint.Enable("github.com/tikv/pd/client/fastCheckAvailable", "return(true)"))
	time.Sleep(100 * time.Millisecond)
	cnt = 0
	for i := 0; i < 100; i++ {
		resp, err := cli.GetRegion(ctx, []byte("a"), pd.WithAllowFollowerHandle())
		if err == nil && resp != nil {
			cnt++
		}
		re.Equal(resp.Meta.Id, suite.regionID)
	}
	re.Equal(100, cnt)
	re.NoError(failpoint.Disable("github.com/tikv/pd/client/unreachableNetwork1"))
	re.NoError(failpoint.Disable("github.com/tikv/pd/client/fastCheckAvailable"))
}

func (suite *followerForwardAndHandleTestSuite) TestGetTSFuture() {
	re := suite.Require()
	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()

	re.NoError(failpoint.Enable("github.com/tikv/pd/client/shortDispatcherChannel", "return(true)"))

	cli := setupCli(ctx, re, suite.endpoints)

	ctxs := make([]context.Context, 20)
	cancels := make([]context.CancelFunc, 20)
	for i := 0; i < 20; i++ {
		ctxs[i], cancels[i] = context.WithCancel(ctx)
	}
	start := time.Now()
	wg1 := sync.WaitGroup{}
	wg2 := sync.WaitGroup{}
	wg3 := sync.WaitGroup{}
	wg1.Add(1)
	go func() {
		<-time.After(time.Second)
		for i := 0; i < 20; i++ {
			cancels[i]()
		}
		wg1.Done()
	}()
	wg2.Add(1)
	go func() {
		cli.Close()
		wg2.Done()
	}()
	wg3.Add(1)
	go func() {
		for i := 0; i < 20; i++ {
			cli.GetTSAsync(ctxs[i])
		}
		wg3.Done()
	}()
	wg1.Wait()
	wg2.Wait()
	wg3.Wait()
	re.Less(time.Since(start), time.Second*2)
	re.NoError(failpoint.Disable("github.com/tikv/pd/client/shortDispatcherChannel"))
}

func checkTS(re *require.Assertions, cli pd.Client, lastTS uint64) uint64 {
	for i := 0; i < tsoRequestRound; i++ {
		physical, logical, err := cli.GetTS(context.TODO())
		if err == nil {
			ts := tsoutil.ComposeTS(physical, logical)
			re.Less(lastTS, ts)
			lastTS = ts
		}
		time.Sleep(time.Millisecond)
	}
	return lastTS
}

func runServer(re *require.Assertions, cluster *tests.TestCluster) []string {
	err := cluster.RunInitialServers()
	re.NoError(err)
	re.NotEmpty(cluster.WaitLeader())
	leaderServer := cluster.GetLeaderServer()
	re.NoError(leaderServer.BootstrapCluster())

	testServers := cluster.GetServers()
	endpoints := make([]string, 0, len(testServers))
	for _, s := range testServers {
		endpoints = append(endpoints, s.GetConfig().AdvertiseClientUrls)
	}
	return endpoints
}

func setupCli(ctx context.Context, re *require.Assertions, endpoints []string, opts ...pd.ClientOption) pd.Client {
	cli, err := pd.NewClientWithContext(ctx, endpoints, pd.SecurityOption{}, opts...)
	re.NoError(err)
	return cli
}

func waitLeader(re *require.Assertions, cli pd.ServiceDiscovery, leader *tests.TestServer) {
	testutil.Eventually(re, func() bool {
		cli.ScheduleCheckMemberChanged()
		return cli.GetServingURL() == leader.GetConfig().ClientUrls && leader.GetAddr() == cli.GetServingURL()
	})
}

func TestConfigTTLAfterTransferLeader(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 3)
	re.NoError(err)
	defer cluster.Destroy()
	err = cluster.RunInitialServers()
	re.NoError(err)
	leader := cluster.GetServer(cluster.WaitLeader())
	re.NoError(leader.BootstrapCluster())
	addr := fmt.Sprintf("%s/pd/api/v1/config?ttlSecond=5", leader.GetAddr())
	postData, err := json.Marshal(map[string]any{
		"schedule.max-snapshot-count":             999,
		"schedule.enable-location-replacement":    false,
		"schedule.max-merge-region-size":          999,
		"schedule.max-merge-region-keys":          999,
		"schedule.scheduler-max-waiting-operator": 999,
		"schedule.leader-schedule-limit":          999,
		"schedule.region-schedule-limit":          999,
		"schedule.hot-region-schedule-limit":      999,
		"schedule.replica-schedule-limit":         999,
		"schedule.merge-schedule-limit":           999,
	})
	re.NoError(err)
	resp, err := leader.GetHTTPClient().Post(addr, "application/json", bytes.NewBuffer(postData))
	resp.Body.Close()
	re.NoError(err)
	time.Sleep(2 * time.Second)
	re.NoError(leader.Destroy())
	time.Sleep(2 * time.Second)
	leader = cluster.GetServer(cluster.WaitLeader())
	re.NotNil(leader)
	options := leader.GetPersistOptions()
	re.NotNil(options)
	re.Equal(uint64(999), options.GetMaxSnapshotCount())
	re.False(options.IsLocationReplacementEnabled())
	re.Equal(uint64(999), options.GetMaxMergeRegionSize())
	re.Equal(uint64(999), options.GetMaxMergeRegionKeys())
	re.Equal(uint64(999), options.GetSchedulerMaxWaitingOperator())
	re.Equal(uint64(999), options.GetLeaderScheduleLimit())
	re.Equal(uint64(999), options.GetRegionScheduleLimit())
	re.Equal(uint64(999), options.GetHotRegionScheduleLimit())
	re.Equal(uint64(999), options.GetReplicaScheduleLimit())
	re.Equal(uint64(999), options.GetMergeScheduleLimit())
}

func TestCloseClient(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer cluster.Destroy()
	endpoints := runServer(re, cluster)
	cli := setupCli(ctx, re, endpoints)
	ts := cli.GetTSAsync(context.TODO())
	time.Sleep(time.Second)
	cli.Close()
	physical, logical, err := ts.Wait()
	if err == nil {
		re.Greater(physical, int64(0))
		re.Greater(logical, int64(0))
	} else {
		re.ErrorIs(err, context.Canceled)
		re.Zero(physical)
		re.Zero(logical)
	}
	ts = cli.GetTSAsync(context.TODO())
	physical, logical, err = ts.Wait()
	re.ErrorIs(err, context.Canceled)
	re.Zero(physical)
	re.Zero(logical)
}

type idAllocator struct {
	allocator *mockid.IDAllocator
}

func (i *idAllocator) alloc() uint64 {
	id, _ := i.allocator.Alloc()
	return id
}

var (
	regionIDAllocator = &idAllocator{allocator: &mockid.IDAllocator{}}
	// Note: IDs below are entirely arbitrary. They are only for checking
	// whether GetRegion/GetStore works.
	// If we alloc ID in client in the future, these IDs must be updated.
	stores = []*metapb.Store{
		{Id: 1,
			Address: "localhost:1",
		},
		{Id: 2,
			Address: "localhost:2",
		},
		{Id: 3,
			Address: "localhost:3",
		},
		{Id: 4,
			Address: "localhost:4",
		},
	}

	peers = []*metapb.Peer{
		{Id: regionIDAllocator.alloc(),
			StoreId: stores[0].GetId(),
		},
		{Id: regionIDAllocator.alloc(),
			StoreId: stores[1].GetId(),
		},
		{Id: regionIDAllocator.alloc(),
			StoreId: stores[2].GetId(),
		},
	}
)

type clientTestSuite struct {
	suite.Suite
	cleanup         testutil.CleanupFunc
	ctx             context.Context
	clean           context.CancelFunc
	srv             *server.Server
	grpcSvr         *server.GrpcServer
	client          pd.Client
	grpcPDClient    pdpb.PDClient
	regionHeartbeat pdpb.PD_RegionHeartbeatClient
	reportBucket    pdpb.PD_ReportBucketsClient
}

func TestClientTestSuite(t *testing.T) {
	suite.Run(t, new(clientTestSuite))
}

func (suite *clientTestSuite) SetupSuite() {
	var err error
	re := suite.Require()
	suite.srv, suite.cleanup, err = server.NewTestServer(re, assertutil.CheckerWithNilAssert(re))
	re.NoError(err)
	suite.grpcPDClient = testutil.MustNewGrpcClient(re, suite.srv.GetAddr())
	suite.grpcSvr = &server.GrpcServer{Server: suite.srv}

	server.MustWaitLeader(re, []*server.Server{suite.srv})
	bootstrapServer(re, newHeader(suite.srv), suite.grpcPDClient)

	suite.ctx, suite.clean = context.WithCancel(context.Background())
	suite.client = setupCli(suite.ctx, re, suite.srv.GetEndpoints())

	suite.regionHeartbeat, err = suite.grpcPDClient.RegionHeartbeat(suite.ctx)
	re.NoError(err)
	suite.reportBucket, err = suite.grpcPDClient.ReportBuckets(suite.ctx)
	re.NoError(err)
	cluster := suite.srv.GetRaftCluster()
	re.NotNil(cluster)
	now := time.Now().UnixNano()
	for _, store := range stores {
		suite.grpcSvr.PutStore(context.Background(), &pdpb.PutStoreRequest{
			Header: newHeader(suite.srv),
			Store: &metapb.Store{
				Id:            store.Id,
				Address:       store.Address,
				LastHeartbeat: now,
			},
		})

		storeInfo := suite.grpcSvr.GetRaftCluster().GetStore(store.GetId())
		newStore := storeInfo.Clone(core.SetStoreStats(&pdpb.StoreStats{
			Capacity:  uint64(10 * units.GiB),
			UsedSize:  uint64(9 * units.GiB),
			Available: uint64(1 * units.GiB),
		}))
		suite.grpcSvr.GetRaftCluster().GetBasicCluster().PutStore(newStore)
	}
	cluster.GetOpts().(*config.PersistOptions).SetRegionBucketEnabled(true)
}

func (suite *clientTestSuite) TearDownSuite() {
	suite.client.Close()
	suite.clean()
	suite.cleanup()
}

func newHeader(srv *server.Server) *pdpb.RequestHeader {
	return &pdpb.RequestHeader{
		ClusterId: srv.ClusterID(),
	}
}

func bootstrapServer(re *require.Assertions, header *pdpb.RequestHeader, client pdpb.PDClient) {
	regionID := regionIDAllocator.alloc()
	region := &metapb.Region{
		Id: regionID,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
		Peers: peers[:1],
	}
	req := &pdpb.BootstrapRequest{
		Header: header,
		Store:  stores[0],
		Region: region,
	}
	resp, err := client.Bootstrap(context.Background(), req)
	re.NoError(err)
	re.Equal(pdpb.ErrorType_OK, resp.GetHeader().GetError().GetType())
}

func (suite *clientTestSuite) TestGetRegion() {
	re := suite.Require()
	regionID := regionIDAllocator.alloc()
	region := &metapb.Region{
		Id: regionID,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
		Peers: peers,
	}
	req := &pdpb.RegionHeartbeatRequest{
		Header: newHeader(suite.srv),
		Region: region,
		Leader: peers[0],
	}
	err := suite.regionHeartbeat.Send(req)
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		r, err := suite.client.GetRegion(context.Background(), []byte("a"))
		re.NoError(err)
		if r == nil {
			return false
		}
		return reflect.DeepEqual(region, r.Meta) &&
			reflect.DeepEqual(peers[0], r.Leader) &&
			r.Buckets == nil
	})
	breq := &pdpb.ReportBucketsRequest{
		Header: newHeader(suite.srv),
		Buckets: &metapb.Buckets{
			RegionId:   regionID,
			Version:    1,
			Keys:       [][]byte{[]byte("a"), []byte("z")},
			PeriodInMs: 2000,
			Stats: &metapb.BucketStats{
				ReadBytes:  []uint64{1},
				ReadKeys:   []uint64{1},
				ReadQps:    []uint64{1},
				WriteBytes: []uint64{1},
				WriteKeys:  []uint64{1},
				WriteQps:   []uint64{1},
			},
		},
	}
	re.NoError(suite.reportBucket.Send(breq))
	testutil.Eventually(re, func() bool {
		r, err := suite.client.GetRegion(context.Background(), []byte("a"), pd.WithBuckets())
		re.NoError(err)
		if r == nil {
			return false
		}
		return r.Buckets != nil
	})
	suite.srv.GetRaftCluster().GetOpts().(*config.PersistOptions).SetRegionBucketEnabled(false)

	testutil.Eventually(re, func() bool {
		r, err := suite.client.GetRegion(context.Background(), []byte("a"), pd.WithBuckets())
		re.NoError(err)
		if r == nil {
			return false
		}
		return r.Buckets == nil
	})
	suite.srv.GetRaftCluster().GetOpts().(*config.PersistOptions).SetRegionBucketEnabled(true)

	re.NoError(failpoint.Enable("github.com/tikv/pd/server/grpcClientClosed", `return(true)`))
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/useForwardRequest", `return(true)`))
	re.NoError(suite.reportBucket.Send(breq))
	re.Error(suite.reportBucket.RecvMsg(breq))
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/grpcClientClosed"))
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/useForwardRequest"))
}

func (suite *clientTestSuite) TestGetPrevRegion() {
	re := suite.Require()
	regionLen := 10
	regions := make([]*metapb.Region, 0, regionLen)
	for i := 0; i < regionLen; i++ {
		regionID := regionIDAllocator.alloc()
		r := &metapb.Region{
			Id: regionID,
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: 1,
				Version: 1,
			},
			StartKey: []byte{byte(i)},
			EndKey:   []byte{byte(i + 1)},
			Peers:    peers,
		}
		regions = append(regions, r)
		req := &pdpb.RegionHeartbeatRequest{
			Header: newHeader(suite.srv),
			Region: r,
			Leader: peers[0],
		}
		err := suite.regionHeartbeat.Send(req)
		re.NoError(err)
	}
	time.Sleep(500 * time.Millisecond)
	for i := 0; i < 20; i++ {
		testutil.Eventually(re, func() bool {
			r, err := suite.client.GetPrevRegion(context.Background(), []byte{byte(i)})
			re.NoError(err)
			if i > 0 && i < regionLen {
				return reflect.DeepEqual(peers[0], r.Leader) &&
					reflect.DeepEqual(regions[i-1], r.Meta)
			}
			return r == nil
		})
	}
}

func (suite *clientTestSuite) TestScanRegions() {
	re := suite.Require()
	regionLen := 10
	regions := make([]*metapb.Region, 0, regionLen)
	for i := 0; i < regionLen; i++ {
		regionID := regionIDAllocator.alloc()
		r := &metapb.Region{
			Id: regionID,
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: 1,
				Version: 1,
			},
			StartKey: []byte{byte(i)},
			EndKey:   []byte{byte(i + 1)},
			Peers:    peers,
		}
		regions = append(regions, r)
		req := &pdpb.RegionHeartbeatRequest{
			Header: newHeader(suite.srv),
			Region: r,
			Leader: peers[0],
		}
		err := suite.regionHeartbeat.Send(req)
		re.NoError(err)
	}

	// Wait for region heartbeats.
	testutil.Eventually(re, func() bool {
		scanRegions, err := suite.client.BatchScanRegions(context.Background(), []pd.KeyRange{{StartKey: []byte{0}, EndKey: nil}}, 10)
		return err == nil && len(scanRegions) == 10
	})

	// Set leader of region3 to nil.
	region3 := core.NewRegionInfo(regions[3], nil)
	suite.srv.GetRaftCluster().HandleRegionHeartbeat(region3)

	// Add down peer for region4.
	region4 := core.NewRegionInfo(regions[4], regions[4].Peers[0], core.WithDownPeers([]*pdpb.PeerStats{{Peer: regions[4].Peers[1]}}))
	suite.srv.GetRaftCluster().HandleRegionHeartbeat(region4)

	// Add pending peers for region5.
	region5 := core.NewRegionInfo(regions[5], regions[5].Peers[0], core.WithPendingPeers([]*metapb.Peer{regions[5].Peers[1], regions[5].Peers[2]}))
	suite.srv.GetRaftCluster().HandleRegionHeartbeat(region5)

	t := suite.T()
	check := func(start, end []byte, limit int, expect []*metapb.Region) {
		scanRegions, err := suite.client.BatchScanRegions(context.Background(), []pd.KeyRange{{StartKey: start, EndKey: end}}, limit)
		re.NoError(err)
		re.Len(scanRegions, len(expect))
		t.Log("scanRegions", scanRegions)
		t.Log("expect", expect)
		for i := range expect {
			re.Equal(expect[i], scanRegions[i].Meta)

			if scanRegions[i].Meta.GetId() == region3.GetID() {
				re.Equal(&metapb.Peer{}, scanRegions[i].Leader)
			} else {
				re.Equal(expect[i].Peers[0], scanRegions[i].Leader)
			}

			if scanRegions[i].Meta.GetId() == region4.GetID() {
				re.Equal([]*metapb.Peer{expect[i].Peers[1]}, scanRegions[i].DownPeers)
			}

			if scanRegions[i].Meta.GetId() == region5.GetID() {
				re.Equal([]*metapb.Peer{expect[i].Peers[1], expect[i].Peers[2]}, scanRegions[i].PendingPeers)
			}
		}
	}

	check([]byte{0}, nil, 10, regions)
	check([]byte{1}, nil, 5, regions[1:6])
	check([]byte{100}, nil, 1, nil)
	check([]byte{1}, []byte{6}, 0, regions[1:6])
	check([]byte{1}, []byte{6}, 2, regions[1:3])
}

func (suite *clientTestSuite) TestGetRegionByID() {
	re := suite.Require()
	regionID := regionIDAllocator.alloc()
	region := &metapb.Region{
		Id: regionID,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
		Peers: peers,
	}
	req := &pdpb.RegionHeartbeatRequest{
		Header: newHeader(suite.srv),
		Region: region,
		Leader: peers[0],
	}
	err := suite.regionHeartbeat.Send(req)
	re.NoError(err)

	testutil.Eventually(re, func() bool {
		r, err := suite.client.GetRegionByID(context.Background(), regionID)
		re.NoError(err)
		if r == nil {
			return false
		}
		return reflect.DeepEqual(region, r.Meta) &&
			reflect.DeepEqual(peers[0], r.Leader)
	})
}

func (suite *clientTestSuite) TestGetStore() {
	re := suite.Require()
	cluster := suite.srv.GetRaftCluster()
	re.NotNil(cluster)
	store := stores[0]

	// Get an up store should be OK.
	n, err := suite.client.GetStore(context.Background(), store.GetId())
	re.NoError(err)
	re.Equal(store, n)

	actualStores, err := suite.client.GetAllStores(context.Background())
	re.NoError(err)
	re.Len(actualStores, len(stores))
	stores = actualStores

	// Mark the store as offline.
	err = cluster.RemoveStore(store.GetId(), false)
	re.NoError(err)
	offlineStore := typeutil.DeepClone(store, core.StoreFactory)
	offlineStore.State = metapb.StoreState_Offline
	offlineStore.NodeState = metapb.NodeState_Removing

	// Get an offline store should be OK.
	n, err = suite.client.GetStore(context.Background(), store.GetId())
	re.NoError(err)
	re.Equal(offlineStore, n)

	// Should return offline stores.
	contains := false
	stores, err = suite.client.GetAllStores(context.Background())
	re.NoError(err)
	for _, store := range stores {
		if store.GetId() == offlineStore.GetId() {
			contains = true
			re.Equal(offlineStore, store)
		}
	}
	re.True(contains)

	// Mark the store as physically destroyed and offline.
	err = cluster.RemoveStore(store.GetId(), true)
	re.NoError(err)
	physicallyDestroyedStoreID := store.GetId()

	// Get a physically destroyed and offline store
	// It should be Tombstone(become Tombstone automatically) or Offline
	n, err = suite.client.GetStore(context.Background(), physicallyDestroyedStoreID)
	re.NoError(err)
	if n != nil { // store is still offline and physically destroyed
		re.Equal(metapb.NodeState_Removing, n.GetNodeState())
		re.True(n.PhysicallyDestroyed)
	}
	// Should return tombstone stores.
	contains = false
	stores, err = suite.client.GetAllStores(context.Background())
	re.NoError(err)
	for _, store := range stores {
		if store.GetId() == physicallyDestroyedStoreID {
			contains = true
			re.NotEqual(metapb.StoreState_Up, store.GetState())
			re.True(store.PhysicallyDestroyed)
		}
	}
	re.True(contains)

	// Should not return tombstone stores.
	stores, err = suite.client.GetAllStores(context.Background(), pd.WithExcludeTombstone())
	re.NoError(err)
	for _, store := range stores {
		if store.GetId() == physicallyDestroyedStoreID {
			re.Equal(metapb.StoreState_Offline, store.GetState())
			re.True(store.PhysicallyDestroyed)
		}
	}
}

func (suite *clientTestSuite) checkGCSafePoint(re *require.Assertions, expectedSafePoint uint64) {
	req := &pdpb.GetGCSafePointRequest{
		Header: newHeader(suite.srv),
	}
	resp, err := suite.grpcSvr.GetGCSafePoint(context.Background(), req)
	re.NoError(err)
	re.Equal(expectedSafePoint, resp.SafePoint)
}

func (suite *clientTestSuite) TestUpdateGCSafePoint() {
	re := suite.Require()
	suite.checkGCSafePoint(re, 0)
	for _, safePoint := range []uint64{0, 1, 2, 3, 233, 23333, 233333333333, math.MaxUint64} {
		newSafePoint, err := suite.client.UpdateGCSafePoint(context.Background(), safePoint)
		re.NoError(err)
		re.Equal(safePoint, newSafePoint)
		suite.checkGCSafePoint(re, safePoint)
	}
	// If the new safe point is less than the old one, it should not be updated.
	newSafePoint, err := suite.client.UpdateGCSafePoint(context.Background(), 1)
	re.Equal(uint64(math.MaxUint64), newSafePoint)
	re.NoError(err)
	suite.checkGCSafePoint(re, math.MaxUint64)
}

func (suite *clientTestSuite) TestUpdateServiceGCSafePoint() {
	re := suite.Require()
	serviceSafePoints := []struct {
		ServiceID string
		TTL       int64
		SafePoint uint64
	}{
		{"b", 1000, 2},
		{"a", 1000, 1},
		{"c", 1000, 3},
	}
	for _, ssp := range serviceSafePoints {
		min, err := suite.client.UpdateServiceGCSafePoint(context.Background(),
			ssp.ServiceID, 1000, ssp.SafePoint)
		re.NoError(err)
		// An service safepoint of ID "gc_worker" is automatically initialized as 0
		re.Equal(uint64(0), min)
	}

	min, err := suite.client.UpdateServiceGCSafePoint(context.Background(),
		"gc_worker", math.MaxInt64, 10)
	re.NoError(err)
	re.Equal(uint64(1), min)

	min, err = suite.client.UpdateServiceGCSafePoint(context.Background(),
		"a", 1000, 4)
	re.NoError(err)
	re.Equal(uint64(2), min)

	min, err = suite.client.UpdateServiceGCSafePoint(context.Background(),
		"b", -100, 2)
	re.NoError(err)
	re.Equal(uint64(3), min)

	// Minimum safepoint does not regress
	min, err = suite.client.UpdateServiceGCSafePoint(context.Background(),
		"b", 1000, 2)
	re.NoError(err)
	re.Equal(uint64(3), min)

	// Update only the TTL of the minimum safepoint
	oldMinSsp, err := suite.srv.GetStorage().LoadMinServiceGCSafePoint(time.Now())
	re.NoError(err)
	re.Equal("c", oldMinSsp.ServiceID)
	re.Equal(uint64(3), oldMinSsp.SafePoint)
	min, err = suite.client.UpdateServiceGCSafePoint(context.Background(),
		"c", 2000, 3)
	re.NoError(err)
	re.Equal(uint64(3), min)
	minSsp, err := suite.srv.GetStorage().LoadMinServiceGCSafePoint(time.Now())
	re.NoError(err)
	re.Equal("c", minSsp.ServiceID)
	re.Equal(uint64(3), oldMinSsp.SafePoint)
	suite.GreaterOrEqual(minSsp.ExpiredAt-oldMinSsp.ExpiredAt, int64(1000))

	// Shrinking TTL is also allowed
	min, err = suite.client.UpdateServiceGCSafePoint(context.Background(),
		"c", 1, 3)
	re.NoError(err)
	re.Equal(uint64(3), min)
	minSsp, err = suite.srv.GetStorage().LoadMinServiceGCSafePoint(time.Now())
	re.NoError(err)
	re.Equal("c", minSsp.ServiceID)
	re.Less(minSsp.ExpiredAt, oldMinSsp.ExpiredAt)

	// TTL can be infinite (represented by math.MaxInt64)
	min, err = suite.client.UpdateServiceGCSafePoint(context.Background(),
		"c", math.MaxInt64, 3)
	re.NoError(err)
	re.Equal(uint64(3), min)
	minSsp, err = suite.srv.GetStorage().LoadMinServiceGCSafePoint(time.Now())
	re.NoError(err)
	re.Equal("c", minSsp.ServiceID)
	re.Equal(minSsp.ExpiredAt, int64(math.MaxInt64))

	// Delete "a" and "c"
	min, err = suite.client.UpdateServiceGCSafePoint(context.Background(),
		"c", -1, 3)
	re.NoError(err)
	re.Equal(uint64(4), min)
	min, err = suite.client.UpdateServiceGCSafePoint(context.Background(),
		"a", -1, 4)
	re.NoError(err)
	// Now gc_worker is the only remaining service safe point.
	re.Equal(uint64(10), min)

	// gc_worker cannot be deleted.
	_, err = suite.client.UpdateServiceGCSafePoint(context.Background(),
		"gc_worker", -1, 10)
	re.Error(err)

	// Cannot set non-infinity TTL for gc_worker
	_, err = suite.client.UpdateServiceGCSafePoint(context.Background(),
		"gc_worker", 10000000, 10)
	re.Error(err)

	// Service safepoint must have a non-empty ID
	_, err = suite.client.UpdateServiceGCSafePoint(context.Background(),
		"", 1000, 15)
	re.Error(err)

	// Put some other safepoints to test fixing gc_worker's safepoint when there exists other safepoints.
	_, err = suite.client.UpdateServiceGCSafePoint(context.Background(),
		"a", 1000, 11)
	re.NoError(err)
	_, err = suite.client.UpdateServiceGCSafePoint(context.Background(),
		"b", 1000, 12)
	re.NoError(err)
	_, err = suite.client.UpdateServiceGCSafePoint(context.Background(),
		"c", 1000, 13)
	re.NoError(err)

	// Force set invalid ttl to gc_worker
	gcWorkerKey := path.Join("gc", "safe_point", "service", "gc_worker")
	{
		gcWorkerSsp := &endpoint.ServiceSafePoint{
			ServiceID: "gc_worker",
			ExpiredAt: -12345,
			SafePoint: 10,
		}
		value, err := json.Marshal(gcWorkerSsp)
		re.NoError(err)
		err = suite.srv.GetStorage().Save(gcWorkerKey, string(value))
		re.NoError(err)
	}

	minSsp, err = suite.srv.GetStorage().LoadMinServiceGCSafePoint(time.Now())
	re.NoError(err)
	re.Equal("gc_worker", minSsp.ServiceID)
	re.Equal(uint64(10), minSsp.SafePoint)
	re.Equal(int64(math.MaxInt64), minSsp.ExpiredAt)

	// Force delete gc_worker, then the min service safepoint is 11 of "a".
	err = suite.srv.GetStorage().Remove(gcWorkerKey)
	re.NoError(err)
	minSsp, err = suite.srv.GetStorage().LoadMinServiceGCSafePoint(time.Now())
	re.NoError(err)
	re.Equal(uint64(11), minSsp.SafePoint)
	// After calling LoadMinServiceGCS when "gc_worker"'s service safepoint is missing, "gc_worker"'s service safepoint
	// will be newly created.
	// Increase "a" so that "gc_worker" is the only minimum that will be returned by LoadMinServiceGCSafePoint.
	_, err = suite.client.UpdateServiceGCSafePoint(context.Background(),
		"a", 1000, 14)
	re.NoError(err)

	minSsp, err = suite.srv.GetStorage().LoadMinServiceGCSafePoint(time.Now())
	re.NoError(err)
	re.Equal("gc_worker", minSsp.ServiceID)
	re.Equal(uint64(11), minSsp.SafePoint)
	re.Equal(int64(math.MaxInt64), minSsp.ExpiredAt)
}

func (suite *clientTestSuite) TestScatterRegion() {
	re := suite.Require()
	CreateRegion := func() uint64 {
		regionID := regionIDAllocator.alloc()
		region := &metapb.Region{
			Id: regionID,
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: 1,
				Version: 1,
			},
			Peers:    peers,
			StartKey: []byte("fff"),
			EndKey:   []byte("ggg"),
		}
		req := &pdpb.RegionHeartbeatRequest{
			Header: newHeader(suite.srv),
			Region: region,
			Leader: peers[0],
		}
		err := suite.regionHeartbeat.Send(req)
		re.NoError(err)
		return regionID
	}
	var regionID = CreateRegion()
	regionsID := []uint64{regionID}
	// Test interface `ScatterRegions`.
	testutil.Eventually(re, func() bool {
		scatterResp, err := suite.client.ScatterRegions(context.Background(), regionsID, pd.WithGroup("test"), pd.WithRetry(1))
		if err != nil {
			return false
		}
		if scatterResp.FinishedPercentage != uint64(100) {
			return false
		}
		resp, err := suite.client.GetOperator(context.Background(), regionID)
		if err != nil {
			return false
		}
		return resp.GetRegionId() == regionID &&
			string(resp.GetDesc()) == "scatter-region" &&
			resp.GetStatus() == pdpb.OperatorStatus_RUNNING
	}, testutil.WithTickInterval(time.Second))

	// Test interface `ScatterRegion`.
	// TODO: Deprecate interface `ScatterRegion`.
	// create a new region as scatter operation from previous test might be running

	regionID = CreateRegion()
	testutil.Eventually(re, func() bool {
		err := suite.client.ScatterRegion(context.Background(), regionID)
		if err != nil {
			return false
		}
		resp, err := suite.client.GetOperator(context.Background(), regionID)
		if err != nil {
			return false
		}
		return resp.GetRegionId() == regionID &&
			string(resp.GetDesc()) == "scatter-region" &&
			resp.GetStatus() == pdpb.OperatorStatus_RUNNING
	}, testutil.WithTickInterval(time.Second))
}

func TestWatch(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer cluster.Destroy()
	endpoints := runServer(re, cluster)
	client := setupCli(ctx, re, endpoints)
	defer client.Close()

	key := "test"
	resp, err := client.Get(ctx, []byte(key))
	re.NoError(err)
	rev := resp.GetHeader().GetRevision()
	ch, err := client.Watch(ctx, []byte(key), pd.WithRev(rev))
	re.NoError(err)
	exit := make(chan struct{})
	go func() {
		var events []*meta_storagepb.Event
		for e := range ch {
			events = append(events, e...)
			if len(events) >= 3 {
				break
			}
		}
		re.Equal(meta_storagepb.Event_PUT, events[0].GetType())
		re.Equal("1", string(events[0].GetKv().GetValue()))
		re.Equal(meta_storagepb.Event_PUT, events[1].GetType())
		re.Equal("2", string(events[1].GetKv().GetValue()))
		re.Equal(meta_storagepb.Event_DELETE, events[2].GetType())
		exit <- struct{}{}
	}()

	cli, err := clientv3.NewFromURLs(endpoints)
	re.NoError(err)
	defer cli.Close()
	cli.Put(context.Background(), key, "1")
	cli.Put(context.Background(), key, "2")
	cli.Delete(context.Background(), key)
	<-exit
}

func TestPutGet(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer cluster.Destroy()
	endpoints := runServer(re, cluster)
	client := setupCli(ctx, re, endpoints)
	defer client.Close()

	key := []byte("test")
	putResp, err := client.Put(context.Background(), key, []byte("1"))
	re.NoError(err)
	re.Empty(putResp.GetPrevKv())
	getResp, err := client.Get(context.Background(), key)
	re.NoError(err)
	re.Equal([]byte("1"), getResp.GetKvs()[0].Value)
	re.NotEqual(0, getResp.GetHeader().GetRevision())
	putResp, err = client.Put(context.Background(), key, []byte("2"), pd.WithPrevKV())
	re.NoError(err)
	re.Equal([]byte("1"), putResp.GetPrevKv().Value)
	getResp, err = client.Get(context.Background(), key)
	re.NoError(err)
	re.Equal([]byte("2"), getResp.GetKvs()[0].Value)
	s := cluster.GetLeaderServer()
	// use etcd client delete the key
	_, err = s.GetEtcdClient().Delete(context.Background(), string(key))
	re.NoError(err)
	getResp, err = client.Get(context.Background(), key)
	re.NoError(err)
	re.Empty(getResp.GetKvs())
}

// TestClientWatchWithRevision is the same as TestClientWatchWithRevision in global config.
func TestClientWatchWithRevision(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer cluster.Destroy()
	endpoints := runServer(re, cluster)
	client := setupCli(ctx, re, endpoints)
	defer client.Close()
	s := cluster.GetLeaderServer()
	watchPrefix := "watch_test"
	defer func() {
		_, err := s.GetEtcdClient().Delete(context.Background(), watchPrefix+"test")
		re.NoError(err)

		for i := 3; i < 9; i++ {
			_, err := s.GetEtcdClient().Delete(context.Background(), watchPrefix+strconv.Itoa(i))
			re.NoError(err)
		}
	}()
	// Mock get revision by loading
	r, err := s.GetEtcdClient().Put(context.Background(), watchPrefix+"test", "test")
	re.NoError(err)
	res, err := client.Get(context.Background(), []byte(watchPrefix), pd.WithPrefix())
	re.NoError(err)
	re.Len(res.Kvs, 1)
	re.LessOrEqual(r.Header.GetRevision(), res.GetHeader().GetRevision())
	// Mock when start watcher there are existed some keys, will load firstly

	for i := 0; i < 6; i++ {
		_, err = s.GetEtcdClient().Put(context.Background(), watchPrefix+strconv.Itoa(i), strconv.Itoa(i))
		re.NoError(err)
	}
	// Start watcher at next revision
	ch, err := client.Watch(context.Background(), []byte(watchPrefix), pd.WithRev(res.GetHeader().GetRevision()), pd.WithPrefix(), pd.WithPrevKV())
	re.NoError(err)
	// Mock delete
	for i := 0; i < 3; i++ {
		_, err = s.GetEtcdClient().Delete(context.Background(), watchPrefix+strconv.Itoa(i))
		re.NoError(err)
	}
	// Mock put
	for i := 6; i < 9; i++ {
		_, err = s.GetEtcdClient().Put(context.Background(), watchPrefix+strconv.Itoa(i), strconv.Itoa(i))
		re.NoError(err)
	}
	var watchCount int
	for {
		select {
		case <-time.After(1 * time.Second):
			re.Equal(13, watchCount)
			return
		case res := <-ch:
			for _, r := range res {
				watchCount++
				if r.GetType() == meta_storagepb.Event_DELETE {
					re.Equal(watchPrefix+string(r.PrevKv.Value), string(r.Kv.Key))
				} else {
					re.Equal(watchPrefix+string(r.Kv.Value), string(r.Kv.Key))
				}
			}
		}
	}
}

func (suite *clientTestSuite) TestMemberUpdateBackOff() {
	re := suite.Require()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 3)
	re.NoError(err)
	defer cluster.Destroy()

	endpoints := runServer(re, cluster)
	cli := setupCli(ctx, re, endpoints)
	defer cli.Close()
	innerCli, ok := cli.(interface{ GetServiceDiscovery() pd.ServiceDiscovery })
	re.True(ok)

	leader := cluster.GetLeader()
	waitLeader(re, innerCli.GetServiceDiscovery(), cluster.GetServer(leader))
	memberID := cluster.GetServer(leader).GetLeader().GetMemberId()

	re.NoError(failpoint.Enable("github.com/tikv/pd/server/leaderLoopCheckAgain", fmt.Sprintf("return(\"%d\")", memberID)))
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/exitCampaignLeader", fmt.Sprintf("return(\"%d\")", memberID)))
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/timeoutWaitPDLeader", `return(true)`))
	// make sure back off executed.
	re.NoError(failpoint.Enable("github.com/tikv/pd/client/retry/backOffExecute", `return(true)`))
	leader2 := waitLeaderChange(re, cluster, leader, innerCli.GetServiceDiscovery())
	re.True(retry.TestBackOffExecute())

	re.NotEqual(leader, leader2)

	re.NoError(failpoint.Disable("github.com/tikv/pd/server/leaderLoopCheckAgain"))
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/exitCampaignLeader"))
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/timeoutWaitPDLeader"))
	re.NoError(failpoint.Disable("github.com/tikv/pd/client/retry/backOffExecute"))
}

func waitLeaderChange(re *require.Assertions, cluster *tests.TestCluster, old string, cli pd.ServiceDiscovery) string {
	var leader string
	testutil.Eventually(re, func() bool {
		cli.ScheduleCheckMemberChanged()
		leader = cluster.GetLeader()
		if leader == old || leader == "" {
			return false
		}
		return true
	})
	return leader
}

func (suite *clientTestSuite) TestBatchScanRegions() {
	var (
		re        = suite.Require()
		ctx       = context.Background()
		regionLen = 10
		regions   = make([]*metapb.Region, 0, regionLen)
	)

	for i := 0; i < regionLen; i++ {
		regionID := regionIDAllocator.alloc()
		r := &metapb.Region{
			Id: regionID,
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: 1,
				Version: 1,
			},
			StartKey: []byte{byte(i)},
			EndKey:   []byte{byte(i + 1)},
			Peers:    peers,
		}
		regions = append(regions, r)
		req := &pdpb.RegionHeartbeatRequest{
			Header: newHeader(suite.srv),
			Region: r,
			Leader: peers[0],
		}
		err := suite.regionHeartbeat.Send(req)
		re.NoError(err)
	}

	// Wait for region heartbeats.
	testutil.Eventually(re, func() bool {
		scanRegions, err := suite.client.BatchScanRegions(ctx, []pd.KeyRange{{StartKey: []byte{0}, EndKey: nil}}, 10)
		return err == nil && len(scanRegions) == 10
	})

	// Set leader of region3 to nil.
	region3 := core.NewRegionInfo(regions[3], nil)
	suite.srv.GetRaftCluster().HandleRegionHeartbeat(region3)

	// Add down peer for region4.
	region4 := core.NewRegionInfo(regions[4], regions[4].Peers[0], core.WithDownPeers([]*pdpb.PeerStats{{Peer: regions[4].Peers[1]}}))
	suite.srv.GetRaftCluster().HandleRegionHeartbeat(region4)

	// Add pending peers for region5.
	region5 := core.NewRegionInfo(regions[5], regions[5].Peers[0], core.WithPendingPeers([]*metapb.Peer{regions[5].Peers[1], regions[5].Peers[2]}))
	suite.srv.GetRaftCluster().HandleRegionHeartbeat(region5)

	// Add buckets for region6.
	region6 := core.NewRegionInfo(regions[6], regions[6].Peers[0], core.SetBuckets(&metapb.Buckets{RegionId: regions[6].Id, Version: 2}))
	suite.srv.GetRaftCluster().HandleRegionHeartbeat(region6)

	t := suite.T()
	var outputMustContainAllKeyRangeOptions []bool
	check := func(ranges []pd.KeyRange, limit int, expect []*metapb.Region) {
		for _, bucket := range []bool{false, true} {
			for _, outputMustContainAllKeyRange := range outputMustContainAllKeyRangeOptions {
				var opts []pd.GetRegionOption
				if bucket {
					opts = append(opts, pd.WithBuckets())
				}
				if outputMustContainAllKeyRange {
					opts = append(opts, pd.WithOutputMustContainAllKeyRange())
				}
				scanRegions, err := suite.client.BatchScanRegions(ctx, ranges, limit, opts...)
				re.NoError(err)
				t.Log("scanRegions", scanRegions)
				t.Log("expect", expect)
				re.Len(scanRegions, len(expect))
				for i := range expect {
					re.Equal(expect[i], scanRegions[i].Meta)

					if scanRegions[i].Meta.GetId() == region3.GetID() {
						re.Equal(&metapb.Peer{}, scanRegions[i].Leader)
					} else {
						re.Equal(expect[i].Peers[0], scanRegions[i].Leader)
					}

					if scanRegions[i].Meta.GetId() == region4.GetID() {
						re.Equal([]*metapb.Peer{expect[i].Peers[1]}, scanRegions[i].DownPeers)
					}

					if scanRegions[i].Meta.GetId() == region5.GetID() {
						re.Equal([]*metapb.Peer{expect[i].Peers[1], expect[i].Peers[2]}, scanRegions[i].PendingPeers)
					}

					if scanRegions[i].Meta.GetId() == region6.GetID() {
						if !bucket {
							re.Nil(scanRegions[i].Buckets)
						} else {
							re.Equal(scanRegions[i].Buckets, region6.GetBuckets())
						}
					}
				}
			}
		}
	}

	// valid ranges
	outputMustContainAllKeyRangeOptions = []bool{false, true}
	check([]pd.KeyRange{{StartKey: []byte{0}, EndKey: nil}}, 10, regions)
	check([]pd.KeyRange{{StartKey: []byte{1}, EndKey: nil}}, 5, regions[1:6])
	check([]pd.KeyRange{
		{StartKey: []byte{0}, EndKey: []byte{1}},
		{StartKey: []byte{2}, EndKey: []byte{3}},
		{StartKey: []byte{4}, EndKey: []byte{5}},
		{StartKey: []byte{6}, EndKey: []byte{7}},
		{StartKey: []byte{8}, EndKey: []byte{9}},
	}, 10, []*metapb.Region{regions[0], regions[2], regions[4], regions[6], regions[8]})
	check([]pd.KeyRange{
		{StartKey: []byte{0}, EndKey: []byte{1}},
		{StartKey: []byte{2}, EndKey: []byte{3}},
		{StartKey: []byte{4}, EndKey: []byte{5}},
		{StartKey: []byte{6}, EndKey: []byte{7}},
		{StartKey: []byte{8}, EndKey: []byte{9}},
	}, 3, []*metapb.Region{regions[0], regions[2], regions[4]})

	outputMustContainAllKeyRangeOptions = []bool{false}
	check([]pd.KeyRange{
		{StartKey: []byte{0}, EndKey: []byte{0, 1}}, // non-continuous ranges in a region
		{StartKey: []byte{0, 2}, EndKey: []byte{0, 3}},
		{StartKey: []byte{0, 3}, EndKey: []byte{0, 4}},
		{StartKey: []byte{0, 5}, EndKey: []byte{0, 6}},
		{StartKey: []byte{0, 7}, EndKey: []byte{3}},
		{StartKey: []byte{4}, EndKey: []byte{5}},
	}, 10, []*metapb.Region{regions[0], regions[1], regions[2], regions[4]})
	outputMustContainAllKeyRangeOptions = []bool{false}
	check([]pd.KeyRange{
		{StartKey: []byte{9}, EndKey: []byte{10, 1}},
	}, 10, []*metapb.Region{regions[9]})

	// invalid ranges
	_, err := suite.client.BatchScanRegions(
		ctx,
		[]pd.KeyRange{{StartKey: []byte{1}, EndKey: []byte{0}}},
		10,
		pd.WithOutputMustContainAllKeyRange(),
	)
	re.ErrorContains(err, "invalid key range, start key > end key")
	_, err = suite.client.BatchScanRegions(ctx, []pd.KeyRange{
		{StartKey: []byte{0}, EndKey: []byte{2}},
		{StartKey: []byte{1}, EndKey: []byte{3}},
	}, 10)
	re.ErrorContains(err, "invalid key range, ranges overlapped")
	_, err = suite.client.BatchScanRegions(
		ctx,
		[]pd.KeyRange{{StartKey: []byte{9}, EndKey: []byte{10, 1}}},
		10,
		pd.WithOutputMustContainAllKeyRange(),
	)
	re.ErrorContains(err, "found a hole region in the last")
	req := &pdpb.RegionHeartbeatRequest{
		Header: newHeader(suite.srv),
		Region: &metapb.Region{
			Id: 100,
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: 1,
				Version: 1,
			},
			StartKey: []byte{100},
			EndKey:   []byte{101},
			Peers:    peers,
		},
		Leader: peers[0],
	}
	re.NoError(suite.regionHeartbeat.Send(req))

	// Wait for region heartbeats.
	testutil.Eventually(re, func() bool {
		_, err = suite.client.BatchScanRegions(
			ctx,
			[]pd.KeyRange{{StartKey: []byte{9}, EndKey: []byte{101}}},
			10,
			pd.WithOutputMustContainAllKeyRange(),
		)
		return err != nil && strings.Contains(err.Error(), "found a hole region between")
	})
}
