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
	"sync"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/pkg/assertutil"
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/pkg/tsoutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/storage/endpoint"
	"github.com/tikv/pd/server/tso"
	"github.com/tikv/pd/tests"
	"go.uber.org/goleak"
)

const (
	tsoRequestConcurrencyNumber = 5
	tsoRequestRound             = 30
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

type client interface {
	GetLeaderAddr() string
	ScheduleCheckLeader()
	GetURLs() []string
	GetAllocatorLeaderURLs() map[string]string
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
	re.Contains(err.Error(), "ErrClientGetLeader")
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
	cli := setupCli(re, ctx, endpoints)

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
	waitLeader(re, cli.(client), cluster.GetServer(leader).GetConfig().ClientUrls)

	err = cluster.GetServer(leader).Stop()
	re.NoError(err)
	leader = cluster.WaitLeader()
	re.NotEmpty(leader)
	waitLeader(re, cli.(client), cluster.GetServer(leader).GetConfig().ClientUrls)

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
	urls := cli.(client).GetURLs()
	sort.Strings(urls)
	sort.Strings(endpoints)
	re.Equal(endpoints, urls)
}

func TestLeaderTransfer(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 2)
	re.NoError(err)
	defer cluster.Destroy()

	endpoints := runServer(re, cluster)
	cli := setupCli(re, ctx, endpoints)

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
	for i := 0; i < 5; i++ {
		oldLeaderName := cluster.WaitLeader()
		err := cluster.GetServer(oldLeaderName).ResignLeader()
		re.NoError(err)
		newLeaderName := cluster.WaitLeader()
		re.NotEqual(oldLeaderName, newLeaderName)
	}
	close(quit)
	wg.Wait()
}

// More details can be found in this issue: https://github.com/tikv/pd/issues/4884
func TestUpdateAfterResetTSO(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 2)
	re.NoError(err)
	defer cluster.Destroy()

	endpoints := runServer(re, cluster)
	cli := setupCli(re, ctx, endpoints)

	testutil.Eventually(re, func() bool {
		_, _, err := cli.GetTS(context.TODO())
		return err == nil
	})
	// Transfer leader to trigger the TSO resetting.
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/updateAfterResetTSO", "return(true)"))
	oldLeaderName := cluster.WaitLeader()
	err = cluster.GetServer(oldLeaderName).ResignLeader()
	re.NoError(err)
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/updateAfterResetTSO"))
	newLeaderName := cluster.WaitLeader()
	re.NotEqual(oldLeaderName, newLeaderName)
	// Request a new TSO.
	testutil.Eventually(re, func() bool {
		_, _, err := cli.GetTS(context.TODO())
		return err == nil
	})
	// Transfer leader back.
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/tso/delaySyncTimestamp", `return(true)`))
	err = cluster.GetServer(newLeaderName).ResignLeader()
	re.NoError(err)
	// Should NOT panic here.
	testutil.Eventually(re, func() bool {
		_, _, err := cli.GetTS(context.TODO())
		return err == nil
	})
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/tso/delaySyncTimestamp"))
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
	cli := setupCli(re, ctx, endpoints)

	// Check allocator leaders URL map.
	cli.Close()
	for dcLocation, url := range cli.(client).GetAllocatorLeaderURLs() {
		if dcLocation == tso.GlobalDCLocation {
			urls := cli.(client).GetURLs()
			sort.Strings(urls)
			sort.Strings(endpoints)
			re.Equal(endpoints, urls)
			continue
		}
		pdName, exist := allocatorLeaderMap[dcLocation]
		re.True(exist)
		re.Greater(len(pdName), 0)
		pdURL, exist := endpointsMap[pdName]
		re.True(exist)
		re.Greater(len(pdURL), 0)
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
	cli1 := setupCli(re, ctx, endpoints)
	cli2 := setupCli(re, ctx, endpoints)
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
	cli := setupCli(re, ctx, endpoints)

	// Wait for all nodes becoming healthy.
	time.Sleep(time.Second * 5)

	// Join a new dc-location
	pd4, err := cluster.Join(ctx, func(conf *config.Config, serverName string) {
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
	cluster.WaitLeader()
	_, _, err = cli.GetTS(ctx)
	re.Error(err)
	re.True(pd.IsLeaderChange(err))
	_, _, err = cli.GetTS(ctx)
	re.NoError(err)
	re.NoError(failpoint.Disable("github.com/tikv/pd/client/skipUpdateMember"))

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
					re.NoError(err)
					globalTS1 := tsoutil.ComposeTS(globalPhysical1, globalLogical1)
					localPhysical, localLogical, err := cli.GetLocalTS(context.TODO(), dc)
					re.NoError(err)
					localTS := tsoutil.ComposeTS(localPhysical, localLogical)
					globalPhysical2, globalLogical2, err := cli.GetTS(context.TODO())
					re.NoError(err)
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

func TestCustomTimeout(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer cluster.Destroy()

	endpoints := runServer(re, cluster)
	cli := setupCli(re, ctx, endpoints, pd.WithCustomTimeoutOption(time.Second))

	start := time.Now()
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/customTimeout", "return(true)"))
	_, err = cli.GetAllStores(context.TODO())
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/customTimeout"))
	re.Error(err)
	re.GreaterOrEqual(time.Since(start), time.Second)
	re.Less(time.Since(start), 2*time.Second)
}

func TestGetRegionFromFollowerClient(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pd.LeaderHealthCheckInterval = 100 * time.Millisecond
	cluster, err := tests.NewTestCluster(ctx, 3)
	re.NoError(err)
	defer cluster.Destroy()

	endpoints := runServer(re, cluster)
	cli := setupCli(re, ctx, endpoints, pd.WithForwardingOption(true))

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
func TestGetTsoFromFollowerClient1(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pd.LeaderHealthCheckInterval = 100 * time.Millisecond
	cluster, err := tests.NewTestCluster(ctx, 3)
	re.NoError(err)
	defer cluster.Destroy()

	endpoints := runServer(re, cluster)
	cli := setupCli(re, ctx, endpoints, pd.WithForwardingOption(true))

	re.NoError(failpoint.Enable("github.com/tikv/pd/client/unreachableNetwork", "return(true)"))
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

	lastTS = checkTS(re, cli, lastTS)
	re.NoError(failpoint.Disable("github.com/tikv/pd/client/unreachableNetwork"))
	time.Sleep(2 * time.Second)
	checkTS(re, cli, lastTS)
}

// case 2: unreachable -> leader transfer -> normal
func TestGetTsoFromFollowerClient2(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pd.LeaderHealthCheckInterval = 100 * time.Millisecond
	cluster, err := tests.NewTestCluster(ctx, 3)
	re.NoError(err)
	defer cluster.Destroy()

	endpoints := runServer(re, cluster)
	cli := setupCli(re, ctx, endpoints, pd.WithForwardingOption(true))

	re.NoError(failpoint.Enable("github.com/tikv/pd/client/unreachableNetwork", "return(true)"))
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

	lastTS = checkTS(re, cli, lastTS)
	re.NoError(cluster.GetServer(cluster.GetLeader()).ResignLeader())
	cluster.WaitLeader()
	lastTS = checkTS(re, cli, lastTS)

	re.NoError(failpoint.Disable("github.com/tikv/pd/client/unreachableNetwork"))
	time.Sleep(5 * time.Second)
	checkTS(re, cli, lastTS)
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
	cluster.WaitLeader()
	leaderServer := cluster.GetServer(cluster.GetLeader())
	re.NoError(leaderServer.BootstrapCluster())

	testServers := cluster.GetServers()
	endpoints := make([]string, 0, len(testServers))
	for _, s := range testServers {
		endpoints = append(endpoints, s.GetConfig().AdvertiseClientUrls)
	}
	return endpoints
}

func setupCli(re *require.Assertions, ctx context.Context, endpoints []string, opts ...pd.ClientOption) pd.Client {
	cli, err := pd.NewClientWithContext(ctx, endpoints, pd.SecurityOption{}, opts...)
	re.NoError(err)
	return cli
}

func waitLeader(re *require.Assertions, cli client, leader string) {
	testutil.Eventually(re, func() bool {
		cli.ScheduleCheckLeader()
		return cli.GetLeaderAddr() == leader
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
	postData, err := json.Marshal(map[string]interface{}{
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
	cli := setupCli(re, ctx, endpoints)
	cli.GetTSAsync(context.TODO())
	time.Sleep(time.Second)
	cli.Close()
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
	cleanup         server.CleanupFunc
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
	suite.srv, suite.cleanup, err = server.NewTestServer(assertutil.CheckerWithNilAssert(re))
	suite.NoError(err)
	suite.grpcPDClient = testutil.MustNewGrpcClient(re, suite.srv.GetAddr())
	suite.grpcSvr = &server.GrpcServer{Server: suite.srv}

	server.MustWaitLeader(re, []*server.Server{suite.srv})
	suite.bootstrapServer(newHeader(suite.srv), suite.grpcPDClient)

	suite.ctx, suite.clean = context.WithCancel(context.Background())
	suite.client = setupCli(re, suite.ctx, suite.srv.GetEndpoints())

	suite.regionHeartbeat, err = suite.grpcPDClient.RegionHeartbeat(suite.ctx)
	suite.NoError(err)
	suite.reportBucket, err = suite.grpcPDClient.ReportBuckets(suite.ctx)
	suite.NoError(err)
	cluster := suite.srv.GetRaftCluster()
	suite.NotNil(cluster)
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
	}
	config := cluster.GetStoreConfig()
	config.EnableRegionBucket = true
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

func (suite *clientTestSuite) bootstrapServer(header *pdpb.RequestHeader, client pdpb.PDClient) {
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
	_, err := client.Bootstrap(context.Background(), req)
	suite.NoError(err)
}

func (suite *clientTestSuite) TestNormalTSO() {
	var wg sync.WaitGroup
	wg.Add(tsoRequestConcurrencyNumber)
	for i := 0; i < tsoRequestConcurrencyNumber; i++ {
		go func() {
			defer wg.Done()
			var lastTS uint64
			for i := 0; i < tsoRequestRound; i++ {
				physical, logical, err := suite.client.GetTS(context.Background())
				suite.NoError(err)
				ts := tsoutil.ComposeTS(physical, logical)
				suite.Less(lastTS, ts)
				lastTS = ts
			}
		}()
	}
	wg.Wait()
}

func (suite *clientTestSuite) TestGetTSAsync() {
	var wg sync.WaitGroup
	wg.Add(tsoRequestConcurrencyNumber)
	for i := 0; i < tsoRequestConcurrencyNumber; i++ {
		go func() {
			defer wg.Done()
			tsFutures := make([]pd.TSFuture, tsoRequestRound)
			for i := range tsFutures {
				tsFutures[i] = suite.client.GetTSAsync(context.Background())
			}
			var lastTS uint64 = math.MaxUint64
			for i := len(tsFutures) - 1; i >= 0; i-- {
				physical, logical, err := tsFutures[i].Wait()
				suite.NoError(err)
				ts := tsoutil.ComposeTS(physical, logical)
				suite.Greater(lastTS, ts)
				lastTS = ts
			}
		}()
	}
	wg.Wait()
}

func (suite *clientTestSuite) TestGetRegion() {
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
	suite.NoError(err)
	re := suite.Require()
	testutil.Eventually(re, func() bool {
		r, err := suite.client.GetRegion(context.Background(), []byte("a"))
		suite.NoError(err)
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
	suite.NoError(suite.reportBucket.Send(breq))
	testutil.Eventually(re, func() bool {
		r, err := suite.client.GetRegion(context.Background(), []byte("a"), pd.WithBuckets())
		suite.NoError(err)
		if r == nil {
			return false
		}
		return r.Buckets != nil
	})
	config := suite.srv.GetRaftCluster().GetStoreConfig()
	config.EnableRegionBucket = false
	testutil.Eventually(re, func() bool {
		r, err := suite.client.GetRegion(context.Background(), []byte("a"), pd.WithBuckets())
		suite.NoError(err)
		if r == nil {
			return false
		}
		return r.Buckets == nil
	})
	config.EnableRegionBucket = true
}

func (suite *clientTestSuite) TestGetPrevRegion() {
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
		suite.NoError(err)
	}
	time.Sleep(500 * time.Millisecond)
	for i := 0; i < 20; i++ {
		testutil.Eventually(suite.Require(), func() bool {
			r, err := suite.client.GetPrevRegion(context.Background(), []byte{byte(i)})
			suite.NoError(err)
			if i > 0 && i < regionLen {
				return reflect.DeepEqual(peers[0], r.Leader) &&
					reflect.DeepEqual(regions[i-1], r.Meta)
			}
			return r == nil
		})
	}
}

func (suite *clientTestSuite) TestScanRegions() {
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
		suite.NoError(err)
	}

	// Wait for region heartbeats.
	testutil.Eventually(suite.Require(), func() bool {
		scanRegions, err := suite.client.ScanRegions(context.Background(), []byte{0}, nil, 10)
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
		scanRegions, err := suite.client.ScanRegions(context.Background(), start, end, limit)
		suite.NoError(err)
		suite.Len(scanRegions, len(expect))
		t.Log("scanRegions", scanRegions)
		t.Log("expect", expect)
		for i := range expect {
			suite.Equal(expect[i], scanRegions[i].Meta)

			if scanRegions[i].Meta.GetId() == region3.GetID() {
				suite.Equal(&metapb.Peer{}, scanRegions[i].Leader)
			} else {
				suite.Equal(expect[i].Peers[0], scanRegions[i].Leader)
			}

			if scanRegions[i].Meta.GetId() == region4.GetID() {
				suite.Equal([]*metapb.Peer{expect[i].Peers[1]}, scanRegions[i].DownPeers)
			}

			if scanRegions[i].Meta.GetId() == region5.GetID() {
				suite.Equal([]*metapb.Peer{expect[i].Peers[1], expect[i].Peers[2]}, scanRegions[i].PendingPeers)
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
	suite.NoError(err)

	testutil.Eventually(suite.Require(), func() bool {
		r, err := suite.client.GetRegionByID(context.Background(), regionID)
		suite.NoError(err)
		if r == nil {
			return false
		}
		return reflect.DeepEqual(region, r.Meta) &&
			reflect.DeepEqual(peers[0], r.Leader)
	})
}

func (suite *clientTestSuite) TestGetStore() {
	cluster := suite.srv.GetRaftCluster()
	suite.NotNil(cluster)
	store := stores[0]

	// Get an up store should be OK.
	n, err := suite.client.GetStore(context.Background(), store.GetId())
	suite.NoError(err)
	suite.Equal(store, n)

	actualStores, err := suite.client.GetAllStores(context.Background())
	suite.NoError(err)
	suite.Len(actualStores, len(stores))
	stores = actualStores

	// Mark the store as offline.
	err = cluster.RemoveStore(store.GetId(), false)
	suite.NoError(err)
	offlineStore := proto.Clone(store).(*metapb.Store)
	offlineStore.State = metapb.StoreState_Offline
	offlineStore.NodeState = metapb.NodeState_Removing

	// Get an offline store should be OK.
	n, err = suite.client.GetStore(context.Background(), store.GetId())
	suite.NoError(err)
	suite.Equal(offlineStore, n)

	// Should return offline stores.
	contains := false
	stores, err = suite.client.GetAllStores(context.Background())
	suite.NoError(err)
	for _, store := range stores {
		if store.GetId() == offlineStore.GetId() {
			contains = true
			suite.Equal(offlineStore, store)
		}
	}
	suite.True(contains)

	// Mark the store as physically destroyed and offline.
	err = cluster.RemoveStore(store.GetId(), true)
	suite.NoError(err)
	physicallyDestroyedStoreID := store.GetId()

	// Get a physically destroyed and offline store
	// It should be Tombstone(become Tombstone automically) or Offline
	n, err = suite.client.GetStore(context.Background(), physicallyDestroyedStoreID)
	suite.NoError(err)
	if n != nil { // store is still offline and physically destroyed
		suite.Equal(metapb.NodeState_Removing, n.GetNodeState())
		suite.True(n.PhysicallyDestroyed)
	}
	// Should return tombstone stores.
	contains = false
	stores, err = suite.client.GetAllStores(context.Background())
	suite.NoError(err)
	for _, store := range stores {
		if store.GetId() == physicallyDestroyedStoreID {
			contains = true
			suite.NotEqual(metapb.StoreState_Up, store.GetState())
			suite.True(store.PhysicallyDestroyed)
		}
	}
	suite.True(contains)

	// Should not return tombstone stores.
	stores, err = suite.client.GetAllStores(context.Background(), pd.WithExcludeTombstone())
	suite.NoError(err)
	for _, store := range stores {
		if store.GetId() == physicallyDestroyedStoreID {
			suite.Equal(metapb.StoreState_Offline, store.GetState())
			suite.True(store.PhysicallyDestroyed)
		}
	}
}

func (suite *clientTestSuite) checkGCSafePoint(expectedSafePoint uint64) {
	req := &pdpb.GetGCSafePointRequest{
		Header: newHeader(suite.srv),
	}
	resp, err := suite.grpcSvr.GetGCSafePoint(context.Background(), req)
	suite.NoError(err)
	suite.Equal(expectedSafePoint, resp.SafePoint)
}

func (suite *clientTestSuite) TestUpdateGCSafePoint() {
	suite.checkGCSafePoint(0)
	for _, safePoint := range []uint64{0, 1, 2, 3, 233, 23333, 233333333333, math.MaxUint64} {
		newSafePoint, err := suite.client.UpdateGCSafePoint(context.Background(), safePoint)
		suite.NoError(err)
		suite.Equal(safePoint, newSafePoint)
		suite.checkGCSafePoint(safePoint)
	}
	// If the new safe point is less than the old one, it should not be updated.
	newSafePoint, err := suite.client.UpdateGCSafePoint(context.Background(), 1)
	suite.Equal(uint64(math.MaxUint64), newSafePoint)
	suite.NoError(err)
	suite.checkGCSafePoint(math.MaxUint64)
}

func (suite *clientTestSuite) TestUpdateServiceGCSafePoint() {
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
		suite.NoError(err)
		// An service safepoint of ID "gc_worker" is automatically initialized as 0
		suite.Equal(uint64(0), min)
	}

	min, err := suite.client.UpdateServiceGCSafePoint(context.Background(),
		"gc_worker", math.MaxInt64, 10)
	suite.NoError(err)
	suite.Equal(uint64(1), min)

	min, err = suite.client.UpdateServiceGCSafePoint(context.Background(),
		"a", 1000, 4)
	suite.NoError(err)
	suite.Equal(uint64(2), min)

	min, err = suite.client.UpdateServiceGCSafePoint(context.Background(),
		"b", -100, 2)
	suite.NoError(err)
	suite.Equal(uint64(3), min)

	// Minimum safepoint does not regress
	min, err = suite.client.UpdateServiceGCSafePoint(context.Background(),
		"b", 1000, 2)
	suite.NoError(err)
	suite.Equal(uint64(3), min)

	// Update only the TTL of the minimum safepoint
	oldMinSsp, err := suite.srv.GetStorage().LoadMinServiceGCSafePoint(time.Now())
	suite.NoError(err)
	suite.Equal("c", oldMinSsp.ServiceID)
	suite.Equal(uint64(3), oldMinSsp.SafePoint)
	min, err = suite.client.UpdateServiceGCSafePoint(context.Background(),
		"c", 2000, 3)
	suite.NoError(err)
	suite.Equal(uint64(3), min)
	minSsp, err := suite.srv.GetStorage().LoadMinServiceGCSafePoint(time.Now())
	suite.NoError(err)
	suite.Equal("c", minSsp.ServiceID)
	suite.Equal(uint64(3), oldMinSsp.SafePoint)
	suite.GreaterOrEqual(minSsp.ExpiredAt-oldMinSsp.ExpiredAt, int64(1000))

	// Shrinking TTL is also allowed
	min, err = suite.client.UpdateServiceGCSafePoint(context.Background(),
		"c", 1, 3)
	suite.NoError(err)
	suite.Equal(uint64(3), min)
	minSsp, err = suite.srv.GetStorage().LoadMinServiceGCSafePoint(time.Now())
	suite.NoError(err)
	suite.Equal("c", minSsp.ServiceID)
	suite.Less(minSsp.ExpiredAt, oldMinSsp.ExpiredAt)

	// TTL can be infinite (represented by math.MaxInt64)
	min, err = suite.client.UpdateServiceGCSafePoint(context.Background(),
		"c", math.MaxInt64, 3)
	suite.NoError(err)
	suite.Equal(uint64(3), min)
	minSsp, err = suite.srv.GetStorage().LoadMinServiceGCSafePoint(time.Now())
	suite.NoError(err)
	suite.Equal("c", minSsp.ServiceID)
	suite.Equal(minSsp.ExpiredAt, int64(math.MaxInt64))

	// Delete "a" and "c"
	min, err = suite.client.UpdateServiceGCSafePoint(context.Background(),
		"c", -1, 3)
	suite.NoError(err)
	suite.Equal(uint64(4), min)
	min, err = suite.client.UpdateServiceGCSafePoint(context.Background(),
		"a", -1, 4)
	suite.NoError(err)
	// Now gc_worker is the only remaining service safe point.
	suite.Equal(uint64(10), min)

	// gc_worker cannot be deleted.
	_, err = suite.client.UpdateServiceGCSafePoint(context.Background(),
		"gc_worker", -1, 10)
	suite.Error(err)

	// Cannot set non-infinity TTL for gc_worker
	_, err = suite.client.UpdateServiceGCSafePoint(context.Background(),
		"gc_worker", 10000000, 10)
	suite.Error(err)

	// Service safepoint must have a non-empty ID
	_, err = suite.client.UpdateServiceGCSafePoint(context.Background(),
		"", 1000, 15)
	suite.Error(err)

	// Put some other safepoints to test fixing gc_worker's safepoint when there exists other safepoints.
	_, err = suite.client.UpdateServiceGCSafePoint(context.Background(),
		"a", 1000, 11)
	suite.NoError(err)
	_, err = suite.client.UpdateServiceGCSafePoint(context.Background(),
		"b", 1000, 12)
	suite.NoError(err)
	_, err = suite.client.UpdateServiceGCSafePoint(context.Background(),
		"c", 1000, 13)
	suite.NoError(err)

	// Force set invalid ttl to gc_worker
	gcWorkerKey := path.Join("gc", "safe_point", "service", "gc_worker")
	{
		gcWorkerSsp := &endpoint.ServiceSafePoint{
			ServiceID: "gc_worker",
			ExpiredAt: -12345,
			SafePoint: 10,
		}
		value, err := json.Marshal(gcWorkerSsp)
		suite.NoError(err)
		err = suite.srv.GetStorage().Save(gcWorkerKey, string(value))
		suite.NoError(err)
	}

	minSsp, err = suite.srv.GetStorage().LoadMinServiceGCSafePoint(time.Now())
	suite.NoError(err)
	suite.Equal("gc_worker", minSsp.ServiceID)
	suite.Equal(uint64(10), minSsp.SafePoint)
	suite.Equal(int64(math.MaxInt64), minSsp.ExpiredAt)

	// Force delete gc_worker, then the min service safepoint is 11 of "a".
	err = suite.srv.GetStorage().Remove(gcWorkerKey)
	suite.NoError(err)
	minSsp, err = suite.srv.GetStorage().LoadMinServiceGCSafePoint(time.Now())
	suite.NoError(err)
	suite.Equal(uint64(11), minSsp.SafePoint)
	// After calling LoadMinServiceGCS when "gc_worker"'s service safepoint is missing, "gc_worker"'s service safepoint
	// will be newly created.
	// Increase "a" so that "gc_worker" is the only minimum that will be returned by LoadMinServiceGCSafePoint.
	_, err = suite.client.UpdateServiceGCSafePoint(context.Background(),
		"a", 1000, 14)
	suite.NoError(err)

	minSsp, err = suite.srv.GetStorage().LoadMinServiceGCSafePoint(time.Now())
	suite.NoError(err)
	suite.Equal("gc_worker", minSsp.ServiceID)
	suite.Equal(uint64(11), minSsp.SafePoint)
	suite.Equal(int64(math.MaxInt64), minSsp.ExpiredAt)
}

func (suite *clientTestSuite) TestScatterRegion() {
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
	regionsID := []uint64{regionID}
	suite.NoError(err)
	// Test interface `ScatterRegions`.
	re := suite.Require()
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
	testutil.Eventually(re, func() bool {
		err := suite.client.ScatterRegion(context.Background(), regionID)
		if err != nil {
			fmt.Println(err)
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
