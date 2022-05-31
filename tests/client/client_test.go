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
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
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

func Test(t *testing.T) {
	TestingT(t)
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

var _ = Suite(&clientTestSuite{})

type clientTestSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *clientTestSuite) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	server.EnableZap = true
}

func (s *clientTestSuite) TearDownSuite(c *C) {
	s.cancel()
}

type client interface {
	GetLeaderAddr() string
	ScheduleCheckLeader()
	GetURLs() []string
	GetAllocatorLeaderURLs() map[string]string
}

func (s *clientTestSuite) TestClientLeaderChange(c *C) {
	cluster, err := tests.NewTestCluster(s.ctx, 3)
	c.Assert(err, IsNil)
	defer cluster.Destroy()

	endpoints := s.runServer(c, cluster)
	cli := setupCli(c, s.ctx, endpoints)

	var ts1, ts2 uint64
	testutil.WaitUntil(c, func() bool {
		p1, l1, err := cli.GetTS(context.TODO())
		if err == nil {
			ts1 = tsoutil.ComposeTS(p1, l1)
			return true
		}
		c.Log(err)
		return false
	})
	c.Assert(cluster.CheckTSOUnique(ts1), IsTrue)

	leader := cluster.GetLeader()
	waitLeader(c, cli.(client), cluster.GetServer(leader).GetConfig().ClientUrls)

	err = cluster.GetServer(leader).Stop()
	c.Assert(err, IsNil)
	leader = cluster.WaitLeader()
	c.Assert(leader, Not(Equals), "")
	waitLeader(c, cli.(client), cluster.GetServer(leader).GetConfig().ClientUrls)

	// Check TS won't fall back after leader changed.
	testutil.WaitUntil(c, func() bool {
		p2, l2, err := cli.GetTS(context.TODO())
		if err == nil {
			ts2 = tsoutil.ComposeTS(p2, l2)
			return true
		}
		c.Log(err)
		return false
	})
	c.Assert(cluster.CheckTSOUnique(ts2), IsTrue)
	c.Assert(ts1, Less, ts2)

	// Check URL list.
	cli.Close()
	urls := cli.(client).GetURLs()
	sort.Strings(urls)
	sort.Strings(endpoints)
	c.Assert(urls, DeepEquals, endpoints)
}

func (s *clientTestSuite) TestLeaderTransfer(c *C) {
	cluster, err := tests.NewTestCluster(s.ctx, 2)
	c.Assert(err, IsNil)
	defer cluster.Destroy()

	endpoints := s.runServer(c, cluster)
	cli := setupCli(c, s.ctx, endpoints)

	var lastTS uint64
	testutil.WaitUntil(c, func() bool {
		physical, logical, err := cli.GetTS(context.TODO())
		if err == nil {
			lastTS = tsoutil.ComposeTS(physical, logical)
			return true
		}
		c.Log(err)
		return false
	})
	c.Assert(cluster.CheckTSOUnique(lastTS), IsTrue)

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
				c.Assert(cluster.CheckTSOUnique(ts), IsTrue)
				c.Assert(lastTS, Less, ts)
				lastTS = ts
			}
			time.Sleep(time.Millisecond)
		}
	}()

	// Transfer leader.
	for i := 0; i < 5; i++ {
		oldLeaderName := cluster.WaitLeader()
		err := cluster.GetServer(oldLeaderName).ResignLeader()
		c.Assert(err, IsNil)
		newLeaderName := cluster.WaitLeader()
		c.Assert(newLeaderName, Not(Equals), oldLeaderName)
	}
	close(quit)
	wg.Wait()
}

// More details can be found in this issue: https://github.com/tikv/pd/issues/4884
func (s *clientTestSuite) TestUpdateAfterResetTSO(c *C) {
	cluster, err := tests.NewTestCluster(s.ctx, 2)
	c.Assert(err, IsNil)
	defer cluster.Destroy()

	endpoints := s.runServer(c, cluster)
	cli := setupCli(c, s.ctx, endpoints)

	testutil.WaitUntil(c, func() bool {
		_, _, err := cli.GetTS(context.TODO())
		return err == nil
	})
	// Transfer leader to trigger the TSO resetting.
	c.Assert(failpoint.Enable("github.com/tikv/pd/server/updateAfterResetTSO", "return(true)"), IsNil)
	oldLeaderName := cluster.WaitLeader()
	err = cluster.GetServer(oldLeaderName).ResignLeader()
	c.Assert(err, IsNil)
	c.Assert(failpoint.Disable("github.com/tikv/pd/server/updateAfterResetTSO"), IsNil)
	newLeaderName := cluster.WaitLeader()
	c.Assert(newLeaderName, Not(Equals), oldLeaderName)
	// Request a new TSO.
	testutil.WaitUntil(c, func() bool {
		_, _, err := cli.GetTS(context.TODO())
		return err == nil
	})
	// Transfer leader back.
	c.Assert(failpoint.Enable("github.com/tikv/pd/server/tso/delaySyncTimestamp", `return(true)`), IsNil)
	err = cluster.GetServer(newLeaderName).ResignLeader()
	c.Assert(err, IsNil)
	// Should NOT panic here.
	testutil.WaitUntil(c, func() bool {
		_, _, err := cli.GetTS(context.TODO())
		return err == nil
	})
	c.Assert(failpoint.Disable("github.com/tikv/pd/server/tso/delaySyncTimestamp"), IsNil)
}

func (s *clientTestSuite) TestTSOAllocatorLeader(c *C) {
	dcLocationConfig := map[string]string{
		"pd1": "dc-1",
		"pd2": "dc-2",
		"pd3": "dc-3",
	}
	dcLocationNum := len(dcLocationConfig)
	cluster, err := tests.NewTestCluster(s.ctx, dcLocationNum, func(conf *config.Config, serverName string) {
		conf.EnableLocalTSO = true
		conf.Labels[config.ZoneLabel] = dcLocationConfig[serverName]
	})
	c.Assert(err, IsNil)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitAllLeaders(c, dcLocationConfig)

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
		testutil.WaitUntil(c, func() bool {
			pdName = cluster.WaitAllocatorLeader(dcLocation)
			return len(pdName) > 0
		})
		allocatorLeaderMap[dcLocation] = pdName
	}
	cli := setupCli(c, s.ctx, endpoints)

	// Check allocator leaders URL map.
	cli.Close()
	for dcLocation, url := range cli.(client).GetAllocatorLeaderURLs() {
		if dcLocation == tso.GlobalDCLocation {
			urls := cli.(client).GetURLs()
			sort.Strings(urls)
			sort.Strings(endpoints)
			c.Assert(urls, DeepEquals, endpoints)
			continue
		}
		pdName, exist := allocatorLeaderMap[dcLocation]
		c.Assert(exist, IsTrue)
		c.Assert(len(pdName), Greater, 0)
		pdURL, exist := endpointsMap[pdName]
		c.Assert(exist, IsTrue)
		c.Assert(len(pdURL), Greater, 0)
		c.Assert(url, Equals, pdURL)
	}
}

func (s *clientTestSuite) TestTSOFollowerProxy(c *C) {
	cluster, err := tests.NewTestCluster(s.ctx, 3)
	c.Assert(err, IsNil)
	defer cluster.Destroy()

	endpoints := s.runServer(c, cluster)
	cli1 := setupCli(c, s.ctx, endpoints)
	cli2 := setupCli(c, s.ctx, endpoints)
	cli2.UpdateOption(pd.EnableTSOFollowerProxy, true)

	var wg sync.WaitGroup
	wg.Add(tsoRequestConcurrencyNumber)
	for i := 0; i < tsoRequestConcurrencyNumber; i++ {
		go func() {
			defer wg.Done()
			var lastTS uint64
			for i := 0; i < tsoRequestRound; i++ {
				physical, logical, err := cli2.GetTS(context.Background())
				c.Assert(err, IsNil)
				ts := tsoutil.ComposeTS(physical, logical)
				c.Assert(lastTS, Less, ts)
				lastTS = ts
				// After requesting with the follower proxy, request with the leader directly.
				physical, logical, err = cli1.GetTS(context.Background())
				c.Assert(err, IsNil)
				ts = tsoutil.ComposeTS(physical, logical)
				c.Assert(lastTS, Less, ts)
				lastTS = ts
			}
		}()
	}
	wg.Wait()
}

func (s *clientTestSuite) TestGlobalAndLocalTSO(c *C) {
	dcLocationConfig := map[string]string{
		"pd1": "dc-1",
		"pd2": "dc-2",
		"pd3": "dc-3",
	}
	dcLocationNum := len(dcLocationConfig)
	cluster, err := tests.NewTestCluster(s.ctx, dcLocationNum, func(conf *config.Config, serverName string) {
		conf.EnableLocalTSO = true
		conf.Labels[config.ZoneLabel] = dcLocationConfig[serverName]
	})
	c.Assert(err, IsNil)
	defer cluster.Destroy()

	endpoints := s.runServer(c, cluster)
	cli := setupCli(c, s.ctx, endpoints)

	// Wait for all nodes becoming healthy.
	time.Sleep(time.Second * 5)

	// Join a new dc-location
	pd4, err := cluster.Join(s.ctx, func(conf *config.Config, serverName string) {
		conf.EnableLocalTSO = true
		conf.Labels[config.ZoneLabel] = "dc-4"
	})
	c.Assert(err, IsNil)
	err = pd4.Run()
	c.Assert(err, IsNil)
	dcLocationConfig["pd4"] = "dc-4"
	cluster.CheckClusterDCLocation()
	cluster.WaitAllLeaders(c, dcLocationConfig)

	// Test a nonexistent dc-location for Local TSO
	p, l, err := cli.GetLocalTS(context.TODO(), "nonexistent-dc")
	c.Assert(p, Equals, int64(0))
	c.Assert(l, Equals, int64(0))
	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, ".*unknown dc-location.*")

	wg := &sync.WaitGroup{}
	requestGlobalAndLocalTSO(c, wg, dcLocationConfig, cli)

	// assert global tso after resign leader
	c.Assert(failpoint.Enable("github.com/tikv/pd/client/skipUpdateMember", `return(true)`), IsNil)
	err = cluster.ResignLeader()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	_, _, err = cli.GetTS(s.ctx)
	c.Assert(err, NotNil)
	c.Assert(pd.IsLeaderChange(err), IsTrue)
	_, _, err = cli.GetTS(s.ctx)
	c.Assert(err, IsNil)
	c.Assert(failpoint.Disable("github.com/tikv/pd/client/skipUpdateMember"), IsNil)

	// Test the TSO follower proxy while enabling the Local TSO.
	cli.UpdateOption(pd.EnableTSOFollowerProxy, true)
	// Sleep a while here to prevent from canceling the ongoing TSO request.
	time.Sleep(time.Millisecond * 50)
	requestGlobalAndLocalTSO(c, wg, dcLocationConfig, cli)
	cli.UpdateOption(pd.EnableTSOFollowerProxy, false)
	time.Sleep(time.Millisecond * 50)
	requestGlobalAndLocalTSO(c, wg, dcLocationConfig, cli)
}

func requestGlobalAndLocalTSO(c *C, wg *sync.WaitGroup, dcLocationConfig map[string]string, cli pd.Client) {
	for _, dcLocation := range dcLocationConfig {
		wg.Add(tsoRequestConcurrencyNumber)
		for i := 0; i < tsoRequestConcurrencyNumber; i++ {
			go func(dc string) {
				defer wg.Done()
				var lastTS uint64
				for i := 0; i < tsoRequestRound; i++ {
					globalPhysical1, globalLogical1, err := cli.GetTS(context.TODO())
					c.Assert(err, IsNil)
					globalTS1 := tsoutil.ComposeTS(globalPhysical1, globalLogical1)
					localPhysical, localLogical, err := cli.GetLocalTS(context.TODO(), dc)
					c.Assert(err, IsNil)
					localTS := tsoutil.ComposeTS(localPhysical, localLogical)
					globalPhysical2, globalLogical2, err := cli.GetTS(context.TODO())
					c.Assert(err, IsNil)
					globalTS2 := tsoutil.ComposeTS(globalPhysical2, globalLogical2)
					c.Assert(lastTS, Less, globalTS1)
					c.Assert(globalTS1, Less, localTS)
					c.Assert(localTS, Less, globalTS2)
					lastTS = globalTS2
				}
				c.Assert(lastTS, Greater, uint64(0))
			}(dcLocation)
		}
	}
	wg.Wait()
}

func (s *clientTestSuite) TestCustomTimeout(c *C) {
	cluster, err := tests.NewTestCluster(s.ctx, 1)
	c.Assert(err, IsNil)
	defer cluster.Destroy()

	endpoints := s.runServer(c, cluster)
	cli := setupCli(c, s.ctx, endpoints, pd.WithCustomTimeoutOption(1*time.Second))

	start := time.Now()
	c.Assert(failpoint.Enable("github.com/tikv/pd/server/customTimeout", "return(true)"), IsNil)
	_, err = cli.GetAllStores(context.TODO())
	c.Assert(failpoint.Disable("github.com/tikv/pd/server/customTimeout"), IsNil)
	c.Assert(err, NotNil)
	c.Assert(time.Since(start), GreaterEqual, 1*time.Second)
	c.Assert(time.Since(start), Less, 2*time.Second)
}

func (s *clientTestSuite) TestGetRegionFromFollowerClient(c *C) {
	pd.LeaderHealthCheckInterval = 100 * time.Millisecond
	cluster, err := tests.NewTestCluster(s.ctx, 3)
	c.Assert(err, IsNil)
	defer cluster.Destroy()

	endpoints := s.runServer(c, cluster)
	cli := setupCli(c, s.ctx, endpoints, pd.WithForwardingOption(true))

	c.Assert(failpoint.Enable("github.com/tikv/pd/client/unreachableNetwork1", "return(true)"), IsNil)
	time.Sleep(200 * time.Millisecond)
	r, err := cli.GetRegion(context.Background(), []byte("a"))
	c.Assert(err, IsNil)
	c.Assert(r, NotNil)

	c.Assert(failpoint.Disable("github.com/tikv/pd/client/unreachableNetwork1"), IsNil)
	time.Sleep(200 * time.Millisecond)
	r, err = cli.GetRegion(context.Background(), []byte("a"))
	c.Assert(err, IsNil)
	c.Assert(r, NotNil)
}

// case 1: unreachable -> normal
func (s *clientTestSuite) TestGetTsoFromFollowerClient1(c *C) {
	pd.LeaderHealthCheckInterval = 100 * time.Millisecond
	cluster, err := tests.NewTestCluster(s.ctx, 3)
	c.Assert(err, IsNil)
	defer cluster.Destroy()

	endpoints := s.runServer(c, cluster)
	cli := setupCli(c, s.ctx, endpoints, pd.WithForwardingOption(true))

	c.Assert(failpoint.Enable("github.com/tikv/pd/client/unreachableNetwork", "return(true)"), IsNil)
	var lastTS uint64
	testutil.WaitUntil(c, func() bool {
		physical, logical, err := cli.GetTS(context.TODO())
		if err == nil {
			lastTS = tsoutil.ComposeTS(physical, logical)
			return true
		}
		c.Log(err)
		return false
	})

	lastTS = checkTS(c, cli, lastTS)
	c.Assert(failpoint.Disable("github.com/tikv/pd/client/unreachableNetwork"), IsNil)
	time.Sleep(2 * time.Second)
	checkTS(c, cli, lastTS)
}

// case 2: unreachable -> leader transfer -> normal
func (s *clientTestSuite) TestGetTsoFromFollowerClient2(c *C) {
	pd.LeaderHealthCheckInterval = 100 * time.Millisecond
	cluster, err := tests.NewTestCluster(s.ctx, 3)
	c.Assert(err, IsNil)
	defer cluster.Destroy()

	endpoints := s.runServer(c, cluster)
	cli := setupCli(c, s.ctx, endpoints, pd.WithForwardingOption(true))

	c.Assert(failpoint.Enable("github.com/tikv/pd/client/unreachableNetwork", "return(true)"), IsNil)
	var lastTS uint64
	testutil.WaitUntil(c, func() bool {
		physical, logical, err := cli.GetTS(context.TODO())
		if err == nil {
			lastTS = tsoutil.ComposeTS(physical, logical)
			return true
		}
		c.Log(err)
		return false
	})

	lastTS = checkTS(c, cli, lastTS)
	c.Assert(cluster.GetServer(cluster.GetLeader()).ResignLeader(), IsNil)
	cluster.WaitLeader()
	lastTS = checkTS(c, cli, lastTS)

	c.Assert(failpoint.Disable("github.com/tikv/pd/client/unreachableNetwork"), IsNil)
	time.Sleep(5 * time.Second)
	checkTS(c, cli, lastTS)
}

func checkTS(c *C, cli pd.Client, lastTS uint64) uint64 {
	for i := 0; i < tsoRequestRound; i++ {
		physical, logical, err := cli.GetTS(context.TODO())
		if err == nil {
			ts := tsoutil.ComposeTS(physical, logical)
			c.Assert(lastTS, Less, ts)
			lastTS = ts
		}
		time.Sleep(time.Millisecond)
	}
	return lastTS
}

func (s *clientTestSuite) runServer(c *C, cluster *tests.TestCluster) []string {
	err := cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)

	testServers := cluster.GetServers()
	endpoints := make([]string, 0, len(testServers))
	for _, s := range testServers {
		endpoints = append(endpoints, s.GetConfig().AdvertiseClientUrls)
	}
	return endpoints
}

func setupCli(c *C, ctx context.Context, endpoints []string, opts ...pd.ClientOption) pd.Client {
	cli, err := pd.NewClientWithContext(ctx, endpoints, pd.SecurityOption{}, opts...)
	c.Assert(err, IsNil)
	return cli
}

func waitLeader(c *C, cli client, leader string) {
	testutil.WaitUntil(c, func() bool {
		cli.ScheduleCheckLeader()
		return cli.GetLeaderAddr() == leader
	})
}

func (s *clientTestSuite) TestCloseClient(c *C) {
	cluster, err := tests.NewTestCluster(s.ctx, 1)
	c.Assert(err, IsNil)
	defer cluster.Destroy()
	endpoints := s.runServer(c, cluster)
	cli := setupCli(c, s.ctx, endpoints)
	cli.GetTSAsync(context.TODO())
	time.Sleep(time.Second)
	cli.Close()
}

var _ = Suite(&testClientSuite{})

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

type testClientSuite struct {
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

func checkerWithNilAssert(c *C) *assertutil.Checker {
	checker := assertutil.NewChecker(c.FailNow)
	checker.IsNil = func(obtained interface{}) {
		c.Assert(obtained, IsNil)
	}
	return checker
}

func (s *testClientSuite) SetUpSuite(c *C) {
	var err error
	s.srv, s.cleanup, err = server.NewTestServer(checkerWithNilAssert(c))
	c.Assert(err, IsNil)
	s.grpcPDClient = testutil.MustNewGrpcClient(c, s.srv.GetAddr())
	s.grpcSvr = &server.GrpcServer{Server: s.srv}

	mustWaitLeader(c, map[string]*server.Server{s.srv.GetAddr(): s.srv})
	bootstrapServer(c, newHeader(s.srv), s.grpcPDClient)

	s.ctx, s.clean = context.WithCancel(context.Background())
	s.client = setupCli(c, s.ctx, s.srv.GetEndpoints())

	c.Assert(err, IsNil)
	s.regionHeartbeat, err = s.grpcPDClient.RegionHeartbeat(s.ctx)
	c.Assert(err, IsNil)
	s.reportBucket, err = s.grpcPDClient.ReportBuckets(s.ctx)
	c.Assert(err, IsNil)
	cluster := s.srv.GetRaftCluster()
	c.Assert(cluster, NotNil)
	now := time.Now().UnixNano()
	for _, store := range stores {
		s.grpcSvr.PutStore(context.Background(), &pdpb.PutStoreRequest{
			Header: newHeader(s.srv),
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

func (s *testClientSuite) TearDownSuite(c *C) {
	s.client.Close()
	s.clean()
	s.cleanup()
}

func mustWaitLeader(c *C, svrs map[string]*server.Server) *server.Server {
	for i := 0; i < 500; i++ {
		for _, s := range svrs {
			if !s.IsClosed() && s.GetMember().IsLeader() {
				return s
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.Fatal("no leader")
	return nil
}

func newHeader(srv *server.Server) *pdpb.RequestHeader {
	return &pdpb.RequestHeader{
		ClusterId: srv.ClusterID(),
	}
}

func bootstrapServer(c *C, header *pdpb.RequestHeader, client pdpb.PDClient) {
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
	c.Assert(err, IsNil)
}

func (s *testClientSuite) TestNormalTSO(c *C) {
	var wg sync.WaitGroup
	wg.Add(tsoRequestConcurrencyNumber)
	for i := 0; i < tsoRequestConcurrencyNumber; i++ {
		go func() {
			defer wg.Done()
			var lastTS uint64
			for i := 0; i < tsoRequestRound; i++ {
				physical, logical, err := s.client.GetTS(context.Background())
				c.Assert(err, IsNil)
				ts := tsoutil.ComposeTS(physical, logical)
				c.Assert(lastTS, Less, ts)
				lastTS = ts
			}
		}()
	}
	wg.Wait()
}

func (s *testClientSuite) TestGetTSAsync(c *C) {
	var wg sync.WaitGroup
	wg.Add(tsoRequestConcurrencyNumber)
	for i := 0; i < tsoRequestConcurrencyNumber; i++ {
		go func() {
			defer wg.Done()
			tsFutures := make([]pd.TSFuture, tsoRequestRound)
			for i := range tsFutures {
				tsFutures[i] = s.client.GetTSAsync(context.Background())
			}
			var lastTS uint64 = math.MaxUint64
			for i := len(tsFutures) - 1; i >= 0; i-- {
				physical, logical, err := tsFutures[i].Wait()
				c.Assert(err, IsNil)
				ts := tsoutil.ComposeTS(physical, logical)
				c.Assert(lastTS, Greater, ts)
				lastTS = ts
			}
		}()
	}
	wg.Wait()
}

func (s *testClientSuite) TestGetRegion(c *C) {
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
		Header: newHeader(s.srv),
		Region: region,
		Leader: peers[0],
	}
	err := s.regionHeartbeat.Send(req)
	c.Assert(err, IsNil)
	testutil.WaitUntil(c, func() bool {
		r, err := s.client.GetRegion(context.Background(), []byte("a"))
		c.Assert(err, IsNil)
		if r == nil {
			return false
		}
		return c.Check(r.Meta, DeepEquals, region) &&
			c.Check(r.Leader, DeepEquals, peers[0]) &&
			c.Check(r.Buckets, IsNil)
	})
	breq := &pdpb.ReportBucketsRequest{
		Header: newHeader(s.srv),
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
	c.Assert(s.reportBucket.Send(breq), IsNil)
	testutil.WaitUntil(c, func() bool {
		r, err := s.client.GetRegion(context.Background(), []byte("a"), pd.WithBuckets())
		c.Assert(err, IsNil)
		if r == nil {
			return false
		}
		return c.Check(r.Buckets, NotNil)
	})
	config := s.srv.GetRaftCluster().GetStoreConfig()
	config.EnableRegionBucket = false
	testutil.WaitUntil(c, func() bool {
		r, err := s.client.GetRegion(context.Background(), []byte("a"), pd.WithBuckets())
		c.Assert(err, IsNil)
		if r == nil {
			return false
		}
		return c.Check(r.Buckets, IsNil)
	})
	config.EnableRegionBucket = true
	c.Succeed()
}

func (s *testClientSuite) TestGetPrevRegion(c *C) {
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
			Header: newHeader(s.srv),
			Region: r,
			Leader: peers[0],
		}
		err := s.regionHeartbeat.Send(req)
		c.Assert(err, IsNil)
	}
	time.Sleep(500 * time.Millisecond)
	for i := 0; i < 20; i++ {
		testutil.WaitUntil(c, func() bool {
			r, err := s.client.GetPrevRegion(context.Background(), []byte{byte(i)})
			c.Assert(err, IsNil)
			if i > 0 && i < regionLen {
				return c.Check(r.Leader, DeepEquals, peers[0]) &&
					c.Check(r.Meta, DeepEquals, regions[i-1])
			}
			return c.Check(r, IsNil)
		})
	}
	c.Succeed()
}

func (s *testClientSuite) TestScanRegions(c *C) {
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
			Header: newHeader(s.srv),
			Region: r,
			Leader: peers[0],
		}
		err := s.regionHeartbeat.Send(req)
		c.Assert(err, IsNil)
	}

	// Wait for region heartbeats.
	testutil.WaitUntil(c, func() bool {
		scanRegions, err := s.client.ScanRegions(context.Background(), []byte{0}, nil, 10)
		return err == nil && len(scanRegions) == 10
	})

	// Set leader of region3 to nil.
	region3 := core.NewRegionInfo(regions[3], nil)
	s.srv.GetRaftCluster().HandleRegionHeartbeat(region3)

	// Add down peer for region4.
	region4 := core.NewRegionInfo(regions[4], regions[4].Peers[0], core.WithDownPeers([]*pdpb.PeerStats{{Peer: regions[4].Peers[1]}}))
	s.srv.GetRaftCluster().HandleRegionHeartbeat(region4)

	// Add pending peers for region5.
	region5 := core.NewRegionInfo(regions[5], regions[5].Peers[0], core.WithPendingPeers([]*metapb.Peer{regions[5].Peers[1], regions[5].Peers[2]}))
	s.srv.GetRaftCluster().HandleRegionHeartbeat(region5)

	check := func(start, end []byte, limit int, expect []*metapb.Region) {
		scanRegions, err := s.client.ScanRegions(context.Background(), start, end, limit)
		c.Assert(err, IsNil)
		c.Assert(scanRegions, HasLen, len(expect))
		c.Log("scanRegions", scanRegions)
		c.Log("expect", expect)
		for i := range expect {
			c.Assert(scanRegions[i].Meta, DeepEquals, expect[i])

			if scanRegions[i].Meta.GetId() == region3.GetID() {
				c.Assert(scanRegions[i].Leader, DeepEquals, &metapb.Peer{})
			} else {
				c.Assert(scanRegions[i].Leader, DeepEquals, expect[i].Peers[0])
			}

			if scanRegions[i].Meta.GetId() == region4.GetID() {
				c.Assert(scanRegions[i].DownPeers, DeepEquals, []*metapb.Peer{expect[i].Peers[1]})
			}

			if scanRegions[i].Meta.GetId() == region5.GetID() {
				c.Assert(scanRegions[i].PendingPeers, DeepEquals, []*metapb.Peer{expect[i].Peers[1], expect[i].Peers[2]})
			}
		}
	}

	check([]byte{0}, nil, 10, regions)
	check([]byte{1}, nil, 5, regions[1:6])
	check([]byte{100}, nil, 1, nil)
	check([]byte{1}, []byte{6}, 0, regions[1:6])
	check([]byte{1}, []byte{6}, 2, regions[1:3])
}

func (s *testClientSuite) TestGetRegionByID(c *C) {
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
		Header: newHeader(s.srv),
		Region: region,
		Leader: peers[0],
	}
	err := s.regionHeartbeat.Send(req)
	c.Assert(err, IsNil)

	testutil.WaitUntil(c, func() bool {
		r, err := s.client.GetRegionByID(context.Background(), regionID)
		c.Assert(err, IsNil)
		if r == nil {
			return false
		}
		return c.Check(r.Meta, DeepEquals, region) &&
			c.Check(r.Leader, DeepEquals, peers[0])
	})
	c.Succeed()
}

func (s *testClientSuite) TestGetStore(c *C) {
	cluster := s.srv.GetRaftCluster()
	c.Assert(cluster, NotNil)
	store := stores[0]

	// Get an up store should be OK.
	n, err := s.client.GetStore(context.Background(), store.GetId())
	c.Assert(err, IsNil)
	c.Assert(n, DeepEquals, store)

	stores, err := s.client.GetAllStores(context.Background())
	c.Assert(err, IsNil)
	c.Assert(stores, DeepEquals, stores)

	// Mark the store as offline.
	err = cluster.RemoveStore(store.GetId(), false)
	c.Assert(err, IsNil)
	offlineStore := proto.Clone(store).(*metapb.Store)
	offlineStore.State = metapb.StoreState_Offline
	offlineStore.NodeState = metapb.NodeState_Removing

	// Get an offline store should be OK.
	n, err = s.client.GetStore(context.Background(), store.GetId())
	c.Assert(err, IsNil)
	c.Assert(n, DeepEquals, offlineStore)

	// Should return offline stores.
	contains := false
	stores, err = s.client.GetAllStores(context.Background())
	c.Assert(err, IsNil)
	for _, store := range stores {
		if store.GetId() == offlineStore.GetId() {
			contains = true
			c.Assert(store, DeepEquals, offlineStore)
		}
	}
	c.Assert(contains, IsTrue)

	// Mark the store as physically destroyed and offline.
	err = cluster.RemoveStore(store.GetId(), true)
	c.Assert(err, IsNil)
	physicallyDestroyedStoreID := store.GetId()

	// Get a physically destroyed and offline store
	// It should be Tombstone(become Tombstone automically) or Offline
	n, err = s.client.GetStore(context.Background(), physicallyDestroyedStoreID)
	c.Assert(err, IsNil)
	if n != nil { // store is still offline and physically destroyed
		c.Assert(n.GetNodeState(), Equals, metapb.NodeState_Removing)
		c.Assert(n.PhysicallyDestroyed, IsTrue)
	}
	// Should return tombstone stores.
	contains = false
	stores, err = s.client.GetAllStores(context.Background())
	c.Assert(err, IsNil)
	for _, store := range stores {
		if store.GetId() == physicallyDestroyedStoreID {
			contains = true
			c.Assert(store.GetState(), Not(Equals), metapb.StoreState_Up)
			c.Assert(store.PhysicallyDestroyed, IsTrue)
		}
	}
	c.Assert(contains, IsTrue)

	// Should not return tombstone stores.
	stores, err = s.client.GetAllStores(context.Background(), pd.WithExcludeTombstone())
	c.Assert(err, IsNil)
	for _, store := range stores {
		if store.GetId() == physicallyDestroyedStoreID {
			c.Assert(store.GetState(), Equals, metapb.StoreState_Offline)
			c.Assert(store.PhysicallyDestroyed, IsTrue)
		}
	}
}

func (s *testClientSuite) checkGCSafePoint(c *C, expectedSafePoint uint64) {
	req := &pdpb.GetGCSafePointRequest{
		Header: newHeader(s.srv),
	}
	resp, err := s.grpcSvr.GetGCSafePoint(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(resp.SafePoint, Equals, expectedSafePoint)
}

func (s *testClientSuite) TestUpdateGCSafePoint(c *C) {
	s.checkGCSafePoint(c, 0)
	for _, safePoint := range []uint64{0, 1, 2, 3, 233, 23333, 233333333333, math.MaxUint64} {
		newSafePoint, err := s.client.UpdateGCSafePoint(context.Background(), safePoint)
		c.Assert(err, IsNil)
		c.Assert(newSafePoint, Equals, safePoint)
		s.checkGCSafePoint(c, safePoint)
	}
	// If the new safe point is less than the old one, it should not be updated.
	newSafePoint, err := s.client.UpdateGCSafePoint(context.Background(), 1)
	c.Assert(newSafePoint, Equals, uint64(math.MaxUint64))
	c.Assert(err, IsNil)
	s.checkGCSafePoint(c, math.MaxUint64)
}

func (s *testClientSuite) TestUpdateServiceGCSafePoint(c *C) {
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
		min, err := s.client.UpdateServiceGCSafePoint(context.Background(),
			ssp.ServiceID, 1000, ssp.SafePoint)
		c.Assert(err, IsNil)
		// An service safepoint of ID "gc_worker" is automatically initialized as 0
		c.Assert(min, Equals, uint64(0))
	}

	min, err := s.client.UpdateServiceGCSafePoint(context.Background(),
		"gc_worker", math.MaxInt64, 10)
	c.Assert(err, IsNil)
	c.Assert(min, Equals, uint64(1))

	min, err = s.client.UpdateServiceGCSafePoint(context.Background(),
		"a", 1000, 4)
	c.Assert(err, IsNil)
	c.Assert(min, Equals, uint64(2))

	min, err = s.client.UpdateServiceGCSafePoint(context.Background(),
		"b", -100, 2)
	c.Assert(err, IsNil)
	c.Assert(min, Equals, uint64(3))

	// Minimum safepoint does not regress
	min, err = s.client.UpdateServiceGCSafePoint(context.Background(),
		"b", 1000, 2)
	c.Assert(err, IsNil)
	c.Assert(min, Equals, uint64(3))

	// Update only the TTL of the minimum safepoint
	oldMinSsp, err := s.srv.GetStorage().LoadMinServiceGCSafePoint(time.Now())
	c.Assert(err, IsNil)
	c.Assert(oldMinSsp.ServiceID, Equals, "c")
	c.Assert(oldMinSsp.SafePoint, Equals, uint64(3))
	min, err = s.client.UpdateServiceGCSafePoint(context.Background(),
		"c", 2000, 3)
	c.Assert(err, IsNil)
	c.Assert(min, Equals, uint64(3))
	minSsp, err := s.srv.GetStorage().LoadMinServiceGCSafePoint(time.Now())
	c.Assert(err, IsNil)
	c.Assert(minSsp.ServiceID, Equals, "c")
	c.Assert(oldMinSsp.SafePoint, Equals, uint64(3))
	c.Assert(minSsp.ExpiredAt-oldMinSsp.ExpiredAt, GreaterEqual, int64(1000))

	// Shrinking TTL is also allowed
	min, err = s.client.UpdateServiceGCSafePoint(context.Background(),
		"c", 1, 3)
	c.Assert(err, IsNil)
	c.Assert(min, Equals, uint64(3))
	minSsp, err = s.srv.GetStorage().LoadMinServiceGCSafePoint(time.Now())
	c.Assert(err, IsNil)
	c.Assert(minSsp.ServiceID, Equals, "c")
	c.Assert(minSsp.ExpiredAt, Less, oldMinSsp.ExpiredAt)

	// TTL can be infinite (represented by math.MaxInt64)
	min, err = s.client.UpdateServiceGCSafePoint(context.Background(),
		"c", math.MaxInt64, 3)
	c.Assert(err, IsNil)
	c.Assert(min, Equals, uint64(3))
	minSsp, err = s.srv.GetStorage().LoadMinServiceGCSafePoint(time.Now())
	c.Assert(err, IsNil)
	c.Assert(minSsp.ServiceID, Equals, "c")
	c.Assert(minSsp.ExpiredAt, Equals, int64(math.MaxInt64))

	// Delete "a" and "c"
	min, err = s.client.UpdateServiceGCSafePoint(context.Background(),
		"c", -1, 3)
	c.Assert(err, IsNil)
	c.Assert(min, Equals, uint64(4))
	min, err = s.client.UpdateServiceGCSafePoint(context.Background(),
		"a", -1, 4)
	c.Assert(err, IsNil)
	// Now gc_worker is the only remaining service safe point.
	c.Assert(min, Equals, uint64(10))

	// gc_worker cannot be deleted.
	_, err = s.client.UpdateServiceGCSafePoint(context.Background(),
		"gc_worker", -1, 10)
	c.Assert(err, NotNil)

	// Cannot set non-infinity TTL for gc_worker
	_, err = s.client.UpdateServiceGCSafePoint(context.Background(),
		"gc_worker", 10000000, 10)
	c.Assert(err, NotNil)

	// Service safepoint must have a non-empty ID
	_, err = s.client.UpdateServiceGCSafePoint(context.Background(),
		"", 1000, 15)
	c.Assert(err, NotNil)

	// Put some other safepoints to test fixing gc_worker's safepoint when there exists other safepoints.
	_, err = s.client.UpdateServiceGCSafePoint(context.Background(),
		"a", 1000, 11)
	c.Assert(err, IsNil)
	_, err = s.client.UpdateServiceGCSafePoint(context.Background(),
		"b", 1000, 12)
	c.Assert(err, IsNil)
	_, err = s.client.UpdateServiceGCSafePoint(context.Background(),
		"c", 1000, 13)
	c.Assert(err, IsNil)

	// Force set invalid ttl to gc_worker
	gcWorkerKey := path.Join("gc", "safe_point", "service", "gc_worker")
	{
		gcWorkerSsp := &endpoint.ServiceSafePoint{
			ServiceID: "gc_worker",
			ExpiredAt: -12345,
			SafePoint: 10,
		}
		value, err := json.Marshal(gcWorkerSsp)
		c.Assert(err, IsNil)
		err = s.srv.GetStorage().Save(gcWorkerKey, string(value))
		c.Assert(err, IsNil)
	}

	minSsp, err = s.srv.GetStorage().LoadMinServiceGCSafePoint(time.Now())
	c.Assert(err, IsNil)
	c.Assert(minSsp.ServiceID, Equals, "gc_worker")
	c.Assert(minSsp.SafePoint, Equals, uint64(10))
	c.Assert(minSsp.ExpiredAt, Equals, int64(math.MaxInt64))

	// Force delete gc_worker, then the min service safepoint is 11 of "a".
	err = s.srv.GetStorage().Remove(gcWorkerKey)
	c.Assert(err, IsNil)
	minSsp, err = s.srv.GetStorage().LoadMinServiceGCSafePoint(time.Now())
	c.Assert(err, IsNil)
	c.Assert(minSsp.SafePoint, Equals, uint64(11))
	// After calling LoadMinServiceGCS when "gc_worker"'s service safepoint is missing, "gc_worker"'s service safepoint
	// will be newly created.
	// Increase "a" so that "gc_worker" is the only minimum that will be returned by LoadMinServiceGCSafePoint.
	_, err = s.client.UpdateServiceGCSafePoint(context.Background(),
		"a", 1000, 14)
	c.Assert(err, IsNil)

	minSsp, err = s.srv.GetStorage().LoadMinServiceGCSafePoint(time.Now())
	c.Assert(err, IsNil)
	c.Assert(minSsp.ServiceID, Equals, "gc_worker")
	c.Assert(minSsp.SafePoint, Equals, uint64(11))
	c.Assert(minSsp.ExpiredAt, Equals, int64(math.MaxInt64))
}

func (s *testClientSuite) TestScatterRegion(c *C) {
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
		Header: newHeader(s.srv),
		Region: region,
		Leader: peers[0],
	}
	err := s.regionHeartbeat.Send(req)
	regionsID := []uint64{regionID}
	c.Assert(err, IsNil)
	// Test interface `ScatterRegions`.
	testutil.WaitUntil(c, func() bool {
		scatterResp, err := s.client.ScatterRegions(context.Background(), regionsID, pd.WithGroup("test"), pd.WithRetry(1))
		if c.Check(err, NotNil) {
			return false
		}
		if c.Check(scatterResp.FinishedPercentage, Not(Equals), uint64(100)) {
			return false
		}
		resp, err := s.client.GetOperator(context.Background(), regionID)
		if c.Check(err, NotNil) {
			return false
		}
		return c.Check(resp.GetRegionId(), Equals, regionID) && c.Check(string(resp.GetDesc()), Equals, "scatter-region") && c.Check(resp.GetStatus(), Equals, pdpb.OperatorStatus_RUNNING)
	}, testutil.WithSleepInterval(1*time.Second))

	// Test interface `ScatterRegion`.
	// TODO: Deprecate interface `ScatterRegion`.
	testutil.WaitUntil(c, func() bool {
		err := s.client.ScatterRegion(context.Background(), regionID)
		if c.Check(err, NotNil) {
			fmt.Println(err)
			return false
		}
		resp, err := s.client.GetOperator(context.Background(), regionID)
		if c.Check(err, NotNil) {
			return false
		}
		return c.Check(resp.GetRegionId(), Equals, regionID) && c.Check(string(resp.GetDesc()), Equals, "scatter-region") && c.Check(resp.GetStatus(), Equals, pdpb.OperatorStatus_RUNNING)
	}, testutil.WithSleepInterval(1*time.Second))

	c.Succeed()
}

type testConfigTTLSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *testConfigTTLSuite) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	server.EnableZap = true
}

func (s *testConfigTTLSuite) TearDownSuite(c *C) {
	s.cancel()
}

var _ = SerialSuites(&testConfigTTLSuite{})

var ttlConfig = map[string]interface{}{
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
}

func assertTTLConfig(c *C, options *config.PersistOptions, checker Checker) {
	c.Assert(options.GetMaxSnapshotCount(), checker, uint64(999))
	c.Assert(options.IsLocationReplacementEnabled(), checker, false)
	c.Assert(options.GetMaxMergeRegionSize(), checker, uint64(999))
	c.Assert(options.GetMaxMergeRegionKeys(), checker, uint64(999))
	c.Assert(options.GetSchedulerMaxWaitingOperator(), checker, uint64(999))
	c.Assert(options.GetLeaderScheduleLimit(), checker, uint64(999))
	c.Assert(options.GetRegionScheduleLimit(), checker, uint64(999))
	c.Assert(options.GetHotRegionScheduleLimit(), checker, uint64(999))
	c.Assert(options.GetReplicaScheduleLimit(), checker, uint64(999))
	c.Assert(options.GetMergeScheduleLimit(), checker, uint64(999))
}

func (s *testConfigTTLSuite) TestConfigTTLAfterTransferLeader(c *C) {
	cluster, err := tests.NewTestCluster(s.ctx, 3)
	c.Assert(err, IsNil)
	defer cluster.Destroy()
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	leader := cluster.GetServer(cluster.WaitLeader())
	c.Assert(leader.BootstrapCluster(), IsNil)
	addr := fmt.Sprintf("%s/pd/api/v1/config?ttlSecond=5", leader.GetAddr())
	postData, err := json.Marshal(ttlConfig)
	c.Assert(err, IsNil)
	resp, err := leader.GetHTTPClient().Post(addr, "application/json", bytes.NewBuffer(postData))
	resp.Body.Close()
	c.Assert(err, IsNil)
	time.Sleep(2 * time.Second)
	_ = leader.Destroy()
	time.Sleep(2 * time.Second)
	leader = cluster.GetServer(cluster.WaitLeader())
	assertTTLConfig(c, leader.GetPersistOptions(), Equals)
}
