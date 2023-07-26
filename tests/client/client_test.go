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
// See the License for the specific language governing permissions and
// limitations under the License.

package client_test

import (
	"context"
	"math"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/tests"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/goleak"
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
}

func (s *clientTestSuite) TestClientLeaderChange(c *C) {
	cluster, err := tests.NewTestCluster(s.ctx, 3)
	c.Assert(err, IsNil)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()

	var endpoints []string
	for _, s := range cluster.GetServers() {
		endpoints = append(endpoints, s.GetConfig().AdvertiseClientUrls)
	}
	cli, err := pd.NewClientWithContext(s.ctx, endpoints, pd.SecurityOption{})
	c.Assert(err, IsNil)

	var p1, l1 int64
	testutil.WaitUntil(c, func(c *C) bool {
		p1, l1, err = cli.GetTS(context.TODO())
		if err == nil {
			return true
		}
		c.Log(err)
		return false
	})

	leader := cluster.GetLeader()
	s.waitLeader(c, cli.(client), cluster.GetServer(leader).GetConfig().ClientUrls)

	err = cluster.GetServer(leader).Stop()
	c.Assert(err, IsNil)
	leader = cluster.WaitLeader()
	c.Assert(leader, Not(Equals), "")
	s.waitLeader(c, cli.(client), cluster.GetServer(leader).GetConfig().ClientUrls)

	// Check TS won't fall back after leader changed.
	testutil.WaitUntil(c, func(c *C) bool {
		p2, l2, err := cli.GetTS(context.TODO())
		if err != nil {
			c.Log(err)
			return false
		}
		c.Assert(p1<<18+l1, Less, p2<<18+l2)
		return true
	})

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

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()

	var endpoints []string
	for _, s := range cluster.GetServers() {
		endpoints = append(endpoints, s.GetConfig().AdvertiseClientUrls)
	}
	cli, err := pd.NewClientWithContext(s.ctx, endpoints, pd.SecurityOption{})
	c.Assert(err, IsNil)

	var physical, logical int64
	testutil.WaitUntil(c, func(c *C) bool {
		physical, logical, err = cli.GetTS(context.TODO())
		if err == nil {
			return true
		}
		c.Log(err)
		return false
	})
	lastTS := s.makeTS(physical, logical)
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

			physical, logical, err1 := cli.GetTS(context.TODO())
			if err1 == nil {
				ts := s.makeTS(physical, logical)
				c.Assert(lastTS, Less, ts)
				lastTS = ts
			}
			time.Sleep(time.Millisecond)
		}
	}()
	// Transfer leader.
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: time.Second,
	})
	c.Assert(err, IsNil)
	leaderPath := filepath.Join("/pd", strconv.FormatUint(cli.GetClusterID(context.Background()), 10), "leader")
	for i := 0; i < 10; i++ {
		cluster.WaitLeader()
		_, err = etcdCli.Delete(context.TODO(), leaderPath)
		c.Assert(err, IsNil)
		// Sleep to make sure all servers are notified and starts campaign.
		time.Sleep(time.Second)
	}
	close(quit)
	wg.Wait()
}

func (s *clientTestSuite) TestCustomTimeout(c *C) {
	cluster, err := tests.NewTestCluster(s.ctx, 1)
	c.Assert(err, IsNil)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()

	var endpoints []string
	for _, s := range cluster.GetServers() {
		endpoints = append(endpoints, s.GetConfig().AdvertiseClientUrls)
	}
	cli, err := pd.NewClientWithContext(s.ctx, endpoints, pd.SecurityOption{}, pd.WithCustomTimeoutOption(1*time.Second))
	c.Assert(err, IsNil)

	start := time.Now()
	c.Assert(failpoint.Enable("github.com/tikv/pd/server/customTimeout", "return(true)"), IsNil)
	_, err = cli.GetAllStores(context.TODO())
	c.Assert(failpoint.Disable("github.com/tikv/pd/server/customTimeout"), IsNil)
	c.Assert(err, NotNil)
	c.Assert(time.Since(start), GreaterEqual, 1*time.Second)
	c.Assert(time.Since(start), Less, 2*time.Second)
}

func (s *clientTestSuite) waitLeader(c *C, cli client, leader string) {
	testutil.WaitUntil(c, func(c *C) bool {
		cli.ScheduleCheckLeader()
		return cli.GetLeaderAddr() == leader
	})
}

func (s *clientTestSuite) makeTS(physical, logical int64) uint64 {
	return uint64(physical<<18 + logical)
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
	client          pd.Client
	grpcPDClient    pdpb.PDClient
	regionHeartbeat pdpb.PD_RegionHeartbeatClient
}

func (s *testClientSuite) SetUpSuite(c *C) {
	var err error
	s.srv, s.cleanup, err = server.NewTestServer(c)
	c.Assert(err, IsNil)
	s.grpcPDClient = testutil.MustNewGrpcClient(c, s.srv.GetAddr())

	mustWaitLeader(c, map[string]*server.Server{s.srv.GetAddr(): s.srv})
	bootstrapServer(c, newHeader(s.srv), s.grpcPDClient)

	s.ctx, s.clean = context.WithCancel(context.Background())
	s.client, err = pd.NewClientWithContext(s.ctx, s.srv.GetEndpoints(), pd.SecurityOption{})
	c.Assert(err, IsNil)
	s.regionHeartbeat, err = s.grpcPDClient.RegionHeartbeat(s.ctx)
	c.Assert(err, IsNil)
	cluster := s.srv.GetRaftCluster()
	c.Assert(cluster, NotNil)
	now := time.Now().UnixNano()
	for _, store := range stores {
		s.srv.PutStore(context.Background(), &pdpb.PutStoreRequest{
			Header: newHeader(s.srv),
			Store: &metapb.Store{
				Id:            store.Id,
				Address:       store.Address,
				LastHeartbeat: now,
			},
		})
	}
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

func (s *testClientSuite) TestTSO(c *C) {
	var tss []int64
	for i := 0; i < 100; i++ {
		p, l, err := s.client.GetTS(context.Background())
		c.Assert(err, IsNil)
		tss = append(tss, p<<18+l)
	}

	var last int64
	for _, ts := range tss {
		c.Assert(ts, Greater, last)
		last = ts
	}
}

func (s *testClientSuite) TestTSORace(c *C) {
	var wg sync.WaitGroup
	begin := make(chan struct{})
	count := 10
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func() {
			<-begin
			for i := 0; i < 100; i++ {
				_, _, err := s.client.GetTS(context.Background())
				c.Assert(err, IsNil)
			}
			wg.Done()
		}()
	}
	close(begin)
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

	testutil.WaitUntil(c, func(c *C) bool {
		r, err := s.client.GetRegion(context.Background(), []byte("a"))
		c.Assert(err, IsNil)
		if r == nil {
			return false
		}
		return c.Check(r.Meta, DeepEquals, region) &&
			c.Check(r.Leader, DeepEquals, peers[0])
	})
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
		testutil.WaitUntil(c, func(c *C) bool {
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
	testutil.WaitUntil(c, func(c *C) bool {
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

	testutil.WaitUntil(c, func(c *C) bool {
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
	err = cluster.RemoveStore(store.GetId())
	c.Assert(err, IsNil)
	offlineStore := proto.Clone(store).(*metapb.Store)
	offlineStore.State = metapb.StoreState_Offline

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

	// Mark the store as tombstone.
	err = cluster.BuryStore(store.GetId(), true)
	c.Assert(err, IsNil)
	tombstoneStore := proto.Clone(store).(*metapb.Store)
	tombstoneStore.State = metapb.StoreState_Tombstone

	// Get a tombstone store should fail.
	n, err = s.client.GetStore(context.Background(), store.GetId())
	c.Assert(err, IsNil)
	c.Assert(n, IsNil)

	// Should return tombstone stores.
	contains = false
	stores, err = s.client.GetAllStores(context.Background())
	c.Assert(err, IsNil)
	for _, store := range stores {
		if store.GetId() == tombstoneStore.GetId() {
			contains = true
			c.Assert(store, DeepEquals, tombstoneStore)
		}
	}
	c.Assert(contains, IsTrue)

	// Should not return tombstone stores.
	stores, err = s.client.GetAllStores(context.Background(), pd.WithExcludeTombstone())
	c.Assert(err, IsNil)
	for _, store := range stores {
		c.Assert(store, Not(Equals), tombstoneStore)
	}
}

func (s *testClientSuite) checkGCSafePoint(c *C, expectedSafePoint uint64) {
	req := &pdpb.GetGCSafePointRequest{
		Header: newHeader(s.srv),
	}
	resp, err := s.srv.GetGCSafePoint(context.Background(), req)
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
		{"a", 1000, 1},
		{"b", 1000, 2},
		{"c", 1000, 3},
	}
	for _, ssp := range serviceSafePoints {
		min, err := s.client.UpdateServiceGCSafePoint(context.Background(),
			ssp.ServiceID, 1000, ssp.SafePoint)
		c.Assert(err, IsNil)
		c.Assert(min, Equals, uint64(1))
	}
	min, err := s.client.UpdateServiceGCSafePoint(context.Background(),
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
	c.Assert(oldMinSsp.SafePoint, Equals, uint64(3))
	c.Assert(minSsp.ExpiredAt, Less, oldMinSsp.ExpiredAt)
}

func (s *testClientSuite) TestScatterRegion(c *C) {
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
	testutil.WaitUntil(c, func(c *C) bool {
		err := s.client.ScatterRegion(context.Background(), regionID)
		if c.Check(err, NotNil) {
			return false
		}
		resp, err := s.client.GetOperator(context.Background(), regionID)
		if c.Check(err, NotNil) {
			return false
		}
		return c.Check(resp.GetRegionId(), Equals, regionID) && c.Check(string(resp.GetDesc()), Equals, "scatter-region") && c.Check(resp.GetStatus(), Equals, pdpb.OperatorStatus_RUNNING)
	})
	c.Succeed()
}

func (s *testClientSuite) TestScatterRegionWithOption(c *C) {
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
	testutil.WaitUntil(c, func(c *C) bool {
		err := s.client.ScatterRegionWithOption(context.Background(), regionID, pd.WithGroup("test-group"))
		if c.Check(err, NotNil) {
			return false
		}
		resp, err := s.client.GetOperator(context.Background(), regionID)
		if c.Check(err, NotNil) {
			return false
		}
		return c.Check(resp.GetRegionId(), Equals, regionID) && c.Check(string(resp.GetDesc()), Equals, "scatter-region") && c.Check(resp.GetStatus(), Equals, pdpb.OperatorStatus_RUNNING)
	})
	c.Succeed()
}
