// Copyright 2022 TiKV Project Authors.
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

package gc_test

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/gcpb"
	"github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/storage/endpoint"
	"github.com/tikv/pd/tests"
	"go.uber.org/goleak"
)

func Test(t *testing.T) {
	TestingT(t)
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

var _ = Suite(&testGCSuite{})

func newRequestHeader(clusterID uint64) *gcpb.RequestHeader {
	return &gcpb.RequestHeader{
		ClusterId: clusterID,
	}
}

type testGCSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *testGCSuite) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	server.EnableZap = true
}

func (s *testGCSuite) TearDownSuite(c *C) {
	s.cancel()
}

func (s *testGCSuite) mustNewGCService(c *C) (addr string, cluster *tests.TestCluster, clusterID uint64) {
	var err error
	cluster, err = tests.NewTestCluster(s.ctx, 1)
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	leader := cluster.GetServer(cluster.GetLeader())
	c.Assert(leader.BootstrapCluster(), IsNil)

	clusterID = leader.GetClusterID()
	addr = leader.GetAddr()
	return
}

type testClient struct {
	cli       gcpb.GCClient
	clusterID uint64
	c         *C
	ctx       context.Context
}

func (c *testClient) mustGetAllServiceGroups() [][]byte {
	req := &gcpb.GetAllServiceGroupsRequest{
		Header: newRequestHeader(c.clusterID),
	}
	resp, err := c.cli.GetAllServiceGroups(c.ctx, req)
	c.c.Assert(err, IsNil)
	return resp.ServiceGroupId
}

func (c *testClient) mustUpdateServiceSafePoint(serviceGroupID []byte, serviceID []byte, ttl int64, safepoint uint64) *gcpb.UpdateServiceSafePointByServiceGroupResponse {
	req := &gcpb.UpdateServiceSafePointByServiceGroupRequest{
		Header:         newRequestHeader(c.clusterID),
		ServiceGroupId: serviceGroupID,
		ServiceId:      serviceID,
		TTL:            ttl,
		SafePoint:      safepoint,
	}
	resp, err := c.cli.UpdateServiceSafePointByServiceGroup(c.ctx, req)
	c.c.Assert(err, IsNil)
	return resp
}

func (c *testClient) mustGetMinServiceSafePoint(serviceGroupID []byte) (safepoint uint64, revision int64) {
	req := &gcpb.GetMinServiceSafePointByServiceGroupRequest{
		Header:         newRequestHeader(c.clusterID),
		ServiceGroupId: serviceGroupID,
	}
	resp, err := c.cli.GetMinServiceSafePointByServiceGroup(c.ctx, req)
	c.c.Assert(err, IsNil)
	return resp.GetSafePoint(), resp.GetRevision()
}

func (c *testClient) mustUpdateGCSafePoint(serviceGroupID []byte, safepoint uint64, revision int64) *gcpb.UpdateGCSafePointByServiceGroupResponse {
	req := &gcpb.UpdateGCSafePointByServiceGroupRequest{
		Header:         newRequestHeader(c.clusterID),
		ServiceGroupId: serviceGroupID,
		SafePoint:      safepoint,
		Revision:       revision,
	}
	resp, err := c.cli.UpdateGCSafePointByServiceGroup(c.ctx, req)
	c.c.Assert(err, IsNil)
	return resp
}

func (c *testClient) mustGetAllGCSafePoint() []*gcpb.ServiceGroupSafePoint {
	req := &gcpb.GetAllServiceGroupGCSafePointsRequest{
		Header: newRequestHeader(c.clusterID),
	}
	resp, err := c.cli.GetAllServiceGroupGCSafePoints(c.ctx, req)
	c.c.Assert(err, IsNil)
	return resp.GetSafePoints()
}

func (s *testGCSuite) TestGCService(c *C) {
	addr, cluster, clusterID := s.mustNewGCService(c)
	defer cluster.Destroy()

	client := testClient{
		cli:       testutil.MustNewGCClient(c, addr),
		clusterID: clusterID,
		c:         c,
		ctx:       s.ctx,
	}

	serviceGroupRawKV := []byte(endpoint.ServiceGroupRawKVDefault)
	serviceGroupTxnKV := []byte("default_txnkv")
	serviceID1 := []byte("svc1")
	serviceID2 := []byte("svc2")

	c.Assert(client.mustGetAllServiceGroups(), DeepEquals, [][]byte{serviceGroupRawKV})

	// Update service safe point
	{
		resp := client.mustUpdateServiceSafePoint(serviceGroupRawKV, serviceID1, math.MaxInt64, 100)
		expected := &gcpb.UpdateServiceSafePointByServiceGroupResponse{
			Header:       resp.GetHeader(),
			Succeeded:    true,
			GcSafePoint:  0,
			OldSafePoint: 0,
			NewSafePoint: 100,
		}
		c.Assert(resp, DeepEquals, expected)

		// Safe point roll back
		resp = client.mustUpdateServiceSafePoint(serviceGroupRawKV, serviceID1, math.MaxInt64, 99)
		c.Assert(resp.GetHeader().GetError().GetType(), Equals, gcpb.ErrorType_SAFEPOINT_ROLLBACK)
		c.Assert(resp.GetSucceeded(), IsFalse)
	}
	// now: svc1: 100

	// Update GC safe point with revision mismatch
	{
		safepoint, revision := client.mustGetMinServiceSafePoint(serviceGroupRawKV)
		c.Assert(safepoint, Equals, uint64(100))
		// c.Assert(revision, Equals, ?): Revision value is not stable. Don't check it.

		// Add a new service safe point
		respSvc := client.mustUpdateServiceSafePoint(serviceGroupRawKV, serviceID2, math.MaxInt64, 50)
		expected := &gcpb.UpdateServiceSafePointByServiceGroupResponse{
			Header:       respSvc.GetHeader(),
			Succeeded:    true,
			GcSafePoint:  0,
			OldSafePoint: 0,
			NewSafePoint: 50,
		}
		c.Assert(respSvc, DeepEquals, expected)

		// Revision mismatch
		respUpdate := client.mustUpdateGCSafePoint(serviceGroupRawKV, 100, revision)
		c.Assert(respUpdate.Succeeded, IsFalse)
		c.Assert(respUpdate.GetHeader().GetError().GetType(), Equals, gcpb.ErrorType_REVISION_MISMATCH)
	}
	// now: svc1: 100, svc2: 50

	// Retry update GC safe point
	{
		safepoint, revision := client.mustGetMinServiceSafePoint(serviceGroupRawKV)
		c.Assert(safepoint, Equals, uint64(50))

		respSvc := client.mustUpdateServiceSafePoint(serviceGroupRawKV, serviceID2, math.MaxInt64, 80)
		expected := &gcpb.UpdateServiceSafePointByServiceGroupResponse{
			Header:       respSvc.GetHeader(),
			Succeeded:    true,
			GcSafePoint:  0,
			OldSafePoint: 50,
			NewSafePoint: 80,
		}
		c.Assert(respSvc, DeepEquals, expected)

		respUpdate := client.mustUpdateGCSafePoint(serviceGroupRawKV, 50, revision)
		c.Assert(respUpdate.Succeeded, IsTrue)
		c.Assert(respUpdate.GetNewSafePoint(), Equals, uint64(50))

		// GC safe point roll back
		respUpdate = client.mustUpdateGCSafePoint(serviceGroupRawKV, 49, revision)
		c.Assert(respUpdate.Succeeded, IsFalse)
		c.Assert(respUpdate.GetHeader().GetError().GetType(), Equals, gcpb.ErrorType_SAFEPOINT_ROLLBACK)
	}
	// now: svc1: 100, svc2: 80, gc: 50

	// Remove svc2
	{
		respSvc := client.mustUpdateServiceSafePoint(serviceGroupRawKV, serviceID2, 0, 0)
		expected := &gcpb.UpdateServiceSafePointByServiceGroupResponse{
			Header:    respSvc.GetHeader(),
			Succeeded: true,
		}
		c.Assert(respSvc, DeepEquals, expected)

		safepoint, _ := client.mustGetMinServiceSafePoint(serviceGroupRawKV)
		c.Assert(safepoint, Equals, uint64(100))
	}
	// now: svc1: 100, gc: 50

	// Add svc2 with safe point roll back
	{
		respSvc := client.mustUpdateServiceSafePoint(serviceGroupRawKV, serviceID2, math.MaxInt64, 49)
		c.Assert(respSvc.Succeeded, IsFalse)
		c.Assert(respSvc.GetHeader().GetError().GetType(), Equals, gcpb.ErrorType_SAFEPOINT_ROLLBACK)
	}

	// Another service group with no service safe point
	{
		safepoint, revision := client.mustGetMinServiceSafePoint(serviceGroupTxnKV)
		c.Assert(safepoint, Equals, uint64(0))
		c.Assert(revision, Equals, int64(-1))

		respUpdate := client.mustUpdateGCSafePoint(serviceGroupTxnKV, 100, -1)
		c.Assert(respUpdate.Succeeded, IsTrue)
		c.Assert(respUpdate.GetNewSafePoint(), Equals, uint64(100))
	}

	// Get all service group GC safe points
	{
		safepoints := client.mustGetAllGCSafePoint()
		expected := []*gcpb.ServiceGroupSafePoint{
			{ServiceGroupId: serviceGroupRawKV, SafePoint: 50},
			{ServiceGroupId: serviceGroupTxnKV, SafePoint: 100},
		}
		c.Assert(safepoints, DeepEquals, expected)
	}
}

func (s *testGCSuite) TestConcurrency(c *C) {
	count := 500
	concurrency := 10

	addr, cluster, clusterID := s.mustNewGCService(c)
	defer cluster.Destroy()

	newClient := func() testClient {
		return testClient{
			cli:       testutil.MustNewGCClient(c, addr),
			clusterID: clusterID,
			c:         c,
			ctx:       s.ctx,
		}
	}

	serviceGroupID := []byte(endpoint.ServiceGroupRawKVDefault)
	closeCh := make(chan struct{})

	{ // Initialize GC safe point to make sure that tikvThread will get a valid safe point.
		client := newClient()
		client.mustUpdateGCSafePoint(serviceGroupID, 0, -1)
	}

	gcWorkerThread := func() {
		client := newClient()
		for {
			safepoint, revision := client.mustGetMinServiceSafePoint(serviceGroupID)
			if safepoint == 0 {
				continue
			}

			client.mustUpdateGCSafePoint(serviceGroupID, safepoint, revision)

			select {
			case <-closeCh:
				return
			default:
			}
		}
	}

	svcThread := func(svcName string) {
		client := newClient()
		for i := 1; i <= count; i++ {
			client.mustUpdateServiceSafePoint(serviceGroupID, []byte(svcName), math.MaxInt64, uint64(i*10))
		}
	}

	tikvThread := func() {
		client := newClient()
		for {
			safepoints := client.mustGetAllGCSafePoint()
			c.Assert(len(safepoints), Equals, 1)
			gcSafePoint := safepoints[0].SafePoint

			svcSafePoint, _ := client.mustGetMinServiceSafePoint(serviceGroupID)
			c.Assert(gcSafePoint <= svcSafePoint, IsTrue)

			select {
			case <-closeCh:
				return
			default:
			}
		}
	}

	wgSvc := sync.WaitGroup{}
	wgGc := sync.WaitGroup{}

	for i := 0; i < concurrency; i++ {
		i := i
		wgSvc.Add(1)
		go func() {
			defer wgSvc.Done()
			svcThread(fmt.Sprintf("svc_%v", i))
		}()
	}

	wgGc.Add(1)
	go func() {
		defer wgGc.Done()
		gcWorkerThread()
	}()

	wgGc.Add(1)
	go func() {
		defer wgGc.Done()
		tikvThread()
	}()

	wgSvc.Wait()
	close(closeCh)
	wgGc.Wait()
}
