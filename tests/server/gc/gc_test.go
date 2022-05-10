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

func (s *testGCSuite) mustNewGCService(c *C) (gcSvc *server.GcServer, cli gcpb.GCClient, cluster *tests.TestCluster, clusterID uint64) {
	var err error
	cluster, err = tests.NewTestCluster(s.ctx, 1)
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	leader := cluster.GetServer(cluster.GetLeader())
	c.Assert(leader.BootstrapCluster(), IsNil)

	clusterID = leader.GetClusterID()
	gcSvc = leader.GetGCService()

	cli = testutil.MustNewGCClient(c, leader.GetAddr())

	return
}

func (s *testGCSuite) TestGCService(c *C) {
	_, cli, cluster, clusterID := s.mustNewGCService(c)
	defer cluster.Destroy()

	serviceGroupRawKV := []byte(endpoint.ServiceGroupRawKVDefault)
	serviceGroupTxnKV := []byte("default_txnkv")
	serviceID1 := []byte("svc1")
	serviceID2 := []byte("svc2")

	{
		req := &gcpb.GetAllServiceGroupsRequest{
			Header: newRequestHeader(clusterID),
		}
		resp, err := cli.GetAllServiceGroups(s.ctx, req)
		c.Assert(err, IsNil)
		c.Assert(resp.ServiceGroupId, DeepEquals, [][]byte{serviceGroupRawKV})
	}

	// Update service safe point
	{
		req := &gcpb.UpdateServiceSafePointByServiceGroupRequest{
			Header:         newRequestHeader(clusterID),
			ServiceGroupId: serviceGroupRawKV,
			ServiceId:      serviceID1,
			TTL:            math.MaxInt64,
			SafePoint:      100,
		}
		resp, err := cli.UpdateServiceSafePointByServiceGroup(s.ctx, req)
		c.Assert(err, IsNil)
		expected := &gcpb.UpdateServiceSafePointByServiceGroupResponse{
			Header:       resp.GetHeader(),
			Succeeded:    true,
			GcSafePoint:  0,
			OldSafePoint: 0,
			NewSafePoint: 100,
		}
		c.Assert(resp, DeepEquals, expected)

		// Safe point roll back
		req.SafePoint = 99
		resp, err = cli.UpdateServiceSafePointByServiceGroup(s.ctx, req)
		c.Assert(err, IsNil)
		c.Assert(resp.GetHeader().GetError().GetType(), Equals, gcpb.ErrorType_SAFEPOINT_ROLLBACK)
		c.Assert(resp.GetSucceeded(), IsFalse)
	}
	// now: svc1: 100

	// Update GC safe point with revision mismatch
	{
		reqGc := &gcpb.GetMinServiceSafePointByServiceGroupRequest{
			Header:         newRequestHeader(clusterID),
			ServiceGroupId: serviceGroupRawKV,
		}
		respGc, err := cli.GetMinServiceSafePointByServiceGroup(s.ctx, reqGc)
		c.Assert(err, IsNil)
		c.Assert(respGc.SafePoint, Equals, uint64(100))
		// c.Assert(respGc.Revision, Equals, ?): Revision value is not stable. Don't check it.

		reqSvc := &gcpb.UpdateServiceSafePointByServiceGroupRequest{
			Header:         newRequestHeader(clusterID),
			ServiceGroupId: serviceGroupRawKV,
			ServiceId:      serviceID2,
			TTL:            math.MaxInt64,
			SafePoint:      50,
		}
		respSvc, err := cli.UpdateServiceSafePointByServiceGroup(s.ctx, reqSvc)
		c.Assert(err, IsNil)
		expected := &gcpb.UpdateServiceSafePointByServiceGroupResponse{
			Header:       respSvc.GetHeader(),
			Succeeded:    true,
			GcSafePoint:  0,
			OldSafePoint: 0,
			NewSafePoint: 50,
		}
		c.Assert(respSvc, DeepEquals, expected)

		reqUpdate := &gcpb.UpdateGCSafePointByServiceGroupRequest{
			Header:         newRequestHeader(clusterID),
			ServiceGroupId: serviceGroupRawKV,
			SafePoint:      100,
			Revision:       respGc.Revision,
		}
		respUpdate, err := cli.UpdateGCSafePointByServiceGroup(s.ctx, reqUpdate)
		c.Assert(err, IsNil)
		c.Assert(respUpdate.Succeeded, IsFalse)
		c.Assert(respUpdate.GetHeader().GetError().GetType(), Equals, gcpb.ErrorType_REVISION_MISMATCH)
	}
	// now: svc1: 100, svc2: 50

	// Retry update GC safe point
	{
		reqGc := &gcpb.GetMinServiceSafePointByServiceGroupRequest{
			Header:         newRequestHeader(clusterID),
			ServiceGroupId: serviceGroupRawKV,
		}
		respGc, err := cli.GetMinServiceSafePointByServiceGroup(s.ctx, reqGc)
		c.Assert(err, IsNil)
		c.Assert(respGc.SafePoint, Equals, uint64(50))

		reqSvc := &gcpb.UpdateServiceSafePointByServiceGroupRequest{
			Header:         newRequestHeader(clusterID),
			ServiceGroupId: serviceGroupRawKV,
			ServiceId:      serviceID2,
			TTL:            math.MaxInt64,
			SafePoint:      80,
		}
		respSvc, err := cli.UpdateServiceSafePointByServiceGroup(s.ctx, reqSvc)
		c.Assert(err, IsNil)
		expected := &gcpb.UpdateServiceSafePointByServiceGroupResponse{
			Header:       respSvc.GetHeader(),
			Succeeded:    true,
			GcSafePoint:  0,
			OldSafePoint: 50,
			NewSafePoint: 80,
		}
		c.Assert(respSvc, DeepEquals, expected)

		reqUpdate := &gcpb.UpdateGCSafePointByServiceGroupRequest{
			Header:         newRequestHeader(clusterID),
			ServiceGroupId: serviceGroupRawKV,
			SafePoint:      50,
			Revision:       respGc.Revision,
		}
		respUpdate, err := cli.UpdateGCSafePointByServiceGroup(s.ctx, reqUpdate)
		c.Assert(err, IsNil)
		c.Assert(respUpdate.Succeeded, IsTrue)
		c.Assert(respUpdate.GetNewSafePoint(), Equals, uint64(50))

		// GC safe point roll back
		reqUpdate.SafePoint = 49
		respUpdate, err = cli.UpdateGCSafePointByServiceGroup(s.ctx, reqUpdate)
		c.Assert(err, IsNil)
		c.Assert(respUpdate.Succeeded, IsFalse)
		c.Assert(respUpdate.GetHeader().GetError().GetType(), Equals, gcpb.ErrorType_SAFEPOINT_ROLLBACK)
	}
	// now: svc1: 100, svc2: 80, gc: 50

	// Remove svc2
	{
		reqSvc := &gcpb.UpdateServiceSafePointByServiceGroupRequest{
			Header:         newRequestHeader(clusterID),
			ServiceGroupId: serviceGroupRawKV,
			ServiceId:      serviceID2,
			TTL:            0,
		}
		respSvc, err := cli.UpdateServiceSafePointByServiceGroup(s.ctx, reqSvc)
		c.Assert(err, IsNil)
		expected := &gcpb.UpdateServiceSafePointByServiceGroupResponse{
			Header:    respSvc.GetHeader(),
			Succeeded: true,
		}
		c.Assert(respSvc, DeepEquals, expected)

		reqGc := &gcpb.GetMinServiceSafePointByServiceGroupRequest{
			Header:         newRequestHeader(clusterID),
			ServiceGroupId: serviceGroupRawKV,
		}
		respGc, err := cli.GetMinServiceSafePointByServiceGroup(s.ctx, reqGc)
		c.Assert(err, IsNil)
		c.Assert(respGc.SafePoint, Equals, uint64(100))
	}
	// now: svc1: 100, gc: 50

	// Add svc2 with safe point roll back
	{
		reqSvc := &gcpb.UpdateServiceSafePointByServiceGroupRequest{
			Header:         newRequestHeader(clusterID),
			ServiceGroupId: serviceGroupRawKV,
			ServiceId:      serviceID2,
			TTL:            math.MaxInt64,
			SafePoint:      49,
		}
		respSvc, err := cli.UpdateServiceSafePointByServiceGroup(s.ctx, reqSvc)
		c.Assert(err, IsNil)
		c.Assert(respSvc.Succeeded, IsFalse)
		c.Assert(respSvc.GetHeader().GetError().GetType(), Equals, gcpb.ErrorType_SAFEPOINT_ROLLBACK)
	}

	// Another service group with no service safe point
	{
		reqGc := &gcpb.GetMinServiceSafePointByServiceGroupRequest{
			Header:         newRequestHeader(clusterID),
			ServiceGroupId: serviceGroupTxnKV,
		}
		respGc, err := cli.GetMinServiceSafePointByServiceGroup(s.ctx, reqGc)
		c.Assert(err, IsNil)
		c.Assert(respGc.SafePoint, Equals, uint64(0))
		c.Assert(respGc.Revision, Equals, int64(-1))

		reqUpdate := &gcpb.UpdateGCSafePointByServiceGroupRequest{
			Header:         newRequestHeader(clusterID),
			ServiceGroupId: serviceGroupTxnKV,
			SafePoint:      100,
			Revision:       -1,
		}
		respUpdate, err := cli.UpdateGCSafePointByServiceGroup(s.ctx, reqUpdate)
		c.Assert(err, IsNil)
		c.Assert(respUpdate.Succeeded, IsTrue)
		c.Assert(respUpdate.GetNewSafePoint(), Equals, uint64(100))
	}

	// Get all service group GC safe points
	{
		req := &gcpb.GetAllServiceGroupGCSafePointsRequest{
			Header: newRequestHeader(clusterID),
		}
		resp, err := cli.GetAllServiceGroupGCSafePoints(s.ctx, req)
		c.Assert(err, IsNil)
		expected := []*gcpb.ServiceGroupSafePoint{
			{ServiceGroupId: serviceGroupRawKV, SafePoint: 50},
			{ServiceGroupId: serviceGroupTxnKV, SafePoint: 100},
		}
		c.Assert(resp.GetSafePoints(), DeepEquals, expected)
	}
}

func (s *testGCSuite) TestConcurrency(c *C) {
	count := 500
	concurrency := 10

	svc, _, cluster, clusterID := s.mustNewGCService(c)
	defer cluster.Destroy()

	serviceGroupID := []byte(endpoint.ServiceGroupRawKVDefault)
	closeCh := make(chan struct{})

	updateGcSafePoint := func(safepoint uint64, revision int64) {
		reqUpdate := &gcpb.UpdateGCSafePointByServiceGroupRequest{
			Header:         newRequestHeader(clusterID),
			ServiceGroupId: serviceGroupID,
			SafePoint:      safepoint,
			Revision:       revision,
		}
		_, err := svc.UpdateGCSafePointByServiceGroup(s.ctx, reqUpdate)
		c.Assert(err, IsNil)

	}
	updateGcSafePoint(0, -1)

	gcWorkerThread := func() {
		for {
			reqMin := &gcpb.GetMinServiceSafePointByServiceGroupRequest{
				Header:         newRequestHeader(clusterID),
				ServiceGroupId: serviceGroupID,
			}
			respMin, err := svc.GetMinServiceSafePointByServiceGroup(s.ctx, reqMin)
			c.Assert(err, IsNil)

			if respMin.SafePoint == 0 {
				continue
			}

			updateGcSafePoint(respMin.SafePoint, respMin.Revision)

			select {
			case <-closeCh:
				return
			default:
			}
		}
	}

	updateSvcSafePoint := func(svcName string, safepoint uint64) {
		reqSvc := &gcpb.UpdateServiceSafePointByServiceGroupRequest{
			Header:         newRequestHeader(clusterID),
			ServiceGroupId: serviceGroupID,
			ServiceId:      []byte(svcName),
			TTL:            math.MaxInt64,
			SafePoint:      safepoint,
		}
		respSvc, err := svc.UpdateServiceSafePointByServiceGroup(s.ctx, reqSvc)
		c.Assert(err, IsNil)
		c.Assert(respSvc.Succeeded, IsTrue)
	}

	svcThread := func(svcName string) {
		for i := 1; i <= count; i++ {
			updateSvcSafePoint(svcName, uint64(i*10))
		}
	}

	tikvThread := func() {
		for {
			reqGc := &gcpb.GetAllServiceGroupGCSafePointsRequest{
				Header: newRequestHeader(clusterID),
			}
			respGc, err := svc.GetAllServiceGroupGCSafePoints(s.ctx, reqGc)
			c.Assert(err, IsNil)
			c.Assert(len(respGc.GetSafePoints()), Equals, 1)

			gcSafePoint := respGc.GetSafePoints()[0].SafePoint

			reqMin := &gcpb.GetMinServiceSafePointByServiceGroupRequest{
				Header:         newRequestHeader(clusterID),
				ServiceGroupId: serviceGroupID,
			}
			respMin, err := svc.GetMinServiceSafePointByServiceGroup(s.ctx, reqMin)
			c.Assert(err, IsNil)

			c.Assert(gcSafePoint <= respMin.SafePoint, IsTrue)

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
