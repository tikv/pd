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
	"math"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/gcpb"
	"github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server"
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

func (s *testGCSuite) TestXxx(c *C) {
	_, cli, cluster, clusterID := s.mustNewGCService(c)
	defer cluster.Destroy()

	serviceGroupRawKV := []byte("default_rawkv")
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

	// Update GC safe point with revision mismatch
	{
		reqGc := &gcpb.GetMinServiceSafePointByServiceGroupRequest{
			Header:         newRequestHeader(clusterID),
			ServiceGroupId: serviceGroupRawKV,
		}
		respGc, err := cli.GetMinServiceSafePointByServiceGroup(s.ctx, reqGc)
		c.Assert(err, IsNil)
		c.Assert(respGc.SafePoint, Equals, uint64(100))
		// c.Assert(respGc.Revision, Equals, int64(12))

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

	// Retry update GC safe point
	{
		reqGc := &gcpb.GetMinServiceSafePointByServiceGroupRequest{
			Header:         newRequestHeader(clusterID),
			ServiceGroupId: serviceGroupRawKV,
		}
		respGc, err := cli.GetMinServiceSafePointByServiceGroup(s.ctx, reqGc)
		c.Assert(err, IsNil)
		c.Assert(respGc.SafePoint, Equals, uint64(50))
		// c.Assert(respGc.Revision, Equals, int64(12))

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
