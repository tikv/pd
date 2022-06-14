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
	"io"
	"math"
	"sync"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/gcpb"
	"github.com/tikv/pd/pkg/testutil"
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

func (c *testClient) mustListKeySpaces(withGCSafePoint bool) []*gcpb.KeySpace {
	req := &gcpb.ListKeySpacesRequest{
		Header:          newRequestHeader(c.clusterID),
		WithGcSafePoint: withGCSafePoint,
	}
	respStream, err := c.cli.ListKeySpaces(c.ctx, req)
	c.c.Assert(err, IsNil)
	keySpaces := make([]*gcpb.KeySpace, 0, 5)
	for {
		resp, err := respStream.Recv()
		if err == io.EOF {
			break
		}
		c.c.Assert(err, IsNil)
		keySpaces = append(keySpaces, resp.KeySpace)
	}
	return keySpaces
}

func (c *testClient) mustUpdateServiceSafePoint(spaceID uint32, serviceID []byte, ttl int64, safepoint uint64) *gcpb.UpdateServiceSafePointResponse {
	req := &gcpb.UpdateServiceSafePointRequest{
		Header:     newRequestHeader(c.clusterID),
		SpaceId:    spaceID,
		ServiceId:  serviceID,
		TimeToLive: ttl,
		SafePoint:  safepoint,
	}
	resp, err := c.cli.UpdateServiceSafePoint(c.ctx, req)
	c.c.Assert(err, IsNil)
	return resp
}

func (c *testClient) mustGetMinServiceSafePoint(spaceID uint32) (safepoint uint64, revision int64) {
	req := &gcpb.GetMinServiceSafePointRequest{
		Header:  newRequestHeader(c.clusterID),
		SpaceId: spaceID,
	}
	resp, err := c.cli.GetMinServiceSafePoint(c.ctx, req)
	c.c.Assert(err, IsNil)
	return resp.GetSafePoint(), resp.GetRevision()
}

func (c *testClient) mustUpdateGCSafePoint(spaceID uint32, safepoint uint64, revision int64) *gcpb.UpdateGCSafePointResponse {
	req := &gcpb.UpdateGCSafePointRequest{
		Header:    newRequestHeader(c.clusterID),
		SpaceId:   spaceID,
		SafePoint: safepoint,
		Revision:  revision,
	}
	resp, err := c.cli.UpdateGCSafePoint(c.ctx, req)
	c.c.Assert(err, IsNil)
	return resp
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

	keySpaceRawKV := uint32(1)
	keySpaceTxnKV := uint32(2)
	serviceID1 := []byte("svc1")
	serviceID2 := []byte("svc2")

	c.Assert(client.mustListKeySpaces(false), HasLen, 0)

	// Update service safe point
	{
		resp := client.mustUpdateServiceSafePoint(keySpaceRawKV, serviceID1, math.MaxInt64, 100)
		expected := &gcpb.UpdateServiceSafePointResponse{
			Header:       resp.GetHeader(),
			Succeeded:    true,
			GcSafePoint:  0,
			OldSafePoint: 0,
			NewSafePoint: 100,
		}
		c.Assert(resp, DeepEquals, expected)

		// Safe point roll back
		resp = client.mustUpdateServiceSafePoint(keySpaceRawKV, serviceID1, math.MaxInt64, 99)
		c.Assert(resp.GetHeader().GetError().GetType(), Equals, gcpb.ErrorType_SAFEPOINT_ROLLBACK)
		c.Assert(resp.GetSucceeded(), IsFalse)
	}
	// now: svc1: 100

	// Update GC safe point with revision mismatch
	{
		safepoint, revision := client.mustGetMinServiceSafePoint(keySpaceRawKV)
		c.Assert(safepoint, Equals, uint64(100))
		// c.Assert(revision, Equals, ?): Revision value is not stable. Don't check it.

		// Add a new service safe point
		respSvc := client.mustUpdateServiceSafePoint(keySpaceRawKV, serviceID2, math.MaxInt64, 50)
		expected := &gcpb.UpdateServiceSafePointResponse{
			Header:       respSvc.GetHeader(),
			Succeeded:    true,
			GcSafePoint:  0,
			OldSafePoint: 0,
			NewSafePoint: 50,
		}
		c.Assert(respSvc, DeepEquals, expected)

		// Revision mismatch
		respUpdate := client.mustUpdateGCSafePoint(keySpaceRawKV, 100, revision)
		c.Assert(respUpdate.Succeeded, IsFalse)
		c.Assert(respUpdate.GetHeader().GetError().GetType(), Equals, gcpb.ErrorType_REVISION_MISMATCH)
	}
	// now: svc1: 100, svc2: 50

	// Retry update GC safe point
	{
		safepoint, revision := client.mustGetMinServiceSafePoint(keySpaceRawKV)
		c.Assert(safepoint, Equals, uint64(50))

		respSvc := client.mustUpdateServiceSafePoint(keySpaceRawKV, serviceID2, math.MaxInt64, 80)
		expected := &gcpb.UpdateServiceSafePointResponse{
			Header:       respSvc.GetHeader(),
			Succeeded:    true,
			GcSafePoint:  0,
			OldSafePoint: 50,
			NewSafePoint: 80,
		}
		c.Assert(respSvc, DeepEquals, expected)

		respUpdate := client.mustUpdateGCSafePoint(keySpaceRawKV, 50, revision)
		c.Assert(respUpdate.Succeeded, IsTrue)

		// GC safe point roll back
		respUpdate = client.mustUpdateGCSafePoint(keySpaceRawKV, 49, revision)
		c.Assert(respUpdate.Succeeded, IsFalse)
		c.Assert(respUpdate.GetHeader().GetError().GetType(), Equals, gcpb.ErrorType_SAFEPOINT_ROLLBACK)
	}
	// now: svc1: 100, svc2: 80, gc: 50

	// Remove svc2
	{
		respSvc := client.mustUpdateServiceSafePoint(keySpaceRawKV, serviceID2, 0, 0)
		expected := &gcpb.UpdateServiceSafePointResponse{
			Header:    respSvc.GetHeader(),
			Succeeded: true,
		}
		c.Assert(respSvc, DeepEquals, expected)

		safepoint, _ := client.mustGetMinServiceSafePoint(keySpaceRawKV)
		c.Assert(safepoint, Equals, uint64(100))
	}
	// now: svc1: 100, gc: 50

	// Add svc2 with safe point roll back
	{
		respSvc := client.mustUpdateServiceSafePoint(keySpaceRawKV, serviceID2, math.MaxInt64, 49)
		c.Assert(respSvc.Succeeded, IsFalse)
		c.Assert(respSvc.GetHeader().GetError().GetType(), Equals, gcpb.ErrorType_SAFEPOINT_ROLLBACK)
	}

	// Another key space with no service safe point
	{
		safepoint, revision := client.mustGetMinServiceSafePoint(keySpaceTxnKV)
		c.Assert(safepoint, Equals, uint64(0))
		c.Assert(revision, Equals, int64(-1))

		respUpdate := client.mustUpdateGCSafePoint(keySpaceTxnKV, 100, -1)
		c.Assert(respUpdate.Succeeded, IsTrue)
	}

	// Get all key space GC safe points
	{
		safepoints := client.mustListKeySpaces(true)
		expected := []*gcpb.KeySpace{
			{SpaceId: keySpaceRawKV, GcSafePoint: 50},
			{SpaceId: keySpaceTxnKV, GcSafePoint: 100},
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

	spaceID := uint32(100)
	closeCh := make(chan struct{})

	{ // Initialize GC safe point to make sure that tikv thread will get a valid safe point.
		client := newClient()
		client.mustUpdateGCSafePoint(spaceID, 0, -1)
	}

	gcWorkerThread := func() {
		client := newClient()
		for {
			safepoint, revision := client.mustGetMinServiceSafePoint(spaceID)
			if safepoint == 0 {
				continue
			}

			client.mustUpdateGCSafePoint(spaceID, safepoint, revision)

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
			client.mustUpdateServiceSafePoint(spaceID, []byte(svcName), math.MaxInt64, uint64(i*10))
		}
	}

	tikvThread := func() {
		client := newClient()
		for {
			keySpaces := client.mustListKeySpaces(true)
			c.Assert(len(keySpaces), Equals, 1)
			gcSafePoint := keySpaces[0].GcSafePoint

			svcSafePoint, _ := client.mustGetMinServiceSafePoint(spaceID)
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
