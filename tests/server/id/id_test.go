// Copyright 2016 TiKV Project Authors.
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

package idtest

import (
	"context"
	"sync"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/pdpb"
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

const allocStep = uint64(1000)

var _ = Suite(&testAllocIDSuite{})

type testAllocIDSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *testAllocIDSuite) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	server.EnableZap = true
}

func (s *testAllocIDSuite) TearDownSuite(c *C) {
	s.cancel()
}

func (s *testAllocIDSuite) TestID(c *C) {
	cluster, err := tests.NewTestCluster(s.ctx, 1)
	c.Assert(err, IsNil)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()

	leaderServer := cluster.GetServer(cluster.GetLeader())
	var last uint64
	for i := uint64(0); i < allocStep; i++ {
		id, err := leaderServer.GetAllocator().Alloc()
		c.Assert(err, IsNil)
		c.Assert(id, Greater, last)
		last = id
	}

	var wg sync.WaitGroup

	var m sync.Mutex
	ids := make(map[uint64]struct{})

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for i := 0; i < 200; i++ {
				id, err := leaderServer.GetAllocator().Alloc()
				c.Assert(err, IsNil)
				m.Lock()
				_, ok := ids[id]
				ids[id] = struct{}{}
				m.Unlock()
				c.Assert(ok, IsFalse)
			}
		}()
	}

	wg.Wait()
}

func (s *testAllocIDSuite) TestCommand(c *C) {
	cluster, err := tests.NewTestCluster(s.ctx, 1)
	c.Assert(err, IsNil)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()

	leaderServer := cluster.GetServer(cluster.GetLeader())
	req := &pdpb.AllocIDRequest{Header: testutil.NewRequestHeader(leaderServer.GetClusterID())}

	grpcPDClient := testutil.MustNewGrpcClient(c, leaderServer.GetAddr())
	var last uint64
	for i := uint64(0); i < 2*allocStep; i++ {
		resp, err := grpcPDClient.AllocID(context.Background(), req)
		c.Assert(err, IsNil)
		c.Assert(resp.GetId(), Greater, last)
		last = resp.GetId()
	}
}

func (s *testAllocIDSuite) TestMonotonicID(c *C) {
	cluster, err := tests.NewTestCluster(s.ctx, 2)
	c.Assert(err, IsNil)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()

	leaderServer := cluster.GetServer(cluster.GetLeader())
	var last1 uint64
	for i := uint64(0); i < 10; i++ {
		id, err := leaderServer.GetAllocator().Alloc()
		c.Assert(err, IsNil)
		c.Assert(id, Greater, last1)
		last1 = id
	}
	err = cluster.ResignLeader()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	leaderServer = cluster.GetServer(cluster.GetLeader())
	var last2 uint64
	for i := uint64(0); i < 10; i++ {
		id, err := leaderServer.GetAllocator().Alloc()
		c.Assert(err, IsNil)
		c.Assert(id, Greater, last2)
		last2 = id
	}
	err = cluster.ResignLeader()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	leaderServer = cluster.GetServer(cluster.GetLeader())
	id, err := leaderServer.GetAllocator().Alloc()
	c.Assert(err, IsNil)
	c.Assert(id, Greater, last2)
	var last3 uint64
	for i := uint64(0); i < 1000; i++ {
		id, err := leaderServer.GetAllocator().Alloc()
		c.Assert(err, IsNil)
		c.Assert(id, Greater, last3)
		last3 = id
	}
}

func (s *testAllocIDSuite) TestPDRestart(c *C) {
	cluster, err := tests.NewTestCluster(s.ctx, 1)
	c.Assert(err, IsNil)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	leaderServer := cluster.GetServer(cluster.GetLeader())

	var last uint64
	for i := uint64(0); i < 10; i++ {
		id, err := leaderServer.GetAllocator().Alloc()
		c.Assert(err, IsNil)
		c.Assert(id, Greater, last)
		last = id
	}

	c.Assert(leaderServer.Stop(), IsNil)
	c.Assert(leaderServer.Run(), IsNil)
	cluster.WaitLeader()

	for i := uint64(0); i < 10; i++ {
		id, err := leaderServer.GetAllocator().Alloc()
		c.Assert(err, IsNil)
		c.Assert(id, Greater, last)
		last = id
	}
}
