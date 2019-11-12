// Copyright 2016 PingCAP, Inc.
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

package server

import (
	"context"
	"strings"
	"sync"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"go.etcd.io/etcd/clientv3"
)

var _ = Suite(&testTsoSuite{})

type testTsoSuite struct {
	client       *clientv3.Client
	svr          *Server
	cleanup      CleanupFunc
	grpcPDClient pdpb.PDClient
}

func (s *testTsoSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = mustRunTestServer(c)
	s.client = s.svr.client
	mustWaitLeader(c, []*Server{s.svr})
	s.grpcPDClient = mustNewGrpcClient(c, s.svr.GetAddr())
}

func (s *testTsoSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testTsoSuite) testGetTimestamp(c *C, n int) *pdpb.Timestamp {
	req := &pdpb.TsoRequest{
		Header: newRequestHeader(s.svr.clusterID),
		Count:  uint32(n),
	}

	tsoClient, err := s.grpcPDClient.Tso(context.Background())
	c.Assert(err, IsNil)
	defer tsoClient.CloseSend()
	err = tsoClient.Send(req)
	c.Assert(err, IsNil)
	resp, err := tsoClient.Recv()
	c.Assert(err, IsNil)
	c.Assert(resp.GetCount(), Equals, uint32(n))

	res := resp.GetTimestamp()
	c.Assert(res.GetLogical(), Greater, int64(0))

	return res
}

func (s *testTsoSuite) TestTso(c *C) {
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			last := &pdpb.Timestamp{
				Physical: 0,
				Logical:  0,
			}

			for j := 0; j < 50; j++ {
				ts := s.testGetTimestamp(c, 10)
				c.Assert(ts.GetPhysical(), Not(Less), last.GetPhysical())
				if ts.GetPhysical() == last.GetPhysical() {
					c.Assert(ts.GetLogical(), Greater, last.GetLogical())
				}
				last = ts
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}

	wg.Wait()
}

func (s *testTsoSuite) TestTsoCount0(c *C) {
	req := &pdpb.TsoRequest{Header: newRequestHeader(s.svr.clusterID)}
	tsoClient, err := s.grpcPDClient.Tso(context.Background())
	c.Assert(err, IsNil)
	defer tsoClient.CloseSend()
	err = tsoClient.Send(req)
	c.Assert(err, IsNil)
	_, err = tsoClient.Recv()
	c.Assert(err, NotNil)
}

var _ = Suite(&testTimeFallBackSuite{})

type testTimeFallBackSuite struct {
	client       *clientv3.Client
	svr          *Server
	cleanup      CleanupFunc
	grpcPDClient pdpb.PDClient
}

func (s *testTimeFallBackSuite) SetUpSuite(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/pd/server/fallBackSync", `return(true)`), IsNil)
	c.Assert(failpoint.Enable("github.com/pingcap/pd/server/fallBackUpdate", `return(true)`), IsNil)
	s.svr, s.cleanup = mustRunTestServer(c)
	s.client = s.svr.client
	mustWaitLeader(c, []*Server{s.svr})
	s.grpcPDClient = mustNewGrpcClient(c, s.svr.GetAddr())
	s.svr.Close()
	failpoint.Disable("github.com/pingcap/pd/server/fallBackSync")
	failpoint.Disable("github.com/pingcap/pd/server/fallBackUpdate")
	err := s.svr.Run(context.TODO())
	c.Assert(err, IsNil)
	mustWaitLeader(c, []*Server{s.svr})
}

func (s *testTimeFallBackSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testTimeFallBackSuite) testGetTimestamp(c *C, n int) *pdpb.Timestamp {
	req := &pdpb.TsoRequest{
		Header: newRequestHeader(s.svr.clusterID),
		Count:  uint32(n),
	}

	tsoClient, err := s.grpcPDClient.Tso(context.Background())
	c.Assert(err, IsNil)
	defer tsoClient.CloseSend()
	err = tsoClient.Send(req)
	c.Assert(err, IsNil)
	resp, err := tsoClient.Recv()
	c.Assert(err, IsNil)
	c.Assert(resp.GetCount(), Equals, uint32(n))

	res := resp.GetTimestamp()
	c.Assert(res.GetLogical(), Greater, int64(0))
	c.Assert(res.GetPhysical(), Greater, time.Now().UnixNano()/int64(time.Millisecond))

	return res
}

func (s *testTimeFallBackSuite) TestTimeFallBack(c *C) {
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			last := &pdpb.Timestamp{
				Physical: 0,
				Logical:  0,
			}

			for j := 0; j < 50; j++ {
				ts := s.testGetTimestamp(c, 10)
				c.Assert(ts.GetPhysical(), Not(Less), last.GetPhysical())
				if ts.GetPhysical() == last.GetPhysical() {
					c.Assert(ts.GetLogical(), Greater, last.GetLogical())
				}
				last = ts
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}

	wg.Wait()
}

func mustGetLeader(c *C, client *clientv3.Client, leaderPath string) *pdpb.Member {
	for i := 0; i < 20; i++ {
		leader, _, err := getLeader(client, leaderPath)
		c.Assert(err, IsNil)
		if leader != nil {
			return leader
		}
		time.Sleep(500 * time.Millisecond)
	}

	c.Fatal("get leader error")
	return nil
}

var _ = Suite(&testFollowerTsoSuite{})

type testFollowerTsoSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
	svrs   []*Server
}

func (s *testFollowerTsoSuite) SetUpSuite(c *C) {
	s.svrs = make([]*Server, 0, 2)

	cfgs := NewTestMultiConfig(c, 2)
	ch := make(chan *Server, 2)
	for i := 0; i < 2; i++ {
		cfg := cfgs[i]
		go func() {
			svr, err := CreateServer(cfg, nil)
			c.Assert(err, IsNil)
			c.Assert(svr, NotNil)
			err = svr.Run(context.TODO())
			c.Assert(err, IsNil)
			ch <- svr
		}()
	}

	for i := 0; i < 2; i++ {
		svr := <-ch
		s.svrs = append(s.svrs, svr)
	}
	mustWaitLeader(c, s.svrs)
}

func (s *testFollowerTsoSuite) TearDownSuite(c *C) {
	for _, svr := range s.svrs {
		svr.Close()
		cleanServer(svr.cfg)
	}
}

func (s *testFollowerTsoSuite) TestRequest(c *C) {
	var err error

	var followerServer *Server
	for _, s := range s.svrs {
		if !s.IsLeader() {
			followerServer = s
		}
	}
	c.Assert(followerServer, NotNil)
	grpcPDClient := mustNewGrpcClient(c, followerServer.GetAddr())
	clusterID := followerServer.ClusterID()

	req := &pdpb.TsoRequest{Header: newRequestHeader(clusterID), Count: 1}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tsoClient, err := grpcPDClient.Tso(ctx)
	c.Assert(err, IsNil)
	defer tsoClient.CloseSend()
	err = tsoClient.Send(req)
	c.Assert(err, IsNil)
	_, err = tsoClient.Recv()
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "not leader"), IsTrue)
}
