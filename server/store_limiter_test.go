// Copyright 2019 PingCAP, Inc.
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
// limitations under the License

package server

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/pkg/testutil"
	"github.com/pingcap/pd/server/schedule"
)

var _ = Suite(&testStoreLimiterSuite{})

type testStoreLimiterSuite struct {
	cleanup CleanupFunc
	oc      *schedule.OperatorController
}

func (s *testStoreLimiterSuite) SetUpSuite(c *C) {
	// Create a server for testing
	serv, cleanup, err := NewTestServer(c)
	c.Assert(err, IsNil)
	c.Assert(serv, NotNil)
	c.Assert(cleanup, NotNil)

	// The server newly created is not ready to serve, we
	// wait for a leader choosing by raft, and then send a
	// request to bootstrap the service
	mustWaitLeader(c, []*Server{serv})
	base := baseCluster{
		svr:          serv,
		grpcPDClient: testutil.MustNewGrpcClient(c, serv.GetAddr()),
	}
	base.bootstrapCluster(c, serv.clusterID, "127.0.0.1:0")

	// The raft cluster is running now
	cluster := serv.GetRaftCluster()
	c.Assert(cluster, NotNil)

	s.oc = cluster.GetOperatorController()
	s.cleanup = cleanup
}
func (s *testStoreLimiterSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testStoreLimiterSuite) TestCollect(c *C) {
	limiter := NewStoreLimiter(s.oc)

	limiter.Collect(&pdpb.StoreStats{})
	c.Assert(limiter.state.cst.total, Equals, int64(1))
}

func (s *testStoreLimiterSuite) TestStoreLimitScene(c *C) {
	limiter := NewStoreLimiter(s.oc)
	c.Assert(limiter.scene, DeepEquals, schedule.DefaultStoreLimitScene())
}

func (s *testStoreLimiterSuite) TestReplaceStoreLimitScene(c *C) {
	limiter := NewStoreLimiter(s.oc)

	scene := &schedule.StoreLimitScene{Idle: 4, Low: 3, Normal: 2, High: 1}
	limiter.ReplaceStoreLimitScene(scene)

	c.Assert(limiter.scene, DeepEquals, scene)
}
