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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

var _ = Suite(&testConnSuite{})

type testConnSuite struct {
}

func (s *testConnSuite) SetUpSuite(c *C) {
}

func (s *testConnSuite) TearDownSuite(c *C) {
}

func (s *testConnSuite) TestRedirect(c *C) {
	svrs, cleanup := newMultiTestServers(c, 3)
	defer cleanup()

	for _, svr := range svrs {
		mustRequestSuccess(c, svr)
	}
}

func (s *testConnSuite) TestReconnect(c *C) {
	svrs, cleanup := newMultiTestServers(c, 3)
	defer cleanup()

	// Collect two followers.
	var followers []*Server
	leader := mustWaitLeader(c, svrs)
	for _, svr := range svrs {
		if svr != leader {
			followers = append(followers, svr)
		}
	}

	// Make connections to followers.
	// Make sure they proxy requests to the leader.
	for i := 0; i < 2; i++ {
		svr := followers[i]
		mustRequestSuccess(c, svr)
	}

	// Close the leader and wait for a new one.
	leader.Close()
	newLeader := mustWaitLeader(c, followers)

	// Make sure we can still request on the connections,
	// and the new leader will handle request itself.
	for i := 0; i < 2; i++ {
		svr := followers[i]
		mustRequestSuccess(c, svr)
	}

	// Close the new leader and we have only one node now.
	newLeader.Close()
	time.Sleep(time.Second)

	// Request will failed with no leader.
	for i := 0; i < 2; i++ {
		svr := followers[i]
		if svr != newLeader {
			resp := mustRequest(c, svr)
			err := resp.GetHeader().GetError()
			c.Assert(err, NotNil)
			c.Logf("Response error: %v", err)
			c.Assert(svr.IsLeader(), IsFalse)
		}
	}
}

func mustRequest(c *C, s *Server) *pdpb.Response {
	req := &pdpb.Request{
		CmdType: pdpb.CommandType_AllocId.Enum(),
		AllocId: &pdpb.AllocIdRequest{},
	}
	conn, err := rpcConnect(s.GetAddr())
	c.Assert(err, IsNil)
	defer conn.Close()
	resp, err := rpcCall(conn, 0, req)
	c.Assert(err, IsNil)
	return resp
}

func mustRequestSuccess(c *C, s *Server) {
	resp := mustRequest(c, s)
	c.Assert(resp.GetHeader().GetError(), IsNil)
}
