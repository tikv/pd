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
	"net/url"
	"time"

	"github.com/ngaut/log"
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
	servers, cleanup := newMultiTestServers(c, 3)
	defer cleanup()

	// Make a slice [proxy0, proxy1, leader].
	var svrs []*Server
	leader := mustWaitLeader(servers)
	for _, svr := range servers {
		if svr != leader {
			svrs = append(svrs, svr)
		}
	}
	svrs = append(svrs, leader)

	// Make connections to proxy0 and proxy1.
	// Make sure they proxy request to leader.
	for i := 0; i < 2; i++ {
		svr := svrs[i]
		mustRequestSuccess(c, svr)
		checkServerLeaderConns(c, svr, leader)
	}

	// Close the leader and wait for a new one.
	leader.Close()
	newLeader := mustWaitLeader(svrs[:2])

	// Make sure we can still request on the connections,
	// and the new leader will handle request itself.
	for i := 0; i < 2; i++ {
		svr := svrs[i]
		mustRequestSuccess(c, svr)
		if svr == newLeader {
			checkServerLeaderConns(c, svr, nil)
		} else {
			checkServerLeaderConns(c, svr, newLeader)
		}
	}

	// Close the new leader and we have only one node now.
	newLeader.Close()
	time.Sleep(time.Second)

	// Request will failed with no leader.
	for i := 0; i < 2; i++ {
		svr := svrs[i]
		if svr != newLeader {
			resp := mustRequest(c, svr)
			error := resp.GetHeader().GetError()
			c.Assert(error, NotNil)
			log.Debugf("Response error: %v", error)
			c.Assert(svr.IsLeader(), Equals, false)
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

func checkServerLeaderConns(c *C, s *Server, leader *Server) {
	for conn := range s.conns {
		if leader == nil {
			c.Assert(conn.leaderConn, IsNil)
		} else {
			u, err := url.Parse(leader.GetAddr())
			c.Assert(err, IsNil)
			c.Assert(conn.leaderConn.RemoteAddr().String(), Equals, u.Host)
		}
	}
}
