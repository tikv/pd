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

package api

import (
	"net/http"
	"strings"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/pd/server"
)

var _ = Suite(&testRedirectorSuite{})

type testRedirectorSuite struct {
}

func (s *testRedirectorSuite) SetUpSuite(c *C) {
}

func (s *testRedirectorSuite) TearDownSuite(c *C) {
}

func (s *testRedirectorSuite) TestRedirect(c *C) {
	_, svrs, cleanup := mustNewCluster(c, 3)
	defer cleanup()

	for _, svr := range svrs {
		mustRequestSuccess(c, svr)
	}
}

func (s *testRedirectorSuite) TestNoLeader(c *C) {
	_, servers, cleanup := mustNewCluster(c, 3)
	defer cleanup()

	// Make a slice [proxy0, proxy1, leader].
	var svrs []*server.Server
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
	}

	// Close the leader and wait for a new one.
	leader.Close()
	newLeader := mustWaitLeader(svrs[:2])

	// Make sure we can still request on the connections.
	for i := 0; i < 2; i++ {
		svr := svrs[i]
		mustRequestSuccess(c, svr)
	}

	// Close the new leader and we have only one node now.
	newLeader.Close()
	time.Sleep(time.Second)

	// Request will failed with no leader.
	for i := 0; i < 2; i++ {
		svr := svrs[i]
		if svr != newLeader {
			resp := mustRequest(c, svr)
			c.Assert(resp.StatusCode, Equals, http.StatusInternalServerError)
		}
	}
}

func mustRequest(c *C, s *server.Server) *http.Response {
	unixAddr := []string{s.GetAddr(), apiPrefix, "/api/v1/version"}
	httpAddr := mustUnixAddrToHTTPAddr(c, strings.Join(unixAddr, ""))
	client := newUnixSocketClient()
	resp, err := client.Get(httpAddr)
	c.Assert(err, IsNil)
	return resp
}

func mustRequestSuccess(c *C, s *server.Server) {
	resp := mustRequest(c, s)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
}
