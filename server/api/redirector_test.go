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
	"io/ioutil"
	"net/http"

	. "github.com/pingcap/check"
	"github.com/pingcap/pd/server"
)

var _ = Suite(&testRedirectorSuite{})

type testRedirectorSuite struct {
	servers []*server.Server
	cleanup func()
}

func (s *testRedirectorSuite) SetUpSuite(c *C) {
	_, s.servers, s.cleanup = mustNewCluster(c, 3)
}

func (s *testRedirectorSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testRedirectorSuite) TestRedirect(c *C) {
	leader := mustWaitLeader(c, s.servers)
	header := mustRequestSuccess(c, leader)
	header.Del("Date")
	for _, svr := range s.servers {
		if svr != leader {
			h := mustRequestSuccess(c, svr)
			h.Del("Date")
			c.Assert(header, DeepEquals, h)
		}
	}
}

func (s *testRedirectorSuite) TestAllowFollowerHandle(c *C) {
	// Find a follower.
	var follower *server.Server
	leader := mustWaitLeader(c, s.servers)
	for _, svr := range s.servers {
		if svr != leader {
			follower = svr
			break
		}
	}

	addr := follower.GetAddr() + apiPrefix + "/api/v1/version"
	request, err := http.NewRequest("GET", addr, nil)
	c.Assert(err, IsNil)
	request.Header.Add(allowFollowerHandle, "true")
	resp, err := dialClient.Do(request)
	c.Assert(err, IsNil)
	c.Assert(resp.Header.Get(followerHandle), Equals, "true")
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	_, err = ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
}

func (s *testRedirectorSuite) TestNotLeader(c *C) {
	// Find a follower.
	var follower *server.Server
	leader := mustWaitLeader(c, s.servers)
	for _, svr := range s.servers {
		if svr != leader {
			follower = svr
			break
		}
	}

	addr := follower.GetAddr() + apiPrefix + "/api/v1/version"
	// Request to follower without redirectorHeader is OK.
	request, err := http.NewRequest("GET", addr, nil)
	c.Assert(err, IsNil)
	resp, err := dialClient.Do(request)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	_, err = ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)

	// Request to follower with redirectorHeader will fail.
	request.RequestURI = ""
	request.Header.Set(redirectorHeader, "pd")
	resp1, err := dialClient.Do(request)
	c.Assert(err, IsNil)
	defer resp1.Body.Close()
	c.Assert(resp1.StatusCode, Not(Equals), http.StatusOK)
	_, err = ioutil.ReadAll(resp1.Body)
	c.Assert(err, IsNil)
}

func mustRequestSuccess(c *C, s *server.Server) http.Header {
	resp, err := dialClient.Get(s.GetAddr() + apiPrefix + "/api/v1/version")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	_, err = ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	return resp.Header
}
