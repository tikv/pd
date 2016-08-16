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

	. "github.com/pingcap/check"
	"github.com/pingcap/pd/server"
)

var _ = Suite(&testRedirectorSuite{})

type testRedirectorSuite struct {
	svrs    []*server.Server
	cleanup cleanUpFunc
}

func (s *testRedirectorSuite) SetUpSuite(c *C) {
	_, s.svrs, s.cleanup = mustNewCluster(c, 3)
}

func (s *testRedirectorSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testRedirectorSuite) TestRedirect(c *C) {
	for _, svr := range s.svrs {
		mustRequestSuccess(c, svr)
	}
}

func mustRequestSuccess(c *C, s *server.Server) {
	client := newUnixSocketClient()
	unixAddr := []string{s.GetAddr(), apiPrefix, "/api/v1/version"}
	httpAddr := mustUnixAddrToHTTPAddr(c, strings.Join(unixAddr, ""))
	resp, err := client.Get(httpAddr)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
}
