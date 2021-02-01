// Copyright 2021 TiKV Project Authors.
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
	"fmt"
	"github.com/pingcap/failpoint"

	. "github.com/pingcap/check"
	"github.com/tikv/pd/server"
)

var _ = Suite(&testTsoSuite{})

type testTsoSuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func (s *testTsoSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = mustNewServer(c)
	mustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)
}

func (s *testTsoSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testTsoSuite) TestTransferAllocator(c *C) {
	addr := s.urlPrefix + "/tso/allocator/transfer/pd2?dcLocation=dc-1"
	c.Assert(failpoint.Enable("github.com/tikv/pd/server/api/mockTransferAllocatorResponse", `return("pd2,dc-1")`), IsNil)
	defer failpoint.Disable("github.com/tikv/pd/server/api/mockTransferAllocatorResponse")
	err := postJSON(testDialClient, addr, nil)
	c.Assert(err, IsNil)
}
