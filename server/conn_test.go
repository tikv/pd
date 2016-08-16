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
	"os"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

var _ = Suite(&testConnSuite{})

type testConnSuite struct {
	svrs []*Server
}

func (s *testConnSuite) SetUpSuite(c *C) {
	s.svrs = newMultiTestServers(c, 3)
}

func (s *testConnSuite) TearDownSuite(c *C) {
	for _, svr := range s.svrs {
		svr.Close()
		os.RemoveAll(svr.cfg.DataDir)
	}
}

func (s *testConnSuite) TestLeader(c *C) {
	for _, svr := range s.svrs {
		mustRequestSuccess(c, svr)
	}
}

func mustRequestSuccess(c *C, s *Server) {
	req := &pdpb.Request{
		CmdType: pdpb.CommandType_AllocId.Enum(),
		AllocId: &pdpb.AllocIdRequest{},
	}
	conn, err := rpcConnect(s.GetAddr())
	c.Assert(err, IsNil)
	resp, err := rpcCall(conn, 0, req)
	c.Assert(err, IsNil)
	c.Assert(resp.GetHeader().GetError(), IsNil)
}
