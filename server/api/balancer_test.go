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
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server"
)

var _ = Suite(&testBalancerSuite{})

type testBalancerSuite struct {
	svr         *server.Server
	cleanup     cleanUpFunc
	url         string
	testStoreID uint64
}

func (s *testBalancerSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = mustNewServer(c)
	mustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	httpAddr := mustUnixAddrToHTTPAddr(c, addr)
	s.url = fmt.Sprintf("%s%s/api/v1/balancers", httpAddr, apiPrefix)
	s.testStoreID = uint64(11111)
	store := &metapb.Store{
		Id:      s.testStoreID,
		Address: fmt.Sprintf("localhost:%v", s.testStoreID),
	}
	mustBootstrapCluster(c, s.svr)
	mustPutStore(c, s.svr, store)
}

func (s *testBalancerSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testBalancerSuite) TestGet(c *C) *balancersInfo {
	client := newUnixSocketClient()
	resp, err := client.Get(s.url)
	c.Assert(err, IsNil)
	info := new(balancersInfo)
	err = readJSON(resp.Body, info)
	c.Assert(err, IsNil)
	return info
}
