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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/server"
)

var _ = Suite(&testBalancerSuite{})

var (
	clusterID = uint64(time.Now().Unix())
	store     = &metapb.Store{
		Id:      1,
		Address: "localhost",
	}
	peers = []*metapb.Peer{
		{
			Id:      2,
			StoreId: store.GetId(),
		},
	}
	region = &metapb.Region{
		Id: 8,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
		Peers: peers,
	}
)

func mustRPCRequest(c *C, addr string, request *pdpb.Request) *pdpb.Response {
	resp, err := server.RPCRequest(addr, 0, request)
	c.Assert(err, IsNil)
	return resp
}

func newRequestHeader(clusterID uint64) *pdpb.RequestHeader {
	return &pdpb.RequestHeader{
		ClusterId: clusterID,
	}
}

func mustBootstrapCluster(c *C, s *server.Server) {
	req := &pdpb.Request{
		Header:  newRequestHeader(s.ClusterID()),
		CmdType: pdpb.CommandType_Bootstrap,
		Bootstrap: &pdpb.BootstrapRequest{
			Store:  store,
			Region: region,
		},
	}
	mustRPCRequest(c, s.GetAddr(), req)
}

func mustPutStore(c *C, s *server.Server, storeID uint64) {
	req := &pdpb.Request{
		Header:  newRequestHeader(s.ClusterID()),
		CmdType: pdpb.CommandType_PutStore,
		PutStore: &pdpb.PutStoreRequest{
			Store: &metapb.Store{
				Id:      storeID,
				Address: fmt.Sprintf("localhost:%v", storeID),
			},
		},
	}
	mustRPCRequest(c, s.GetAddr(), req)
}

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

	mustBootstrapCluster(c, s.svr)
	mustPutStore(c, s.svr, s.testStoreID)
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
