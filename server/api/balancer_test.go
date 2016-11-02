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
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
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

func mustBootstrapCluster(c *C, addr string) {
	req := &pdpb.Request{
		CmdType: pdpb.CommandType_Bootstrap,
		Bootstrap: &pdpb.BootstrapRequest{
			Store:  store,
			Region: region,
		},
	}
	mustRPCRequest(c, addr, req)
}

func mustPutStore(c *C, addr string, storeID uint64) {
	req := &pdpb.Request{
		CmdType: pdpb.CommandType_PutStore,
		PutStore: &pdpb.PutStoreRequest{
			Store: &metapb.Store{
				Id:      storeID,
				Address: fmt.Sprintf("localhost:%v", storeID),
			},
		},
	}
	mustRPCRequest(c, addr, req)
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

	mustBootstrapCluster(c, addr)
	mustPutStore(c, addr, s.testStoreID)
}

func (s *testBalancerSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testBalancerSuite) testGet(c *C) *balancersInfo {
	client := newUnixSocketClient()
	resp, err := client.Get(s.url)
	c.Assert(err, IsNil)
	info := new(balancersInfo)
	err = readJSON(resp.Body, info)
	c.Assert(err, IsNil)
	return info
}

func (s *testBalancerSuite) testPost(c *C, data string) {
	client := newUnixSocketClient()
	resp, err := client.Post(s.url, "application/json", strings.NewReader(data))
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
}

type testChangePeerOperator struct {
	ChangePeer *pdpb.ChangePeer `json:"operator"`
	RegionID   uint64           `json:"regionid"`
	Name       string           `json:"name"`
}

// Since those operators are not exported, we need to do some tricks to verify them.
func mustParseOperator(c *C, bop server.Operator, ops []*testChangePeerOperator) {
	data, err := json.Marshal(bop)
	c.Assert(err, IsNil)
	tmp := make(map[string]json.RawMessage)
	err = json.Unmarshal(data, &tmp)
	c.Assert(err, IsNil)
	err = json.Unmarshal(tmp["operators"], &ops)
	c.Assert(err, IsNil)
}

func (s *testBalancerSuite) mustGetOperators(c *C) map[uint64]server.Operator {
	cluster := s.svr.GetRaftCluster()
	c.Assert(cluster, NotNil)
	return cluster.GetBalanceOperators()
}

func (s *testBalancerSuite) TestAddPeer(c *C) {
	data := fmt.Sprintf(`[{"name": "add_peer", "region_id": %v, "store_id": %v}]`,
		region.GetId(), s.testStoreID)
	s.testPost(c, data)

	bops := s.mustGetOperators(c)
	c.Assert(bops, HasLen, 1)
	op := new(testChangePeerOperator)
	mustParseOperator(c, bops[region.GetId()], []*testChangePeerOperator{op})

	c.Assert(op.Name, Equals, "add_peer")
	c.Assert(op.RegionID, Equals, region.GetId())
	c.Assert(op.ChangePeer.GetPeer().GetStoreId(), Equals, s.testStoreID)
}

func (s *testBalancerSuite) TestRemovePeer(c *C) {
	peer := region.GetPeers()[0]
	data := fmt.Sprintf(`[{"name": "remove_peer", "region_id": %v, "peer_id": %v}]`,
		region.GetId(), peer.GetId())
	s.testPost(c, data)

	bops := s.mustGetOperators(c)
	c.Assert(bops, HasLen, 1)
	op := new(testChangePeerOperator)
	mustParseOperator(c, bops[region.GetId()], []*testChangePeerOperator{op})

	c.Assert(op.Name, Equals, "remove_peer")
	c.Assert(op.RegionID, Equals, region.GetId())
	c.Assert(op.ChangePeer.GetPeer().GetStoreId(), Equals, peer.GetStoreId())
}

func (s *testBalancerSuite) TestAddAndRemovePeer(c *C) {
	peer := region.GetPeers()[0]
	data := fmt.Sprintf(`[
      {"name": "add_peer", "region_id": %v, "store_id": %v},
	  {"name": "remove_peer", "region_id": %v, "peer_id": %v}
    ]`, region.GetId(), s.testStoreID,
		region.GetId(), peer.GetId())
	s.testPost(c, data)

	bops := s.mustGetOperators(c)
	c.Assert(bops, HasLen, 1)
	addPeer := new(testChangePeerOperator)
	removePeer := new(testChangePeerOperator)
	mustParseOperator(c, bops[region.GetId()], []*testChangePeerOperator{addPeer, removePeer})

	c.Assert(addPeer.Name, Equals, "add_peer")
	c.Assert(addPeer.RegionID, Equals, region.GetId())
	c.Assert(addPeer.ChangePeer.GetPeer().GetStoreId(), Equals, s.testStoreID)

	c.Assert(removePeer.Name, Equals, "remove_peer")
	c.Assert(removePeer.RegionID, Equals, region.GetId())
	c.Assert(removePeer.ChangePeer.GetPeer().GetStoreId(), Equals, peer.GetStoreId())
}
