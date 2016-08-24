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

package pd

import (
	"os"
	"strings"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/msgpb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/util"
	"github.com/pingcap/pd/server"
	"github.com/twinj/uuid"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testClientSuite{})

var (
	// Note: IDs below are entirely arbitrary. They are only for checking
	// whether GetRegion/GetStore works.
	// If we alloc ID in client in the future, these IDs must be updated.
	clusterID = uint64(time.Now().Unix())
	store     = &metapb.Store{
		Id:      1,
		Address: "localhost",
	}
	peer = &metapb.Peer{
		Id:      2,
		StoreId: store.GetId(),
	}
	region = &metapb.Region{
		Id: 3,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
		Peers: []*metapb.Peer{peer},
	}
)

var stripUnix = strings.NewReplacer("unix://", "")

func cleanUnixSocket(cfg *server.Config) {
	os.Remove(stripUnix.Replace(cfg.PeerUrls))
	os.Remove(stripUnix.Replace(cfg.ClientUrls))
	os.Remove(stripUnix.Replace(cfg.AdvertisePeerUrls))
	os.Remove(stripUnix.Replace(cfg.AdvertiseClientUrls))
}

type cleanupFunc func()

type testClientSuite struct {
	srv     *server.Server
	client  Client
	cleanup cleanupFunc
}

func (s *testClientSuite) SetUpSuite(c *C) {
	s.srv, s.cleanup = newServer(c, clusterID)

	// wait for srv to become leader
	time.Sleep(time.Second * 3)

	bootstrapServer(c, s.srv.GetAddr())

	var err error
	s.client, err = NewClient(s.srv.GetEndpoints(), clusterID)
	c.Assert(err, IsNil)
}

func (s *testClientSuite) TearDownSuite(c *C) {
	s.client.Close()

	s.cleanup()
}

func newServer(c *C, clusterID uint64) (*server.Server, cleanupFunc) {
	cfg := server.NewTestSingleConfig()
	cfg.ClusterID = clusterID

	s, err := server.NewServer(cfg)
	c.Assert(err, IsNil)

	go s.Run()

	cleanup := func() {
		s.Close()
		os.RemoveAll(cfg.DataDir)

		cleanUnixSocket(cfg)
	}

	return s, cleanup
}

func bootstrapServer(c *C, addr string) {
	req := &pdpb.Request{
		Header: &pdpb.RequestHeader{
			Uuid:      uuid.NewV4().Bytes(),
			ClusterId: clusterID,
		},
		CmdType: pdpb.CommandType_Bootstrap,
		Bootstrap: &pdpb.BootstrapRequest{
			Store:  store,
			Region: region,
		},
	}
	msg := &msgpb.Message{
		MsgType: msgpb.MessageType_PdReq,
		PdReq:   req,
	}

	conn, err := rpcConnect(addr)
	c.Assert(err, IsNil)
	err = util.WriteMessage(conn, 0, msg)
	c.Assert(err, IsNil)

	_, err = util.ReadMessage(conn, msg)
	c.Assert(err, IsNil)
}

func heartbeatRegion(c *C, addr string) {
	req := &pdpb.Request{
		Header: &pdpb.RequestHeader{
			Uuid:      uuid.NewV4().Bytes(),
			ClusterId: clusterID,
		},
		CmdType: pdpb.CommandType_RegionHeartbeat,
		RegionHeartbeat: &pdpb.RegionHeartbeatRequest{
			Region: region,
			Leader: peer,
		},
	}
	msg := &msgpb.Message{
		MsgType: msgpb.MessageType_PdReq,
		PdReq:   req,
	}

	conn, err := rpcConnect(addr)
	c.Assert(err, IsNil)
	err = util.WriteMessage(conn, 0, msg)
	c.Assert(err, IsNil)

	_, err = util.ReadMessage(conn, msg)
	c.Assert(err, IsNil)
}

func (s *testClientSuite) TestTSO(c *C) {
	var tss []int64
	for i := 0; i < 100; i++ {
		p, l, err := s.client.GetTS()
		c.Assert(err, IsNil)
		tss = append(tss, p<<18+l)
	}

	var last int64
	for _, ts := range tss {
		c.Assert(ts, Greater, last)
		last = ts
	}
}

func (s *testClientSuite) TestGetRegion(c *C) {
	heartbeatRegion(c, s.srv.GetAddr())

	r, leader, err := s.client.GetRegion([]byte("a"))
	c.Assert(err, IsNil)
	c.Assert(r, DeepEquals, region)
	c.Assert(leader, DeepEquals, peer)
}

func (s *testClientSuite) TestGetStore(c *C) {
	n, err := s.client.GetStore(store.GetId())
	c.Assert(err, IsNil)
	c.Assert(n, DeepEquals, store)
}
