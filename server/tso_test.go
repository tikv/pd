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
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/msgpb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/util"
)

var _ = Suite(&testTsoSuite{})

type testTsoSuite struct {
	client  *clientv3.Client
	svr     *Server
	cleanup cleanUpFunc
}

func (s *testTsoSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = newTestServer(c)
	s.client = s.svr.client

	go s.svr.Run()
}

func (s *testTsoSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func sendRequest(c *C, conn net.Conn, msgID uint64, request *pdpb.Request) {
	msg := &msgpb.Message{
		MsgType: msgpb.MessageType_PdReq,
		PdReq:   request,
	}
	err := util.WriteMessage(conn, msgID, msg)
	c.Assert(err, IsNil)
}

func recvResponse(c *C, conn net.Conn) (uint64, *pdpb.Response) {
	msg := &msgpb.Message{}
	msgID, err := util.ReadMessage(conn, msg)
	c.Assert(err, IsNil)
	c.Assert(msg.GetMsgType(), Equals, msgpb.MessageType_PdResp)
	resp := msg.GetPdResp()
	return msgID, resp
}

func (s *testTsoSuite) testGetTimestamp(c *C, conn net.Conn, n int) {
	tso := &pdpb.TsoRequest{
		Count: uint32(n),
	}

	req := &pdpb.Request{
		CmdType: pdpb.CommandType_Tso,
		Tso:     tso,
	}

	rawMsgID := uint64(rand.Int63())
	sendRequest(c, conn, rawMsgID, req)
	msgID, resp := recvResponse(c, conn)
	c.Assert(rawMsgID, Equals, msgID)
	c.Assert(resp.Tso, NotNil)
	c.Assert(resp.Tso.GetCount(), Equals, uint32(n))

	res := resp.Tso.Timestamp
	c.Assert(res.GetLogical(), Greater, int64(0))
}

func mustGetLeader(c *C, client *clientv3.Client, leaderPath string) *pdpb.Leader {
	for i := 0; i < 20; i++ {
		leader, err := getLeader(client, leaderPath)
		c.Assert(err, IsNil)
		if leader != nil {
			return leader
		}
		time.Sleep(500 * time.Millisecond)
	}

	c.Fatal("get leader error")
	return nil
}

func (s *testTsoSuite) TestTso(c *C) {
	leader := mustGetLeader(c, s.client, s.svr.getLeaderPath())

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			conn, err := rpcConnect(leader.GetAddr())
			c.Assert(err, IsNil)
			defer conn.Close()

			s.testGetTimestamp(c, conn, 10)
		}()
	}

	wg.Wait()
}
