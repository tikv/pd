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
	"sync"

	"github.com/coreos/etcd/clientv3"
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

var _ = Suite(&testAllocIDSuite{})

type testAllocIDSuite struct {
	client  *clientv3.Client
	alloc   *idAllocator
	svr     *Server
	cleanup cleanUpFunc
}

func (s *testAllocIDSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = newTestServer(c)
	s.client = s.svr.client
	s.alloc = s.svr.idAlloc

	go s.svr.Run()
}

func (s *testAllocIDSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testAllocIDSuite) TestID(c *C) {
	mustGetLeader(c, s.svr)

	var last uint64
	for i := uint64(0); i < allocStep; i++ {
		id, err := s.alloc.Alloc()
		c.Assert(err, IsNil)
		c.Assert(id, Greater, last)
		last = id
	}

	var wg sync.WaitGroup

	var m sync.Mutex
	ids := make(map[uint64]struct{})

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for i := 0; i < 200; i++ {
				id, err := s.alloc.Alloc()
				c.Assert(err, IsNil)
				m.Lock()
				_, ok := ids[id]
				ids[id] = struct{}{}
				m.Unlock()
				c.Assert(ok, IsFalse)
			}
		}()
	}

	wg.Wait()
}

func (s *testAllocIDSuite) TestCommand(c *C) {
	leader := mustGetLeader(c, s.svr)

	conn, err := rpcConnect(leader.GetAddr())
	c.Assert(err, IsNil)
	defer conn.Close()

	idReq := &pdpb.AllocIdRequest{}

	req := &pdpb.Request{
		CmdType: pdpb.CommandType_AllocId,
		AllocId: idReq,
	}

	var last uint64
	for i := uint64(0); i < 2*allocStep; i++ {
		rawMsgID := uint64(rand.Int63())
		sendRequest(c, conn, rawMsgID, req)
		msgID, resp := recvResponse(c, conn)
		c.Assert(rawMsgID, Equals, msgID)
		c.Assert(resp.AllocId, NotNil)
		c.Assert(resp.AllocId.GetId(), Greater, last)
		last = resp.AllocId.GetId()
	}
}
