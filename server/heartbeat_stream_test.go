// Copyright 2018 PingCAP, Inc.
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
	"context"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	log "github.com/pingcap/log"
	"github.com/pingcap/pd/pkg/testutil"
	"github.com/pingcap/pd/pkg/typeutil"
	"go.uber.org/zap"
)

var _ = Suite(&testHeartbeatStreamSuite{})

type testHeartbeatStreamSuite struct {
	baseCluster
	region *metapb.Region
}

func (s *testHeartbeatStreamSuite) TestActivity(c *C) {
	var err error
	var cleanup func()
	_, s.svr, cleanup, err = NewTestServer(c)
	c.Assert(err, IsNil)
	s.svr.cfg.heartbeatStreamBindInterval = typeutil.NewDuration(time.Second)
	mustWaitLeader(c, []*Server{s.svr})
	s.grpcPDClient = mustNewGrpcClient(c, s.svr.GetAddr())
	defer cleanup()

	bootstrapReq := s.newBootstrapRequest(c, s.svr.clusterID, "127.0.0.1:0")
	_, err = s.svr.bootstrapCluster(bootstrapReq)
	c.Assert(err, IsNil)
	s.region = bootstrapReq.Region

	// Add a new store and an addPeer operator.
	storeID, err := s.svr.idAlloc.Alloc()
	c.Assert(err, IsNil)
	_, err = putStore(c, s.grpcPDClient, s.svr.clusterID, &metapb.Store{Id: storeID, Address: "127.0.0.1:1"})
	c.Assert(err, IsNil)
	err = newHandler(s.svr).AddAddPeerOperator(s.region.GetId(), storeID)
	c.Assert(err, IsNil)

	stream1, stream2 := newRegionheartbeatClient(c, s.grpcPDClient), newRegionheartbeatClient(c, s.grpcPDClient)
	defer stream1.close()
	defer stream2.close()
	checkActiveStream := func() int {
		select {
		case <-stream1.respCh:
			return 1
		case <-stream2.respCh:
			return 2
		case <-time.After(time.Second):
			return 0
		}
	}

	req := &pdpb.RegionHeartbeatRequest{
		Header: newRequestHeader(s.svr.clusterID),
		Leader: s.region.Peers[0],
		Region: s.region,
	}
	// Active stream is stream1.
	c.Assert(stream1.stream.Send(req), IsNil)
	c.Assert(checkActiveStream(), Equals, 1)
	// Rebind to stream2.
	c.Assert(stream2.stream.Send(req), IsNil)
	c.Assert(checkActiveStream(), Equals, 2)
	// Rebind to stream1 if no more heartbeats sent through stream2.
	testutil.WaitUntil(c, func(c *C) bool {
		c.Assert(stream1.stream.Send(req), IsNil)
		return checkActiveStream() == 1
	})
}

type regionHeartbeatClient struct {
	stream pdpb.PD_RegionHeartbeatClient
	respCh chan *pdpb.RegionHeartbeatResponse
}

func newRegionheartbeatClient(c *C, grpcClient pdpb.PDClient) *regionHeartbeatClient {
	stream, err := grpcClient.RegionHeartbeat(context.Background())
	c.Assert(err, IsNil)
	ch := make(chan *pdpb.RegionHeartbeatResponse)
	go func() {
		for {
			res, err := stream.Recv()
			if err != nil {
				return
			}
			ch <- res
		}
	}()
	return &regionHeartbeatClient{
		stream: stream,
		respCh: ch,
	}
}

func (c *regionHeartbeatClient) close() {
	if err := c.stream.CloseSend(); err != nil {
		log.Error("failed to terminate client stream", zap.Error(err))
	}
}

func (c *regionHeartbeatClient) SendRecv(msg *pdpb.RegionHeartbeatRequest, timeout time.Duration) *pdpb.RegionHeartbeatResponse {
	if err := c.stream.Send(msg); err != nil {
		log.Error("send heartbeat message fail", zap.Error(err))
	}
	select {
	case <-time.After(timeout):
		return nil
	case res := <-c.respCh:
		return res
	}
}
