// Copyright 2019 PingCAP, Inc.
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

package mockhbstream

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/v4/server/core"
	"github.com/pingcap/pd/v4/server/schedule/opt"
)

// HeartbeatStream is used to mock HeartbeatStream for test use.
type HeartbeatStream struct {
	ch chan *pdpb.RegionHeartbeatResponse
}

// NewHeartbeatStream creates a new HeartbeatStream.
func NewHeartbeatStream() HeartbeatStream {
	return HeartbeatStream{
		ch: make(chan *pdpb.RegionHeartbeatResponse),
	}
}

// Send mocks method.
func (s HeartbeatStream) Send(m *pdpb.RegionHeartbeatResponse) error {
	select {
	case <-time.After(time.Second):
		return errors.New("timeout")
	case s.ch <- m:
	}
	return nil
}

// SendMsg is used to send the message.
func (s HeartbeatStream) SendMsg(region *core.RegionInfo, msg *pdpb.RegionHeartbeatResponse) {}

// BindStream mock method.
func (s HeartbeatStream) BindStream(storeID uint64, stream opt.HeartbeatStream) {}

// Recv mocks method.
func (s HeartbeatStream) Recv() *pdpb.RegionHeartbeatResponse {
	select {
	case <-time.After(time.Millisecond * 10):
		return nil
	case res := <-s.ch:
		return res
	}
}

type streamUpdate struct {
	storeID uint64
	stream  opt.HeartbeatStream
}

// HeartbeatStreams is used to mock heartbeatstreams for test use.
type HeartbeatStreams struct {
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	clusterID uint64
	streams   map[uint64]opt.HeartbeatStream
	streamCh  chan streamUpdate
	msgCh     chan *pdpb.RegionHeartbeatResponse
}

// NewHeartbeatStreams creates a new HeartbeatStreams.
func NewHeartbeatStreams(clusterID uint64, noNeedRun bool) *HeartbeatStreams {
	ctx, cancel := context.WithCancel(context.Background())
	hs := &HeartbeatStreams{
		ctx:       ctx,
		cancel:    cancel,
		clusterID: clusterID,
		streams:   make(map[uint64]opt.HeartbeatStream),
		streamCh:  make(chan streamUpdate, 1),
		msgCh:     make(chan *pdpb.RegionHeartbeatResponse, 1024),
	}
	if noNeedRun {
		return hs
	}
	hs.wg.Add(1)
	go hs.run()
	return hs
}

func (mhs *HeartbeatStreams) run() {
	defer mhs.wg.Done()
	for {
		select {
		case update := <-mhs.streamCh:
			mhs.streams[update.storeID] = update.stream
		case msg := <-mhs.msgCh:
			storeID := msg.GetTargetPeer().GetStoreId()
			if stream, ok := mhs.streams[storeID]; ok {
				stream.Send(msg)
			}
		case <-mhs.ctx.Done():
			return
		}
	}
}

// Close mock method.
func (mhs *HeartbeatStreams) Close() {
	mhs.cancel()
	mhs.wg.Wait()
}

// SendMsg is used to send the message.
func (mhs *HeartbeatStreams) SendMsg(region *core.RegionInfo, msg *pdpb.RegionHeartbeatResponse) {
	if region.GetLeader() == nil {
		return
	}

	msg.Header = &pdpb.ResponseHeader{ClusterId: mhs.clusterID}
	msg.RegionId = region.GetID()
	msg.RegionEpoch = region.GetRegionEpoch()
	msg.TargetPeer = region.GetLeader()

	select {
	case mhs.msgCh <- msg:
	case <-mhs.ctx.Done():
	}
}

// MsgCh returns the internal channel which contains the heartbeat responses
// from PD. It can be used to inspect the content of a PD response
func (mhs *HeartbeatStreams) MsgCh() chan *pdpb.RegionHeartbeatResponse {
	return mhs.msgCh
}

// BindStream mock method.
func (mhs *HeartbeatStreams) BindStream(storeID uint64, stream opt.HeartbeatStream) {
	update := streamUpdate{
		storeID: storeID,
		stream:  stream,
	}
	select {
	case mhs.streamCh <- update:
	case <-mhs.ctx.Done():
	}
}
