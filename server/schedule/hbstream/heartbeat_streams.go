// Copyright 2017 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hbstream

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/logutil"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/opt"
	"go.uber.org/zap"
)

const (
	heartbeatStreamKeepAliveInterval = time.Minute
	heartbeatChanCapacity            = 1024
)

type streamUpdate struct {
	storeID uint64
	stream  opt.HeartbeatStream
}

// HeartbeatStreams is the bridge of communication with TIKV instance.
type HeartbeatStreams struct {
	wg             sync.WaitGroup
	hbStreamCtx    context.Context
	hbStreamCancel context.CancelFunc
	clusterID      uint64
	streams        map[uint64]opt.HeartbeatStream
	msgCh          chan *pdpb.RegionHeartbeatResponse
	streamCh       chan streamUpdate
	storeInformer  core.StoreSetInformer
	needRun        bool // For test only.
}

// NewHeartbeatStreams creates a new HeartbeatStreams which enable background running by default.
func NewHeartbeatStreams(ctx context.Context, clusterID uint64, storeInformer core.StoreSetInformer) *HeartbeatStreams {
	return newHbStreams(ctx, clusterID, storeInformer, true)
}

// NewTestHeartbeatStreams creates a new HeartbeatStreams for test purpose only.
// Please use NewHeartbeatStreams for other usage.
func NewTestHeartbeatStreams(ctx context.Context, clusterID uint64, storeInformer core.StoreSetInformer, needRun bool) *HeartbeatStreams {
	return newHbStreams(ctx, clusterID, storeInformer, needRun)
}

func newHbStreams(ctx context.Context, clusterID uint64, storeInformer core.StoreSetInformer, needRun bool) *HeartbeatStreams {
	hbStreamCtx, hbStreamCancel := context.WithCancel(ctx)
	hs := &HeartbeatStreams{
		hbStreamCtx:    hbStreamCtx,
		hbStreamCancel: hbStreamCancel,
		clusterID:      clusterID,
		streams:        make(map[uint64]opt.HeartbeatStream),
		msgCh:          make(chan *pdpb.RegionHeartbeatResponse, heartbeatChanCapacity),
		streamCh:       make(chan streamUpdate, 1),
		storeInformer:  storeInformer,
		needRun:        needRun,
	}
	if needRun {
		hs.wg.Add(1)
		go hs.run()
	}
	return hs
}

func (s *HeartbeatStreams) run() {
	defer logutil.LogPanic()

	defer s.wg.Done()

	keepAliveTicker := time.NewTicker(heartbeatStreamKeepAliveInterval)
	defer keepAliveTicker.Stop()

	keepAlive := &pdpb.RegionHeartbeatResponse{Header: &pdpb.ResponseHeader{ClusterId: s.clusterID}}

	for {
		select {
		case update := <-s.streamCh:
			s.streams[update.storeID] = update.stream
		case msg := <-s.msgCh:
			storeID := msg.GetTargetPeer().GetStoreId()
			storeLabel := strconv.FormatUint(storeID, 10)
			store := s.storeInformer.GetStore(storeID)
			if store == nil {
				log.Error("failed to get store",
					zap.Uint64("region-id", msg.RegionId),
					zap.Uint64("store-id", storeID), errs.ZapError(errs.ErrGetSourceStore))
				delete(s.streams, storeID)
				continue
			}
			storeAddress := store.GetAddress()
			if stream, ok := s.streams[storeID]; ok {
				if err := stream.Send(msg); err != nil {
					log.Error("send heartbeat message fail",
						zap.Uint64("region-id", msg.RegionId), errs.ZapError(errs.ErrGRPCSend.Wrap(err).GenWithStackByArgs()))
					delete(s.streams, storeID)
					heartbeatStreamCounter.WithLabelValues(storeAddress, storeLabel, "push", "err").Inc()
				} else {
					heartbeatStreamCounter.WithLabelValues(storeAddress, storeLabel, "push", "ok").Inc()
				}
			} else {
				log.Debug("heartbeat stream not found, skip send message",
					zap.Uint64("region-id", msg.RegionId),
					zap.Uint64("store-id", storeID))
				heartbeatStreamCounter.WithLabelValues(storeAddress, storeLabel, "push", "skip").Inc()
			}
		case <-keepAliveTicker.C:
			for storeID, stream := range s.streams {
				store := s.storeInformer.GetStore(storeID)
				if store == nil {
					log.Error("failed to get store", zap.Uint64("store-id", storeID), errs.ZapError(errs.ErrGetSourceStore))
					delete(s.streams, storeID)
					continue
				}
				storeAddress := store.GetAddress()
				storeLabel := strconv.FormatUint(storeID, 10)
				if err := stream.Send(keepAlive); err != nil {
					log.Warn("send keepalive message fail, store maybe disconnected",
						zap.Uint64("target-store-id", storeID),
						errs.ZapError(err))
					delete(s.streams, storeID)
					heartbeatStreamCounter.WithLabelValues(storeAddress, storeLabel, "keepalive", "err").Inc()
				} else {
					heartbeatStreamCounter.WithLabelValues(storeAddress, storeLabel, "keepalive", "ok").Inc()
				}
			}
		case <-s.hbStreamCtx.Done():
			return
		}
	}
}

// Close closes background running.
func (s *HeartbeatStreams) Close() {
	s.hbStreamCancel()
	s.wg.Wait()
}

// BindStream binds a stream with a specified store.
func (s *HeartbeatStreams) BindStream(storeID uint64, stream opt.HeartbeatStream) {
	update := streamUpdate{
		storeID: storeID,
		stream:  stream,
	}
	select {
	case s.streamCh <- update:
	case <-s.hbStreamCtx.Done():
	}
}

// SendMsg sends a message to related store.
func (s *HeartbeatStreams) SendMsg(region *core.RegionInfo, msg *pdpb.RegionHeartbeatResponse) {
	if region.GetLeader() == nil {
		return
	}

	msg.Header = &pdpb.ResponseHeader{ClusterId: s.clusterID}
	msg.RegionId = region.GetID()
	msg.RegionEpoch = region.GetRegionEpoch()
	msg.TargetPeer = region.GetLeader()

	select {
	case s.msgCh <- msg:
	case <-s.hbStreamCtx.Done():
	}
}

// SendErr sends a error message to related store.
func (s *HeartbeatStreams) SendErr(errType pdpb.ErrorType, errMsg string, targetPeer *metapb.Peer) {
	msg := &pdpb.RegionHeartbeatResponse{
		Header: &pdpb.ResponseHeader{
			ClusterId: s.clusterID,
			Error: &pdpb.Error{
				Type:    errType,
				Message: errMsg,
			},
		},
		TargetPeer: targetPeer,
	}

	select {
	case s.msgCh <- msg:
	case <-s.hbStreamCtx.Done():
	}
}

// MsgLength gets the length of msgCh.
// For test only.
func (s *HeartbeatStreams) MsgLength() int {
	return len(s.msgCh)
}

// Drain consumes message from msgCh when disable background running.
// For test only.
func (s *HeartbeatStreams) Drain(count int) error {
	if s.needRun {
		return errors.Normalize("hbstream running enabled")
	}
	for i := 0; i < count; i++ {
		<-s.msgCh
	}
	return nil
}
