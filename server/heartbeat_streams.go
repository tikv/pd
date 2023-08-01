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
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	log "github.com/pingcap/log"
	"github.com/tikv/pd/pkg/logutil"
	"github.com/tikv/pd/server/core"
	"go.uber.org/zap"
)

const heartbeatStreamKeepAliveInterval = time.Minute

type heartbeatStream interface {
	Send(*pdpb.RegionHeartbeatResponse) error
}

type streamUpdate struct {
	storeID uint64
	stream  heartbeatStream
}

type heartbeatStreams struct {
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	clusterID uint64
	streams   map[uint64]heartbeatStream
	msgCh     chan *pdpb.RegionHeartbeatResponse
	streamCh  chan streamUpdate
	cluster   *RaftCluster
}

func newHeartbeatStreams(clusterID uint64, cluster *RaftCluster) *heartbeatStreams {
	ctx, cancel := context.WithCancel(context.Background())
	hs := &heartbeatStreams{
		ctx:       ctx,
		cancel:    cancel,
		clusterID: clusterID,
		streams:   make(map[uint64]heartbeatStream),
		msgCh:     make(chan *pdpb.RegionHeartbeatResponse, regionheartbeatSendChanCap),
		streamCh:  make(chan streamUpdate, 1),
		cluster:   cluster,
	}
	hs.wg.Add(1)
	go hs.run()
	return hs
}

func (s *heartbeatStreams) run() {
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
			store, err := s.cluster.GetStore(storeID)
			if err != nil {
				log.Error("fail to get store",
					zap.Uint64("region-id", msg.RegionId),
					zap.Uint64("store-id", storeID),
					zap.Error(err))
				delete(s.streams, storeID)
				continue
			}
			storeAddress := store.GetAddress()
			if stream, ok := s.streams[storeID]; ok {
				if err := stream.Send(msg); err != nil {
					log.Error("send heartbeat message fail",
						zap.Uint64("region-id", msg.RegionId), zap.Error(err))
					delete(s.streams, storeID)
					regionHeartbeatCounter.WithLabelValues(storeAddress, storeLabel, "push", "err").Inc()
				} else {
					regionHeartbeatCounter.WithLabelValues(storeAddress, storeLabel, "push", "ok").Inc()
				}
			} else {
				log.Debug("heartbeat stream not found, skip send message",
					zap.Uint64("region-id", msg.RegionId),
					zap.Uint64("store-id", storeID))
				regionHeartbeatCounter.WithLabelValues(storeAddress, storeLabel, "push", "skip").Inc()
			}
		case <-keepAliveTicker.C:
			for storeID, stream := range s.streams {
				store, err := s.cluster.GetStore(storeID)
				if err != nil {
					log.Error("fail to get store", zap.Uint64("store-id", storeID), zap.Error(err))
					delete(s.streams, storeID)
					continue
				}
				storeAddress := store.GetAddress()
				storeLabel := strconv.FormatUint(storeID, 10)
				log.Info("[heartbeatStreams] send keepalive message", zap.Uint64("target-store-id", storeID), zap.String("target-store-address", storeAddress))
				if err := stream.Send(keepAlive); err != nil {
					log.Error("send keepalive message fail",
						zap.Uint64("target-store-id", storeID),
						zap.Error(err))
					delete(s.streams, storeID)
					regionHeartbeatCounter.WithLabelValues(storeAddress, storeLabel, "keepalive", "err").Inc()
				} else {
					regionHeartbeatCounter.WithLabelValues(storeAddress, storeLabel, "keepalive", "ok").Inc()
				}
			}
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *heartbeatStreams) Close() {
	s.cancel()
	s.wg.Wait()
}

func (s *heartbeatStreams) bindStream(storeID uint64, stream heartbeatStream) {
	update := streamUpdate{
		storeID: storeID,
		stream:  stream,
	}
	select {
	case s.streamCh <- update:
	case <-s.ctx.Done():
	}
}

func (s *heartbeatStreams) SendMsg(region *core.RegionInfo, msg *pdpb.RegionHeartbeatResponse) {
	if region.GetLeader() == nil {
		return
	}

	msg.Header = &pdpb.ResponseHeader{ClusterId: s.clusterID}
	msg.RegionId = region.GetID()
	msg.RegionEpoch = region.GetRegionEpoch()
	msg.TargetPeer = region.GetLeader()

	select {
	case s.msgCh <- msg:
	case <-s.ctx.Done():
	}
}

func (s *heartbeatStreams) sendErr(errType pdpb.ErrorType, errMsg string, targetPeer *metapb.Peer, storeAddress, storeLabel string) {
	regionHeartbeatCounter.WithLabelValues(storeAddress, storeLabel, "report", "err").Inc()

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
	case <-s.ctx.Done():
	}
}
