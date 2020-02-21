// Copyright 2017 PingCAP, Inc.
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
	"github.com/pingcap/log"
	"github.com/pingcap/pd/v4/pkg/logutil"
	"github.com/pingcap/pd/v4/server/cluster"
	"github.com/pingcap/pd/v4/server/core"
	"github.com/pingcap/pd/v4/server/schedule/opt"
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

type heartbeatStreams struct {
	wg             sync.WaitGroup
	hbStreamCtx    context.Context
	hbStreamCancel context.CancelFunc
	clusterID      uint64
	streams        map[uint64]opt.HeartbeatStream
	msgCh          chan *pdpb.RegionHeartbeatResponse
	streamCh       chan streamUpdate
	cluster        *cluster.RaftCluster
}

func newHeartbeatStreams(ctx context.Context, clusterID uint64, cluster *cluster.RaftCluster) *heartbeatStreams {
	hbStreamCtx, hbStreamCancel := context.WithCancel(ctx)
	hs := &heartbeatStreams{
		hbStreamCtx:    hbStreamCtx,
		hbStreamCancel: hbStreamCancel,
		clusterID:      clusterID,
		streams:        make(map[uint64]opt.HeartbeatStream),
		msgCh:          make(chan *pdpb.RegionHeartbeatResponse, heartbeatChanCapacity),
		streamCh:       make(chan streamUpdate, 1),
		cluster:        cluster,
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
			store := s.cluster.GetStore(storeID)
			if store == nil {
				log.Error("failed to get store",
					zap.Uint64("region-id", msg.RegionId),
					zap.Uint64("store-id", storeID))
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
				store := s.cluster.GetStore(storeID)
				if store == nil {
					log.Error("failed to get store", zap.Uint64("store-id", storeID))
					delete(s.streams, storeID)
					continue
				}
				storeAddress := store.GetAddress()
				storeLabel := strconv.FormatUint(storeID, 10)
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
		case <-s.hbStreamCtx.Done():
			return
		}
	}
}

func (s *heartbeatStreams) Close() {
	s.hbStreamCancel()
	s.wg.Wait()
}

func (s *heartbeatStreams) BindStream(storeID uint64, stream opt.HeartbeatStream) {
	update := streamUpdate{
		storeID: storeID,
		stream:  stream,
	}
	select {
	case s.streamCh <- update:
	case <-s.hbStreamCtx.Done():
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
	case <-s.hbStreamCtx.Done():
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
	case <-s.hbStreamCtx.Done():
	}
}
