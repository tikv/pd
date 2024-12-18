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

	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/schedulingpb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/logutil"
)

// Operation is detailed scheduling step of a region.
type Operation struct {
	ChangePeer *pdpb.ChangePeer
	// Pd can return transfer_leader to let TiKV does leader transfer itself.
	TransferLeader *pdpb.TransferLeader
	Merge          *pdpb.Merge
	// PD sends split_region to let TiKV split a region into two regions.
	SplitRegion     *pdpb.SplitRegion
	ChangePeerV2    *pdpb.ChangePeerV2
	SwitchWitnesses *pdpb.BatchSwitchWitness
}

// HeartbeatStream is an interface.
type HeartbeatStream interface {
	Send(core.RegionHeartbeatResponse) error
}

const (
	heartbeatStreamKeepAliveInterval = time.Minute
	heartbeatChanCapacity            = 1024
)

type streamUpdate struct {
	storeID uint64
	stream  HeartbeatStream
}

// HeartbeatStreams is the bridge of communication with TIKV instance.
type HeartbeatStreams struct {
	wg             sync.WaitGroup
	hbStreamCtx    context.Context
	hbStreamCancel context.CancelFunc
	clusterID      uint64
	streams        map[uint64]HeartbeatStream
	msgCh          chan core.RegionHeartbeatResponse
	streamCh       chan streamUpdate
	storeInformer  core.StoreSetInformer
	typ            string
	needRun        bool // For test only.
}

// NewHeartbeatStreams creates a new HeartbeatStreams which enable background running by default.
func NewHeartbeatStreams(ctx context.Context, typ string, storeInformer core.StoreSetInformer) *HeartbeatStreams {
	return newHbStreams(ctx, typ, storeInformer, true)
}

// NewTestHeartbeatStreams creates a new HeartbeatStreams for test purpose only.
// Please use NewHeartbeatStreams for other usage.
func NewTestHeartbeatStreams(ctx context.Context, storeInformer core.StoreSetInformer, needRun bool) *HeartbeatStreams {
	return newHbStreams(ctx, "", storeInformer, needRun)
}

func newHbStreams(ctx context.Context, typ string, storeInformer core.StoreSetInformer, needRun bool) *HeartbeatStreams {
	hbStreamCtx, hbStreamCancel := context.WithCancel(ctx)
	hs := &HeartbeatStreams{
		hbStreamCtx:    hbStreamCtx,
		hbStreamCancel: hbStreamCancel,
		clusterID:      keypath.ClusterID(),
		streams:        make(map[uint64]HeartbeatStream),
		msgCh:          make(chan core.RegionHeartbeatResponse, heartbeatChanCapacity),
		streamCh:       make(chan streamUpdate, 1),
		storeInformer:  storeInformer,
		typ:            typ,
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

	var keepAlive core.RegionHeartbeatResponse
	switch s.typ {
	case constant.SchedulingServiceName:
		keepAlive = &schedulingpb.RegionHeartbeatResponse{Header: &schedulingpb.ResponseHeader{ClusterId: s.clusterID}}
	default:
		keepAlive = &pdpb.RegionHeartbeatResponse{Header: &pdpb.ResponseHeader{ClusterId: s.clusterID}}
	}

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
					zap.Uint64("region-id", msg.GetRegionId()),
					zap.Uint64("store-id", storeID), errs.ZapError(errs.ErrGetSourceStore))
				delete(s.streams, storeID)
				continue
			}
			storeAddress := store.GetAddress()
			if stream, ok := s.streams[storeID]; ok {
				if err := stream.Send(msg); err != nil {
					log.Error("send heartbeat message fail",
						zap.Uint64("region-id", msg.GetRegionId()), errs.ZapError(errs.ErrGRPCSend, err))
					delete(s.streams, storeID)
					heartbeatStreamCounter.WithLabelValues(storeAddress, storeLabel, "push", "err").Inc()
				} else {
					heartbeatStreamCounter.WithLabelValues(storeAddress, storeLabel, "push", "ok").Inc()
				}
			} else {
				log.Debug("heartbeat stream not found, skip send message",
					zap.Uint64("region-id", msg.GetRegionId()),
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
func (s *HeartbeatStreams) BindStream(storeID uint64, stream HeartbeatStream) {
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
func (s *HeartbeatStreams) SendMsg(region *core.RegionInfo, op *Operation) {
	if region.GetLeader() == nil {
		return
	}

	// TODO: use generic
	var resp core.RegionHeartbeatResponse
	switch s.typ {
	case constant.SchedulingServiceName:
		resp = &schedulingpb.RegionHeartbeatResponse{
			Header:          &schedulingpb.ResponseHeader{ClusterId: s.clusterID},
			RegionId:        region.GetID(),
			RegionEpoch:     region.GetRegionEpoch(),
			TargetPeer:      region.GetLeader(),
			ChangePeer:      op.ChangePeer,
			TransferLeader:  op.TransferLeader,
			Merge:           op.Merge,
			SplitRegion:     op.SplitRegion,
			ChangePeerV2:    op.ChangePeerV2,
			SwitchWitnesses: op.SwitchWitnesses,
		}
	default:
		resp = &pdpb.RegionHeartbeatResponse{
			Header:          &pdpb.ResponseHeader{ClusterId: s.clusterID},
			RegionId:        region.GetID(),
			RegionEpoch:     region.GetRegionEpoch(),
			TargetPeer:      region.GetLeader(),
			ChangePeer:      op.ChangePeer,
			TransferLeader:  op.TransferLeader,
			Merge:           op.Merge,
			SplitRegion:     op.SplitRegion,
			ChangePeerV2:    op.ChangePeerV2,
			SwitchWitnesses: op.SwitchWitnesses,
		}
	}

	select {
	case s.msgCh <- resp:
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
	for range count {
		<-s.msgCh
	}
	return nil
}
