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
	"net/url"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	msgSize                 = 8 * 1024 * 1024
	maxSyncRegionBatchSize  = 100
	syncerKeepAliveInterval = 10 * time.Second
)

type syncerClient interface {
	Recv() (*pdpb.SyncRegionResponse, error)
	CloseSend() error
}
type syncerServer interface {
	Send(regions *pdpb.SyncRegionResponse) error
}

type regionSyncer struct {
	sync.RWMutex
	streams map[string]syncerServer
	ctx     context.Context
	cancel  context.CancelFunc
	server  *Server
	closed  chan struct{}
	wg      sync.WaitGroup
}

func newRegionSyncer(server *Server) *regionSyncer {
	return &regionSyncer{
		streams: make(map[string]syncerServer),
		server:  server,
		closed:  make(chan struct{}),
	}
}

func (s *regionSyncer) bindStream(name string, stream syncerServer) {
	s.Lock()
	defer s.Unlock()
	s.streams[name] = stream
}

func (s *regionSyncer) broadcast(regions *pdpb.SyncRegionResponse) {
	failed := make([]string, 0, 3)
	s.RLock()
	for name, sender := range s.streams {
		err := sender.Send(regions)
		if err != nil {
			log.Error("region syncer send data meet error:", err)
			failed = append(failed, name)
		}
	}
	s.RUnlock()
	if len(failed) > 0 {
		s.Lock()
		for _, name := range failed {
			delete(s.streams, name)
			log.Infof("region syncer delete the stream of %s", name)
		}
		s.Unlock()
	}
}

func (s *regionSyncer) stopSyncWithLeader() {
	s.reset()
	s.Lock()
	close(s.closed)
	s.closed = make(chan struct{})
	s.Unlock()
	s.wg.Wait()
}

func (s *regionSyncer) reset() {
	s.Lock()
	defer s.Unlock()

	if s.cancel == nil {
		return
	}
	s.cancel()
	s.cancel, s.ctx = nil, nil
}

func (s *regionSyncer) establish(addr string) (syncerClient, error) {
	s.reset()

	u, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}

	cc, err := grpc.Dial(u.Host, grpc.WithInsecure(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(msgSize)))
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(s.server.serverLoopCtx)
	client, err := pdpb.NewPDClient(cc).SyncRegions(ctx)
	if err != nil {
		cancel()
		return nil, err
	}
	err = client.Send(&pdpb.SyncRegionRequest{
		Header: &pdpb.RequestHeader{ClusterId: s.server.clusterID},
		Member: s.server.member,
	})
	if err != nil {
		cancel()
		return nil, err
	}
	s.Lock()
	s.ctx, s.cancel = ctx, cancel
	s.Unlock()
	return client, nil
}

func (s *regionSyncer) startSyncWithLeader(addr string) {
	s.wg.Add(1)
	s.RLock()
	closed := s.closed
	s.RUnlock()
	go func() {
		defer s.wg.Done()
		for {
			select {
			case <-closed:
				return
			default:
			}
			// establish client
			client, err := s.establish(addr)
			if err != nil {
				if ev, ok := status.FromError(err); ok {
					if ev.Code() == codes.Canceled {
						return
					}
				}
				log.Errorf("%s failed to establish sync stream with leader %s: %s", s.server.member.GetName(), s.server.GetLeader().GetName(), err)
				time.Sleep(time.Second)
				continue
			}
			log.Infof("%s start sync with leader %s", s.server.member.GetName(), s.server.GetLeader().GetName())
			for {
				resp, err := client.Recv()
				if err != nil {
					log.Error("region sync with leader meet error:", err)
					client.CloseSend()
					break
				}
				for _, r := range resp.GetRegions() {
					s.server.kv.SaveRegion(r)
				}
			}
		}
	}()
}
