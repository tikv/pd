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
)

const (
	msgSize                = 8 * 1024 * 1024
	maxSyncRegionBatchSize = 100
)

type regionSyncer struct {
	sync.RWMutex
	streams map[string]pdpb.PD_SyncRegionsServer
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	server  *Server
}

func newRegionSyncer(server *Server) *regionSyncer {
	return &regionSyncer{
		streams: make(map[string]pdpb.PD_SyncRegionsServer),
		server:  server,
	}
}

func (s *regionSyncer) bindStream(name string, stream pdpb.PD_SyncRegionsServer) {
	s.Lock()
	s.streams[name] = stream
	s.Unlock()
}

func (s *regionSyncer) broadcast(regions *pdpb.SyncRegionResponse) {
	s.Lock()
	for _, sender := range s.streams {
		err := sender.Send(regions)
		if err != nil {
			log.Error("region syncer send data meet error:", err)
		}
	}
	s.Unlock()
}

func (s *regionSyncer) stopSyncWithLeader() {
	if s.cancel == nil {
		return
	}
	s.cancel()
	s.cancel, s.ctx = nil, nil
	s.wg.Wait()
}

func (s *regionSyncer) establish(addr string) (pdpb.PD_SyncRegionsClient, error) {
	if s.cancel != nil {
		s.stopSyncWithLeader()
	}
	u, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}

	cc, err := grpc.Dial(u.Host, grpc.WithInsecure(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(msgSize)))
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
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
	s.ctx, s.cancel = ctx, cancel
	return client, nil
}

func (s *regionSyncer) startSyncWithLeader(addr string) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			// establish client
			client, err := s.establish(addr)
			if err != nil {
				log.Errorf("%s failed to establish sync stream with leader %s: %s", s.server.member.GetName(), s.server.GetLeader().GetName(), err)
				time.Sleep(time.Second)
				continue
			}
			log.Infof("%s start sync with leader %s", s.server.member.GetName(), s.server.GetLeader().GetName())
			for {
				resp, err := client.Recv()
				if err != nil {
					log.Error("region sync with leader meet error:", err)
					break
				}
				for _, r := range resp.GetRegions() {
					s.server.kv.SaveRegion(r)
				}
			}
		}
	}()
}
