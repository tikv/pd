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

	"github.com/pingcap/kvproto/pkg/pdpb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type regionSyncer struct {
	sync.Mutex
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

func (s *regionSyncer) stopSyncerWithLeader() {
	if s.cancel == nil {
		return
	}
	s.cancel()
	s.cancel, s.ctx = nil, nil
	s.wg.Wait()
}

func (s *regionSyncer) statSyncerWithLeader(addr string) error {
	if s.cancel != nil {
		s.stopSyncerWithLeader()
	}
	u, err := url.Parse(addr)
	if err != nil {
		return err
	}

	cc, err := grpc.Dial(u.Host, grpc.WithInsecure(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(8*1024*1024)))
	if err != nil {
		return err
	}

	s.ctx, s.cancel = context.WithCancel(context.Background())
	client, err := pdpb.NewPDClient(cc).SyncRegions(s.ctx)
	if err != nil {
		return err
	}
	err = client.Send(&pdpb.SyncRegionRequest{
		Header: &pdpb.RequestHeader{ClusterId: s.server.clusterID},
		Member: s.server.member,
		Tp:     1,
	})
	if err != nil {
		return err
	}

	log.Infof("%s start sync with leader %s", s.server.member.GetName(), s.server.GetLeader().GetName())
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			resp, err := client.Recv()
			if err != nil {
				log.Error("region sync with leader meet error:", err)
				return
			}
			metas := &pdpb.MetaRegions{}
			metas.Unmarshal(resp.Data)
			for i := uint32(0); i < metas.GetCount(); i++ {
				s.server.kv.SaveRegion(metas.Regions[i])
			}
		}
	}()
	return nil
}
