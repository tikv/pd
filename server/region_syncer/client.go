// Copyright 2018 TiKV Project Authors.
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

package syncer

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/grpcutil"
	"github.com/tikv/pd/server/core"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

const (
	keepaliveTime    = 10 * time.Second
	keepaliveTimeout = 3 * time.Second
)

// StopSyncWithLeader stop to sync the region with leader.
func (s *RegionSyncer) StopSyncWithLeader() {
	s.reset()
	s.Lock()
	close(s.closed)
	s.closed = make(chan struct{})
	s.Unlock()
	s.wg.Wait()
}

func (s *RegionSyncer) reset() {
	s.Lock()
	defer s.Unlock()

	if s.regionSyncerCancel == nil {
		return
	}
	s.regionSyncerCancel()
	s.regionSyncerCancel, s.regionSyncerCtx = nil, nil
}

func (s *RegionSyncer) establish(addr string) (*grpc.ClientConn, error) {
	s.reset()
	ctx, cancel := context.WithCancel(s.server.LoopContext())
	tlsCfg, err := s.tlsConfig.ToTLSConfig()
	if err != nil {
		cancel()
		return nil, err
	}
	cc, err := grpcutil.GetClientConn(
		ctx,
		addr,
		tlsCfg,
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(msgSize)),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                keepaliveTime,
			Timeout:             keepaliveTimeout,
			PermitWithoutStream: true,
		}),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  time.Second,     // Default was 1s.
				Multiplier: 1.6,             // Default
				Jitter:     0.2,             // Default
				MaxDelay:   3 * time.Second, // Default was 120s.
			},
			MinConnectTimeout: 5 * time.Second,
		}),
		// WithBlock will block the dial step until success or cancel the context.
		grpc.WithBlock(),
	)
	if err != nil {
		cancel()
		return nil, errors.WithStack(err)
	}

	s.Lock()
	s.regionSyncerCtx, s.regionSyncerCancel = ctx, cancel
	s.Unlock()
	return cc, nil
}

func (s *RegionSyncer) syncRegion(conn *grpc.ClientConn) (ClientStream, error) {
	cli := pdpb.NewPDClient(conn)
	syncStream, err := cli.SyncRegions(s.regionSyncerCtx)
	if err != nil {
		return syncStream, errs.ErrGRPCCreateStream.Wrap(err).FastGenWithCause()
	}
	err = syncStream.Send(&pdpb.SyncRegionRequest{
		Header:     &pdpb.RequestHeader{ClusterId: s.server.ClusterID()},
		Member:     s.server.GetMemberInfo(),
		StartIndex: s.history.GetNextIndex(),
	})
	if err != nil {
		return syncStream, errs.ErrGRPCSend.Wrap(err).FastGenWithCause()
	}

	return syncStream, nil
}

// StartSyncWithLeader starts to sync with leader.
func (s *RegionSyncer) StartSyncWithLeader(addr string) {
	s.wg.Add(1)
	s.RLock()
	closed := s.closed
	s.RUnlock()
	go func() {
		defer s.wg.Done()
		// used to load region from kv storage to cache storage.
		err := s.server.GetStorage().LoadRegionsOnce(s.server.GetBasicCluster().CheckAndPutRegion)
		if err != nil {
			log.Warn("failed to load regions.", errs.ZapError(err))
		}
		// establish client.
		var conn *grpc.ClientConn
		for {
			select {
			case <-closed:
				return
			default:
			}
			conn, err = s.establish(addr)
			if err != nil {
				log.Error("cannot establish connection with leader", zap.String("server", s.server.Name()), zap.String("leader", s.server.GetLeader().GetName()), errs.ZapError(err))
				continue
			}
			defer conn.Close()
			break
		}

		// Start syncing data.
		for {
			select {
			case <-closed:
				return
			default:
			}

			stream, err := s.syncRegion(conn)
			if err != nil {
				if ev, ok := status.FromError(err); ok {
					if ev.Code() == codes.Canceled {
						return
					}
				}
				log.Error("server failed to establish sync stream with leader", zap.String("server", s.server.Name()), zap.String("leader", s.server.GetLeader().GetName()), errs.ZapError(err))
				time.Sleep(time.Second)
				continue
			}
			log.Info("server starts to synchronize with leader", zap.String("server", s.server.Name()), zap.String("leader", s.server.GetLeader().GetName()), zap.Uint64("request-index", s.history.GetNextIndex()))
			for {
				resp, err := stream.Recv()
				if err != nil {
					log.Error("region sync with leader meet error", errs.ZapError(errs.ErrGRPCRecv, err))
					if err = stream.CloseSend(); err != nil {
						log.Error("failed to terminate client stream", errs.ZapError(errs.ErrGRPCCloseSend, err))
					}
					time.Sleep(time.Second)
					break
				}
				if s.history.GetNextIndex() != resp.GetStartIndex() {
					log.Warn("server sync index not match the leader",
						zap.String("server", s.server.Name()),
						zap.Uint64("own", s.history.GetNextIndex()),
						zap.Uint64("leader", resp.GetStartIndex()),
						zap.Int("records-length", len(resp.GetRegions())))
					// reset index
					s.history.ResetWithIndex(resp.GetStartIndex())
				}
				stats := resp.GetRegionStats()
				regions := resp.GetRegions()
				regionLeaders := resp.GetRegionLeaders()
				hasStats := len(stats) == len(regions)
				for i, r := range regions {
					var (
						region       *core.RegionInfo
						regionLeader *metapb.Peer
					)
					if len(regionLeaders) > i && regionLeaders[i].Id != 0 {
						regionLeader = regionLeaders[i]
					}
					if hasStats {
						region = core.NewRegionInfo(r, regionLeader,
							core.SetWrittenBytes(stats[i].BytesWritten),
							core.SetWrittenKeys(stats[i].KeysWritten),
							core.SetReadBytes(stats[i].BytesRead),
							core.SetReadKeys(stats[i].KeysRead),
						)
					} else {
						region = core.NewRegionInfo(r, regionLeader)
					}

					s.server.GetBasicCluster().CheckAndPutRegion(region)
					err = s.server.GetStorage().SaveRegion(r)
					if err == nil {
						s.history.Record(region)
					}
				}
			}
		}
	}()
}
