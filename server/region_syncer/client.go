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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
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
	s.wg.Wait()
}

func (s *RegionSyncer) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.mu.clientCancel != nil {
		s.mu.clientCancel()
	}
	s.mu.clientCancel, s.mu.clientCtx = nil, nil
}

func (s *RegionSyncer) establish(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	tlsCfg, err := s.tlsConfig.ToTLSConfig()
	if err != nil {
		return nil, err
	}
	cc, err := grpcutil.GetClientConn(
		ctx,
		addr,
		tlsCfg,
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(msgSize)),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    keepaliveTime,
			Timeout: keepaliveTimeout,
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
		return nil, errors.WithStack(err)
	}
	return cc, nil
}

func (s *RegionSyncer) syncRegion(ctx context.Context, conn *grpc.ClientConn) (ClientStream, error) {
	cli := pdpb.NewPDClient(conn)
	syncStream, err := cli.SyncRegions(ctx)
	if err != nil {
		return nil, errs.ErrGRPCCreateStream.Wrap(err).FastGenWithCause()
	}
	err = syncStream.Send(&pdpb.SyncRegionRequest{
		Header:     &pdpb.RequestHeader{ClusterId: s.server.ClusterID()},
		Member:     s.server.GetMemberInfo(),
		StartIndex: s.history.GetNextIndex(),
	})
	if err != nil {
		return nil, errs.ErrGRPCSend.Wrap(err).FastGenWithCause()
	}

	return syncStream, nil
}

var regionGuide = core.GenerateRegionGuideFunc(false)

// StartSyncWithLeader starts to sync with leader.
func (s *RegionSyncer) StartSyncWithLeader(addr string) {
	s.wg.Add(1)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.clientCtx, s.mu.clientCancel = context.WithCancel(s.server.LoopContext())
	ctx := s.mu.clientCtx

	go func() {
		defer s.wg.Done()
		// used to load region from kv storage to cache storage.
		bc := s.server.GetBasicCluster()
		storage := s.server.GetStorage()
		log.Info("region syncer start load region")
		start := time.Now()
		err := storage.LoadRegionsOnce(ctx, bc.CheckAndPutRegion)
		log.Info("region syncer finished load region", zap.Duration("time-cost", time.Since(start)))
		if err != nil {
			log.Warn("failed to load regions.", errs.ZapError(err))
		}
		// establish client.
		var conn *grpc.ClientConn
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			conn, err = s.establish(ctx, addr)
			if err != nil {
				log.Error("cannot establish connection with leader", zap.String("server", s.server.Name()), zap.String("leader", s.server.GetLeader().GetName()), errs.ZapError(err))
				continue
			}
			break
		}
		defer conn.Close()

		// Start syncing data.
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			stream, err := s.syncRegion(ctx, conn)
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
					if len(regionLeaders) > i && regionLeaders[i].GetId() != 0 {
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

					origin, err := bc.PreCheckPutRegion(region)
					if err != nil {
						log.Debug("region is stale", zap.Stringer("origin", origin.GetMeta()), errs.ZapError(err))
						continue
					}
					_, saveKV, _, _ := regionGuide(region, origin)
					overlaps := bc.PutRegion(region)

					if saveKV {
						err = storage.SaveRegion(r)
					}
					if err == nil {
						s.history.Record(region)
					}
					for _, old := range overlaps {
						_ = storage.DeleteRegion(old.GetMeta())
					}
				}
			}
		}
	}()
}
