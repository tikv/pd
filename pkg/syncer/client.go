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

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/logutil"
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
	msgSize          = 8 * units.MiB
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

func (s *RegionSyncer) syncRegion(ctx context.Context, conn *grpc.ClientConn) (ClientStream, error) {
	cli := pdpb.NewPDClient(conn)
	syncStream, err := cli.SyncRegions(ctx)
	if err != nil {
		return nil, err
	}
	err = syncStream.Send(&pdpb.SyncRegionRequest{
		Header:     &pdpb.RequestHeader{ClusterId: s.server.ClusterID()},
		Member:     s.server.GetMemberInfo(),
		StartIndex: s.history.GetNextIndex(),
	})
	if err != nil {
		return nil, err
	}

	return syncStream, nil
}

var regionGuide = core.GenerateRegionGuideFunc(false)

// IsRunning returns whether the region syncer client is running.
func (s *RegionSyncer) IsRunning() bool {
	return s.streamingRunning.Load()
}

// StartSyncWithLeader starts to sync with leader.
func (s *RegionSyncer) StartSyncWithLeader(addr string) {
	s.wg.Add(1)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.clientCtx, s.mu.clientCancel = context.WithCancel(s.server.LoopContext())
	ctx := s.mu.clientCtx

	go func() {
		defer logutil.LogPanic()
		defer s.wg.Done()
		defer s.streamingRunning.Store(false)
		// used to load region from kv storage to cache storage.
		bc := s.server.GetBasicCluster()
		regionStorage := s.server.GetStorage()
		log.Info("region syncer start load region")
		start := time.Now()
		err := storage.TryLoadRegionsOnce(ctx, regionStorage, bc.CheckAndPutRegion)
		log.Info("region syncer finished load regions", zap.Duration("time-cost", time.Since(start)))
		if err != nil {
			log.Warn("failed to load regions", errs.ZapError(err))
		}
		// establish client.
		conn := grpcutil.CreateClientConn(ctx, addr, s.tlsConfig,
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
			grpc.WithBlock())
		// it means the context is canceled.
		if conn == nil {
			return
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
			failpoint.Inject("disableClientStreaming", func() {
				err = errors.Errorf("no stream")
			})
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
					s.streamingRunning.Store(false)
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
				buckets := resp.GetBuckets()
				regionLeaders := resp.GetRegionLeaders()
				hasStats := len(stats) == len(regions)
				hasBuckets := len(buckets) == len(regions)
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
							core.SetSource(core.Sync),
						)
					} else {
						region = core.NewRegionInfo(r, regionLeader, core.SetSource(core.Sync))
					}

					origin, _, err := bc.PreCheckPutRegion(region)
					if err != nil {
						log.Debug("region is stale", zap.Stringer("origin", origin.GetMeta()), errs.ZapError(err))
						continue
					}
					_, saveKV, _, _ := regionGuide(region, origin)
					overlaps := bc.PutRegion(region)

					if hasBuckets {
						if old := origin.GetBuckets(); buckets[i].GetVersion() > old.GetVersion() {
							region.UpdateBuckets(buckets[i], old)
						}
					}
					if saveKV {
						err = regionStorage.SaveRegion(r)
					}
					if err == nil {
						s.history.Record(region)
					}
					for _, old := range overlaps {
						_ = regionStorage.DeleteRegion(old.GetMeta())
					}
				}
				// mark the client as running status when it finished the first history region sync.
				s.streamingRunning.Store(true)
			}
		}
	}()
}
