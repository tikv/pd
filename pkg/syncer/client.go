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
	"io"
	"time"

	"github.com/docker/go-units"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/ratelimit"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/logutil"
)

const (
	keepaliveTime    = 10 * time.Second
	keepaliveTimeout = 3 * time.Second
	msgSize          = 8 * units.MiB
	retryInterval    = time.Second
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
		Header:     &pdpb.RequestHeader{ClusterId: keypath.ClusterID()},
		Member:     s.server.GetMemberInfo(),
		StartIndex: s.history.getNextIndex(),
	})
	if err != nil {
		return nil, err
	}

	return syncStream, nil
}

var regionGuide = core.GenerateRegionGuideFunc(false)

type regionSyncState struct {
	syncingHistory bool
	fullSyncing    bool
}

func (*RegionSyncer) resetRegionCacheAndStorage(ctx context.Context, bc *core.BasicCluster, regionStorage storage.Storage) error {
	if err := regionStorage.Flush(); err != nil {
		return err
	}
	for _, region := range bc.GetRegions() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := regionStorage.DeleteRegion(region.GetMeta()); err != nil {
			return err
		}
	}
	bc.ResetRegionCache()
	return nil
}

func (s *RegionSyncer) handleRegionSyncResponse(
	ctx context.Context,
	resp *pdpb.SyncRegionResponse,
	bc *core.BasicCluster,
	regionStorage storage.Storage,
	state *regionSyncState,
) (bool, error) {
	if syncErr := resp.GetHeader().GetError(); syncErr != nil {
		s.streamingRunning.Store(false)
		log.Warn("region sync with leader received error response",
			zap.String("server", s.server.Name()),
			zap.String("error-type", syncErr.GetType().String()),
			zap.String("error-message", syncErr.GetMessage()))
		return false, nil
	}
	if state.syncingHistory && resp.GetStartIndex() == 0 && s.history.getNextIndex() != 0 {
		s.streamingRunning.Store(false)
		if err := s.resetRegionCacheAndStorage(ctx, bc, regionStorage); err != nil {
			return true, err
		}
		state.fullSyncing = true
	}
	if s.history.getNextIndex() != resp.GetStartIndex() {
		log.Warn("server sync index not match the leader",
			zap.String("server", s.server.Name()),
			zap.Uint64("own", s.history.getNextIndex()),
			zap.Uint64("leader", resp.GetStartIndex()),
			zap.Int("records-length", len(resp.GetRegions())))
		// reset index
		s.history.resetWithIndex(resp.GetStartIndex())
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
			opts         = []core.RegionCreateOption{core.SetSource(core.Sync)}
		)
		if len(regionLeaders) > i && regionLeaders[i].GetId() != 0 {
			regionLeader = regionLeaders[i]
		}
		if hasStats {
			opts = append(opts,
				core.SetWrittenBytes(stats[i].BytesWritten),
				core.SetWrittenKeys(stats[i].KeysWritten),
				core.SetReadBytes(stats[i].BytesRead),
				core.SetReadKeys(stats[i].KeysRead))
		}
		if hasBuckets {
			opts = append(opts, core.SetBuckets(buckets[i]))
		}
		region = core.NewRegionInfo(r, regionLeader, opts...)

		origin, _, err := bc.PreCheckPutRegion(region)
		if err != nil {
			log.Debug("region is stale", zap.Stringer("origin", origin.GetMeta()), errs.ZapError(err))
			continue
		}
		cctx := &core.MetaProcessContext{
			Context:    ctx,
			TaskRunner: ratelimit.NewSyncRunner(),
			Tracer:     core.NewNoopHeartbeatProcessTracer(),
			// no limit for followers.
		}
		saveKV, _, _, _ := regionGuide(cctx, region, origin)
		overlaps := bc.PutRegion(region)

		if saveKV {
			err = regionStorage.SaveRegion(r)
		}
		if err == nil {
			s.history.record(region)
		}
		for _, old := range overlaps {
			_ = regionStorage.DeleteRegion(old.GetMeta())
		}
	}
	if state.fullSyncing && len(regions) == 0 {
		if err := regionStorage.Flush(); err != nil {
			return true, err
		}
		state.fullSyncing = false
		state.syncingHistory = false
	} else if state.syncingHistory && !state.fullSyncing {
		state.syncingHistory = false
	}
	// mark the client as running status when it finished the first history region sync.
	if !state.fullSyncing {
		s.streamingRunning.Store(true)
	}
	return true, nil
}

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
		if err != nil {
			log.Warn("region syncer failed to load regions", errs.ZapError(err), zap.Duration("time-cost", time.Since(start)))
		} else {
			log.Info("region syncer finished load regions", zap.Duration("time-cost", time.Since(start)))
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
			// TODO: remove grpc.WithBlock to adopt the latest best practices.
			//nolint:staticcheck
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
				log.Warn("server failed to establish sync stream with leader", zap.String("server", s.server.Name()), zap.String("leader", s.server.GetLeader().GetName()), errs.ZapError(err))
				select {
				case <-ctx.Done():
					log.Info("stop synchronizing with leader due to context canceled")
					return
				case <-time.After(retryInterval):
				}
				continue
			}
			log.Info("server starts to synchronize with leader", zap.String("server", s.server.Name()), zap.String("leader", s.server.GetLeader().GetName()), zap.Uint64("request-index", s.history.getNextIndex()))
			syncState := &regionSyncState{syncingHistory: true}
			for {
				resp, err := stream.Recv()
				if err == io.EOF {
					log.Info("server region sync with leader meets EOF, stop syncing", zap.String("server", s.server.Name()))
					return
				}
				if err != nil {
					s.streamingRunning.Store(false)
					log.Warn("region sync with leader meet error", errs.ZapError(errs.ErrGRPCRecv, err))
					if err = stream.CloseSend(); err != nil {
						log.Warn("failed to terminate client stream", errs.ZapError(errs.ErrGRPCCloseSend, err))
					}
					if !s.waitRegionSyncRetryInterval(ctx) {
						return
					}
					break
				}
				handled, err := s.handleRegionSyncResponse(ctx, resp, bc, regionStorage, syncState)
				if err != nil {
					log.Warn("failed to handle region sync response",
						zap.String("server", s.server.Name()),
						zap.String("leader", s.server.GetLeader().GetName()),
						errs.ZapError(err))
					if err = stream.CloseSend(); err != nil {
						log.Warn("failed to terminate client stream", errs.ZapError(errs.ErrGRPCCloseSend, err))
					}
					break
				}
				if !handled {
					if err = stream.CloseSend(); err != nil {
						log.Warn("failed to terminate client stream", errs.ZapError(errs.ErrGRPCCloseSend, err))
					}
					if !s.waitRegionSyncRetryInterval(ctx) {
						return
					}
					break
				}
			}
		}
	}()
}

func (s *RegionSyncer) waitRegionSyncRetryInterval(ctx context.Context) bool {
	// Check if the leader is still there to avoid waiting for a `retryInterval`.
	if s.server.GetLeader() == nil {
		log.Warn("stop synchronizing with leader due to leader stepped down",
			zap.String("server", s.server.Name()), zap.Uint64("next-index", s.history.getNextIndex()))
		return false
	}
	select {
	case <-ctx.Done():
		log.Info("stop synchronizing with leader due to context canceled",
			zap.String("server", s.server.Name()), zap.Uint64("next-index", s.history.getNextIndex()))
		return false
	case <-time.After(retryInterval):
		return true
	}
}
