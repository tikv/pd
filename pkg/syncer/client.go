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
	// If this sync session never observed an end-of-history marker, the
	// in-memory history index reflects a partial transfer that the leader
	// never confirmed. Roll back to the last committed (persisted) index
	// so the next leader sees a StartIndex that triggers a fresh bulk
	// rather than a stale offset into a different leader's history space.
	if !s.historySynced.Load() {
		s.history.rollback()
	}
}

func (s *RegionSyncer) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.mu.clientCancel != nil {
		s.mu.clientCancel()
	}
	s.mu.clientCancel, s.mu.clientCtx = nil, nil
}

// ResetHistoryIndex resets and persists the next region sync history index.
func (s *RegionSyncer) ResetHistoryIndex(index uint64) {
	s.history.resetWithIndexAndPersist(index)
}

// HasAttemptedSync reports whether StartSyncWithLeader has ever been called
// during this process's lifetime. Used by the leader-election path to tell
// "I am a fresh single-node cluster" apart from "I am a follower that needs
// to be caught up before campaigning".
func (s *RegionSyncer) HasAttemptedSync() bool {
	return s.attemptedSync.Load()
}

// IsHistorySynced reports whether this server has, at some point during its
// lifetime, observed that it was caught up to a leader's history. The signal
// is sticky: once true, the local region storage is durably populated and
// further syncs only extend that state.
func (s *RegionSyncer) IsHistorySynced() bool {
	return s.historySynced.Load()
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

func (s *RegionSyncer) handleRegionSyncResponse(
	ctx context.Context,
	resp *pdpb.SyncRegionResponse,
	bc *core.BasicCluster,
	regionStorage storage.Storage,
	fullSyncing bool,
) (handled bool, nextFullSyncing bool) {
	nextFullSyncing = fullSyncing
	if syncErr := resp.GetHeader().GetError(); syncErr != nil {
		s.streamingRunning.Store(false)
		log.Warn("region sync with leader received error response",
			zap.String("server", s.server.Name()),
			zap.String("error-type", syncErr.GetType().String()),
			zap.String("error-message", syncErr.GetMessage()))
		return false, nextFullSyncing
	}
	stats := resp.GetRegionStats()
	regions := resp.GetRegions()
	buckets := resp.GetBuckets()
	regionLeaders := resp.GetRegionLeaders()
	startFullSync := !fullSyncing && !s.IsRunning() && resp.GetStartIndex() == 0 && len(regions) > 0
	inFullSync := fullSyncing || startFullSync
	// During a full sync, intermediate data frames carry a positional
	// offset, not a reusable history index.
	isPositionalBatch := inFullSync && !startFullSync && len(regions) > 0
	if !isPositionalBatch && s.history.getNextIndex() != resp.GetStartIndex() {
		log.Warn("server sync index not match the leader",
			zap.String("server", s.server.Name()),
			zap.Uint64("own", s.history.getNextIndex()),
			zap.Uint64("leader", resp.GetStartIndex()),
			zap.Int("records-length", len(resp.GetRegions())))
		// reset index
		s.history.resetWithIndex(resp.GetStartIndex())
	}
	hasStats := len(stats) == len(regions)
	hasBuckets := len(buckets) == len(regions)
	// An empty-regions response is the explicit end-of-history
	// marker: sent by the leader at the end of a bulk transfer,
	// at the end of an incremental catch-up, as an "already in
	// sync" reply, or as a keepalive. Receiving one commits the
	// historical phase — we flush the accumulated history index
	// to disk and flip historySynced, which both opens the
	// leader-election gate and switches subsequent records from
	// the non-persisting catch-up path to the normal persisting
	// live-stream path.
	if len(regions) == 0 {
		if !s.historySynced.Load() {
			s.history.commit()
		}
		s.historySynced.Store(true)
	}
	inCatchup := !s.historySynced.Load()
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
		if err == nil && !inFullSync {
			// Full-sync frames carry positional offsets, not reusable history
			// indices, so they are applied to storage but not recorded here.
			// Records during historical catch-up are buffered without persisting
			// and flushed by commit() on the end-of-history marker; live records
			// after catch-up persist normally.
			if inCatchup {
				s.history.recordNoPersist(region)
			} else {
				s.history.record(region)
			}
		}
		for _, old := range overlaps {
			_ = regionStorage.DeleteRegion(old.GetMeta())
		}
	}
	nextFullSyncing = inFullSync && len(regions) > 0
	if !nextFullSyncing {
		// mark the client as running status when it finished the first history region sync.
		s.streamingRunning.Store(true)
	}
	return true, nextFullSyncing
}

// IsRunning returns whether the region syncer client is running.
func (s *RegionSyncer) IsRunning() bool {
	return s.streamingRunning.Load()
}

// StartSyncWithLeader starts to sync with leader.
func (s *RegionSyncer) StartSyncWithLeader(addr string) {
	s.wg.Add(1)
	s.attemptedSync.Store(true)
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
			fullSyncing := false
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
				handled, nextFullSyncing := s.handleRegionSyncResponse(ctx, resp, bc, regionStorage, fullSyncing)
				fullSyncing = nextFullSyncing
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
