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
	keepaliveTime           = 10 * time.Second
	keepaliveTimeout        = 3 * time.Second
	fullSyncProgressTimeout = 30 * time.Second
	msgSize                 = 8 * units.MiB
	retryInterval           = time.Second
)

// StopSyncWithLeader stops Region synchronization with the leader.
func (s *RegionSyncer) StopSyncWithLeader() {
	s.mu.Lock()
	if s.mu.clientCancel != nil {
		s.mu.clientCancel()
	}
	s.mu.clientCancel, s.mu.clientCtx = nil, nil
	s.mu.Unlock()
	s.wg.Wait()
}

// MarkHistoryIncomplete resets the durable Region sync state before a full
// synchronization starts.
func (s *RegionSyncer) MarkHistoryIncomplete() error {
	if err := s.history.saveSynced(false); err != nil {
		return errors.Wrap(err, "clear region sync completion marker")
	}
	s.historySynced.Store(false)
	s.streamingRunning.Store(false)
	s.initialFollowerSyncCompleted.Store(false)
	return s.history.resetWithIndexAndPersist(0)
}

// IsHistorySynced reports whether this member has durably completed at least
// one region history synchronization.
func (s *RegionSyncer) IsHistorySynced() bool {
	return s.historySynced.Load()
}

// MarkHistorySynced flushes the local region data before durably recording
// that this member has completed Region synchronization.
func (s *RegionSyncer) MarkHistorySynced() error {
	if err := s.server.GetStorage().Flush(); err != nil {
		return errors.Wrap(err, "flush region storage before marking sync complete")
	}
	if err := s.history.persist(); err != nil {
		return errors.Wrap(err, "persist completed region sync index")
	}
	if err := s.history.saveSynced(true); err != nil {
		return errors.Wrap(err, "persist region sync completion marker")
	}
	s.historySynced.Store(true)
	return nil
}

func (s *RegionSyncer) syncRegionStartIndex() uint64 {
	if !s.initialFollowerSyncCompleted.Load() || !s.IsHistorySynced() {
		return 0
	}
	return s.history.getNextIndex()
}

func (s *RegionSyncer) loadRegions(ctx context.Context) error {
	s.regionLoadMu.Lock()
	defer s.regionLoadMu.Unlock()
	if s.initialRegionLoadCompleted.Load() {
		return nil
	}
	log.Info("region syncer start load region")
	start := time.Now()
	if err := storage.TryLoadRegionsFromLocalStorageOnce(
		ctx,
		s.server.GetStorage(),
		s.server.GetBasicCluster().CheckAndPutRegion,
	); err != nil {
		return err
	}
	s.initialRegionLoadCompleted.Store(true)
	log.Info("region syncer finished load regions", zap.Duration("time-cost", time.Since(start)))
	return nil
}

func (s *RegionSyncer) syncRegion(ctx context.Context, conn *grpc.ClientConn) (ClientStream, error) {
	cli := pdpb.NewPDClient(conn)
	syncStream, err := cli.SyncRegions(ctx)
	if err != nil {
		return nil, err
	}
	startIndex := s.syncRegionStartIndex()
	err = syncStream.Send(&pdpb.SyncRegionRequest{
		Header:     &pdpb.RequestHeader{ClusterId: keypath.ClusterID()},
		Member:     s.server.GetMemberInfo(),
		StartIndex: startIndex,
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
	startFullSync, startEmptyFullSync := s.isFullSyncStartResponse(resp, fullSyncing)
	if startFullSync || startEmptyFullSync {
		s.streamingRunning.Store(false)
		if err := s.MarkHistoryIncomplete(); err != nil {
			log.Warn("region syncer failed to reset history before full synchronization",
				zap.String("server", s.server.Name()), errs.ZapError(err))
			return false, false
		}
		// RegionStorage buffers SaveRegion calls in memory. Flush before
		// scanning the underlying store so a destructive clear cannot
		// resurrect an older batch on the next flush.
		if err := regionStorage.Flush(); err != nil {
			log.Warn("region syncer failed to flush pending Region writes before full synchronization",
				zap.String("server", s.server.Name()), errs.ZapError(err))
			return false, false
		}
		if err := storage.ClearRegionStorage(ctx, regionStorage); err != nil {
			log.Warn("region syncer failed to clear Region storage before full synchronization",
				zap.String("server", s.server.Name()), errs.ZapError(err))
			return false, false
		}
		if err := ctx.Err(); err != nil {
			return false, false
		}
		bc.ResetRegionCache()
	}
	inFullSync := fullSyncing || startFullSync || startEmptyFullSync
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
	for i, r := range regions {
		if err := ctx.Err(); err != nil {
			return false, false
		}
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

		origin, overlaps, err := bc.PreCheckPutRegion(region)
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
		if saveKV {
			if err = regionStorage.SaveRegion(r); err != nil {
				s.streamingRunning.Store(false)
				log.Warn("region syncer failed to save Region",
					zap.String("server", s.server.Name()),
					zap.Uint64("region-id", region.GetID()),
					errs.ZapError(err))
				return false, false
			}
		}
		for _, old := range overlaps {
			if err = regionStorage.DeleteRegion(old.GetMeta()); err != nil {
				s.streamingRunning.Store(false)
				log.Warn("region syncer failed to delete overlapping Region",
					zap.String("server", s.server.Name()),
					zap.Uint64("region-id", old.GetID()),
					errs.ZapError(err))
				return false, false
			}
		}
		bc.PutRegion(region)
		if !inFullSync {
			s.history.record(region)
		}
	}
	nextFullSyncing = startEmptyFullSync || (inFullSync && len(regions) > 0)
	if !nextFullSyncing && len(regions) == 0 {
		if err := s.MarkHistorySynced(); err != nil {
			s.streamingRunning.Store(false)
			log.Warn("region syncer failed to persist completed synchronization",
				zap.String("server", s.server.Name()), errs.ZapError(err))
			return false, nextFullSyncing
		}
		s.initialFollowerSyncCompleted.Store(true)
		// Mark the client as running only after the initial history phase is
		// complete and the received regions are durable.
		s.streamingRunning.Store(true)
	}
	return true, nextFullSyncing
}

func (s *RegionSyncer) isFullSyncStartResponse(
	resp *pdpb.SyncRegionResponse,
	fullSyncing bool,
) (startFullSync, startEmptyFullSync bool) {
	if fullSyncing || resp.GetStartIndex() != 0 {
		return false, false
	}
	regions := resp.GetRegions()
	startFullSync = !s.IsRunning() && len(regions) > 0
	startEmptyFullSync = len(regions) == 0 &&
		(!s.IsRunning() || !s.initialFollowerSyncCompleted.Load() ||
			!s.IsHistorySynced() || s.history.getNextIndex() != 0)
	return startFullSync, startEmptyFullSync
}

// IsRunning returns whether the region syncer client is running.
func (s *RegionSyncer) IsRunning() bool {
	return s.streamingRunning.Load() && s.IsHistorySynced()
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
		// Fail closed before receiving historical records. This prevents a
		// partial catch-up from being mistaken for legacy completed state after
		// a process restart.
		for !s.historySynced.Load() {
			err := s.history.saveSynced(false)
			if err == nil {
				break
			}
			log.Warn("persist incomplete region sync marker failed",
				zap.String("server", s.server.Name()), errs.ZapError(err))
			select {
			case <-ctx.Done():
				return
			case <-time.After(retryInterval):
			}
		}
		// used to load region from kv storage to cache storage.
		bc := s.server.GetBasicCluster()
		regionStorage := s.server.GetStorage()
		for {
			start := time.Now()
			err := s.loadRegions(ctx)
			if err == nil {
				break
			}
			log.Warn("region syncer failed to load regions; synchronization remains blocked",
				errs.ZapError(err), zap.Duration("time-cost", time.Since(start)))
			if !s.waitRegionSyncRetryInterval(ctx) {
				return
			}
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

			streamCtx, streamCancel := context.WithCancel(ctx)
			stream, err := s.syncRegion(streamCtx, conn)
			failpoint.Inject("disableClientStreaming", func() {
				err = errors.Errorf("no stream")
			})
			if err != nil {
				streamCancel()
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
			log.Info("server starts to synchronize with leader", zap.String("server", s.server.Name()), zap.String("leader", s.server.GetLeader().GetName()), zap.Uint64("request-index", s.syncRegionStartIndex()))
			fullSyncing := false
			var (
				fullSyncProgress     chan<- struct{}
				fullSyncTimedOut     <-chan struct{}
				stopFullSyncWatchdog context.CancelFunc
			)
			finishStream := func() bool {
				timedOut := false
				if fullSyncTimedOut != nil {
					select {
					case <-fullSyncTimedOut:
						timedOut = true
					default:
					}
				}
				if stopFullSyncWatchdog != nil {
					stopFullSyncWatchdog()
				}
				streamCancel()
				s.streamingRunning.Store(false)
				if err := stream.CloseSend(); err != nil {
					log.Warn("failed to terminate client stream", errs.ZapError(errs.ErrGRPCCloseSend, err))
				}
				if timedOut {
					log.Warn("Region full synchronization made no progress; reconnecting",
						zap.String("server", s.server.Name()),
						zap.Duration("timeout", s.fullSyncProgressTimeout))
				}
				return s.waitRegionSyncRetryInterval(ctx)
			}
			for {
				resp, err := stream.Recv()
				if err != nil {
					if err == io.EOF {
						log.Info("server region sync with leader reached EOF; reconnecting",
							zap.String("server", s.server.Name()))
					} else {
						log.Warn("region sync with leader meet error", errs.ZapError(errs.ErrGRPCRecv, err))
					}
					if !finishStream() {
						return
					}
					break
				}
				if fullSyncProgress == nil {
					startFullSync, startEmptyFullSync := s.isFullSyncStartResponse(resp, fullSyncing)
					if startFullSync || startEmptyFullSync {
						fullSyncProgress, fullSyncTimedOut, stopFullSyncWatchdog =
							watchFullSyncProgress(streamCtx, s.fullSyncProgressTimeout, streamCancel)
					}
				}
				handled, nextFullSyncing := s.handleRegionSyncResponse(
					streamCtx, resp, bc, regionStorage, fullSyncing,
				)
				fullSyncing = nextFullSyncing
				if !handled {
					if !finishStream() {
						return
					}
					break
				}
				if fullSyncing {
					select {
					case fullSyncProgress <- struct{}{}:
					default:
					}
				} else if stopFullSyncWatchdog != nil {
					stopFullSyncWatchdog()
					stopFullSyncWatchdog = nil
					fullSyncProgress = nil
					fullSyncTimedOut = nil
				}
			}
		}
	}()
}

func watchFullSyncProgress(
	ctx context.Context,
	timeout time.Duration,
	cancelStream context.CancelFunc,
) (chan<- struct{}, <-chan struct{}, context.CancelFunc) {
	watchdogCtx, stopWatchdog := context.WithCancel(ctx)
	progress := make(chan struct{}, 1)
	timedOut := make(chan struct{})
	go func() {
		defer logutil.LogPanic()
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		for {
			select {
			case <-watchdogCtx.Done():
				return
			case <-progress:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(timeout)
			case <-timer.C:
				close(timedOut)
				cancelStream()
				return
			}
		}
	}()
	return progress, timedOut, stopWatchdog
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
