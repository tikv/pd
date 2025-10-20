// Copyright 2025 TiKV Project Authors.
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

package server

import (
	"context"
	"sync"
	"sync/atomic"
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

	sd "github.com/tikv/pd/client/servicediscovery"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/syncer"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/logutil"
)

// RegionSyncer is used to sync region info from leader.
type RegionSyncer struct {
	wg        sync.WaitGroup
	cluster   *core.BasicCluster
	tlsConfig *grpcutil.TLSConfig

	// status when as client
	streamingRunning atomic.Bool
	nextSyncIndex    uint64
	name             string
	listenURL        string

	svcDiscovery sd.ServiceDiscovery

	leaderCtx context.Context
	cancelCtx context.CancelFunc

	reconnectCh chan struct{}
}

// NewRegionSyncer returns a region syncer.
func NewRegionSyncer(leaderCtx context.Context, cluster *core.BasicCluster, discovery sd.ServiceDiscovery,
	tlsConfig *grpcutil.TLSConfig, name, listenURL string, reconnectCh chan struct{}) *RegionSyncer {
	return &RegionSyncer{
		cluster:      cluster,
		tlsConfig:    tlsConfig,
		name:         name,
		listenURL:    listenURL,
		svcDiscovery: discovery,
		leaderCtx:    leaderCtx,
		reconnectCh:  reconnectCh,
	}
}

const (
	keepaliveTime    = 10 * time.Second
	keepaliveTimeout = 3 * time.Second
	msgSize          = 8 * units.MiB
	retryInterval    = time.Second
)

// Reconnect reconnects to leader.
func (s *RegionSyncer) leaderLoop() {
	s.wg.Add(1)
	defer s.wg.Done()
	for {
		select {
		case <-s.leaderCtx.Done():
			log.Warn("server context has been canceled, stop reconnecting to leader", zap.String("server", s.name))
			return
		case <-s.reconnectCh:
			log.Warn("leader changed, need to reconnect", zap.String("server", s.name))
			s.cancelCtx()
			s.cancelCtx = nil
		}
		s.startSyncWithLeader()
	}
}

func (s *RegionSyncer) startSyncWithLeader() {
	leaderAddr := s.svcDiscovery.GetServingURL()
	ctx, cancelCtx := context.WithCancel(s.leaderCtx)
	s.cancelCtx = cancelCtx
	log.Warn("region syncer reconnect to leader", zap.String("server", s.name), zap.String("leader", leaderAddr))
	go s.sync(ctx, leaderAddr)
}

// startSyncWithLeader starts to sync with leader.
func (s *RegionSyncer) sync(ctx context.Context, leaderAddr string) {
	s.wg.Add(1)
	defer func() {
		log.Info("region syncer stopped syncing with leader", zap.String("server", s.name), zap.String("leader", leaderAddr))
		s.svcDiscovery.ScheduleCheckMemberChanged()
	}()
	defer logutil.LogPanic()
	defer s.wg.Done()
	defer s.streamingRunning.Store(false)
	// used to load region from kv storage to cache storage.
	bc := s.cluster
	// establish client.
	conn := grpcutil.CreateClientConn(ctx, leaderAddr, s.tlsConfig,
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
			log.Warn("server failed to establish sync stream with leader", zap.String("server", s.name), zap.String("leader", leaderAddr), errs.ZapError(err))
			select {
			case <-ctx.Done():
				log.Info("stop synchronizing with leader due to context canceled")
				return
			case <-time.After(retryInterval):
			}
			continue
		}
		log.Info("server starts to synchronize with leader", zap.String("server", s.name), zap.String("leader", leaderAddr), zap.Uint64("request-index", s.nextSyncIndex))
		for {
			resp, err := stream.Recv()
			failpoint.Inject("syncMetError", func() {
				err = errors.Errorf("recv met stream")
			})
			if err != nil {
				s.streamingRunning.Store(false)
				log.Warn("region sync with leader meet error", errs.ZapError(errs.ErrGRPCRecv, err))
				if err = stream.CloseSend(); err != nil {
					log.Warn("failed to terminate client stream", errs.ZapError(errs.ErrGRPCCloseSend, err))
				}
				select {
				case <-ctx.Done():
					log.Info("stop synchronizing with leader due to context canceled",
						zap.String("server", s.name), zap.Uint64("next-index", s.nextSyncIndex))
					return
				case <-time.After(retryInterval):
				}
				break
			}
			if s.nextSyncIndex != resp.GetStartIndex() {
				log.Warn("server sync index not match the leader",
					zap.String("server", s.name),
					zap.Uint64("own", s.nextSyncIndex),
					zap.Uint64("leader", resp.GetStartIndex()),
					zap.Int("records-length", len(resp.GetRegions())))
				// reset index
				s.nextSyncIndex = resp.GetStartIndex()
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
				bc.PutRegion(region)
				if err == nil {
					s.nextSyncIndex++
				}
				// mark the client as running status when it finished the first history region sync.
				s.streamingRunning.Store(true)
			}
		}
	}
}

func (s *RegionSyncer) syncRegion(ctx context.Context, conn *grpc.ClientConn) (syncer.ClientStream, error) {
	cli := pdpb.NewPDClient(conn)
	syncStream, err := cli.SyncRegions(ctx)
	if err != nil {
		return nil, err
	}
	err = syncStream.Send(&pdpb.SyncRegionRequest{
		Header: &pdpb.RequestHeader{ClusterId: keypath.ClusterID()},
		Member: &pdpb.Member{
			Name:       s.name,
			ClientUrls: []string{s.listenURL},
		},
		StartIndex: s.nextSyncIndex,
	})
	if err != nil {
		return nil, err
	}

	return syncStream, nil
}

// IsRunning returns whether the region syncer client is running.
func (s *RegionSyncer) IsRunning() bool {
	return s.streamingRunning.Load()
}

// Stop stops the region syncer.
func (s *RegionSyncer) Stop() {
	if s.cancelCtx != nil {
		s.cancelCtx()
	}
	s.wg.Wait()
}
