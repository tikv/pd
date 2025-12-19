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
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/docker/go-units"
	clientv3 "go.etcd.io/etcd/client/v3"
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
	"github.com/tikv/pd/pkg/syncer"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/logutil"
)

var (
	syncIndexGauge = regionSyncerStatus.WithLabelValues("sync_index")
	lastIndexGauge = regionSyncerStatus.WithLabelValues("last_index")
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

	serverCtx  context.Context
	cancelFunc context.CancelFunc

	// syncLoopCancelCtx is used to cancel the current syncing with leader
	syncLoopCancelCtx context.CancelFunc

	// notify the syncer to reconnect to leader
	// true means force reconnect, it will stop the current syncing stream, such as leader changed
	// false means reconnect only when the current streaming is not running
	reconnectCh chan bool
	// checkMembershipCh is used to notify updating PD member list, such as when leader changed
	checkMembershipCh chan struct{}
	etcdClient        *clientv3.Client
	// pdLeaderAddr stores the current PD leader address and is used to detect leader changes
	pdLeaderAddr atomic.Value
}

// NewRegionSyncer returns a region syncer.
func NewRegionSyncer(serverCtx context.Context, cluster *core.BasicCluster, etcdClient *clientv3.Client,
	tlsConfig *grpcutil.TLSConfig, name, listenURL string) *RegionSyncer {
	checkMembershipCh := make(chan struct{}, 1)
	reconnectCh := make(chan bool, 1)
	ctx, cancelFunc := context.WithCancel(serverCtx)
	return &RegionSyncer{
		cluster:           cluster,
		tlsConfig:         tlsConfig,
		name:              name,
		listenURL:         listenURL,
		serverCtx:         ctx,
		cancelFunc:        cancelFunc,
		checkMembershipCh: checkMembershipCh,
		etcdClient:        etcdClient,
		reconnectCh:       reconnectCh,
	}
}

const (
	keepaliveTime        = 10 * time.Second
	keepaliveTimeout     = 3 * time.Second
	msgSize              = 8 * units.MiB
	retryInterval        = time.Second
	memberUpdateInterval = time.Minute
)

func (s *RegionSyncer) getClient() *clientv3.Client {
	return s.etcdClient
}

func (s *RegionSyncer) updatePDMemberLoop() {
	defer logutil.LogPanic()
	defer s.wg.Done()

	ticker := time.NewTicker(memberUpdateInterval)
	failpoint.Inject("speedUpMemberLoop", func() {
		ticker.Reset(100 * time.Millisecond)
	})
	defer ticker.Stop()
	var curLeader uint64
	for {
		select {
		case <-s.serverCtx.Done():
			log.Info("server is closed, exit update member loop")
			return
		case <-ticker.C:
		case <-s.checkMembershipCh:
		}
		members, err := etcdutil.ListEtcdMembers(s.serverCtx, s.getClient())
		if err != nil {
			log.Warn("failed to list members", errs.ZapError(err))
			continue
		}
		for _, ep := range members.Members {
			if len(ep.GetClientURLs()) == 0 { // This member is not started yet.
				log.Info("member is not started yet", zap.String("member-id", strconv.FormatUint(ep.GetID(), 16)), errs.ZapError(err))
				continue
			}
			status, err := s.getClient().Status(s.serverCtx, ep.ClientURLs[0])
			if err != nil {
				log.Info("failed to get status of member", zap.String("member-id", strconv.FormatUint(ep.ID, 16)), zap.String("endpoint", ep.ClientURLs[0]), errs.ZapError(err))
				continue
			}
			if status.Leader != ep.ID {
				continue
			}
			leaderAddr := ep.ClientURLs[0]
			if s.pdLeaderAddr.CompareAndSwap(s.pdLeaderAddr.Load(), leaderAddr) {
				if status.Leader != curLeader {
					log.Info("switch PD leader", zap.String("leader-id", strconv.FormatUint(ep.ID, 16)), zap.String("endpoint", ep.ClientURLs[0]))
					s.reconnectCh <- true
				}
				curLeader = ep.ID
				break
			}
		}
	}
}

func (s *RegionSyncer) triggerMembershipCheck() {
	select {
	case s.checkMembershipCh <- struct{}{}:
	default: // avoid blocking
	}
}

func (s *RegionSyncer) syncLoop() {
	defer logutil.LogPanic()
	defer s.wg.Done()
	for {
		select {
		case <-s.serverCtx.Done():
			log.Warn("server context has been canceled, stop reconnecting to leader", zap.String("server", s.name))
			return
		case force := <-s.reconnectCh:
			log.Warn("leader changed, need to reconnect", zap.String("server", s.name), zap.Bool("force", force))
			// if not force and the streaming is running, skip reconnecting
			if !force && s.streamingRunning.Load() {
				continue
			}
			if s.syncLoopCancelCtx != nil {
				s.syncLoopCancelCtx()
				s.syncLoopCancelCtx = nil
			}
		}
		s.startSyncWithLeader()
	}
}

func (s *RegionSyncer) startSyncWithLeader() {
	leaderAddr := s.pdLeaderAddr.Load()
	if leaderAddr == nil {
		log.Warn("leader addr is nil", zap.String("server", s.name))
		time.Sleep(retryInterval)
		s.reconnectCh <- false
		return
	}
	ctx, syncCancelFunc := context.WithCancel(s.serverCtx)
	s.syncLoopCancelCtx = syncCancelFunc
	log.Info("region syncer reconnect to leader", zap.String("server", s.name), zap.Any("leader", leaderAddr))
	s.wg.Add(1)
	go s.sync(ctx, leaderAddr.(string))
}

// startSyncWithLeader starts to sync with leader.
func (s *RegionSyncer) sync(ctx context.Context, leaderAddr string) {
	defer func() {
		select {
		case <-ctx.Done():
			log.Info("leader context is done, stop syncing regions",
				zap.String("server", s.name), zap.Uint64("next-index", s.nextSyncIndex))
		case <-time.After(retryInterval):
			s.reconnectCh <- false
		}
		logutil.LogPanic()
		s.wg.Done()
		s.streamingRunning.Store(false)
	}()
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

	// mark the client as running status when it finished the first history region sync.
	if !s.streamingRunning.CompareAndSwap(false, true) {
		log.Warn("sync region is still running, exit")
		return
	}
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
				err = errors.Errorf("sync met error")
			})
			if err == io.EOF {
				log.Info("region sync with leader meets EOF, stop syncing", zap.String("server", s.name))
				return
			}
			if err != nil {
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
			// pd leader expired and needs to reconnect.
			if e := resp.Header.GetError(); e != nil {
				log.Warn("server broken the connection, it needs to reconnect again", zap.String("error-message", e.GetMessage()))
				s.triggerMembershipCheck()
				return
			}
			// client maybe loss some region info, need to reset the nextSyncIndex
			if s.nextSyncIndex != resp.GetStartIndex() {
				log.Warn("server sync index not match the leader",
					zap.String("server", s.name),
					zap.Uint64("own-index", s.nextSyncIndex),
					zap.Uint64("leader-index", resp.GetStartIndex()),
					zap.Int("records-length", len(resp.GetRegions())))
				// reset index
				s.nextSyncIndex = resp.GetStartIndex()
			}
			syncIndexGauge.Set(float64(resp.GetStartIndex()))
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
				bc.PutRegion(region)
				if err == nil {
					lastIndexGauge.Set(float64(s.nextSyncIndex))
					s.nextSyncIndex++
				}
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
	return syncStream, err
}

// IsRunning returns whether the region syncer client is running.
func (s *RegionSyncer) IsRunning() bool {
	return s.streamingRunning.Load()
}

// Stop stops the region syncer.
func (s *RegionSyncer) Stop() {
	if s.cancelFunc != nil {
		s.cancelFunc()
	}
	s.wg.Wait()
}
