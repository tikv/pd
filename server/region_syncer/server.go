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
	"sync"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/ratelimit"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/storage"
	"go.uber.org/zap"
)

const (
	msgSize                  = 8 * units.MiB
	defaultBucketRate        = 20 * units.MiB // 20MB/s
	defaultBucketCapacity    = 20 * units.MiB // 20MB
	maxSyncRegionBatchSize   = 100
	syncerKeepAliveInterval  = 10 * time.Second
	defaultHistoryBufferSize = 10000
)

// WatchClientStream is the client side of watch.
type WatchClientStream interface {
	Recv() (*pdpb.WatchResponse, error)
	CloseSend() error
}

// ListClientStream is the client side of list.
type ListClientStream interface {
	Recv() (*pdpb.ListResponse, error)
	CloseSend() error
}

// ServerStream is the server side of the region syncer.
type ServerStream interface {
	Send(regions *pdpb.WatchResponse) error
}

// Server is the abstraction of the syncer storage server.
type Server interface {
	LoopContext() context.Context
	ClusterID() uint64
	GetMemberInfo() *pdpb.Member
	GetLeader() *pdpb.Member
	GetStorage() storage.Storage
	Name() string
	GetRegions() []*core.RegionInfo
	GetTLSConfig() *grpcutil.TLSConfig
	GetBasicCluster() *core.BasicCluster
}

// RegionSyncer is used to sync the region information without raft.
type RegionSyncer struct {
	mu struct {
		syncutil.RWMutex
		streams      map[string]ServerStream
		clientCtx    context.Context
		clientCancel context.CancelFunc
	}
	server    Server
	wg        sync.WaitGroup
	history   *historyBuffer
	limit     *ratelimit.RateLimiter
	tlsConfig *grpcutil.TLSConfig
}

// NewRegionSyncer returns a region syncer.
// The final consistency is ensured by the heartbeat.
// Strong consistency is not guaranteed.
// Usually open the region syncer in huge cluster and the server
// no longer etcd but go-leveldb.
func NewRegionSyncer(s Server) *RegionSyncer {
	localRegionStorage := storage.TryGetLocalRegionStorage(s.GetStorage())
	if localRegionStorage == nil {
		return nil
	}
	syncer := &RegionSyncer{
		server:    s,
		history:   newHistoryBuffer(defaultHistoryBufferSize, localRegionStorage.(kv.Base)),
		limit:     ratelimit.NewRateLimiter(defaultBucketRate, defaultBucketCapacity),
		tlsConfig: s.GetTLSConfig(),
	}
	syncer.mu.streams = make(map[string]ServerStream)
	return syncer
}

// RunServer runs the server of the region syncer.
// regionNotifier is used to get the changed regions.
func (s *RegionSyncer) RunServer(ctx context.Context, regionNotifier <-chan *core.RegionInfo) {
	var events []*pdpb.Event
	ticker := time.NewTicker(syncerKeepAliveInterval)

	defer func() {
		ticker.Stop()
		s.mu.Lock()
		s.mu.streams = make(map[string]ServerStream)
		s.mu.Unlock()
	}()

	for {
		select {
		case <-ctx.Done():
			log.Info("region syncer has been stopped")
			return
		case first := <-regionNotifier:
			bucket := &metapb.Buckets{}
			if b := first.GetBuckets(); b != nil {
				bucket = b
			}
			events = append(events, &pdpb.Event{Item: &pdpb.Item{Item: &pdpb.Item_RegionStat{RegionStat: &pdpb.RegionStats{
				Region:      first.GetMeta(),
				RegionStats: first.GetStat(),
				Leader:      first.GetLeader(),
				Buckets:     bucket,
			}}}})
			startIndex := s.history.GetNextIndex()
			s.history.Record(first)
			pending := len(regionNotifier)
			for i := 0; i < pending && i < maxSyncRegionBatchSize; i++ {
				region := <-regionNotifier
				bucket := &metapb.Buckets{}
				if b := region.GetBuckets(); b != nil {
					bucket = b
				}
				events = append(events, &pdpb.Event{Item: &pdpb.Item{Item: &pdpb.Item_RegionStat{RegionStat: &pdpb.RegionStats{
					Region:      region.GetMeta(),
					RegionStats: region.GetStat(),
					Leader:      region.GetLeader(),
					Buckets:     bucket,
				}}}})
				s.history.Record(region)
			}
			regions := &pdpb.WatchResponse{
				Revision: startIndex,
				Events:   events,
			}
			s.broadcast(regions)
		case <-ticker.C:
			alive := &pdpb.WatchResponse{
				Revision: s.history.GetNextIndex(),
			}
			s.broadcast(alive)
		}
		events = events[:0]
	}
}

// GetAllDownstreamNames tries to get the all bind stream's name.
// Only for test
func (s *RegionSyncer) GetAllDownstreamNames() []string {
	s.mu.RLock()
	names := make([]string, 0, len(s.mu.streams))
	for name := range s.mu.streams {
		names = append(names, name)
	}
	s.mu.RUnlock()
	return names
}

// FullSync is used to synchronize full regions with followers.
func (s *RegionSyncer) FullSync(ctx context.Context, stream pdpb.PD_ListServer) error {
	// do full synchronization
	regions := s.server.GetRegions()
	regionStats := make([]*pdpb.Item, 0, maxSyncRegionBatchSize)
	lastIndex := 0
	for syncedIndex, r := range regions {
		select {
		case <-ctx.Done():
			log.Info("discontinue sending sync region response")
			failpoint.Inject("noFastExitSync", func() {
				failpoint.Goto("doSync")
			})
			return nil
		default:
		}
		failpoint.Label("doSync")
		leader := &metapb.Peer{}
		if r.GetLeader() != nil {
			leader = r.GetLeader()
		}
		buckets := &metapb.Buckets{}
		if r.GetBuckets() != nil {
			buckets = r.GetBuckets()
		}
		regionStats = append(regionStats, &pdpb.Item{Item: &pdpb.Item_RegionStat{RegionStat: &pdpb.RegionStats{
			Region:      r.GetMeta(),
			RegionStats: r.GetStat(),
			Leader:      leader,
			Buckets:     buckets,
		}}})
		if len(regionStats) < maxSyncRegionBatchSize && syncedIndex < len(regions)-1 {
			continue
		}

		resp := &pdpb.ListResponse{
			Header:   &pdpb.ResponseHeader{ClusterId: s.server.ClusterID()},
			Items:    regionStats,
			Revision: uint64(lastIndex),
		}
		s.limit.WaitN(ctx, resp.Size())
		lastIndex += len(regionStats)
		if err := stream.Send(resp); err != nil {
			log.Error("failed to send sync region response", errs.ZapError(errs.ErrGRPCSend, err))
			return err
		}
		regionStats = regionStats[:0]
	}
	return nil
}

// BindStream binds the established server stream.
func (s *RegionSyncer) BindStream(name string, stream ServerStream) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.streams[name] = stream
}

func (s *RegionSyncer) broadcast(regions *pdpb.WatchResponse) {
	var failed []string
	s.mu.RLock()
	for name, sender := range s.mu.streams {
		err := sender.Send(regions)
		if err != nil {
			log.Error("region syncer send data meet error", errs.ZapError(errs.ErrGRPCSend, err))
			failed = append(failed, name)
		}
	}
	s.mu.RUnlock()
	if len(failed) > 0 {
		s.mu.Lock()
		for _, name := range failed {
			delete(s.mu.streams, name)
			log.Info("region syncer delete the stream", zap.String("stream", name))
		}
		s.mu.Unlock()
	}
}
