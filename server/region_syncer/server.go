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
	"sync"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	msgSize                  = 8 * units.MiB
	defaultBucketRate        = 20 * units.MiB // 20MB/s
	defaultBucketCapacity    = 20 * units.MiB // 20MB
	maxSyncRegionBatchSize   = 100
	keepAliveInterval        = 10 * time.Second
	defaultHistoryBufferSize = 10000
)

// ClientStream is the client side of the region syncer.
type ClientStream interface {
	Recv() (*pdpb.SyncRegionResponse, error)
	CloseSend() error
}

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
	Send(regions *pdpb.SyncRegionResponse) error
}

// WatchStream is the server side of the region syncer.
type WatchStream interface {
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
		watcher      map[string]WatchStream
		clientCtx    context.Context
		clientCancel context.CancelFunc
	}
	server    Server
	wg        sync.WaitGroup
	history   *historyBuffer
	limit     *ratelimit.RateLimiter
	tlsConfig *grpcutil.TLSConfig
	// TODO: make it configurable and use it as a switch.
	enableWatch bool
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
	syncer.enableWatch = false
	if syncer.enableWatch {
		syncer.mu.watcher = make(map[string]WatchStream)
	} else {
		syncer.mu.streams = make(map[string]ServerStream)
	}

	return syncer
}

// RunServer runs the server of the region syncer.
// regionNotifier is used to get the changed regions.
func (s *RegionSyncer) RunServer(ctx context.Context, regionNotifier <-chan *core.RegionInfo) {
	var requests []*metapb.Region
	var stats []*pdpb.RegionStat
	var leaders []*metapb.Peer
	var buckets []*metapb.Buckets
	ticker := time.NewTicker(keepAliveInterval)

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
			requests = append(requests, first.GetMeta())
			stats = append(stats, first.GetStat())
			// bucket should not be nil to avoid grpc marshal panic.
			bucket := &metapb.Buckets{}
			if b := first.GetBuckets(); b != nil {
				bucket = b
			}
			buckets = append(buckets, bucket)
			leaders = append(leaders, first.GetLeader())
			startIndex := s.history.GetNextIndex()
			s.history.Record(first)
			pending := len(regionNotifier)
			for i := 0; i < pending && i < maxSyncRegionBatchSize; i++ {
				region := <-regionNotifier
				requests = append(requests, region.GetMeta())
				stats = append(stats, region.GetStat())
				// bucket should not be nil to avoid grpc marshal panic.
				bucket := &metapb.Buckets{}
				if b := region.GetBuckets(); b != nil {
					bucket = b
				}
				buckets = append(buckets, bucket)
				leaders = append(leaders, region.GetLeader())
				s.history.Record(region)
			}
			regions := &pdpb.SyncRegionResponse{
				Header:        &pdpb.ResponseHeader{ClusterId: s.server.ClusterID()},
				Regions:       requests,
				StartIndex:    startIndex,
				RegionStats:   stats,
				RegionLeaders: leaders,
				Buckets:       buckets,
			}
			s.broadcast(regions)
		case <-ticker.C:
			alive := &pdpb.SyncRegionResponse{
				Header:     &pdpb.ResponseHeader{ClusterId: s.server.ClusterID()},
				StartIndex: s.history.GetNextIndex(),
			}
			s.broadcast(alive)
		}
		requests = requests[:0]
		stats = stats[:0]
		leaders = leaders[:0]
		buckets = buckets[:0]
	}
}

// RunWatchServer runs the server of the region syncer.
// regionNotifier is used to get the changed regions.
func (s *RegionSyncer) RunWatchServer(ctx context.Context, regionNotifier <-chan *core.RegionInfo) {
	var events []*pdpb.Event
	ticker := time.NewTicker(keepAliveInterval)

	defer func() {
		ticker.Stop()
		s.mu.Lock()
		s.mu.watcher = make(map[string]WatchStream)
		s.mu.Unlock()
	}()

	for {
		select {
		case <-ctx.Done():
			log.Info("region syncer has been stopped")
			return
		case first := <-regionNotifier:
			// bucket should not be nil to avoid grpc marshal panic.
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
				// bucket should not be nil to avoid grpc marshal panic.
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
			s.notify(regions)
		case <-ticker.C:
			alive := &pdpb.WatchResponse{
				Revision: s.history.GetNextIndex(),
			}
			s.notify(alive)
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

// Sync firstly tries to sync the history records to client.
// then to sync the latest records.
func (s *RegionSyncer) Sync(ctx context.Context, stream pdpb.PD_SyncRegionsServer) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		request, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return errors.WithStack(err)
		}
		clusterID := request.GetHeader().GetClusterId()
		if clusterID != s.server.ClusterID() {
			return status.Errorf(codes.FailedPrecondition, "mismatch cluster id, need %d but got %d", s.server.ClusterID(), clusterID)
		}
		log.Info("establish sync region stream",
			zap.String("requested-server", request.GetMember().GetName()),
			zap.String("url", request.GetMember().GetClientUrls()[0]))

		err = s.syncHistoryRegion(ctx, request, stream)
		if err != nil {
			return err
		}
		s.bindStream(request.GetMember().GetName(), stream)
	}
}

func (s *RegionSyncer) syncHistoryRegion(ctx context.Context, request *pdpb.SyncRegionRequest, stream pdpb.PD_SyncRegionsServer) error {
	startIndex := request.GetStartIndex()
	name := request.GetMember().GetName()
	records := s.history.RecordsFrom(startIndex)
	if len(records) == 0 {
		if s.history.GetNextIndex() == startIndex {
			log.Info("requested server has already in sync with server",
				zap.String("requested-server", name), zap.String("server", s.server.Name()), zap.Uint64("last-index", startIndex))
			return nil
		}
		// do full synchronization
		if startIndex == 0 {
			regions := s.server.GetRegions()
			lastIndex := 0
			start := time.Now()
			metas := make([]*metapb.Region, 0, maxSyncRegionBatchSize)
			stats := make([]*pdpb.RegionStat, 0, maxSyncRegionBatchSize)
			leaders := make([]*metapb.Peer, 0, maxSyncRegionBatchSize)
			buckets := make([]*metapb.Buckets, 0, maxSyncRegionBatchSize)
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
				metas = append(metas, r.GetMeta())
				stats = append(stats, r.GetStat())
				leader := &metapb.Peer{}
				if r.GetLeader() != nil {
					leader = r.GetLeader()
				}
				leaders = append(leaders, leader)
				bucket := &metapb.Buckets{}
				if r.GetBuckets() != nil {
					bucket = r.GetBuckets()
				}
				buckets = append(buckets, bucket)
				if len(metas) < maxSyncRegionBatchSize && syncedIndex < len(regions)-1 {
					continue
				}
				resp := &pdpb.SyncRegionResponse{
					Header:        &pdpb.ResponseHeader{ClusterId: s.server.ClusterID()},
					Regions:       metas,
					StartIndex:    uint64(lastIndex),
					RegionStats:   stats,
					RegionLeaders: leaders,
					Buckets:       buckets,
				}
				s.limit.WaitN(ctx, resp.Size())
				lastIndex += len(metas)
				if err := stream.Send(resp); err != nil {
					log.Error("failed to send sync region response", errs.ZapError(errs.ErrGRPCSend, err))
					return err
				}
				metas = metas[:0]
				stats = stats[:0]
				leaders = leaders[:0]
				buckets = buckets[:0]
			}
			log.Info("requested server has completed full synchronization with server",
				zap.String("requested-server", name), zap.String("server", s.server.Name()), zap.Duration("cost", time.Since(start)))
			return nil
		}
		log.Warn("no history regions from index, the leader may be restarted", zap.Uint64("index", startIndex))
		return nil
	}
	log.Info("sync the history regions with server",
		zap.String("server", name),
		zap.Uint64("from-index", startIndex),
		zap.Uint64("last-index", s.history.GetNextIndex()),
		zap.Int("records-length", len(records)))
	regions := make([]*metapb.Region, len(records))
	stats := make([]*pdpb.RegionStat, len(records))
	leaders := make([]*metapb.Peer, len(records))
	buckets := make([]*metapb.Buckets, len(records))
	for i, r := range records {
		regions[i] = r.GetMeta()
		stats[i] = r.GetStat()
		leader := &metapb.Peer{}
		if r.GetLeader() != nil {
			leader = r.GetLeader()
		}
		leaders[i] = leader
		// bucket should not be nil to avoid grpc marshal panic.
		buckets[i] = &metapb.Buckets{}
		if r.GetBuckets() != nil {
			buckets[i] = r.GetBuckets()
		}
	}
	resp := &pdpb.SyncRegionResponse{
		Header:        &pdpb.ResponseHeader{ClusterId: s.server.ClusterID()},
		Regions:       regions,
		StartIndex:    startIndex,
		RegionStats:   stats,
		RegionLeaders: leaders,
		Buckets:       buckets,
	}
	return stream.Send(resp)
}

// bindStream binds the established server stream.
func (s *RegionSyncer) bindStream(name string, stream ServerStream) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.streams[name] = stream
}

// AddWatcher adds a watcher for synchronize the regions.
func (s *RegionSyncer) AddWatcher(name string, stream WatchStream) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.watcher[name] = stream
}

func (s *RegionSyncer) broadcast(regions *pdpb.SyncRegionResponse) {
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

func (s *RegionSyncer) notify(regions *pdpb.WatchResponse) {
	var failed []string
	s.mu.RLock()
	for name, sender := range s.mu.watcher {
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
			delete(s.mu.watcher, name)
			log.Info("region syncer delete the watcher", zap.String("watcher", name))
		}
		s.mu.Unlock()
	}
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
			log.Info("stop sending sync region response due to the context canceled")
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
			Header: &pdpb.ResponseHeader{ClusterId: s.server.ClusterID()},
			Items:  regionStats,
			// The revision here is not the real revision.
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
