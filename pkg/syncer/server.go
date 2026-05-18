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
	"sync/atomic"
	"time"

	"github.com/docker/go-units"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
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
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

const (
	defaultBucketRate        = 20 * units.MiB // 20MB/s
	defaultBucketCapacity    = 20 * units.MiB // 20MB
	maxSyncRegionBatchSize   = 1000
	syncerKeepAliveInterval  = 10 * time.Second
	defaultHistoryBufferSize = 10000
)

// ClientStream is the client side of the region syncer.
type ClientStream interface {
	Recv() (*pdpb.SyncRegionResponse, error)
	CloseSend() error
}

// ServerStream is the server side of the region syncer.
type ServerStream interface {
	Send(regions *pdpb.SyncRegionResponse) error
}

type regionSyncStream struct {
	stream ServerStream
	done   chan struct{}
	once   sync.Once
}

func newRegionSyncStream(stream ServerStream) *regionSyncStream {
	return &regionSyncStream{
		stream: stream,
		done:   make(chan struct{}),
	}
}

func (s *regionSyncStream) close() {
	s.once.Do(func() {
		close(s.done)
	})
}

// Server is the abstraction of the syncer storage server.
type Server interface {
	LoopContext() context.Context
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
		streams      map[string]*regionSyncStream
		clientCtx    context.Context
		clientCancel context.CancelFunc
	}
	server      Server
	wg          sync.WaitGroup
	history     *historyBuffer
	limit       *ratelimit.RateLimiter
	sendTimeout time.Duration
	tlsConfig   *grpcutil.TLSConfig
	// status when as client
	streamingRunning atomic.Bool
}

// NewRegionSyncer returns a region syncer that ensures final consistency through the heartbeat,
// but it does not guarantee strong consistency. Using the same storage backend of the region storage.
func NewRegionSyncer(s Server) *RegionSyncer {
	regionStorage := storage.RetrieveRegionStorage(s.GetStorage())
	if regionStorage == nil {
		return nil
	}
	syncer := &RegionSyncer{
		server:      s,
		history:     newHistoryBuffer(defaultHistoryBufferSize, regionStorage.(kv.Base)),
		limit:       ratelimit.NewRateLimiter(defaultBucketRate, defaultBucketCapacity),
		sendTimeout: syncerKeepAliveInterval,
		tlsConfig:   s.GetTLSConfig(),
	}
	syncer.mu.streams = make(map[string]*regionSyncStream)
	return syncer
}

// RunServer runs the server of the region syncer.
// regionNotifier is used to get the changed regions.
func (s *RegionSyncer) RunServer(ctx context.Context, regionNotifier <-chan *core.RegionInfo) {
	var requests []*metapb.Region
	var stats []*pdpb.RegionStat
	var leaders []*metapb.Peer
	var buckets []*metapb.Buckets
	ticker := time.NewTicker(syncerKeepAliveInterval)

	processRegion := func(region *core.RegionInfo) {
		requests = append(requests, region.GetMeta())
		stats = append(stats, region.GetStat())
		// bucket should not be nil to avoid grpc marshal panic.
		bucket := &metapb.Buckets{}
		if b := region.GetBuckets(); b != nil {
			bucket = b
		}
		buckets = append(buckets, bucket)
		leaders = append(leaders, region.GetLeader())
	}

	defer func() {
		ticker.Stop()
		s.mu.Lock()
		for _, stream := range s.mu.streams {
			stream.close()
		}
		s.mu.streams = make(map[string]*regionSyncStream)
		s.mu.Unlock()
	}()

	for {
		select {
		case <-ctx.Done():
			log.Info("region syncer has been stopped")
			return
		case first := <-regionNotifier:
			failpoint.InjectCall("syncRegionChannelFull")

			processRegion(first)
			startIndex := s.history.getNextIndex()
			s.history.record(first)
		loop:
			for range maxSyncRegionBatchSize {
				select {
				case region := <-regionNotifier:
					processRegion(region)
					s.history.record(region)
				default:
					break loop
				}
			}
			regions := &pdpb.SyncRegionResponse{
				Header:        &pdpb.ResponseHeader{ClusterId: keypath.ClusterID()},
				Regions:       requests,
				StartIndex:    startIndex,
				RegionStats:   stats,
				RegionLeaders: leaders,
				Buckets:       buckets,
			}
			s.broadcast(ctx, regions)
		case <-ticker.C:
			alive := &pdpb.SyncRegionResponse{
				Header:     &pdpb.ResponseHeader{ClusterId: keypath.ClusterID()},
				StartIndex: s.history.getNextIndex(),
			}
			s.broadcast(ctx, alive)
		}
		requests = requests[:0]
		stats = stats[:0]
		leaders = leaders[:0]
		buckets = buckets[:0]
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
		request, err := recvSyncRegionRequest(ctx, stream)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			if _, ok := status.FromError(err); ok {
				return err
			}
			return errors.WithStack(err)
		}
		if request == nil {
			return nil
		}
		clusterID := request.GetHeader().GetClusterId()
		if clusterID != keypath.ClusterID() {
			return status.Errorf(codes.FailedPrecondition, "mismatch cluster id, need %d but got %d", keypath.ClusterID(), clusterID)
		}
		log.Info("establish sync region stream",
			zap.String("requested-server", request.GetMember().GetName()),
			zap.String("url", request.GetMember().GetClientUrls()[0]))

		err = s.syncHistoryRegion(ctx, request, stream)
		if err != nil {
			return err
		}
		name := request.GetMember().GetName()
		syncStream := s.bindStream(name, stream)
		select {
		case <-ctx.Done():
			s.unbindStream(name, syncStream)
			return status.Error(codes.Unavailable, "region syncer stopped")
		case <-stream.Context().Done():
			s.unbindStream(name, syncStream)
			return nil
		case <-syncStream.done:
			s.unbindStream(name, syncStream)
			return status.Error(codes.Unavailable, "region syncer stream closed")
		}
	}
}

type syncRegionRequestResult struct {
	request *pdpb.SyncRegionRequest
	err     error
}

func recvSyncRegionRequest(ctx context.Context, stream pdpb.PD_SyncRegionsServer) (*pdpb.SyncRegionRequest, error) {
	resultCh := make(chan syncRegionRequestResult, 1)
	go func() {
		request, err := stream.Recv()
		resultCh <- syncRegionRequestResult{request: request, err: err}
	}()

	select {
	case <-ctx.Done():
		return nil, status.Error(codes.Unavailable, "region syncer stopped")
	case <-stream.Context().Done():
		return nil, nil
	case result := <-resultCh:
		return result.request, result.err
	}
}

func (s *RegionSyncer) syncHistoryRegion(ctx context.Context, request *pdpb.SyncRegionRequest, stream pdpb.PD_SyncRegionsServer) error {
	startIndex := request.GetStartIndex()
	name := request.GetMember().GetName()
	records := s.history.recordsFrom(startIndex)
	if len(records) == 0 {
		if s.history.getNextIndex() == startIndex {
			log.Info("requested server has already in sync with server",
				zap.String("requested-server", name), zap.String("server", s.server.Name()), zap.Uint64("last-index", startIndex))
			// still send a response to follower to show the history region sync.
			resp := &pdpb.SyncRegionResponse{
				Header:        &pdpb.ResponseHeader{ClusterId: keypath.ClusterID()},
				Regions:       nil,
				StartIndex:    startIndex,
				RegionStats:   nil,
				RegionLeaders: nil,
				Buckets:       nil,
			}
			return stream.Send(resp)
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
					Header:        &pdpb.ResponseHeader{ClusterId: keypath.ClusterID()},
					Regions:       metas,
					StartIndex:    uint64(lastIndex),
					RegionStats:   stats,
					RegionLeaders: leaders,
					Buckets:       buckets,
				}
				if err := s.limit.WaitN(ctx, resp.Size()); err != nil {
					log.Error("failed to wait rate limit", errs.ZapError(err))
					return err
				}
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
		zap.Uint64("last-index", s.history.getNextIndex()),
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
		Header:        &pdpb.ResponseHeader{ClusterId: keypath.ClusterID()},
		Regions:       regions,
		StartIndex:    startIndex,
		RegionStats:   stats,
		RegionLeaders: leaders,
		Buckets:       buckets,
	}
	return stream.Send(resp)
}

// bindStream binds the established server stream.
func (s *RegionSyncer) bindStream(name string, stream ServerStream) *regionSyncStream {
	syncStream := newRegionSyncStream(stream)
	s.mu.Lock()
	defer s.mu.Unlock()
	if oldStream := s.mu.streams[name]; oldStream != nil {
		oldStream.close()
	}
	s.mu.streams[name] = syncStream
	return syncStream
}

func (s *RegionSyncer) unbindStream(name string, stream *regionSyncStream) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.streams[name] == stream {
		delete(s.mu.streams, name)
	}
	stream.close()
}

func (s *RegionSyncer) broadcast(ctx context.Context, regions *pdpb.SyncRegionResponse) {
	broadcastDone := make(chan struct{})

	defer logutil.LogPanic()
	var (
		failed sync.Map
		wg     sync.WaitGroup
	)
	s.mu.RLock()
	for name, sender := range s.mu.streams {
		select {
		case <-ctx.Done():
			s.mu.RUnlock()
			return
		default:
		}

		wg.Add(1)
		go func(name string, sender *regionSyncStream) {
			defer wg.Done()
			if !s.sendRegionSyncResponse(ctx, name, sender, regions) {
				failed.Store(name, sender)
			}
		}(name, sender)
	}
	s.mu.RUnlock()

	go func() {
		wg.Wait()
		s.mu.Lock()
		failed.Range(func(key, value any) bool {
			name := key.(string)
			stream := value.(*regionSyncStream)
			if s.mu.streams[name] == stream {
				delete(s.mu.streams, name)
				stream.close()
				log.Info("region syncer delete the stream", zap.String("stream", name))
			}
			return true
		})
		s.mu.Unlock()
		close(broadcastDone)
	}()

	select {
	case <-broadcastDone:
	case <-ctx.Done():
	}
}

func (s *RegionSyncer) sendRegionSyncResponse(
	ctx context.Context,
	name string,
	sender *regionSyncStream,
	regions *pdpb.SyncRegionResponse,
) bool {
	failpoint.Inject("regionSyncerSendFail", func(val failpoint.Value) {
		if targetName, ok := val.(string); ok && targetName == name {
			err := errors.New("injected region sync send failure")
			log.Warn("region syncer send data meet error", zap.String("name", name),
				errs.ZapError(errs.ErrGRPCSend, err))
			failpoint.Return(false)
		}
	})

	resultCh := make(chan error, 1)
	go func() {
		resultCh <- sender.stream.Send(regions)
	}()

	timeout := s.sendTimeout
	if timeout <= 0 {
		timeout = syncerKeepAliveInterval
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case err := <-resultCh:
		if err != nil {
			log.Warn("region syncer send data meet error", zap.String("name", name),
				errs.ZapError(errs.ErrGRPCSend, err))
			return false
		}
		return true
	case <-ctx.Done():
		sender.close()
		log.Warn("region syncer send data canceled", zap.String("name", name),
			errs.ZapError(errs.ErrGRPCSend, ctx.Err()))
		return false
	case <-timer.C:
		sender.close()
		log.Warn("region syncer send data timeout", zap.String("name", name), zap.Duration("timeout", timeout))
		return false
	}
}

func (s *RegionSyncer) closeAllClient() {
	s.mu.RLock()
	streams := make([]*regionSyncStream, 0, len(s.mu.streams))
	for _, sender := range s.mu.streams {
		streams = append(streams, sender)
	}
	s.mu.RUnlock()
	for _, sender := range streams {
		resp := &pdpb.SyncRegionResponse{
			Header: &pdpb.ResponseHeader{
				ClusterId: keypath.ClusterID(),
				Error: &pdpb.Error{
					Type:    pdpb.ErrorType_UNKNOWN,
					Message: "server stopped, close the region syncer client",
				},
			},
		}
		sender.close()
		if err := sender.stream.Send(resp); err != nil {
			log.Warn("region syncer send close message meet error", errs.ZapError(errs.ErrGRPCSend, err))
		}
	}
}
