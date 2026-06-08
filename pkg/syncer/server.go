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
	"github.com/tikv/pd/pkg/memory"
	"github.com/tikv/pd/pkg/ratelimit"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

const (
	defaultBucketRate           = 20 * units.MiB // 20MB/s
	defaultBucketCapacity       = 20 * units.MiB // 20MB
	maxSyncRegionBatchSize      = 1000
	syncerKeepAliveInterval     = 10 * time.Second
	historyBufferShrinkInterval = 5 * time.Minute
	// The frequency to check and publish the region count the leader is
	// serving so that other members can tell whether they are caught up
	// before campaigning.
	committedRegionCountInterval = time.Second
	defaultHistoryBufferSize     = 10000
	historyBufferMemoryStep      = 4 * 1024 * 1024 * 1024
	maxHistoryBufferSize         = 80000
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
	sendMu sync.Mutex
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

func (s *regionSyncStream) send(regions *pdpb.SyncRegionResponse) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	return s.stream.Send(regions)
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
	// attempted sync status as client, sticky for the process lifetime.
	// It is used to distinguish follower from never attempted to sync (e.g., bootstrapp).
	attemptedSync atomic.Bool
	// status of the historitcal catch-up as client, sticky for the process lifetime.
	// set to true once the client has observed that it has completed the historitcal
	// catch-up/ the local region storage is durably populated.
	historySynced atomic.Bool
}

// NewRegionSyncer returns a region syncer that ensures final consistency through the heartbeat,
// but it does not guarantee strong consistency. Using the same storage backend of the region storage.
func NewRegionSyncer(s Server) *RegionSyncer {
	regionStorage := storage.RetrieveRegionStorage(s.GetStorage())
	if regionStorage == nil {
		return nil
	}
	historyBufferMaxSize := historyBufferMaxSizeFromMemory(memory.GetMemTotalIgnoreErr())
	syncer := &RegionSyncer{
		server:      s,
		history:     newHistoryBuffer(defaultHistoryBufferSize, historyBufferMaxSize, regionStorage.(kv.Base)),
		limit:       ratelimit.NewRateLimiter(defaultBucketRate, defaultBucketCapacity),
		sendTimeout: syncerKeepAliveInterval,
		tlsConfig:   s.GetTLSConfig(),
	}
	syncer.mu.streams = make(map[string]*regionSyncStream)
	syncer.reloadHistorySyncedFromDurableState()
	return syncer
}

func historyBufferMaxSizeFromMemory(totalMemory uint64) int {
	if totalMemory == 0 {
		return defaultHistoryBufferSize
	}
	size := int(uint64(defaultHistoryBufferSize) * totalMemory / historyBufferMemoryStep)
	if size < defaultHistoryBufferSize {
		return defaultHistoryBufferSize
	}
	size = normalizeHistoryBufferCapacity(size, historyBufferCapacityUnit)
	if size > maxHistoryBufferSize {
		return maxHistoryBufferSize
	}
	return size
}

// This function seeds the in-memory historySynced
// flag from the persisted historyIndex so a node that previously completed
// a sync isn't permanently locked out of campaigning after a restart
// followed by a leader death mid-sync. A non-zero index only ever lands on
// disk via commit() (after a bulk applied via SaveRegion) or via record()'s
// flushCount path, so a non-zero index implies the local region storage was
// populated by a prior successful catch-up — sufficient evidence of durable
// state without any extra probe. Invoked once from NewRegionSyncer; safe to
// call before any sync session because nothing else has touched historySynced yet.
// TODO : this doesn't attempt to fix the gap problem https://github.com/tikv/pd/issues/10668
func (s *RegionSyncer) reloadHistorySyncedFromDurableState() {
	if s.history.getNextIndex() > 0 {
		s.historySynced.Store(true)
	}
}

// RunServer runs the server of the region syncer.
// regionNotifier is used to get the changed regions.
func (s *RegionSyncer) RunServer(ctx context.Context, regionNotifier <-chan *core.RegionInfo) {
	var requests []*metapb.Region
	var stats []*pdpb.RegionStat
	var leaders []*metapb.Peer
	var buckets []*metapb.Buckets
	keepAliveTicker := time.NewTicker(syncerKeepAliveInterval)
	shrinkTicker := time.NewTicker(historyBufferShrinkInterval)
	committedCountTicker := time.NewTicker(committedRegionCountInterval)
	// -1 forces an initial publish on the first tick.
	lastPublishedRegionCount := -1

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
		keepAliveTicker.Stop()
		shrinkTicker.Stop()
		committedCountTicker.Stop()
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
			s.closeAllClient()
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
		case <-shrinkTicker.C:
			s.history.maybeShrink()
		case <-committedCountTicker.C:
			// Publish the region count this leader is serving so that a member
			// that has not finished a history sync can still decide, after this
			// leader is gone, whether it is caught up enough to campaign. Only
			// the leader runs RunServer, so only the leader writes this.
			if count := s.server.GetBasicCluster().GetTotalRegionCount(); count != lastPublishedRegionCount {
				if err := s.server.GetStorage().SaveRegionSyncerCommittedRegionCount(uint64(count)); err != nil {
					log.Warn("failed to persist committed region count", errs.ZapError(err))
				} else {
					lastPublishedRegionCount = count
				}
			}
		case <-keepAliveTicker.C:
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
			return errs.ErrMismatchClusterID(keypath.ClusterID(), clusterID)
		}
		log.Info("establish sync region stream",
			zap.String("requested-server", request.GetMember().GetName()),
			zap.String("url", request.GetMember().GetClientUrls()[0]))

		name := request.GetMember().GetName()
		syncStream, err := s.syncHistoryRegion(ctx, request, stream)
		if err != nil {
			return err
		}
		if syncStream == nil {
			syncStream = s.bindStream(name, stream)
		}
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

func (s *RegionSyncer) syncHistoryRegion(ctx context.Context, request *pdpb.SyncRegionRequest, stream pdpb.PD_SyncRegionsServer) (*regionSyncStream, error) {
	startIndex := request.GetStartIndex()
	name := request.GetMember().GetName()
	records, nextIndex := s.history.observeAndRecordsFrom(startIndex)
	if len(records) == 0 {
		if nextIndex == startIndex {
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
			return nil, stream.Send(resp)
		}
		if startIndex < nextIndex {
			return s.syncFullRegions(ctx, name, stream)
		}
		log.Warn("no history regions from index, the leader may be restarted", zap.Uint64("index", startIndex))
		return nil, nil
	}
	log.Info("sync the history regions with server",
		zap.String("server", name),
		zap.Uint64("from-index", startIndex),
		zap.Uint64("last-index", nextIndex),
		zap.Int("records-length", len(records)))
	// TODO: Incremental history replay is sent before binding the stream, so
	// broadcasts produced during this replay are not delivered to this follower.
	// Handle this existing ordering gap in a follow-up PR together with stream
	// binding and send-ordering semantics.
	return nil, s.syncHistoryRecords(startIndex, records, stream)
}

func (*RegionSyncer) syncHistoryRecords(startIndex uint64, records []*core.RegionInfo, stream pdpb.PD_SyncRegionsServer) error {
	for start := 0; start < len(records); start += maxSyncRegionBatchSize {
		end := min(start+maxSyncRegionBatchSize, len(records))
		regions := make([]*metapb.Region, end-start)
		stats := make([]*pdpb.RegionStat, end-start)
		leaders := make([]*metapb.Peer, end-start)
		buckets := make([]*metapb.Buckets, end-start)
		for i, r := range records[start:end] {
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
			StartIndex:    startIndex + uint64(start),
			RegionStats:   stats,
			RegionLeaders: leaders,
			Buckets:       buckets,
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
	// End-of-history marker so the follower commits the catch-up records
	// it just received without waiting for the next keepalive tick.
	marker := &pdpb.SyncRegionResponse{
		Header:     &pdpb.ResponseHeader{ClusterId: keypath.ClusterID()},
		StartIndex: startIndex + uint64(len(records)),
	}
	if err := stream.Send(marker); err != nil {
		log.Error("failed to send end-of-history marker", errs.ZapError(errs.ErrGRPCSend, err))
		return status.Errorf(codes.Internal, "failed to send end-of-history marker: %v", err)
	}
	return nil
}

func (s *RegionSyncer) syncFullRegions(ctx context.Context, name string, stream pdpb.PD_SyncRegionsServer) (*regionSyncStream, error) {
	retainIndex, releaseRetain := s.history.retainFromCurrent()
	defer releaseRetain()
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
			return nil, nil
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
			return nil, err
		}
		lastIndex += len(metas)
		if err := stream.Send(resp); err != nil {
			log.Error("failed to send sync region response", errs.ZapError(errs.ErrGRPCSend, err))
			return nil, err
		}
		metas = metas[:0]
		stats = stats[:0]
		leaders = leaders[:0]
		buckets = buckets[:0]
	}
	log.Info("requested server has completed full synchronization with server",
		zap.String("requested-server", name), zap.String("server", s.server.Name()), zap.Duration("cost", time.Since(start)))
	syncStream := newRegionSyncStream(stream)
	syncStream.sendMu.Lock()
	s.bindRegionSyncStream(name, syncStream)
	records, nextIndex, ok := s.history.retainedRecordsFrom(retainIndex)
	if !ok {
		s.unbindStream(name, syncStream)
		syncStream.sendMu.Unlock()
		return nil, status.Errorf(codes.ResourceExhausted,
			"history records from full sync start index %d to %d are no longer available",
			retainIndex, nextIndex)
	}
	// Emit exactly one end-of-history marker, after the retained delta (the
	// region changes that happened during the bulk transfer) has been replayed,
	// so the follower commits and flips historySynced only once it is truly
	// caught up. The marker carries the history index (nextIndex), not the
	// snapshot row count. A transfer cut short by leader failover never reaches
	// this point, so the follower rolls back to its last committed index (0 for
	// a fresh member) and re-syncs from the next leader.
	if len(records) > 0 {
		// syncHistoryRecords replays the records and sends the trailing marker.
		if err := s.syncHistoryRecords(retainIndex, records, stream); err != nil {
			s.unbindStream(name, syncStream)
			syncStream.sendMu.Unlock()
			return nil, err
		}
	} else {
		marker := &pdpb.SyncRegionResponse{
			Header:     &pdpb.ResponseHeader{ClusterId: keypath.ClusterID()},
			StartIndex: nextIndex,
		}
		if err := stream.Send(marker); err != nil {
			s.unbindStream(name, syncStream)
			syncStream.sendMu.Unlock()
			log.Error("failed to send end-of-history marker", errs.ZapError(errs.ErrGRPCSend, err))
			return nil, status.Errorf(codes.Internal, "failed to send end-of-history marker: %v", err)
		}
	}
	syncStream.sendMu.Unlock()
	return syncStream, nil
}

// bindStream binds the established server stream.
func (s *RegionSyncer) bindStream(name string, stream ServerStream) *regionSyncStream {
	syncStream := newRegionSyncStream(stream)
	return s.bindRegionSyncStream(name, syncStream)
}

func (s *RegionSyncer) bindRegionSyncStream(name string, syncStream *regionSyncStream) *regionSyncStream {
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
	var sendErr error
	failpoint.InjectCall("regionSyncerSendFail", name, &sendErr)
	if sendErr != nil {
		log.Warn("region syncer send data meet error", zap.String("name", name),
			errs.ZapError(errs.ErrGRPCSend, sendErr))
		return false
	}

	resultCh := make(chan error, 1)
	go func() {
		resultCh <- sender.send(regions)
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
		if err := sender.send(resp); err != nil {
			log.Warn("region syncer send close message meet error", errs.ZapError(errs.ErrGRPCSend, err))
		}
	}
}
