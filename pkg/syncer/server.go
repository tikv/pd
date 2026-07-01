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
	// stream serializes the underlying gRPC Send calls. A timed-out send may
	// still be blocked after sendMu is released.
	stream struct {
		syncutil.Mutex
		ServerStream
	}
	// sendMu serializes the logical send flow and protects sendIndex.
	sendMu struct {
		syncutil.Mutex
		sendIndex uint64
	}
	notifyCh chan bool
	done     chan struct{}
	once     sync.Once
}

func newRegionSyncStream(stream ServerStream, startIndex uint64) *regionSyncStream {
	return &regionSyncStream{
		stream: struct {
			syncutil.Mutex
			ServerStream
		}{
			ServerStream: stream,
		},
		sendMu: struct {
			syncutil.Mutex
			sendIndex uint64
		}{
			sendIndex: startIndex,
		},
		notifyCh: make(chan bool, 1),
		done:     make(chan struct{}),
	}
}

func (s *regionSyncStream) getSendIndex() uint64 {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	return s.getSendIndexLocked()
}

func (s *regionSyncStream) getSendIndexLocked() uint64 {
	return s.sendMu.sendIndex
}

func (s *regionSyncStream) advanceSendIndex(count int) {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	s.advanceSendIndexLocked(count)
}

func (s *regionSyncStream) advanceSendIndexLocked(count int) {
	s.sendMu.sendIndex += uint64(count)
}

func (s *regionSyncStream) notify(keepAlive bool) {
	select {
	case s.notifyCh <- keepAlive:
	default:
	}
}

func (s *regionSyncStream) close() {
	s.once.Do(func() {
		close(s.done)
	})
}

func (s *regionSyncStream) checkOpen() error {
	select {
	case <-s.done:
		return status.Error(codes.Unavailable, "region syncer stream closed")
	default:
		return nil
	}
}

func (s *regionSyncStream) send(regions *pdpb.SyncRegionResponse) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	return s.sendStream(regions)
}

func (s *regionSyncStream) sendStreamIfOpen(regions *pdpb.SyncRegionResponse) error {
	s.stream.Lock()
	defer s.stream.Unlock()
	if err := s.checkOpen(); err != nil {
		return err
	}
	return s.stream.Send(regions)
}

func (s *regionSyncStream) sendStream(regions *pdpb.SyncRegionResponse) error {
	s.stream.Lock()
	defer s.stream.Unlock()
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

// historyBufferMaxSizeFromMemory returns the maximum history buffer size scaled
// to the total system memory, bounded by the default and maximum sizes.
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

// reloadHistorySyncedFromDurableState seeds the in-memory historySynced
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
	var records []*core.RegionInfo
	keepAliveTicker := time.NewTicker(syncerKeepAliveInterval)
	shrinkTicker := time.NewTicker(historyBufferShrinkInterval)
	committedCountTicker := time.NewTicker(committedRegionCountInterval)
	// -1 forces an initial publish on the first tick.
	lastPublishedRegionCount := -1
	publishCommittedRegionCount := func() {
		count := s.server.GetBasicCluster().GetTotalRegionCount()
		// Only advertise a non-zero committed count once regions have actually
		// entered the syncer history and thus become syncable by followers.
		// Regions present in the basic cluster but that never flowed through the
		// heartbeat -> changedRegions -> history path (most notably the bootstrap
		// region) cannot have been synced by any follower. Counting them would
		// keep the region-syncer campaign gate (canCampaignAsRegionSyncerCaughtUp)
		// permanently closed and stall the next leader election until the grace
		// elapses, even though no follower could possibly be caught up.
		if s.history.getNextIndex() == 0 {
			count = 0
		}
		if count != lastPublishedRegionCount {
			if err := s.server.GetStorage().SaveRegionSyncerCommittedRegionCount(uint64(count)); err != nil {
				log.Warn("failed to persist committed region count", errs.ZapError(err))
			} else {
				lastPublishedRegionCount = count
			}
		}
	}
	// Publish the region count immediately to minimize the poissibility of a leader dies
	// immediately and a new campaign gate misclassifies an unsynced follower as
	// already caught up.
	publishCommittedRegionCount()

	processRegion := func(region *core.RegionInfo) {
		records = append(records, region)
		s.history.record(region)
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
		loop:
			for range maxSyncRegionBatchSize {
				select {
				case region := <-regionNotifier:
					processRegion(region)
				default:
					break loop
				}
			}
			s.broadcast(ctx, records, false)
		case <-shrinkTicker.C:
			s.history.maybeShrink()
		case <-committedCountTicker.C:
			// Publish the region count this leader is serving so that a member
			// that has not finished a history sync can still decide, after this
			// leader is gone, whether it is caught up enough to campaign. Only
			// the leader runs RunServer, so only the leader writes this.
			publishCommittedRegionCount()
		case <-keepAliveTicker.C:
			s.broadcast(ctx, nil, true)
		}
		records = records[:0]
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
		syncStream, syncStartIndex := s.bindStreamForSync(name, stream)
		err = s.syncHistoryRegion(ctx, request, syncStream, syncStartIndex)
		if err != nil {
			s.unbindStream(name, syncStream)
			return err
		}
		go s.runDownstreamSender(ctx, name, syncStream)
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
		select {
		case resultCh <- syncRegionRequestResult{request: request, err: err}:
		default:
		}
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

func (s *RegionSyncer) syncHistoryRegion(
	ctx context.Context,
	request *pdpb.SyncRegionRequest,
	syncStream *regionSyncStream,
	endIndex uint64,
) error {
	syncStream.sendMu.Lock()
	defer syncStream.sendMu.Unlock()
	return s.syncHistoryRegionLocked(ctx, request, syncStream, endIndex)
}

// syncHistoryRegionLocked serves a follower's history sync request: it replays
// the buffered records in [request.StartIndex, endIndex), or falls back to a
// full sync when the requested range is no longer available. The caller must
// hold the stream's send lock.
func (s *RegionSyncer) syncHistoryRegionLocked(
	ctx context.Context,
	request *pdpb.SyncRegionRequest,
	syncStream *regionSyncStream,
	endIndex uint64,
) error {
	startIndex := request.GetStartIndex()
	name := request.GetMember().GetName()
	if startIndex == 0 || startIndex > endIndex {
		return s.syncFullRegionsLocked(ctx, name, syncStream, endIndex)
	}
	if startIndex < endIndex {
		s.history.observeRequiredWindow(endIndex - startIndex)
	}
	records := s.history.recordsBetween(startIndex, endIndex)
	if len(records) == 0 {
		if endIndex == startIndex {
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
			return syncStream.sendStreamIfOpen(resp)
		}
		if startIndex < endIndex {
			return s.syncFullRegionsLocked(ctx, name, syncStream, endIndex)
		}
		log.Warn("no history regions from index, the leader may be restarted", zap.Uint64("index", startIndex))
		return nil
	}
	if len(records) != int(endIndex-startIndex) {
		return s.syncFullRegionsLocked(ctx, name, syncStream, endIndex)
	}
	log.Info("sync the history regions with server",
		zap.String("server", name),
		zap.Uint64("from-index", startIndex),
		zap.Uint64("last-index", endIndex),
		zap.Int("records-length", len(records)))
	return s.syncHistoryRecordsLocked(startIndex, records, syncStream, true)
}

// buildSyncRegionResponse builds a SyncRegionResponse carrying the given records
// starting at startIndex.
func buildSyncRegionResponse(startIndex uint64, records []*core.RegionInfo) *pdpb.SyncRegionResponse {
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
		bucket := &metapb.Buckets{}
		if r.GetBuckets() != nil {
			bucket = r.GetBuckets()
		}
		buckets[i] = bucket
	}
	return &pdpb.SyncRegionResponse{
		Header:        &pdpb.ResponseHeader{ClusterId: keypath.ClusterID()},
		Regions:       regions,
		StartIndex:    startIndex,
		RegionStats:   stats,
		RegionLeaders: leaders,
		Buckets:       buckets,
	}
}

func (s *RegionSyncer) syncHistoryRecords(startIndex uint64, records []*core.RegionInfo, stream *regionSyncStream) error {
	stream.sendMu.Lock()
	defer stream.sendMu.Unlock()
	return s.syncHistoryRecordsLocked(startIndex, records, stream, true)
}

// syncHistoryRecordsLocked streams records in batches. When sendEndMarker is
// true it appends an end-of-history marker (empty regions) so the follower can
// commit its history index and flip historySynced. The full-sync path passes
// false because it streams positional batches and sends its own completion
// marker at the leader's real next index instead.
func (*RegionSyncer) syncHistoryRecordsLocked(startIndex uint64, records []*core.RegionInfo, stream *regionSyncStream, sendEndMarker bool) error {
	for start := 0; start < len(records); start += maxSyncRegionBatchSize {
		end := min(start+maxSyncRegionBatchSize, len(records))
		if err := stream.sendStreamIfOpen(buildSyncRegionResponse(startIndex+uint64(start), records[start:end])); err != nil {
			return err
		}
	}
	if !sendEndMarker {
		return nil
	}
	return stream.sendStreamIfOpen(&pdpb.SyncRegionResponse{
		Header:     &pdpb.ResponseHeader{ClusterId: keypath.ClusterID()},
		StartIndex: startIndex + uint64(len(records)),
	})
}

// syncFullRegionsLocked streams a full snapshot of the leader's regions to a
// follower, followed by any catch-up records and an end-of-history marker at the
// leader's next index so the follower can commit. The caller must hold the
// stream's send lock.
func (s *RegionSyncer) syncFullRegionsLocked(ctx context.Context, name string, syncStream *regionSyncStream, syncStartIndex uint64) error {
	releaseRetain := s.history.retainFrom(syncStartIndex)
	defer releaseRetain()
	regions := s.server.GetRegions()
	start := time.Now()
	lastIndex := 0
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
		if err := syncStream.checkOpen(); err != nil {
			return err
		}
		if err := s.limit.WaitN(ctx, resp.Size()); err != nil {
			log.Error("failed to wait rate limit", errs.ZapError(err))
			return err
		}
		lastIndex += len(metas)
		if err := syncStream.sendStreamIfOpen(resp); err != nil {
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
	records, nextIndex, ok := s.history.retainedRecordsFrom(syncStartIndex)
	if !ok {
		return status.Errorf(codes.ResourceExhausted,
			"history records from full sync start index %d to %d are no longer available",
			syncStartIndex, nextIndex)
	}
	if len(records) > 0 {
		catchUpStartIndex := syncStartIndex
		if len(regions) == 0 {
			catchUpStartIndex = 0
		}
		// Full-sync batches are positional, so stream them without an
		// end-of-history marker; the completion marker below carries the real
		// next index.
		if err := s.syncHistoryRecordsLocked(catchUpStartIndex, records, syncStream, false); err != nil {
			return err
		}
		syncStream.advanceSendIndexLocked(len(records))
	}
	// End-of-history marker at the leader's real next index so the follower
	// commits the correct history index and flips historySynced.
	resp := &pdpb.SyncRegionResponse{
		Header:     &pdpb.ResponseHeader{ClusterId: keypath.ClusterID()},
		StartIndex: nextIndex,
	}
	if err := syncStream.sendStreamIfOpen(resp); err != nil {
		log.Warn("failed to send sync region completion response", errs.ZapError(errs.ErrGRPCSend, err))
		return err
	}
	return nil
}

func (s *RegionSyncer) bindStreamForSync(name string, stream ServerStream) (*regionSyncStream, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	startIndex := s.history.getNextIndex()
	syncStream := newRegionSyncStream(stream, startIndex)
	s.bindStreamLocked(name, syncStream)
	return syncStream, startIndex
}

func (s *RegionSyncer) bindStreamLocked(name string, syncStream *regionSyncStream) {
	if oldStream := s.mu.streams[name]; oldStream != nil {
		oldStream.close()
	}
	s.mu.streams[name] = syncStream
}

func (s *RegionSyncer) unbindStream(name string, stream *regionSyncStream) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.streams[name] == stream {
		delete(s.mu.streams, name)
	}
	stream.close()
}

func (s *RegionSyncer) runDownstreamSender(ctx context.Context, name string, stream *regionSyncStream) {
	defer logutil.LogPanic()
	for {
		select {
		case <-ctx.Done():
			return
		case <-stream.done:
			return
		case keepAlive := <-stream.notifyCh:
			if err := s.sendDownstream(ctx, name, stream, keepAlive); err != nil {
				log.Warn("region syncer send downstream records meet error",
					zap.String("name", name), errs.ZapError(errs.ErrGRPCSend, err))
				s.unbindStream(name, stream)
				return
			}
		}
	}
}

func (s *RegionSyncer) sendDownstream(ctx context.Context, name string, stream *regionSyncStream, keepAlive bool) error {
	stream.sendMu.Lock()
	defer stream.sendMu.Unlock()
	sentRecords, err := s.drainDownstreamLocked(ctx, name, stream)
	if err != nil {
		return err
	}
	if !keepAlive || sentRecords {
		return nil
	}
	resp := &pdpb.SyncRegionResponse{
		Header:     &pdpb.ResponseHeader{ClusterId: keypath.ClusterID()},
		StartIndex: stream.getSendIndexLocked(),
	}
	if !s.sendRegionSyncResponse(ctx, name, stream, resp) {
		return errors.Errorf("send region sync keepalive failed")
	}
	_, err = s.drainDownstreamLocked(ctx, name, stream)
	return err
}

func (s *RegionSyncer) drainDownstreamLocked(ctx context.Context, name string, stream *regionSyncStream) (bool, error) {
	sentRecords := false
	for {
		if ctx.Err() != nil {
			return sentRecords, status.Error(codes.Unavailable, "region syncer stopped")
		}
		s.mu.RLock()
		if s.mu.streams[name] != stream {
			s.mu.RUnlock()
			return sentRecords, status.Error(codes.Unavailable, "region syncer stream closed")
		}
		s.mu.RUnlock()

		sendIndex := stream.getSendIndexLocked()
		firstIndex := s.history.getFirstIndex()
		if sendIndex < firstIndex {
			return sentRecords, errors.Errorf("region syncer buffered records from index %d overflow, first available index is %d", sendIndex, firstIndex)
		}
		bufferNextIndex := s.history.getNextIndex()
		if sendIndex > bufferNextIndex {
			return sentRecords, errors.Errorf("region syncer buffered records index %d exceeds next index %d", sendIndex, bufferNextIndex)
		}
		if sendIndex == bufferNextIndex {
			return sentRecords, nil
		}
		endIndex := bufferNextIndex
		if endIndex-sendIndex > uint64(maxSyncRegionBatchSize) {
			endIndex = sendIndex + uint64(maxSyncRegionBatchSize)
		}
		records := s.history.recordsBetween(sendIndex, endIndex)
		if len(records) == 0 {
			return sentRecords, errors.Errorf("region syncer has no buffered records from index %d to %d", sendIndex, endIndex)
		}
		resp := buildSyncRegionResponse(sendIndex, records)
		if !s.sendRegionSyncResponse(ctx, name, stream, resp) {
			return sentRecords, errors.Errorf("send region sync response failed")
		}
		stream.advanceSendIndexLocked(len(records))
		sentRecords = true
	}
}

func (s *RegionSyncer) broadcast(ctx context.Context, records []*core.RegionInfo, keepAlive bool) {
	defer logutil.LogPanic()
	s.mu.RLock()
	for _, sender := range s.mu.streams {
		if ctx.Err() != nil {
			break
		}
		if len(records) != 0 || keepAlive {
			sender.notify(keepAlive)
		}
	}
	s.mu.RUnlock()
}

func (s *RegionSyncer) sendRegionSyncResponse(
	ctx context.Context,
	name string,
	sender *regionSyncStream,
	regions *pdpb.SyncRegionResponse,
) bool {
	return s.sendRegionSyncResponseWithOptions(ctx, name, sender, regions, true)
}

func (s *RegionSyncer) sendCloseRegionSyncResponse(
	ctx context.Context,
	name string,
	sender *regionSyncStream,
	regions *pdpb.SyncRegionResponse,
) bool {
	return s.sendRegionSyncResponseWithOptions(ctx, name, sender, regions, false)
}

func (s *RegionSyncer) sendRegionSyncResponseWithOptions(
	ctx context.Context,
	name string,
	sender *regionSyncStream,
	regions *pdpb.SyncRegionResponse,
	watchDone bool,
) bool {
	var sendErr error
	failpoint.InjectCall("regionSyncerSendFail", name, &sendErr)
	if sendErr != nil {
		log.Warn("region syncer send data meet error", zap.String("name", name),
			errs.ZapError(errs.ErrGRPCSend, sendErr))
		return false
	}

	var doneCh <-chan struct{}
	if watchDone {
		doneCh = sender.done
		select {
		case <-doneCh:
			return false
		default:
		}
	}

	resultCh := make(chan error, 1)
	go func() {
		resultCh <- sender.sendStream(regions)
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
	case <-doneCh:
		log.Warn("region syncer send data canceled because stream is closed", zap.String("name", name))
		return false
	case <-timer.C:
		sender.close()
		log.Warn("region syncer send data timeout", zap.String("name", name), zap.Duration("timeout", timeout))
		return false
	}
}

func (s *RegionSyncer) closeAllClient() {
	s.mu.RLock()
	streams := make(map[string]*regionSyncStream, len(s.mu.streams))
	for name, sender := range s.mu.streams {
		streams[name] = sender
	}
	s.mu.RUnlock()

	for _, sender := range streams {
		sender.close()
	}

	wg := &sync.WaitGroup{}
	for name, sender := range streams {
		resp := &pdpb.SyncRegionResponse{
			Header: &pdpb.ResponseHeader{
				ClusterId: keypath.ClusterID(),
				Error: &pdpb.Error{
					Type:    pdpb.ErrorType_UNKNOWN,
					Message: "server stopped, close the region syncer client",
				},
			},
		}
		wg.Add(1)
		go func(name string, sender *regionSyncStream, resp *pdpb.SyncRegionResponse) {
			defer logutil.LogPanic()
			defer wg.Done()
			if !s.sendCloseRegionSyncResponse(s.server.LoopContext(), name, sender, resp) {
				log.Warn("region syncer send close message meet error", zap.String("name", name))
			}
		}(name, sender, resp)
	}
	wg.Wait()
}
