// Copyright 2021 TiKV Project Authors.
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
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockserver"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

// For issue https://github.com/tikv/pd/issues/3936
func TestLoadRegion(t *testing.T) {
	re := require.New(t)
	tempDir := t.TempDir()
	rs, err := storage.NewRegionStorageWithLevelDBBackend(context.Background(), tempDir, nil)
	re.NoError(err)
	defer re.NoError(rs.Close())

	server := mockserver.NewMockServer(
		context.Background(),
		nil,
		nil,
		storage.NewCoreStorage(storage.NewStorageWithMemoryBackend(), rs),
		core.NewBasicCluster(),
	)
	for i := range 30 {
		err = rs.SaveRegion(&metapb.Region{Id: uint64(i) + 1})
		re.NoError(err)
	}
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/storage/endpoint/slowLoadRegion", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/storage/endpoint/slowLoadRegion"))
	}()

	rc := NewRegionSyncer(server)
	start := time.Now()
	rc.StartSyncWithLeader("")
	time.Sleep(time.Second)
	rc.StopSyncWithLeader()
	re.Greater(time.Since(start), time.Second) // make sure failpoint is injected
	re.Less(time.Since(start), time.Second*2)
}

func TestErrorCode(t *testing.T) {
	re := require.New(t)
	tempDir := t.TempDir()
	rs, err := storage.NewRegionStorageWithLevelDBBackend(context.Background(), tempDir, nil)
	re.NoError(err)
	defer re.NoError(rs.Close())
	server := mockserver.NewMockServer(
		context.Background(),
		nil,
		nil,
		storage.NewCoreStorage(storage.NewStorageWithMemoryBackend(), rs),
		core.NewBasicCluster(),
	)
	ctx, cancel := context.WithCancel(context.TODO())
	rc := NewRegionSyncer(server)
	conn, err := grpcutil.GetClientConn(ctx, "http://127.0.0.1", nil)
	re.NoError(err)
	cancel()
	_, err = rc.syncRegion(ctx, conn)
	ev, ok := status.FromError(err)
	re.True(ok)
	re.Equal(codes.Canceled, ev.Code())
}

func TestHandleRegionSyncResponseSkipsErrorResponse(t *testing.T) {
	re := require.New(t)
	syncer, _ := newTestRegionSyncer(t)
	syncer.history.resetWithIndex(10)
	syncer.streamingRunning.Store(true)

	handled, fullSyncing := syncer.handleRegionSyncResponse(context.Background(), &pdpb.SyncRegionResponse{
		Header: &pdpb.ResponseHeader{
			ClusterId: keypath.ClusterID(),
			Error: &pdpb.Error{
				Type:    pdpb.ErrorType_UNKNOWN,
				Message: "server stopped, close the region syncer client",
			},
		},
	}, nil, nil, false)

	re.False(handled)
	re.False(fullSyncing)
	re.Equal(uint64(10), syncer.history.getNextIndex())
	re.False(syncer.IsRunning())
}

func TestHistorySyncCompletionMarkerLifecycle(t *testing.T) {
	re := require.New(t)
	regionStorage := storage.NewStorageWithMemoryBackend()
	server := mockserver.NewMockServer(
		context.Background(),
		nil,
		nil,
		regionStorage,
		core.NewBasicCluster(),
	)
	syncer := NewRegionSyncer(server)
	re.False(syncer.IsHistorySynced())

	handled, fullSyncing := syncer.handleRegionSyncResponse(
		context.Background(),
		newTestSyncRegionResponse(10),
		core.NewBasicCluster(),
		regionStorage,
		false,
	)
	re.True(handled)
	re.False(fullSyncing)
	re.True(syncer.IsHistorySynced())

	restarted := NewRegionSyncer(server)
	re.True(restarted.IsHistorySynced())
	re.NoError(restarted.MarkHistoryIncomplete())
	re.False(restarted.IsHistorySynced())
	re.False(NewRegionSyncer(server).IsHistorySynced())
}

func TestPartialHistoryDoesNotMigrateToCompletedSync(t *testing.T) {
	re := require.New(t)
	regionStorage := storage.NewStorageWithMemoryBackend()
	server := mockserver.NewMockServer(
		context.Background(),
		nil,
		nil,
		regionStorage,
		core.NewBasicCluster(),
	)
	syncer := NewRegionSyncer(server)
	re.False(syncer.IsHistorySynced())
	for i := range defaultFlushCount {
		syncer.history.record(newTestSyncRegion(uint64(i+1), uint64(i+101)))
	}
	re.Equal(uint64(defaultFlushCount), syncer.history.getNextIndex())

	restarted := NewRegionSyncer(server)
	re.False(restarted.IsHistorySynced())
}

func TestIncompleteMarkerAlwaysRequestsFullSnapshot(t *testing.T) {
	re := require.New(t)
	syncer, _ := newTestRegionSyncer(t)
	syncer.history.resetWithIndex(42)
	syncer.initialFollowerSyncCompleted.Store(true)
	syncer.streamingRunning.Store(true)
	re.NoError(syncer.history.saveSynced(false))
	syncer.historySynced.Store(false)

	re.Zero(syncer.syncRegionStartIndex())
	re.False(syncer.IsRunning())
}

func TestHistoryIndexPersistenceFailureKeepsFollowerNotReady(t *testing.T) {
	re := require.New(t)
	syncer, _ := newTestRegionSyncer(t)
	syncer.history.kv = &saveFailKV{
		Base:    syncer.history.kv,
		failKey: historyKey,
	}

	re.Error(syncer.MarkHistoryIncomplete())
	re.False(syncer.IsHistorySynced())
	re.Error(syncer.MarkHistorySynced())
	re.False(syncer.IsHistorySynced())
}

func TestInitialFollowerSyncRequestsFullSnapshotWithoutClearingCompletedState(t *testing.T) {
	re := require.New(t)
	pdServer := &captureSyncRequestServer{
		requestCh: make(chan *pdpb.SyncRegionRequest, 1),
	}
	leaderURL := startTestPDServer(t, pdServer)

	regionStorage := storage.NewStorageWithMemoryBackend()
	syncer, cluster := newFollowerTestSyncer(t, regionStorage)
	staleRegion := newTestSyncRegion(9, 19)
	cluster.PutRegion(staleRegion)
	re.NoError(regionStorage.SaveRegion(staleRegion.GetMeta()))
	markHistorySynced(t, syncer)

	syncer.StartSyncWithLeader(leaderURL)
	request := mustRecvWithin(
		t, pdServer.requestCh, 3*time.Second, "initial follower sync request was not received",
	)
	re.Zero(request.GetStartIndex())
	re.True(syncer.IsHistorySynced())
	re.Equal(uint64(42), syncer.history.getNextIndex())
	synced, exists, err := syncer.history.loadSynced()
	re.NoError(err)
	re.True(exists)
	re.True(synced)
	re.NotNil(cluster.GetRegion(staleRegion.GetID()))
	stored := &metapb.Region{}
	ok, err := regionStorage.LoadRegion(staleRegion.GetID(), stored)
	re.NoError(err)
	re.True(ok)
}

func TestInitialRegionLoadFailureBlocksSynchronizationUntilRetrySucceeds(t *testing.T) {
	re := require.New(t)
	pdServer := &captureSyncRequestServer{
		requestCh: make(chan *pdpb.SyncRegionRequest, 1),
	}
	leaderURL := startTestPDServer(t, pdServer)

	defaultStorage := storage.NewStorageWithMemoryBackend()
	localStorage := &loadRegionsFailOnceStorage{
		Storage: storage.NewStorageWithMemoryBackend(),
	}
	persisted := newTestSyncRegion(9, 19)
	re.NoError(localStorage.SaveRegion(persisted.GetMeta()))
	coreStorage := storage.NewCoreStorage(defaultStorage, localStorage)
	syncer, cluster := newFollowerTestSyncer(t, coreStorage)
	markHistorySynced(t, syncer)

	syncer.StartSyncWithLeader(leaderURL)
	select {
	case <-pdServer.requestCh:
		re.FailNow("synchronization started before persisted Regions were loaded")
	case <-time.After(300 * time.Millisecond):
	}
	request := mustRecvWithin(
		t, pdServer.requestCh, 3*time.Second,
		"synchronization did not start after persisted Regions were loaded",
	)
	re.Zero(request.GetStartIndex())
	re.True(syncer.initialRegionLoadCompleted.Load())
	re.NotNil(cluster.GetRegion(persisted.GetID()))
	re.True(syncer.IsHistorySynced())
}

func TestFullSyncWatchdogCoversFirstDestructiveResponse(t *testing.T) {
	re := require.New(t)
	pdServer := &scriptedSyncPDServer{
		requestCh: make(chan syncRequestEvent, 2),
		response:  newTestSyncRegionResponse(0, newTestSyncRegion(9, 19).GetMeta()),
	}
	leaderURL := startTestPDServer(t, pdServer)

	defaultStorage := storage.NewStorageWithMemoryBackend()
	localStorage := &blockFirstTxnUntilCanceledStorage{
		Storage: storage.NewStorageWithMemoryBackend(),
		entered: make(chan struct{}),
	}
	persisted := newTestSyncRegion(7, 17)
	re.NoError(localStorage.SaveRegion(persisted.GetMeta()))
	coreStorage := storage.NewCoreStorage(defaultStorage, localStorage)
	syncer, _ := newFollowerTestSyncer(t, coreStorage)
	markHistorySynced(t, syncer)
	syncer.fullSyncProgressTimeout = 50 * time.Millisecond

	syncer.StartSyncWithLeader(leaderURL)
	mustRecvWithin(
		t,
		localStorage.entered,
		3*time.Second,
		"first destructive Region storage transaction was not reached",
	)
	for expectedRequest := 1; expectedRequest <= 2; expectedRequest++ {
		request := mustRecvWithin(
			t,
			pdServer.requestCh,
			3*time.Second,
			"full sync did not reconnect after first-frame timeout",
		)
		re.Equal(expectedRequest, request.attempt)
		re.Zero(request.startIndex)
	}
	re.Eventually(func() bool {
		synced, exists, err := syncer.history.loadSynced()
		return err == nil && exists && !synced &&
			!syncer.IsHistorySynced() &&
			syncer.history.getNextIndex() == 0
	}, time.Second, 10*time.Millisecond)
	re.False(syncer.IsRunning())
}

func TestStopDuringFullSyncKeepsFollowerIncomplete(t *testing.T) {
	re := require.New(t)
	pdServer := &scriptedSyncPDServer{
		requestCh: make(chan syncRequestEvent, 1),
		response:  newTestSyncRegionResponse(0, newTestSyncRegion(9, 19).GetMeta()),
	}
	leaderURL := startTestPDServer(t, pdServer)

	defaultStorage := storage.NewStorageWithMemoryBackend()
	localStorage := storage.NewStorageWithMemoryBackend()
	coreStorage := storage.NewCoreStorage(defaultStorage, localStorage)
	syncer, cluster := newFollowerTestSyncer(t, coreStorage)
	persisted := newTestSyncRegion(7, 17)
	cluster.PutRegion(persisted)
	re.NoError(localStorage.SaveRegion(persisted.GetMeta()))
	markHistorySynced(t, syncer)

	syncer.StartSyncWithLeader(leaderURL)
	request := mustRecvWithin(
		t, pdServer.requestCh, 3*time.Second, "full sync request was not received",
	)
	re.Zero(request.startIndex)
	re.Eventually(func() bool {
		return !syncer.IsHistorySynced() &&
			syncer.history.getNextIndex() == 0 &&
			cluster.GetRegion(9) != nil
	}, 3*time.Second, 10*time.Millisecond)

	syncer.StopSyncWithLeader()
	re.False(syncer.IsHistorySynced())
	re.False(syncer.IsRunning())
	re.Zero(syncer.syncRegionStartIndex())
	re.Nil(cluster.GetRegion(persisted.GetID()))

	stored := &metapb.Region{}
	ok, err := localStorage.LoadRegion(persisted.GetID(), stored)
	re.NoError(err)
	re.False(ok)
	synced, exists, err := syncer.history.loadSynced()
	re.NoError(err)
	re.True(exists)
	re.False(synced)

	restartedCluster := core.NewBasicCluster()
	restartedServer := mockserver.NewMockServer(
		context.Background(),
		&pdpb.Member{Name: "pd-follower", MemberId: 2},
		nil,
		coreStorage,
		restartedCluster,
	)
	restarted := NewRegionSyncer(restartedServer)
	re.False(restarted.IsHistorySynced())
	re.Zero(restarted.syncRegionStartIndex())
}

func TestFullSyncEOFReconnectsInsteadOfStopping(t *testing.T) {
	re := require.New(t)
	pdServer := &scriptedSyncPDServer{
		requestCh:  make(chan syncRequestEvent, 2),
		response:   newTestSyncRegionResponse(0, newTestSyncRegion(9, 19).GetMeta()),
		closeFirst: true,
	}
	leaderURL := startTestPDServer(t, pdServer)

	defaultStorage := storage.NewStorageWithMemoryBackend()
	localStorage := storage.NewStorageWithMemoryBackend()
	coreStorage := storage.NewCoreStorage(defaultStorage, localStorage)
	syncer, _ := newFollowerTestSyncer(t, coreStorage)
	syncer.StartSyncWithLeader(leaderURL)
	for expectedRequest := 1; expectedRequest <= 2; expectedRequest++ {
		request := mustRecvWithin(
			t, pdServer.requestCh, 3*time.Second, "full sync did not reconnect after EOF",
		)
		re.Equal(expectedRequest, request.attempt)
		re.Zero(request.startIndex)
	}
	re.False(syncer.IsHistorySynced())
	re.False(syncer.IsRunning())
	re.Zero(syncer.history.getNextIndex())
}

func TestFullSyncStorageFailureRetriesFromIncomplete(t *testing.T) {
	tests := []struct {
		name    string
		failure regionWriteFailure
		regions []*metapb.Region
	}{
		{
			name:    "save",
			failure: failRegionSave,
			regions: []*metapb.Region{newTestSyncRegion(9, 19).GetMeta()},
		},
		{
			name:    "flush-before-clear",
			failure: failFirstFlush,
			regions: []*metapb.Region{newTestSyncRegion(9, 19).GetMeta()},
		},
		{
			name:    "flush-before-complete",
			failure: failSecondFlush,
			regions: []*metapb.Region{newTestSyncRegion(9, 19).GetMeta()},
		},
		{
			name:    "completion-marker",
			failure: failCompletionMarker,
			regions: []*metapb.Region{newTestSyncRegion(9, 19).GetMeta()},
		},
		{
			name:    "delete",
			failure: failRegionDelete,
			regions: []*metapb.Region{
				core.NewTestRegionInfo(
					9, 1, []byte{1}, []byte{3}, core.WithNewPeerIDs(19),
				).GetMeta(),
				core.NewTestRegionInfo(
					10, 1, []byte{1}, []byte{3},
					core.WithNewPeerIDs(20), core.SetRegionVersion(2),
				).GetMeta(),
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			re := require.New(t)
			pdServer := &scriptedSyncPDServer{
				requestCh:            make(chan syncRequestEvent, 2),
				response:             newTestSyncRegionResponse(0, test.regions...),
				snapshotEveryAttempt: true,
				completeFirst: test.failure == failSecondFlush ||
					test.failure == failCompletionMarker,
			}
			leaderURL := startTestPDServer(t, pdServer)

			defaultStorage := storage.NewStorageWithMemoryBackend()
			localStorage := &regionWriteFailOnceStorage{
				Storage: storage.NewStorageWithMemoryBackend(),
				failed:  make(chan struct{}, 1),
			}
			persisted := newTestSyncRegion(7, 17)
			re.NoError(localStorage.SaveRegion(persisted.GetMeta()))
			coreStorage := storage.NewCoreStorage(defaultStorage, localStorage)
			syncer, cluster := newFollowerTestSyncer(t, coreStorage)
			markHistorySynced(t, syncer)
			localStorage.setFailure(test.failure)

			syncer.StartSyncWithLeader(leaderURL)
			request := mustRecvWithin(
				t,
				pdServer.requestCh,
				3*time.Second,
				"initial full sync request was not received",
			)
			re.Zero(request.startIndex)
			mustRecvWithin(
				t,
				localStorage.failed,
				3*time.Second,
				"Region storage failure was not injected",
			)
			synced, exists, err := syncer.history.loadSynced()
			re.NoError(err)
			re.True(exists)
			re.False(synced)
			re.False(syncer.IsHistorySynced())
			re.False(syncer.IsRunning())

			request = mustRecvWithin(
				t,
				pdServer.requestCh,
				3*time.Second,
				"full sync did not retry after Region write failure",
			)
			re.Zero(request.startIndex)
			re.Eventually(func() bool {
				return syncer.IsHistorySynced() &&
					syncer.IsRunning() &&
					syncer.history.getNextIndex() == uint64(len(test.regions))
			}, 3*time.Second, 10*time.Millisecond)
			re.Nil(cluster.GetRegion(persisted.GetID()))
			stored := &metapb.Region{}
			ok, err := localStorage.LoadRegion(persisted.GetID(), stored)
			re.NoError(err)
			re.False(ok)

			authoritative := test.regions[len(test.regions)-1]
			re.NotNil(cluster.GetRegion(authoritative.GetId()))
			ok, err = localStorage.LoadRegion(authoritative.GetId(), stored)
			re.NoError(err)
			re.True(ok)
			re.Equal(authoritative, stored)
		})
	}
}

func TestFullSyncClearDoesNotResurrectPendingLevelDBBatch(t *testing.T) {
	re := require.New(t)
	regionStorage, err := storage.NewRegionStorageWithLevelDBBackend(
		context.Background(), t.TempDir(), nil,
	)
	re.NoError(err)
	t.Cleanup(func() {
		re.NoError(regionStorage.Close())
	})
	defaultStorage := storage.NewStorageWithMemoryBackend()
	coreStorage := storage.NewCoreStorage(defaultStorage, regionStorage)
	re.NotNil(storage.TrySwitchRegionStorage(coreStorage, true))
	syncer, cluster := newFollowerTestSyncer(t, coreStorage)
	re.NoError(syncer.history.resetWithIndexAndPersist(42))
	re.NoError(syncer.history.saveSynced(true))
	syncer.historySynced.Store(true)
	syncer.initialFollowerSyncCompleted.Store(true)

	persisted := newTestSyncRegion(7, 17)
	cluster.PutRegion(persisted)
	// Keep the completed follower's latest Region update in the LevelDB
	// in-memory batch to exercise the production storage ordering.
	re.NoError(coreStorage.SaveRegion(persisted.GetMeta()))
	partial := newTestSyncRegion(9, 19)
	handled, fullSyncing := syncer.handleRegionSyncResponse(
		context.Background(),
		newTestSyncRegionResponse(0, partial.GetMeta()),
		cluster,
		coreStorage,
		false,
	)
	re.True(handled)
	re.True(fullSyncing)

	handled, fullSyncing = syncer.handleRegionSyncResponse(
		context.Background(),
		newTestSyncRegionResponse(1),
		cluster,
		coreStorage,
		true,
	)
	re.True(handled)
	re.False(fullSyncing)
	stored := &metapb.Region{}
	ok, err := regionStorage.LoadRegion(persisted.GetID(), stored)
	re.NoError(err)
	re.False(ok)
	ok, err = regionStorage.LoadRegion(partial.GetID(), stored)
	re.NoError(err)
	re.True(ok)
	re.Equal(partial.GetMeta(), stored)
	re.True(syncer.IsHistorySynced())
	re.Equal(uint64(1), syncer.history.getNextIndex())
}

func newFollowerTestSyncer(
	t *testing.T,
	clientStorage storage.Storage,
) (*RegionSyncer, *core.BasicCluster) {
	t.Helper()
	cluster := core.NewBasicCluster()
	server := mockserver.NewMockServer(
		t.Context(),
		&pdpb.Member{Name: "pd-follower", MemberId: 2},
		&pdpb.Member{Name: "pd-leader", MemberId: 1},
		clientStorage,
		cluster,
	)
	syncer := NewRegionSyncer(server)
	t.Cleanup(syncer.StopSyncWithLeader)
	return syncer, cluster
}

func markHistorySynced(t *testing.T, syncer *RegionSyncer) {
	t.Helper()
	syncer.history.resetWithIndex(42)
	require.NoError(t, syncer.MarkHistorySynced())
}

func startTestPDServer(t *testing.T, pdServer pdpb.PDServer) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcServer := grpc.NewServer()
	pdpb.RegisterPDServer(grpcServer, pdServer)
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- grpcServer.Serve(listener)
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
		err := <-serveDone
		if err != nil {
			require.ErrorIs(t, err, grpc.ErrServerStopped)
		}
	})
	return "http://" + listener.Addr().String()
}

func newTestSyncRegionResponse(
	startIndex uint64,
	regions ...*metapb.Region,
) *pdpb.SyncRegionResponse {
	return &pdpb.SyncRegionResponse{
		Header:     &pdpb.ResponseHeader{ClusterId: keypath.ClusterID()},
		Regions:    regions,
		StartIndex: startIndex,
	}
}

func mustRecvWithin[T any](
	t *testing.T,
	ch <-chan T,
	timeout time.Duration,
	message string,
) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(timeout):
		require.FailNow(t, message)
		var zero T
		return zero
	}
}

type loadRegionsFailOnceStorage struct {
	storage.Storage
	mu     sync.Mutex
	failed bool
}

func (s *loadRegionsFailOnceStorage) LoadRegions(
	ctx context.Context,
	f func(region *core.RegionInfo) []*core.RegionInfo,
) error {
	s.mu.Lock()
	if !s.failed {
		s.failed = true
		s.mu.Unlock()
		return errors.New("injected Region load failure")
	}
	s.mu.Unlock()
	return s.Storage.LoadRegions(ctx, f)
}

type blockFirstTxnUntilCanceledStorage struct {
	storage.Storage
	entered chan struct{}
	once    sync.Once
}

type regionWriteFailure uint8

const (
	noRegionWriteFailure regionWriteFailure = iota
	failRegionSave
	failRegionDelete
	failFirstFlush
	failSecondFlush
	failCompletionMarker
)

type regionWriteFailOnceStorage struct {
	storage.Storage
	failure    regionWriteFailure
	flushCalls int
	failed     chan struct{}
}

func (s *regionWriteFailOnceStorage) SaveRegion(region *metapb.Region) error {
	if err := s.failOnce(failRegionSave); err != nil {
		return err
	}
	return s.Storage.SaveRegion(region)
}

func (s *regionWriteFailOnceStorage) DeleteRegion(region *metapb.Region) error {
	if err := s.failOnce(failRegionDelete); err != nil {
		return err
	}
	return s.Storage.DeleteRegion(region)
}

func (s *regionWriteFailOnceStorage) Flush() error {
	s.flushCalls++
	failure := noRegionWriteFailure
	switch s.flushCalls {
	case 1:
		failure = failFirstFlush
	case 2:
		failure = failSecondFlush
	}
	if err := s.failOnce(failure); err != nil {
		return err
	}
	return s.Storage.Flush()
}

func (s *regionWriteFailOnceStorage) Save(key, value string) error {
	if key == historySyncedKey && value == "true" {
		if err := s.failOnce(failCompletionMarker); err != nil {
			return err
		}
	}
	return s.Storage.Save(key, value)
}

func (s *regionWriteFailOnceStorage) setFailure(failure regionWriteFailure) {
	s.failure = failure
	s.flushCalls = 0
}

func (s *regionWriteFailOnceStorage) failOnce(failure regionWriteFailure) error {
	if failure == noRegionWriteFailure || s.failure != failure {
		return nil
	}
	s.failure = noRegionWriteFailure
	s.failed <- struct{}{}
	return errors.New("injected Region storage failure")
}

func (s *blockFirstTxnUntilCanceledStorage) RunInTxn(
	ctx context.Context,
	f func(kv.Txn) error,
) error {
	blocked := false
	s.once.Do(func() {
		blocked = true
		close(s.entered)
	})
	if blocked {
		<-ctx.Done()
		return ctx.Err()
	}
	return s.Storage.RunInTxn(ctx, f)
}

type captureSyncRequestServer struct {
	pdpb.UnimplementedPDServer
	requestCh chan *pdpb.SyncRegionRequest
}

type syncRequestEvent struct {
	attempt    int
	startIndex uint64
}

type scriptedSyncPDServer struct {
	pdpb.UnimplementedPDServer
	mu                   sync.Mutex
	requests             int
	requestCh            chan syncRequestEvent
	response             *pdpb.SyncRegionResponse
	snapshotEveryAttempt bool
	completeFirst        bool
	closeFirst           bool
}

func (s *scriptedSyncPDServer) SyncRegions(stream pdpb.PD_SyncRegionsServer) error {
	request, err := stream.Recv()
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.requests++
	attempt := s.requests
	s.mu.Unlock()
	select {
	case s.requestCh <- syncRequestEvent{attempt: attempt, startIndex: request.GetStartIndex()}:
	default:
	}
	if attempt == 1 || s.snapshotEveryAttempt {
		if err := stream.Send(s.response); err != nil {
			return err
		}
	}
	if attempt == 1 && s.closeFirst {
		return nil
	}
	if s.snapshotEveryAttempt && (attempt > 1 || s.completeFirst) {
		if err := stream.Send(newTestSyncRegionResponse(uint64(len(s.response.GetRegions())))); err != nil {
			return err
		}
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}

func (s *captureSyncRequestServer) SyncRegions(stream pdpb.PD_SyncRegionsServer) error {
	request, err := stream.Recv()
	if err != nil {
		return err
	}
	s.requestCh <- request
	<-stream.Context().Done()
	return stream.Context().Err()
}

func TestLegacyPersistedHistoryMigratesToCompletedSync(t *testing.T) {
	re := require.New(t)
	regionStorage := storage.NewStorageWithMemoryBackend()
	re.NoError(regionStorage.Save(historyKey, "42"))
	server := mockserver.NewMockServer(
		context.Background(),
		nil,
		nil,
		regionStorage,
		core.NewBasicCluster(),
	)

	syncer := NewRegionSyncer(server)
	re.True(syncer.IsHistorySynced())
	synced, exists, err := syncer.history.loadSynced()
	re.NoError(err)
	re.True(exists)
	re.True(synced)
}

func TestLegacyHistoryMarkerSaveFailureKeepsFollowerNotReady(t *testing.T) {
	re := require.New(t)
	baseStorage := storage.NewStorageWithMemoryBackend()
	re.NoError(baseStorage.Save(historyKey, "42"))
	regionStorage := &regionWriteFailOnceStorage{
		Storage: baseStorage,
		failure: failCompletionMarker,
		failed:  make(chan struct{}, 1),
	}
	server := mockserver.NewMockServer(
		context.Background(),
		nil,
		nil,
		regionStorage,
		core.NewBasicCluster(),
	)

	syncer := NewRegionSyncer(server)

	mustRecvWithin(t, regionStorage.failed, time.Second, "legacy marker failure was not injected")
	re.False(syncer.IsHistorySynced())
	synced, exists, err := syncer.history.loadSynced()
	re.NoError(err)
	re.False(exists)
	re.False(synced)
}

type saveFailKV struct {
	kv.Base
	failKey string
}

func (s *saveFailKV) Save(key, value string) error {
	if key == s.failKey {
		return errors.New("injected save failure")
	}
	return s.Base.Save(key, value)
}

func TestFullSyncReplacesNewerCacheAndOlderStorage(t *testing.T) {
	re := require.New(t)
	regionStorage := storage.NewStorageWithMemoryBackend()
	stored := newTestSyncRegion(1, 11).Clone(core.SetRegionVersion(1)).GetMeta()
	re.NoError(regionStorage.SaveRegion(stored))

	cached := newTestSyncRegion(1, 11).Clone(core.SetRegionVersion(3))
	bc := core.NewBasicCluster()
	bc.PutRegion(cached)
	server := mockserver.NewMockServer(context.Background(), nil, nil, regionStorage, bc)
	syncer := NewRegionSyncer(server)
	markHistorySynced(t, syncer)

	leader := newTestSyncRegion(1, 11).Clone(core.SetRegionVersion(2)).GetMeta()
	handled, fullSyncing := syncer.handleRegionSyncResponse(
		context.Background(),
		newTestSyncRegionResponse(0, leader),
		bc,
		regionStorage,
		false,
	)
	re.True(handled)
	re.True(fullSyncing)
	re.Equal(uint64(2), bc.GetRegion(1).GetRegionEpoch().GetVersion())

	persisted := &metapb.Region{}
	ok, err := regionStorage.LoadRegion(1, persisted)
	re.NoError(err)
	re.True(ok)
	re.Equal(uint64(2), persisted.GetRegionEpoch().GetVersion())
}
