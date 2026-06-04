// Copyright 2026 TiKV Project Authors.
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
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockserver"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestHistoryBufferMaxSizeFromMemory(t *testing.T) {
	testCases := []struct {
		name        string
		totalMemory uint64
		expected    int
	}{
		{name: "unknown-memory", totalMemory: 0, expected: defaultHistoryBufferSize},
		{name: "below-step", totalMemory: historyBufferMemoryStep / 2, expected: defaultHistoryBufferSize},
		{name: "one-step", totalMemory: historyBufferMemoryStep, expected: defaultHistoryBufferSize},
		{name: "round-to-two-steps", totalMemory: 2 * historyBufferMemoryStep, expected: 2 * defaultHistoryBufferSize},
		{name: "round-to-four-steps", totalMemory: 3 * historyBufferMemoryStep, expected: 4 * defaultHistoryBufferSize},
		{name: "clamp-to-max", totalMemory: historyBufferMemoryStep * 100, expected: maxHistoryBufferSize},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.expected, historyBufferMaxSizeFromMemory(testCase.totalMemory))
		})
	}
}

func TestSyncHistoryRegionStartIndexZeroDoesNotGrowHistoryWindow(t *testing.T) {
	re := require.New(t)
	syncer := newTestRegionSyncer(t, core.NewBasicCluster())
	syncer.history = newHistoryBufferWithConfig(2, 8, 1, storage.NewStorageWithMemoryBackend())
	syncer.history.resetWithIndex(10)

	stream := newMockSyncRegionsServer()
	syncStream, err := syncer.syncHistoryRegion(context.Background(), &pdpb.SyncRegionRequest{
		Header:     &pdpb.RequestHeader{ClusterId: keypath.ClusterID()},
		Member:     &pdpb.Member{Name: "pd-follower", ClientUrls: []string{"http://127.0.0.1:2379"}},
		StartIndex: 0,
	}, stream)
	if syncStream != nil {
		defer syncer.unbindStream("pd-follower", syncStream)
	}

	re.NoError(err)
	re.Equal(2, syncer.history.capacity())
}

func TestSyncHistoryRecordsSplitBatches(t *testing.T) {
	re := require.New(t)
	syncer := newTestRegionSyncer(t, core.NewBasicCluster())
	records := make([]*core.RegionInfo, 0, maxSyncRegionBatchSize+1)
	for i := range maxSyncRegionBatchSize + 1 {
		records = append(records, newHistoryBufferTestRegion(uint64(i)))
	}
	stream := newMockSyncRegionsServer()
	stream.sendCh = make(chan *pdpb.SyncRegionResponse, 2)

	re.NoError(syncer.syncHistoryRecords(10, records, stream))
	first := <-stream.sendCh
	second := <-stream.sendCh
	re.Equal(uint64(10), first.GetStartIndex())
	re.Len(first.GetRegions(), maxSyncRegionBatchSize)
	re.Equal(uint64(10+maxSyncRegionBatchSize), second.GetStartIndex())
	re.Len(second.GetRegions(), 1)
}

func TestSyncFullRegionsRetainsCatchUpHistory(t *testing.T) {
	re := require.New(t)
	bc := core.NewBasicCluster()
	bc.PutRegion(newHistoryBufferTestRegion(1))
	syncer := newTestRegionSyncer(t, bc)
	syncer.history = newHistoryBufferWithConfig(2, 8, 1, storage.NewStorageWithMemoryBackend())
	syncer.history.resetWithIndex(10)

	stream := newMockSyncRegionsServer()
	unblockSend := stream.blockSend()
	startIndex := syncer.history.getNextIndex()
	done := make(chan syncFullRegionsResult, 1)
	go func() {
		syncStream, err := syncer.syncFullRegions(context.Background(), "pd-follower", stream)
		done <- syncFullRegionsResult{syncStream: syncStream, err: err}
	}()
	testutil.Eventually(re, stream.isSendBlocked)

	for i := range 5 {
		syncer.history.record(newHistoryBufferTestRegion(uint64(100 + i)))
	}
	records := syncer.history.recordsFrom(startIndex)

	close(unblockSend)
	fullResp := <-stream.sendCh
	catchUpResp := <-stream.sendCh
	result := <-done
	if result.syncStream != nil {
		defer syncer.unbindStream("pd-follower", result.syncStream)
	}

	re.NoError(result.err)
	re.Len(fullResp.GetRegions(), 1)
	re.Len(records, 5)
	re.Equal(startIndex, catchUpResp.GetStartIndex())
	re.Len(catchUpResp.GetRegions(), 5)
}

func TestSyncFullRegionsFailsWhenCatchUpHistoryExceedsMax(t *testing.T) {
	re := require.New(t)
	bc := core.NewBasicCluster()
	bc.PutRegion(newHistoryBufferTestRegion(1))
	syncer := newTestRegionSyncer(t, bc)
	syncer.history = newHistoryBufferWithConfig(2, 2, 1, storage.NewStorageWithMemoryBackend())
	syncer.history.resetWithIndex(10)

	stream := newMockSyncRegionsServer()
	unblockSend := stream.blockSend()
	done := make(chan syncFullRegionsResult, 1)
	go func() {
		syncStream, err := syncer.syncFullRegions(context.Background(), "pd-follower", stream)
		done <- syncFullRegionsResult{syncStream: syncStream, err: err}
	}()
	testutil.Eventually(re, stream.isSendBlocked)

	for i := range 3 {
		syncer.history.record(newHistoryBufferTestRegion(uint64(200 + i)))
	}

	close(unblockSend)
	re.NotNil(<-stream.sendCh)
	result := <-done
	if result.syncStream != nil {
		defer syncer.unbindStream("pd-follower", result.syncStream)
	}

	re.Error(result.err)
}

type syncFullRegionsResult struct {
	syncStream *regionSyncStream
	err        error
}

func TestSyncExitsWhenRegionSyncerStops(t *testing.T) {
	re := require.New(t)
	tempDir := t.TempDir()
	regionStorage, err := storage.NewRegionStorageWithLevelDBBackend(context.Background(), tempDir, nil)
	re.NoError(err)
	defer func() {
		re.NoError(regionStorage.Close())
	}()

	server := mockserver.NewMockServer(
		context.Background(),
		nil,
		nil,
		storage.NewCoreStorage(storage.NewStorageWithMemoryBackend(), regionStorage),
		core.NewBasicCluster(),
	)
	syncer := NewRegionSyncer(server)
	ctx, cancel := context.WithCancel(context.Background())
	stream := newMockSyncRegionsServer()
	done := make(chan error, 1)
	go func() {
		done <- syncer.Sync(ctx, stream)
	}()

	stream.recvCh <- &pdpb.SyncRegionRequest{
		Header: &pdpb.RequestHeader{ClusterId: keypath.ClusterID()},
		Member: &pdpb.Member{
			Name:       "pd-follower",
			ClientUrls: []string{"http://127.0.0.1:2379"},
		},
	}
	re.NotNil(<-stream.sendCh)
	testutil.Eventually(re, func() bool {
		names := syncer.GetAllDownstreamNames()
		return len(names) == 1 && names[0] == "pd-follower"
	})

	cancel()
	var syncErr error
	testutil.Eventually(re, func() bool {
		if syncErr == nil {
			select {
			case syncErr = <-done:
			default:
				return false
			}
		}
		st, ok := status.FromError(syncErr)
		return ok && st.Code() == codes.Unavailable
	})
	re.Empty(syncer.GetAllDownstreamNames())
}

func TestSyncExitsWhenBroadcastSendFails(t *testing.T) {
	re := require.New(t)
	tempDir := t.TempDir()
	regionStorage, err := storage.NewRegionStorageWithLevelDBBackend(context.Background(), tempDir, nil)
	re.NoError(err)
	defer func() {
		re.NoError(regionStorage.Close())
	}()

	server := mockserver.NewMockServer(
		context.Background(),
		nil,
		nil,
		storage.NewCoreStorage(storage.NewStorageWithMemoryBackend(), regionStorage),
		core.NewBasicCluster(),
	)
	syncer := NewRegionSyncer(server)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := newMockSyncRegionsServer()
	done := make(chan error, 1)
	go func() {
		done <- syncer.Sync(ctx, stream)
	}()

	stream.recvCh <- &pdpb.SyncRegionRequest{
		Header: &pdpb.RequestHeader{ClusterId: keypath.ClusterID()},
		Member: &pdpb.Member{
			Name:       "pd-follower",
			ClientUrls: []string{"http://127.0.0.1:2379"},
		},
	}
	re.NotNil(<-stream.sendCh)
	testutil.Eventually(re, func() bool {
		names := syncer.GetAllDownstreamNames()
		return len(names) == 1 && names[0] == "pd-follower"
	})

	stream.setSendErr(errors.New("send failed"))
	syncer.broadcast(context.Background(), &pdpb.SyncRegionResponse{
		Header:     &pdpb.ResponseHeader{ClusterId: keypath.ClusterID()},
		StartIndex: syncer.history.getNextIndex(),
	})

	var syncErr error
	testutil.Eventually(re, func() bool {
		if syncErr == nil {
			select {
			case syncErr = <-done:
			default:
				return false
			}
		}
		st, ok := status.FromError(syncErr)
		return ok && st.Code() == codes.Unavailable
	})
	re.Empty(syncer.GetAllDownstreamNames())
}

func TestCloseAllClientClosesStreamsBeforeSend(t *testing.T) {
	re := require.New(t)
	tempDir := t.TempDir()
	regionStorage, err := storage.NewRegionStorageWithLevelDBBackend(context.Background(), tempDir, nil)
	re.NoError(err)
	defer func() {
		re.NoError(regionStorage.Close())
	}()

	server := mockserver.NewMockServer(
		context.Background(),
		nil,
		nil,
		storage.NewCoreStorage(storage.NewStorageWithMemoryBackend(), regionStorage),
		core.NewBasicCluster(),
	)
	syncer := NewRegionSyncer(server)
	syncer.sendTimeout = 10 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := newMockSyncRegionsServer()
	done := make(chan error, 1)
	go func() {
		done <- syncer.Sync(ctx, stream)
	}()

	stream.recvCh <- &pdpb.SyncRegionRequest{
		Header: &pdpb.RequestHeader{ClusterId: keypath.ClusterID()},
		Member: &pdpb.Member{
			Name:       "pd-follower",
			ClientUrls: []string{"http://127.0.0.1:2379"},
		},
	}
	re.NotNil(<-stream.sendCh)
	testutil.Eventually(re, func() bool {
		names := syncer.GetAllDownstreamNames()
		return len(names) == 1 && names[0] == "pd-follower"
	})

	unblockSend := stream.blockSend()
	closeDone := make(chan struct{})
	go func() {
		syncer.closeAllClient()
		close(closeDone)
	}()
	testutil.Eventually(re, stream.isSendBlocked)

	var syncErr error
	testutil.Eventually(re, func() bool {
		if syncErr == nil {
			select {
			case syncErr = <-done:
			default:
				return false
			}
		}
		st, ok := status.FromError(syncErr)
		return ok && st.Code() == codes.Unavailable
	})
	re.Empty(syncer.GetAllDownstreamNames())

	testutil.Eventually(re, func() bool {
		select {
		case <-closeDone:
			return true
		default:
			return false
		}
	})
	close(unblockSend)
}

func TestBroadcastClosesStreamWhenSendBlocks(t *testing.T) {
	re := require.New(t)
	tempDir := t.TempDir()
	regionStorage, err := storage.NewRegionStorageWithLevelDBBackend(context.Background(), tempDir, nil)
	re.NoError(err)
	defer func() {
		re.NoError(regionStorage.Close())
	}()

	server := mockserver.NewMockServer(
		context.Background(),
		nil,
		nil,
		storage.NewCoreStorage(storage.NewStorageWithMemoryBackend(), regionStorage),
		core.NewBasicCluster(),
	)
	syncer := NewRegionSyncer(server)
	syncer.sendTimeout = 10 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := newMockSyncRegionsServer()
	done := make(chan error, 1)
	go func() {
		done <- syncer.Sync(ctx, stream)
	}()

	stream.recvCh <- &pdpb.SyncRegionRequest{
		Header: &pdpb.RequestHeader{ClusterId: keypath.ClusterID()},
		Member: &pdpb.Member{
			Name:       "pd-follower",
			ClientUrls: []string{"http://127.0.0.1:2379"},
		},
	}
	re.NotNil(<-stream.sendCh)
	testutil.Eventually(re, func() bool {
		names := syncer.GetAllDownstreamNames()
		return len(names) == 1 && names[0] == "pd-follower"
	})

	unblockSend := stream.blockSend()
	broadcastDone := make(chan struct{})
	go func() {
		syncer.broadcast(context.Background(), &pdpb.SyncRegionResponse{
			Header:     &pdpb.ResponseHeader{ClusterId: keypath.ClusterID()},
			StartIndex: syncer.history.getNextIndex(),
		})
		close(broadcastDone)
	}()
	testutil.Eventually(re, stream.isSendBlocked)
	testutil.Eventually(re, func() bool {
		select {
		case <-broadcastDone:
			return true
		default:
			return false
		}
	})

	var syncErr error
	testutil.Eventually(re, func() bool {
		if syncErr == nil {
			select {
			case syncErr = <-done:
			default:
				return false
			}
		}
		st, ok := status.FromError(syncErr)
		return ok && st.Code() == codes.Unavailable
	})
	re.Empty(syncer.GetAllDownstreamNames())
	close(unblockSend)
}

func TestSyncExitsWhenContextCanceledBeforeRequest(t *testing.T) {
	re := require.New(t)
	tempDir := t.TempDir()
	regionStorage, err := storage.NewRegionStorageWithLevelDBBackend(context.Background(), tempDir, nil)
	re.NoError(err)
	defer func() {
		re.NoError(regionStorage.Close())
	}()

	server := mockserver.NewMockServer(
		context.Background(),
		nil,
		nil,
		storage.NewCoreStorage(storage.NewStorageWithMemoryBackend(), regionStorage),
		core.NewBasicCluster(),
	)
	syncer := NewRegionSyncer(server)
	ctx, cancel := context.WithCancel(context.Background())
	stream := newMockSyncRegionsServer()
	defer stream.cancel()
	done := make(chan error, 1)
	go func() {
		done <- syncer.Sync(ctx, stream)
	}()

	cancel()
	var syncErr error
	testutil.Eventually(re, func() bool {
		if syncErr == nil {
			select {
			case syncErr = <-done:
			default:
				return false
			}
		}
		st, ok := status.FromError(syncErr)
		return ok && st.Code() == codes.Unavailable
	})
}

type mockSyncRegionsServer struct {
	mu      sync.Mutex
	ctx     context.Context
	cancel  context.CancelFunc
	recvCh  chan *pdpb.SyncRegionRequest
	sendCh  chan *pdpb.SyncRegionResponse
	sendErr error
	blockCh chan struct{}
	blocked chan struct{}
	once    sync.Once
}

func newMockSyncRegionsServer() *mockSyncRegionsServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &mockSyncRegionsServer{
		ctx:    ctx,
		cancel: cancel,
		recvCh: make(chan *pdpb.SyncRegionRequest),
		sendCh: make(chan *pdpb.SyncRegionResponse, 1),
	}
}

func newTestRegionSyncer(t *testing.T, bc *core.BasicCluster) *RegionSyncer {
	t.Helper()
	re := require.New(t)
	tempDir := t.TempDir()
	regionStorage, err := storage.NewRegionStorageWithLevelDBBackend(context.Background(), tempDir, nil)
	re.NoError(err)
	t.Cleanup(func() {
		re.NoError(regionStorage.Close())
	})
	server := mockserver.NewMockServer(
		context.Background(),
		nil,
		nil,
		storage.NewCoreStorage(storage.NewStorageWithMemoryBackend(), regionStorage),
		bc,
	)
	return NewRegionSyncer(server)
}

func (s *mockSyncRegionsServer) Send(resp *pdpb.SyncRegionResponse) error {
	s.mu.Lock()
	err := s.sendErr
	blockCh := s.blockCh
	blocked := s.blocked
	s.mu.Unlock()
	if err != nil {
		return err
	}
	if blockCh != nil {
		if blocked != nil {
			s.once.Do(func() {
				close(blocked)
			})
		}
		<-blockCh
	}
	s.sendCh <- resp
	return nil
}

func (s *mockSyncRegionsServer) setSendErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sendErr = err
}

func (s *mockSyncRegionsServer) blockSend() chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.blockCh = make(chan struct{})
	s.blocked = make(chan struct{})
	s.once = sync.Once{}
	return s.blockCh
}

func (s *mockSyncRegionsServer) isSendBlocked() bool {
	s.mu.Lock()
	blocked := s.blocked
	s.mu.Unlock()
	if blocked == nil {
		return false
	}
	select {
	case <-blocked:
		return true
	default:
		return false
	}
}

func (s *mockSyncRegionsServer) Recv() (*pdpb.SyncRegionRequest, error) {
	select {
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	case req, ok := <-s.recvCh:
		if !ok {
			return nil, io.EOF
		}
		return req, nil
	}
}

func (*mockSyncRegionsServer) SetHeader(metadata.MD) error {
	return nil
}

func (*mockSyncRegionsServer) SendHeader(metadata.MD) error {
	return nil
}

func (*mockSyncRegionsServer) SetTrailer(metadata.MD) {}

func (s *mockSyncRegionsServer) Context() context.Context {
	return s.ctx
}

func (*mockSyncRegionsServer) SendMsg(any) error {
	return nil
}

func (*mockSyncRegionsServer) RecvMsg(any) error {
	return nil
}
