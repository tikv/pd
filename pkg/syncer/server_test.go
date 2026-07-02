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

	"github.com/pingcap/kvproto/pkg/metapb"
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
	syncer, _ := newTestRegionSyncer(t)
	syncer.history = newHistoryBufferWithConfig(2, 8, 1, storage.NewStorageWithMemoryBackend())
	syncer.history.resetWithIndex(10)

	stream := newMockSyncRegionsServer()
	stream.sendCh = make(chan *pdpb.SyncRegionResponse, 2)
	syncStream, syncStartIndex := syncer.bindStreamForSync("pd-follower", stream)
	defer syncer.unbindStream("pd-follower", syncStream)
	err := syncer.syncHistoryRegion(context.Background(), &pdpb.SyncRegionRequest{
		Header:     &pdpb.RequestHeader{ClusterId: keypath.ClusterID()},
		Member:     &pdpb.Member{Name: "pd-follower", ClientUrls: []string{"http://127.0.0.1:2379"}},
		StartIndex: 0,
	}, syncStream, syncStartIndex)

	re.NoError(err)
	re.Equal(2, syncer.history.capacity())
}

func TestSyncHistoryRecordsSplitBatches(t *testing.T) {
	re := require.New(t)
	syncer, _ := newTestRegionSyncer(t)
	records := make([]*core.RegionInfo, 0, maxSyncRegionBatchSize+1)
	for i := range maxSyncRegionBatchSize + 1 {
		records = append(records, newHistoryBufferTestRegion(uint64(i)))
	}
	stream := newMockSyncRegionsServer()
	syncStream := newRegionSyncStream(stream, 10)
	stream.sendCh = make(chan *pdpb.SyncRegionResponse, 2)

	re.NoError(syncer.syncHistoryRecords(10, records, syncStream))
	first := <-stream.sendCh
	second := <-stream.sendCh
	re.Equal(uint64(10), first.GetStartIndex())
	re.Len(first.GetRegions(), maxSyncRegionBatchSize)
	re.Equal(uint64(10+maxSyncRegionBatchSize), second.GetStartIndex())
	re.Len(second.GetRegions(), 1)

	closedStream := &testServerStream{}
	closedSyncStream := newRegionSyncStream(closedStream, 10)
	closedStream.onSend = func(*pdpb.SyncRegionResponse) {
		closedSyncStream.close()
	}

	err := syncer.syncHistoryRecords(10, records, closedSyncStream)

	re.Equal(codes.Unavailable, status.Code(err))
	responses := closedStream.sentResponses()
	re.Len(responses, 1)
	re.Equal(uint64(10), responses[0].GetStartIndex())
	re.Len(responses[0].GetRegions(), maxSyncRegionBatchSize)
}

func TestSyncFullRegionsBuffersLiveRecords(t *testing.T) {
	re := require.New(t)
	syncer, _ := newTestRegionSyncer(t, newHistoryBufferTestRegion(1))
	syncer.history = newHistoryBufferWithConfig(2, 8, 1, storage.NewStorageWithMemoryBackend())
	syncer.history.resetWithIndex(10)

	stream := newMockSyncRegionsServer()
	syncStream, startIndex := syncer.bindStreamForSync("pd-follower", stream)
	defer syncer.unbindStream("pd-follower", syncStream)
	unblockSend := stream.blockSend()
	done := make(chan error, 1)
	go func() {
		done <- syncFullRegionsForTest(context.Background(), syncer, syncStream, startIndex)
	}()
	testutil.Eventually(re, stream.isSendBlocked)

	records := make([]*core.RegionInfo, 0, 5)
	for i := range 5 {
		record := newHistoryBufferTestRegion(uint64(100 + i))
		records = append(records, record)
		syncer.history.record(record)
	}
	syncer.broadcast(context.Background(), records, false)

	close(unblockSend)
	fullResp := <-stream.sendCh
	catchUpResp := <-stream.sendCh
	re.NoError(<-done)
	re.Len(fullResp.GetRegions(), 1)
	re.Equal(startIndex, catchUpResp.GetStartIndex())
	re.Len(catchUpResp.GetRegions(), 5)
	re.Equal(startIndex+5, syncStream.getSendIndex())
	re.Equal(startIndex+5, syncer.history.getNextIndex())

	closedBC := core.NewBasicCluster()
	for i := range maxSyncRegionBatchSize + 1 {
		startKey := []byte{byte(i >> 8), byte(i)}
		endIndex := i + 1
		endKey := []byte{byte(endIndex >> 8), byte(endIndex)}
		region := core.NewRegionInfo(&metapb.Region{
			Id:       uint64(i + 1),
			StartKey: startKey,
			EndKey:   endKey,
		}, nil)
		closedBC.PutRegion(region)
	}
	closedSyncer := newTestRegionSyncerWithBasicCluster(t, closedBC)
	closedSyncer.history = newHistoryBufferWithConfig(2, 8, 1, storage.NewStorageWithMemoryBackend())
	closedSyncer.history.resetWithIndex(10)
	closedStream := &testServerStream{}
	closedSyncStream := newRegionSyncStream(closedStream, 10)
	closedStream.onSend = func(*pdpb.SyncRegionResponse) {
		closedSyncStream.close()
	}

	err := syncFullRegionsForTest(context.Background(), closedSyncer, closedSyncStream, 10)

	re.Equal(codes.Unavailable, status.Code(err))
	closedResponses := closedStream.sentResponses()
	re.Len(closedResponses, 1)
	re.Len(closedResponses[0].GetRegions(), maxSyncRegionBatchSize)
}

func syncFullRegionsForTest(ctx context.Context, syncer *RegionSyncer, syncStream *regionSyncStream, syncStartIndex uint64) error {
	syncStream.sendMu.Lock()
	defer syncStream.sendMu.Unlock()
	return syncer.syncFullRegionsLocked(ctx, "pd-follower", syncStream, syncStartIndex)
}

func TestSyncFullRegionsKeepsLiveRecordsAppendedDuringCatchUp(t *testing.T) {
	re := require.New(t)
	bc := core.NewBasicCluster()
	bc.PutRegion(newHistoryBufferTestRegion(1))
	syncer := newTestRegionSyncerWithBasicCluster(t, bc)
	syncer.history = newHistoryBufferWithConfig(2, 8, 1, storage.NewStorageWithMemoryBackend())
	syncer.history.resetWithIndex(10)

	stream := newBlockingSendStream(2)
	syncStream, startIndex := syncer.bindStreamForSync("pd-follower", stream)
	defer syncer.unbindStream("pd-follower", syncStream)

	catchUpRecords := []*core.RegionInfo{
		newHistoryBufferTestRegion(100),
		newHistoryBufferTestRegion(101),
	}
	for _, record := range catchUpRecords {
		syncer.history.record(record)
	}
	syncer.broadcast(context.Background(), catchUpRecords, false)

	done := make(chan error, 1)
	go func() {
		done <- syncFullRegionsForTest(context.Background(), syncer, syncStream, startIndex)
	}()
	testutil.Eventually(re, stream.isSendBlocked)

	liveStartIndex := startIndex + uint64(len(catchUpRecords))
	liveRecords := []*core.RegionInfo{newHistoryBufferTestRegion(102)}
	syncer.history.record(liveRecords[0])
	syncer.broadcast(context.Background(), liveRecords, false)

	stream.unblockSend()
	re.NoError(<-done)
	re.Equal(liveStartIndex, syncStream.getSendIndex())
	re.Equal(liveStartIndex+1, syncer.history.getNextIndex())
	re.Equal(liveRecords, syncer.history.recordsFrom(liveStartIndex))

	re.NoError(syncer.sendDownstream(context.Background(), "pd-follower", syncStream, false))
	responses := stream.sentResponses()
	re.Len(responses, 4)
	re.Equal(liveStartIndex, responses[2].GetStartIndex())
	re.Empty(responses[2].GetRegions())
	re.Equal(liveStartIndex, responses[3].GetStartIndex())
	re.Equal([]*metapb.Region{{Id: 102}}, responses[3].GetRegions())
	re.Equal(liveStartIndex+1, syncStream.getSendIndex())
}

func TestSyncFullRegionsFailsWhenCatchUpHistoryExceedsMax(t *testing.T) {
	re := require.New(t)
	syncer, bc := newTestRegionSyncer(t, newTestSyncRegion(1, 11))
	syncer.history = newHistoryBufferWithConfig(1, 1, 1, storage.NewStorageWithMemoryBackend())
	syncer.history.resetWithIndex(100)

	stream := newMockSyncRegionsServer()
	syncStream, startIndex := syncer.bindStreamForSync("pd-follower", stream)
	defer syncer.unbindStream("pd-follower", syncStream)
	unblockSend := stream.blockSend()
	done := make(chan error, 1)
	go func() {
		done <- syncFullRegionsForTest(context.Background(), syncer, syncStream, startIndex)
	}()
	testutil.Eventually(re, stream.isSendBlocked)

	for _, region := range []*core.RegionInfo{
		newTestSyncRegion(2, 12),
		newTestSyncRegion(3, 13),
	} {
		bc.PutRegion(region)
		syncer.history.record(region)
	}

	close(unblockSend)
	fullResp := <-stream.sendCh
	re.Equal(uint64(0), fullResp.GetStartIndex())
	re.Len(fullResp.GetRegions(), 1)

	re.Equal(codes.ResourceExhausted, status.Code(<-done))
}

func TestIncrementalHistoryReplayOverflowDisconnectsStream(t *testing.T) {
	re := require.New(t)
	syncer := newTestRegionSyncerWithBasicCluster(t, core.NewBasicCluster())
	syncer.history = newHistoryBufferWithConfig(2, 2, 1, storage.NewStorageWithMemoryBackend())
	syncer.history.resetWithIndex(10)
	syncer.history.record(newHistoryBufferTestRegion(10))

	stream := newMockSyncRegionsServer()
	syncStream, syncStartIndex := syncer.bindStreamForSync("pd-follower", stream)
	defer syncer.unbindStream("pd-follower", syncStream)
	unblockSend := stream.blockSend()
	replayDone := make(chan error, 1)
	go func() {
		replayDone <- syncer.syncHistoryRegion(context.Background(), &pdpb.SyncRegionRequest{
			Header:     &pdpb.RequestHeader{ClusterId: keypath.ClusterID()},
			Member:     &pdpb.Member{Name: "pd-follower", ClientUrls: []string{"http://127.0.0.1:2379"}},
			StartIndex: 10,
		}, syncStream, syncStartIndex)
	}()
	testutil.Eventually(re, stream.isSendBlocked)

	liveRecords := []*core.RegionInfo{
		newHistoryBufferTestRegion(11),
		newHistoryBufferTestRegion(12),
		newHistoryBufferTestRegion(13),
	}
	for _, record := range liveRecords {
		syncer.history.record(record)
	}
	syncer.broadcast(context.Background(), liveRecords, false)
	re.Greater(syncer.history.getFirstIndex(), syncStartIndex)

	close(unblockSend)
	re.NotNil(<-stream.sendCh)
	re.NoError(<-replayDone)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	senderDone := make(chan struct{})
	go func() {
		syncer.runDownstreamSender(ctx, "pd-follower", syncStream)
		close(senderDone)
	}()

	testutil.Eventually(re, func() bool {
		return len(syncer.GetAllDownstreamNames()) == 0
	})
	testutil.Eventually(re, func() bool {
		select {
		case <-senderDone:
			return true
		default:
			return false
		}
	})
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
	syncer.broadcast(context.Background(), nil, true)

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

func TestCloseAllClientTimesOutBlockedSend(t *testing.T) {
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

	testutil.Eventually(re, func() bool {
		select {
		case <-closeDone:
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
		syncer.broadcast(context.Background(), nil, true)
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

func TestSendRegionSyncResponseSerializesStreamSendAfterTimeout(t *testing.T) {
	re := require.New(t)
	stream := newBlockingSendStream(1)
	syncStream := newRegionSyncStream(stream, 10)
	syncer := newTestRegionSyncerWithStreams(t, map[string]*regionSyncStream{
		"pd2": syncStream,
	})
	syncer.sendTimeout = 10 * time.Millisecond
	syncer.history.resetWithIndex(10)
	syncer.history.record(newTestRegion(1))

	err := syncer.sendDownstream(context.Background(), "pd2", syncStream, false)
	re.Error(err)
	testutil.Eventually(re, stream.isSendBlocked)

	closeSendStarted := make(chan struct{})
	closeSendDone := make(chan error, 1)
	go func() {
		close(closeSendStarted)
		closeSendDone <- syncStream.send(&pdpb.SyncRegionResponse{
			Header: &pdpb.ResponseHeader{ClusterId: keypath.ClusterID()},
		})
	}()
	<-closeSendStarted

	time.Sleep(20 * time.Millisecond)
	re.Equal(1, stream.sendCountValue())
	select {
	case err := <-closeSendDone:
		re.NoError(err)
		re.Fail("close response send should wait for the in-flight send")
	default:
	}

	stream.unblockSend()
	var closeSendErr error
	testutil.Eventually(re, func() bool {
		select {
		case closeSendErr = <-closeSendDone:
			return true
		default:
			return false
		}
	})
	re.NoError(closeSendErr)
	re.Equal(2, stream.sendCountValue())
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

func TestBroadcastUsesIndependentDownstreamIndexes(t *testing.T) {
	re := require.New(t)
	pd2Stream := &testServerStream{}
	pd3Stream := &testServerStream{}
	records := []*core.RegionInfo{newTestRegion(1), newTestRegion(2)}
	pd2SyncStream := newRegionSyncStream(pd2Stream, 10)
	pd3SyncStream := newRegionSyncStream(pd3Stream, 10)
	pd3SyncStream.advanceSendIndex(1)
	syncer := newTestRegionSyncerWithStreams(t, map[string]*regionSyncStream{
		"pd2": pd2SyncStream,
		"pd3": pd3SyncStream,
	})
	syncer.history.resetWithIndex(10)
	for _, record := range records {
		syncer.history.record(record)
	}

	syncer.broadcast(context.Background(), records, false)

	testutil.Eventually(re, func() bool {
		return pd2Stream.lastResponse() != nil && pd3Stream.lastResponse() != nil
	})
	re.Equal(uint64(10), pd2Stream.lastResponse().GetStartIndex())
	re.Equal(uint64(11), pd3Stream.lastResponse().GetStartIndex())
	re.Equal([]*metapb.Region{{Id: 1}, {Id: 2}}, pd2Stream.lastResponse().GetRegions())
	re.Equal([]*metapb.Region{{Id: 2}}, pd3Stream.lastResponse().GetRegions())
	re.Equal(uint64(12), pd2SyncStream.getSendIndex())
	re.Equal(uint64(12), pd3SyncStream.getSendIndex())
	re.Equal(records, syncer.history.recordsFrom(10))
}

func TestBroadcastRemovesOnlyFailedDownstream(t *testing.T) {
	re := require.New(t)
	pd2Stream := &testServerStream{sendErr: errors.New("send failed")}
	pd3Stream := &testServerStream{}
	pd2SyncStream := newRegionSyncStream(pd2Stream, 10)
	pd3SyncStream := newRegionSyncStream(pd3Stream, 10)
	syncer := newTestRegionSyncerWithStreams(t, map[string]*regionSyncStream{
		"pd2": pd2SyncStream,
		"pd3": pd3SyncStream,
	})
	records := []*core.RegionInfo{newTestRegion(1)}
	syncer.history.resetWithIndex(10)
	syncer.history.record(records[0])

	syncer.broadcast(context.Background(), records, false)

	testutil.Eventually(re, func() bool {
		return len(syncer.GetAllDownstreamNames()) == 1 && pd3Stream.lastResponse() != nil
	})
	re.ElementsMatch([]string{"pd3"}, syncer.GetAllDownstreamNames())
	re.Nil(pd2Stream.lastResponse())
	re.Equal(uint64(10), pd2SyncStream.getSendIndex())
	re.Equal(uint64(10), pd3Stream.lastResponse().GetStartIndex())
	re.Equal(uint64(11), pd3SyncStream.getSendIndex())
}

func TestBroadcastRecordsBusyDownstreamAndDrainsLater(t *testing.T) {
	re := require.New(t)
	activeStream := &testServerStream{}
	busyStream := &testServerStream{}
	activeSyncStream := newRegionSyncStream(activeStream, 10)
	busySyncStream := newRegionSyncStream(busyStream, 10)
	busySyncStream.sendMu.Lock()
	busyLocked := true
	defer func() {
		if busyLocked {
			busySyncStream.sendMu.Unlock()
		}
	}()
	syncer := newTestRegionSyncerWithStreams(t, map[string]*regionSyncStream{
		"pd2": activeSyncStream,
		"pd3": busySyncStream,
	})
	records := []*core.RegionInfo{newTestRegion(1), newTestRegion(2)}
	syncer.history.resetWithIndex(10)
	for _, record := range records {
		syncer.history.record(record)
	}

	syncer.broadcast(context.Background(), records, false)

	testutil.Eventually(re, func() bool {
		return activeStream.lastResponse() != nil
	})
	re.Equal(uint64(10), activeStream.lastResponse().GetStartIndex())
	re.Equal(uint64(12), activeSyncStream.getSendIndex())
	re.Nil(busyStream.lastResponse())
	re.Equal(uint64(10), busySyncStream.getSendIndex())
	re.Equal(records, syncer.history.recordsFrom(10))

	busySyncStream.sendMu.Unlock()
	busyLocked = false
	testutil.Eventually(re, func() bool {
		return busyStream.lastResponse() != nil
	})
	re.Equal(uint64(10), busyStream.lastResponse().GetStartIndex())
	re.Equal(uint64(12), busySyncStream.getSendIndex())
}

func TestAppendHistoryRecordsKeepsSlowestDownstreamWindow(t *testing.T) {
	re := require.New(t)
	pd2SyncStream := newRegionSyncStream(&testServerStream{}, 10)
	pd3SyncStream := newRegionSyncStream(&testServerStream{}, 10)
	pd3SyncStream.advanceSendIndex(2)
	syncer := newTestRegionSyncerWithStreams(t, map[string]*regionSyncStream{
		"pd2": pd2SyncStream,
		"pd3": pd3SyncStream,
	})
	syncer.history = newTestHistoryBuffer(8)
	syncer.history.resetWithIndex(10)
	syncer.history.record(
		newTestRegion(1),
		newTestRegion(2),
	)

	syncer.appendHistoryRecords([]*core.RegionInfo{
		newTestRegion(3),
		newTestRegion(4),
		newTestRegion(5),
	})

	records := syncer.history.recordsFrom(10)
	re.Len(records, 5)
	re.Equal(uint64(1), records[0].GetID())
	re.Equal(uint64(5), records[4].GetID())
	re.Equal(8, syncer.history.capacity())
	re.Equal(uint64(10), pd2SyncStream.getSendIndex())
	re.Equal(uint64(12), pd3SyncStream.getSendIndex())
}

func TestBindStreamWaitsForAppendHistoryRecordsBoundary(t *testing.T) {
	re := require.New(t)
	stream := &testServerStream{}
	syncer := newTestRegionSyncerWithStreams(t, map[string]*regionSyncStream{})
	syncer.history = newTestHistoryBuffer(8)
	syncer.history.resetWithIndex(10)
	records := []*core.RegionInfo{
		newTestRegion(1),
		newTestRegion(2),
		newTestRegion(3),
	}

	syncer.mu.RLock()
	bindResultCh := make(chan uint64, 1)
	bindDoneCh := make(chan struct{})
	go func() {
		defer close(bindDoneCh)
		syncStream, syncStartIndex := syncer.bindStreamForSync("pd2", stream)
		defer syncer.unbindStream("pd2", syncStream)
		bindResultCh <- syncStartIndex
	}()

	select {
	case <-bindResultCh:
		re.Fail("bindStreamForSync should wait for the append history boundary")
	case <-time.After(10 * time.Millisecond):
	}
	syncer.observeDownstreamReplayWindowLocked(uint64(len(records)))
	syncer.history.record(records...)
	syncer.mu.RUnlock()

	re.Equal(uint64(13), <-bindResultCh)
	<-bindDoneCh
}

func TestAppendHistoryRecordsDoesNotWaitForBusyDownstream(t *testing.T) {
	re := require.New(t)
	busySyncStream := newRegionSyncStream(&testServerStream{}, 10)
	busySyncStream.sendMu.Lock()
	busyLocked := true
	defer func() {
		if busyLocked {
			busySyncStream.sendMu.Unlock()
		}
	}()
	syncer := newTestRegionSyncerWithStreams(t, map[string]*regionSyncStream{
		"pd2": busySyncStream,
	})
	syncer.history = newTestHistoryBuffer(8)
	syncer.history.resetWithIndex(10)
	syncer.history.record(
		newTestRegion(1),
		newTestRegion(2),
	)

	done := make(chan struct{})
	go func() {
		syncer.appendHistoryRecords([]*core.RegionInfo{
			newTestRegion(3),
			newTestRegion(4),
			newTestRegion(5),
		})
		close(done)
	}()

	testutil.Eventually(re, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	})
	records := syncer.history.recordsFrom(10)
	re.Len(records, 5)
	re.Equal(uint64(10), busySyncStream.getSendIndex())

	busySyncStream.sendMu.Unlock()
	busyLocked = false
}

func TestRemovedStreamDoesNotKeepHistoryWindow(t *testing.T) {
	re := require.New(t)
	slowSyncStream := newRegionSyncStream(&testServerStream{}, 10)
	fastSyncStream := newRegionSyncStream(&testServerStream{}, 12)
	syncer := newTestRegionSyncerWithStreams(t, map[string]*regionSyncStream{
		"slow": slowSyncStream,
		"fast": fastSyncStream,
	})
	syncer.history = newTestHistoryBuffer(8)
	syncer.history.resetWithIndex(10)
	syncer.history.record(
		newTestRegion(1),
		newTestRegion(2),
	)
	syncer.appendHistoryRecords([]*core.RegionInfo{
		newTestRegion(3),
		newTestRegion(4),
		newTestRegion(5),
	})
	re.Equal(8, syncer.history.capacity())

	fastSyncStream.advanceSendIndex(3)
	syncer.unbindStream("slow", slowSyncStream)
	minIndex, ok := syncer.minDownstreamSendIndex()
	re.True(ok)
	re.Equal(uint64(15), minIndex)
	for range historyBufferShrinkRounds + 1 {
		syncer.observeDownstreamReplayWindow(0)
		syncer.history.maybeShrink()
	}

	re.Equal(4, syncer.history.capacity())
	re.Equal(uint64(11), syncer.history.getFirstIndex())
}

func TestKeepAliveDoesNotPrecedePendingRecords(t *testing.T) {
	re := require.New(t)
	stream := &testServerStream{}
	syncStream := newRegionSyncStream(stream, 10)
	syncer := newTestRegionSyncerWithStreams(t, map[string]*regionSyncStream{
		"pd2": syncStream,
	})
	syncer.history.resetWithIndex(10)
	syncer.appendHistoryRecords([]*core.RegionInfo{
		newTestRegion(1),
		newTestRegion(2),
	})

	re.NoError(syncer.sendDownstream(context.Background(), "pd2", syncStream, true))
	responses := stream.sentResponses()
	re.Len(responses, 1)
	re.Equal(uint64(10), responses[0].GetStartIndex())
	re.Equal([]*metapb.Region{{Id: 1}, {Id: 2}}, responses[0].GetRegions())
	re.Equal(uint64(12), syncStream.getSendIndex())

	re.NoError(syncer.sendDownstream(context.Background(), "pd2", syncStream, true))
	responses = stream.sentResponses()
	re.Len(responses, 2)
	re.Equal(uint64(12), responses[1].GetStartIndex())
	re.Empty(responses[1].GetRegions())
}

func TestDrainDownstreamSendsPendingRecordsSerially(t *testing.T) {
	re := require.New(t)
	stream := &testServerStream{}
	syncStream := newRegionSyncStream(stream, 10)
	syncer := newTestRegionSyncerWithStreams(t, map[string]*regionSyncStream{
		"pd2": syncStream,
	})
	bufferedRecords := []*core.RegionInfo{newTestRegion(1), newTestRegion(2)}
	syncer.history.resetWithIndex(10)
	for _, record := range bufferedRecords {
		syncer.history.record(record)
	}

	re.NoError(syncer.sendDownstream(context.Background(), "pd2", syncStream, false))
	re.Equal(uint64(12), syncStream.getSendIndex())
	re.Len(stream.sentResponses(), 1)
	re.Equal(uint64(10), stream.lastResponse().GetStartIndex())
	re.Equal([]*metapb.Region{{Id: 1}, {Id: 2}}, stream.lastResponse().GetRegions())

	liveRecords := []*core.RegionInfo{newTestRegion(3)}
	syncer.history.record(liveRecords[0])
	syncer.broadcast(context.Background(), liveRecords, false)

	testutil.Eventually(re, func() bool {
		return len(stream.sentResponses()) == 2
	})
	re.Len(stream.sentResponses(), 2)
	re.Equal(uint64(12), stream.lastResponse().GetStartIndex())
	re.Equal(uint64(13), syncStream.getSendIndex())
}

func TestSyncHistoryRegionStopsAtSyncStartIndex(t *testing.T) {
	re := require.New(t)
	syncer := &RegionSyncer{history: newTestHistoryBuffer(defaultHistoryBufferSize)}
	syncer.history.resetWithIndex(10)
	first := newTestRegion(1)
	second := newTestRegion(2)
	syncer.history.record(first)
	syncStartIndex := syncer.history.getNextIndex()
	syncer.history.record(second)
	stream := &testServerStream{}
	request := &pdpb.SyncRegionRequest{
		Header:     &pdpb.RequestHeader{ClusterId: keypath.ClusterID()},
		Member:     &pdpb.Member{Name: "pd2"},
		StartIndex: 10,
	}

	syncStream := newRegionSyncStream(stream, syncStartIndex)
	err := syncer.syncHistoryRegion(context.Background(), request, syncStream, syncStartIndex)

	re.NoError(err)
	re.Equal(uint64(10), stream.lastResponse().GetStartIndex())
	re.Equal([]*metapb.Region{{Id: 1}}, stream.lastResponse().GetRegions())
}

type testServerStream struct {
	mu        sync.Mutex
	sendErr   error
	onSend    func(*pdpb.SyncRegionResponse)
	responses []*pdpb.SyncRegionResponse
}

type blockingSendStream struct {
	mu          sync.Mutex
	responses   []*pdpb.SyncRegionResponse
	blockOnSend int
	sendCount   int
	blocked     chan struct{}
	unblock     chan struct{}
	once        sync.Once
}

func newBlockingSendStream(blockOnSend int) *blockingSendStream {
	return &blockingSendStream{
		blockOnSend: blockOnSend,
		blocked:     make(chan struct{}),
		unblock:     make(chan struct{}),
	}
}

func (s *blockingSendStream) Send(resp *pdpb.SyncRegionResponse) error {
	s.mu.Lock()
	s.sendCount++
	block := s.sendCount == s.blockOnSend
	if block {
		s.once.Do(func() {
			close(s.blocked)
		})
	}
	s.mu.Unlock()

	if block {
		<-s.unblock
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.responses = append(s.responses, resp)
	return nil
}

func (s *blockingSendStream) isSendBlocked() bool {
	select {
	case <-s.blocked:
		return true
	default:
		return false
	}
}

func (s *blockingSendStream) unblockSend() {
	close(s.unblock)
}

func (s *blockingSendStream) sendCountValue() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sendCount
}

func (s *blockingSendStream) sentResponses() []*pdpb.SyncRegionResponse {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*pdpb.SyncRegionResponse(nil), s.responses...)
}

func (s *testServerStream) Send(resp *pdpb.SyncRegionResponse) error {
	s.mu.Lock()
	if s.sendErr != nil {
		s.mu.Unlock()
		return s.sendErr
	}
	s.responses = append(s.responses, resp)
	onSend := s.onSend
	s.mu.Unlock()
	if onSend != nil {
		onSend(resp)
	}
	return nil
}

func (s *testServerStream) lastResponse() *pdpb.SyncRegionResponse {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.responses) == 0 {
		return nil
	}
	return s.responses[len(s.responses)-1]
}

func (s *testServerStream) sentResponses() []*pdpb.SyncRegionResponse {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*pdpb.SyncRegionResponse(nil), s.responses...)
}

func newTestRegion(id uint64) *core.RegionInfo {
	return core.NewRegionInfo(&metapb.Region{Id: id}, nil)
}

func newTestRegionSyncerWithStreams(t *testing.T, streams map[string]*regionSyncStream) *RegionSyncer {
	syncer := &RegionSyncer{
		history:     newTestHistoryBuffer(defaultHistoryBufferSize),
		sendTimeout: syncerKeepAliveInterval,
	}
	syncer.mu.streams = streams
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		for _, stream := range streams {
			stream.close()
		}
	})
	for name, stream := range streams {
		go syncer.runDownstreamSender(ctx, name, stream)
	}
	return syncer
}

func TestSyncFallsBackToFullSyncWhenHistoryMissing(t *testing.T) {
	re := require.New(t)
	syncer, _ := newTestRegionSyncer(t, newTestSyncRegion(1, 11))
	syncer.history.resetWithIndex(100)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := newMockSyncRegionsServer()
	blockCh := stream.blockSend()
	done := startTestRegionSync(ctx, syncer, stream)

	sendTestSyncRegionRequest(stream, 1)
	testutil.Eventually(re, stream.isSendBlocked)
	syncer.history.record(newTestSyncRegion(2, 12))
	close(blockCh)

	resp := mustRecvSyncRegionResponse(t, stream, "expected full sync response")
	re.Equal(uint64(0), resp.GetStartIndex())
	re.Len(resp.GetRegions(), 1)
	re.Equal(uint64(1), resp.GetRegions()[0].GetId())

	resp = mustRecvSyncRegionResponse(t, stream, "expected full sync catch-up response")
	re.Equal(uint64(100), resp.GetStartIndex())
	re.Len(resp.GetRegions(), 1)
	re.Equal(uint64(2), resp.GetRegions()[0].GetId())

	resp = mustRecvSyncRegionResponse(t, stream, "expected full sync completion response")
	re.Equal(uint64(101), resp.GetStartIndex())
	re.Empty(resp.GetRegions())
	waitTestRegionSyncerBound(re, syncer)

	cancel()
	waitTestRegionSyncerUnavailable(re, done)
}

func TestFullSyncDisconnectsWhenHistoryBufferOverflowsDuringCatchUp(t *testing.T) {
	re := require.New(t)
	syncer, bc := newTestRegionSyncer(t, newTestSyncRegion(1, 11))
	syncer.history = newHistoryBufferWithConfig(1, 1, 1, syncer.history.kv)
	syncer.history.resetWithIndex(100)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := newMockSyncRegionsServer()
	blockCh := stream.blockSend()
	done := startTestRegionSync(ctx, syncer, stream)

	sendTestSyncRegionRequest(stream, 1)
	testutil.Eventually(re, stream.isSendBlocked)
	for _, region := range []*core.RegionInfo{
		newTestSyncRegion(2, 12),
		newTestSyncRegion(3, 13),
	} {
		bc.PutRegion(region)
		syncer.history.record(region)
	}
	close(blockCh)

	resp := mustRecvSyncRegionResponse(t, stream, "expected original full sync response")
	re.Equal(uint64(0), resp.GetStartIndex())
	re.Len(resp.GetRegions(), 1)
	re.Equal(uint64(1), resp.GetRegions()[0].GetId())

	syncErr := <-done
	re.Equal(codes.ResourceExhausted, status.Code(syncErr))
	re.Empty(syncer.GetAllDownstreamNames())
}

func TestClientWaitsForFullSyncCompletionBeforeRunning(t *testing.T) {
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
	bc := core.NewBasicCluster()
	region := &metapb.Region{
		Id:          1,
		StartKey:    []byte{1},
		EndKey:      []byte{2},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
		Peers:       []*metapb.Peer{{Id: 11, StoreId: 1}},
	}

	handled, fullSyncing := syncer.handleRegionSyncResponse(context.Background(), &pdpb.SyncRegionResponse{
		Header:     &pdpb.ResponseHeader{ClusterId: keypath.ClusterID()},
		Regions:    []*metapb.Region{region},
		StartIndex: 0,
	}, bc, regionStorage, false)
	re.True(handled)
	re.True(fullSyncing)
	re.False(syncer.IsRunning())
	re.Equal(uint64(0), syncer.history.getNextIndex())

	handled, fullSyncing = syncer.handleRegionSyncResponse(context.Background(), &pdpb.SyncRegionResponse{
		Header:     &pdpb.ResponseHeader{ClusterId: keypath.ClusterID()},
		StartIndex: 1,
	}, bc, regionStorage, fullSyncing)
	re.True(handled)
	re.False(fullSyncing)
	re.True(syncer.IsRunning())
	re.Equal(uint64(1), syncer.history.getNextIndex())
}

func TestClientFinishesFullSyncWithOldLeaderKeepAlive(t *testing.T) {
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
	syncer.history.resetWithIndex(42)
	bc := core.NewBasicCluster()
	snapshotRegion := newTestSyncRegion(1, 11)
	catchUpRegion := newTestSyncRegion(2, 12)

	handled, fullSyncing := syncer.handleRegionSyncResponse(context.Background(), &pdpb.SyncRegionResponse{
		Header:     &pdpb.ResponseHeader{ClusterId: keypath.ClusterID()},
		Regions:    []*metapb.Region{snapshotRegion.GetMeta()},
		StartIndex: 0,
	}, bc, regionStorage, false)
	re.True(handled)
	re.True(fullSyncing)
	re.False(syncer.IsRunning())
	re.Equal(uint64(0), syncer.history.getNextIndex())
	re.NotNil(bc.GetRegion(1))

	handled, fullSyncing = syncer.handleRegionSyncResponse(context.Background(), &pdpb.SyncRegionResponse{
		Header:     &pdpb.ResponseHeader{ClusterId: keypath.ClusterID()},
		Regions:    []*metapb.Region{catchUpRegion.GetMeta()},
		StartIndex: 42,
	}, bc, regionStorage, fullSyncing)
	re.True(handled)
	re.True(fullSyncing)
	re.False(syncer.IsRunning())
	re.Equal(uint64(0), syncer.history.getNextIndex())
	re.NotNil(bc.GetRegion(2))

	handled, fullSyncing = syncer.handleRegionSyncResponse(context.Background(), &pdpb.SyncRegionResponse{
		Header:     &pdpb.ResponseHeader{ClusterId: keypath.ClusterID()},
		StartIndex: 43,
	}, bc, regionStorage, fullSyncing)
	re.True(handled)
	re.False(fullSyncing)
	re.True(syncer.IsRunning())
	re.Equal(uint64(43), syncer.history.getNextIndex())
}

func TestClientWaitsForEmptySnapshotCatchUpCompletion(t *testing.T) {
	re := require.New(t)
	leaderSyncer, _ := newTestRegionSyncer(t)
	syncStartIndex := uint64(42)
	leaderSyncer.history.resetWithIndex(syncStartIndex)
	for i := range maxSyncRegionBatchSize + 1 {
		id := uint64(i + 1)
		leaderSyncer.history.record(newTestSyncRegionWithRange(id, id+1))
	}
	stream := newMockSyncRegionsServer()
	syncStream := newRegionSyncStream(stream, syncStartIndex)

	re.NoError(syncFullRegionsForTest(context.Background(), leaderSyncer, syncStream, syncStartIndex))
	first := mustRecvSyncRegionResponse(t, stream, "expected first catch-up response")
	re.Equal(uint64(0), first.GetStartIndex())
	re.Len(first.GetRegions(), maxSyncRegionBatchSize)
	second := mustRecvSyncRegionResponse(t, stream, "expected second catch-up response")
	re.Equal(uint64(maxSyncRegionBatchSize), second.GetStartIndex())
	re.Len(second.GetRegions(), 1)
	completion := mustRecvSyncRegionResponse(t, stream, "expected full sync completion response")
	re.Equal(syncStartIndex+uint64(maxSyncRegionBatchSize)+1, completion.GetStartIndex())
	re.Empty(completion.GetRegions())

	regionStorage := storage.NewStorageWithMemoryBackend()
	clientServer := mockserver.NewMockServer(
		context.Background(),
		nil,
		nil,
		regionStorage,
		core.NewBasicCluster(),
	)
	clientSyncer := NewRegionSyncer(clientServer)
	clientSyncer.history.resetWithIndex(syncStartIndex)
	bc := core.NewBasicCluster()

	handled, fullSyncing := clientSyncer.handleRegionSyncResponse(context.Background(), first, bc, regionStorage, false)
	re.True(handled)
	re.True(fullSyncing)
	re.False(clientSyncer.IsRunning())

	handled, fullSyncing = clientSyncer.handleRegionSyncResponse(context.Background(), second, bc, regionStorage, fullSyncing)
	re.True(handled)
	re.True(fullSyncing)
	re.False(clientSyncer.IsRunning())

	handled, fullSyncing = clientSyncer.handleRegionSyncResponse(context.Background(), completion, bc, regionStorage, fullSyncing)
	re.True(handled)
	re.False(fullSyncing)
	re.True(clientSyncer.IsRunning())
	re.Equal(syncStartIndex+uint64(maxSyncRegionBatchSize)+1, clientSyncer.history.getNextIndex())
}

func newTestRegionSyncer(t *testing.T, regions ...*core.RegionInfo) (*RegionSyncer, *core.BasicCluster) {
	t.Helper()
	bc := core.NewBasicCluster()
	for _, region := range regions {
		bc.PutRegion(region)
	}
	return newTestRegionSyncerWithBasicCluster(t, bc), bc
}

func newTestRegionSyncerWithBasicCluster(t *testing.T, bc *core.BasicCluster) *RegionSyncer {
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

func newTestSyncRegion(regionID, peerID uint64) *core.RegionInfo {
	return newTestSyncRegionWithRange(regionID, peerID)
}

func newTestSyncRegionWithRange(regionID, peerID uint64) *core.RegionInfo {
	return core.NewRegionInfo(&metapb.Region{
		Id:          regionID,
		StartKey:    []byte{byte(regionID >> 8), byte(regionID)},
		EndKey:      []byte{byte((regionID + 1) >> 8), byte(regionID + 1)},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
		Peers:       []*metapb.Peer{{Id: peerID, StoreId: 1}},
	}, &metapb.Peer{Id: peerID, StoreId: 1})
}

func startTestRegionSync(ctx context.Context, syncer *RegionSyncer, stream *mockSyncRegionsServer) chan error {
	done := make(chan error, 1)
	go func() {
		done <- syncer.Sync(ctx, stream)
	}()
	return done
}

func sendTestSyncRegionRequest(stream *mockSyncRegionsServer, startIndex uint64) {
	stream.recvCh <- &pdpb.SyncRegionRequest{
		Header:     &pdpb.RequestHeader{ClusterId: keypath.ClusterID()},
		StartIndex: startIndex,
		Member: &pdpb.Member{
			Name:       "pd-follower",
			ClientUrls: []string{"http://127.0.0.1:2379"},
		},
	}
}

func mustRecvSyncRegionResponse(t *testing.T, stream *mockSyncRegionsServer, message string) *pdpb.SyncRegionResponse {
	t.Helper()
	select {
	case resp := <-stream.sendCh:
		return resp
	case <-time.After(3 * time.Second):
		require.FailNow(t, message)
		return nil
	}
}

func waitTestRegionSyncerBound(re *require.Assertions, syncer *RegionSyncer) {
	testutil.Eventually(re, func() bool {
		names := syncer.GetAllDownstreamNames()
		return len(names) == 1 && names[0] == "pd-follower"
	})
}

func waitTestRegionSyncerUnavailable(re *require.Assertions, done <-chan error) {
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
		sendCh: make(chan *pdpb.SyncRegionResponse, 16),
	}
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
