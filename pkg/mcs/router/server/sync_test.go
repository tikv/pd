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

package server

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestRegionSyncerRetryPreservesPendingLeaderChange(t *testing.T) {
	defer goleak.VerifyNone(t)
	re := require.New(t)
	syncer := NewRegionSyncer(t.Context(), nil, nil, nil, "test", "")
	defer syncer.cancelFunc()
	// Leader discovery can enqueue a forced reconnect while a no-leader retry waits.
	syncer.reconnectCh <- true
	done := make(chan struct{})
	go func() {
		defer close(done)
		syncer.startSyncWithLeader()
	}()
	select {
	case <-done:
	case <-time.After(3 * retryInterval):
		// Release the old implementation's blocked send before failing the test.
		<-syncer.reconnectCh
		<-done
		t.Fatal("retry blocked on a pending leader-change notification")
	}
	select {
	case force := <-syncer.reconnectCh:
		re.True(force)
	default:
		t.Fatal("retry lost the pending leader-change notification")
	}
}

func TestRegionSyncerCanceledRetryDoesNotEnqueue(t *testing.T) {
	defer goleak.VerifyNone(t)
	re := require.New(t)
	syncer := NewRegionSyncer(t.Context(), nil, nil, nil, "test", "")
	syncer.cancelFunc()
	syncer.startSyncWithLeader()
	re.Empty(syncer.reconnectCh)
}

func TestRegionSyncerRetryWithoutLeader(t *testing.T) {
	defer goleak.VerifyNone(t)
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		syncer := NewRegionSyncer(t.Context(), nil, nil, nil, "test", "")
		defer syncer.cancelFunc()
		syncer.startSyncWithLeader()
		select {
		case force := <-syncer.reconnectCh:
			re.False(force)
		default:
			t.Fatal("missing retry after leader discovery has not completed")
		}
	})
}

func TestRegionSyncerJoinsPreviousStream(t *testing.T) {
	defer goleak.VerifyNone(t)
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		ctx, cancel := context.WithCancel(t.Context())
		// A canceled context reaches stream cleanup without dialing a real server.
		cancel()
		syncer := NewRegionSyncer(ctx, nil, nil, nil, "test", "")
		syncer.pdLeaderAddr.Store("http://127.0.0.1:1")
		syncer.streamingRunning.Store(true)
		release := make(chan struct{})
		var releaseOnce sync.Once
		events := make(chan string, 2)
		var first atomic.Bool
		// Pause the existing cancellation log while the real stream is still exiting.
		restore := log.ReplaceGlobals(zap.NewExample(zap.Hooks(func(entry zapcore.Entry) error {
			if entry.Message == "leader context is done, stop syncing regions" && first.CompareAndSwap(false, true) {
				<-release
				events <- "previous stream finished"
			}
			return nil
		})), nil)
		defer restore()
		defer func() {
			releaseOnce.Do(func() { close(release) })
			syncer.Stop()
		}()
		syncer.startSyncWithLeader()
		synctest.Wait()
		re.True(first.Load(), "the previous stream must reach cleanup")
		re.False(syncer.IsRunning(), "the exiting stream must clear its state before retry notification")
		go func() {
			syncer.stopSync()
			events <- "replacement started"
			syncer.startSyncWithLeader()
		}()
		synctest.Wait()
		re.Empty(events, "replacement startup must wait for previous stream cleanup")
		releaseOnce.Do(func() { close(release) })
		synctest.Wait()
		re.Equal("previous stream finished", <-events)
		re.Equal("replacement started", <-events)
	})
}

type retryRegionSyncServer struct {
	pdpb.UnimplementedPDServer
	streams atomic.Int32
	started chan struct{}
}

func (s *retryRegionSyncServer) SyncRegions(stream pdpb.PD_SyncRegionsServer) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	s.started <- struct{}{}
	if s.streams.Add(1) == 1 {
		return nil
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}

func TestRegionSyncerRetriesAfterEOFWithPendingRetry(t *testing.T) {
	defer goleak.VerifyNone(t)
	re := require.New(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	re.NoError(err)
	remote := &retryRegionSyncServer{started: make(chan struct{}, 2)}
	grpcServer := grpc.NewServer()
	pdpb.RegisterPDServer(grpcServer, remote)
	served := make(chan error, 1)
	go func() { served <- grpcServer.Serve(listener) }()
	defer func() {
		grpcServer.Stop()
		re.NoError(<-served)
	}()

	syncer := NewRegionSyncer(t.Context(), nil, nil, &grpcutil.TLSConfig{}, "test", "")
	syncer.pdLeaderAddr.Store("http://" + listener.Addr().String())
	defer syncer.Stop()
	// A pending retry must remain useful when EOF schedules another retry.
	syncer.reconnectCh <- false
	syncer.startSyncWithLeader()
	testutil.Eventually(re, func() bool {
		return len(remote.started) == 1 && !syncer.IsRunning()
	})
	re.NotEmpty(remote.started, "the first stream must reach the server before EOF cleanup")
	<-remote.started
	re.False(syncer.IsRunning(), "the exiting stream must clear its state before retry notification")
	re.Len(syncer.reconnectCh, 1)

	syncer.wg.Add(1)
	go syncer.syncLoop()
	testutil.Eventually(re, func() bool { return syncer.IsRunning() && len(remote.started) == 1 })
	syncer.Stop()
	re.False(syncer.IsRunning())
}
