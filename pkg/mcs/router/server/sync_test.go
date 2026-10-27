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
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"

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

func TestRegionSyncerRetriesAfterPreviousStreamFinishes(t *testing.T) {
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
	cleanupStarted := make(chan context.Context, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	var first atomic.Bool
	const hook = "github.com/tikv/pd/pkg/mcs/router/server/beforeRegionSyncStreamDone"
	re.NoError(failpoint.EnableCall(hook, func(ctx context.Context) {
		if first.CompareAndSwap(false, true) {
			cleanupStarted <- ctx
			<-release
		}
	}))
	defer func() {
		releaseOnce.Do(func() { close(release) })
		syncer.Stop()
		re.NoError(failpoint.Disable(hook))
	}()
	// A pending retry must remain useful when EOF schedules another retry.
	syncer.reconnectCh <- false
	syncer.startSyncWithLeader()
	var firstCtx context.Context
	testutil.Eventually(re, func() bool {
		select {
		case firstCtx = <-cleanupStarted:
			return true
		default:
			return false
		}
	})
	re.NotEmpty(remote.started, "the first stream must reach the server before EOF cleanup")
	<-remote.started
	re.False(syncer.IsRunning(), "the exiting stream must clear its state before retry notification")
	re.Len(syncer.reconnectCh, 1)

	syncer.wg.Add(1)
	go syncer.syncLoop()
	testutil.Eventually(re, func() bool { return firstCtx.Err() != nil })
	re.Empty(remote.started, "the replacement must wait for previous stream cleanup")
	releaseOnce.Do(func() { close(release) })
	testutil.Eventually(re, func() bool { return syncer.IsRunning() && len(remote.started) == 1 })
	syncer.Stop()
	re.False(syncer.IsRunning())
}
