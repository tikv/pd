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

package pd

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"

	"github.com/tikv/pd/client/clients/tso"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/utils/grpcutil"
	sd "github.com/tikv/pd/client/servicediscovery"
)

type delayedDialServiceDiscovery struct {
	sd.ServiceDiscovery
	dialStarted  chan struct{}
	resumeDial   chan struct{}
	dialFinished chan struct{}
	dialOnce     sync.Once
	dialErr      error
	clientConns  sync.Map
}

func (*delayedDialServiceDiscovery) ExecAndAddLeaderSwitchedCallback(callback sd.LeaderSwitchedCallbackFunc) {
	_ = callback("http://127.0.0.1:1")
}

func (s *delayedDialServiceDiscovery) GetOrCreateGRPCConn(url string) (*grpc.ClientConn, error) {
	s.dialOnce.Do(func() {
		close(s.dialStarted)
		<-s.resumeDial
		_, s.dialErr = grpcutil.GetOrCreateGRPCConn(context.Background(), &s.clientConns, url, nil)
		close(s.dialFinished)
	})
	if s.dialErr != nil {
		return nil, s.dialErr
	}
	// The update was canceled while dialing, so no TSO stream is needed.
	return nil, context.Canceled
}

func (s *delayedDialServiceDiscovery) Close() {
	s.clientConns.Range(func(_, value any) bool {
		_ = value.(*grpc.ClientConn).Close()
		return true
	})
}

func TestCloseWaitsForTSOConnectionsBeforeClosingDiscovery(t *testing.T) {
	re := require.New(t)
	discovery := &delayedDialServiceDiscovery{
		ServiceDiscovery: sd.NewMockServiceDiscovery(nil, nil),
		dialStarted:      make(chan struct{}),
		resumeDial:       make(chan struct{}),
		dialFinished:     make(chan struct{}),
	}
	resumeDial := sync.OnceFunc(func() { close(discovery.resumeDial) })
	t.Cleanup(discovery.Close)
	t.Cleanup(resumeDial)
	keeper := &serviceModeKeeper{
		tsoSvcDiscovery: discovery,
		tsoClient: tso.NewClient(context.Background(), opt.NewOption(),
			discovery, &tso.MSStreamBuilderFactory{}),
	}
	t.Cleanup(func() {
		resumeDial()
		keeper.close()
	})
	keeper.tsoClient.Setup()
	select {
	case <-discovery.dialStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("TSO connection update did not start")
	}

	closed := make(chan struct{})
	go func() {
		keeper.close()
		close(closed)
	}()
	// Keep the dial in flight until shutdown either completes or waits for it.
	select {
	case <-closed:
	case <-time.After(100 * time.Millisecond):
	}
	resumeDial()
	select {
	case <-discovery.dialFinished:
	case <-time.After(5 * time.Second):
		t.Fatal("TSO connection update did not finish")
	}
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("TSO client did not close")
	}

	re.NoError(discovery.dialErr)
	conn, ok := discovery.clientConns.Load("http://127.0.0.1:1")
	re.True(ok)
	re.Equal(connectivity.Shutdown, conn.(*grpc.ClientConn).GetState(),
		"a connection created during shutdown must be closed")
}
