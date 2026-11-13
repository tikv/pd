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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/goleak"

	"github.com/pingcap/kvproto/pkg/meta_storagepb"

	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(testutil.WaitForEtcdConnections(m), testutil.LeakOptions...)
}

type servingWatchServer struct {
	bs.Server
}

func (servingWatchServer) IsServing() bool { return true }

type metaWatchStream struct {
	meta_storagepb.MetaStorage_WatchServer
	ctx       context.Context
	responses chan *meta_storagepb.WatchResponse
}

func (s *metaWatchStream) Context() context.Context { return s.ctx }

func (s *metaWatchStream) Send(response *meta_storagepb.WatchResponse) error {
	select {
	case s.responses <- response:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func TestWatchExitsOnEtcdLeaderLoss(t *testing.T) {
	re := require.New(t)
	servers, _, clean := etcdutil.NewTestEtcdCluster(t, 3, nil)
	defer clean()
	// Pin the backend connection to one node so endpoint health checks cannot
	// hide a watcher stuck on a partitioned node.
	isolated := servers[0]
	config := isolated.Config()
	client, err := clientv3.NewFromURL(config.ListenClientUrls[0].String())
	re.NoError(err)
	defer client.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	stream := &metaWatchStream{ctx: ctx, responses: make(chan *meta_storagepb.WatchResponse, 2)}
	service := &Service{ctx: ctx, manager: &Manager{srv: servingWatchServer{}, client: client}}
	initial, err := client.Put(ctx, "resource_group/controller", "initial")
	re.NoError(err)
	done := make(chan struct{})
	var watchErr error
	go func() {
		defer close(done)
		watchErr = service.Watch(&meta_storagepb.WatchRequest{
			Key: []byte("resource_group/controller"), StartRevision: initial.Header.Revision + 1,
		}, stream)
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("watch did not exit after cancellation")
		}
	}()
	_, err = client.Put(ctx, "resource_group/controller", "watched")
	re.NoError(err)
	select {
	case response := <-stream.responses:
		re.Len(response.GetEvents(), 1)
		re.Equal("watched", string(response.GetEvents()[0].GetKv().GetValue()))
	case <-time.After(5 * time.Second):
		t.Fatal("watch did not deliver the initial event")
	}

	defer func() {
		for _, peer := range servers[1:] {
			isolated.Server.MendPeer(peer.Server.ID())
			peer.Server.MendPeer(isolated.Server.ID())
		}
	}()
	for _, peer := range servers[1:] {
		isolated.Server.CutPeer(peer.Server.ID())
		peer.Server.CutPeer(isolated.Server.ID())
	}
	select {
	case <-done:
		re.ErrorIs(watchErr, rpctypes.ErrNoLeader)
	case <-time.After(10 * time.Second):
		t.Fatal("watch stayed open on a partitioned etcd node")
	}
	re.NoError(ctx.Err(), "leader loss must end the watch without caller cancellation")
}

type blockedWatchCreation struct {
	clientv3.Watcher
	started chan struct{}
}

func (w *blockedWatchCreation) Watch(ctx context.Context, _ string, _ ...clientv3.OpOption) clientv3.WatchChan {
	close(w.started)
	<-ctx.Done()
	channel := make(chan clientv3.WatchResponse)
	close(channel)
	return channel
}

func TestWatchShutdownCancelsBackendCreation(t *testing.T) {
	re := require.New(t)
	serverCtx, stopServer := context.WithCancel(t.Context())
	defer stopServer()
	watcher := &blockedWatchCreation{started: make(chan struct{})}
	service := &Service{ctx: serverCtx, manager: &Manager{
		srv: servingWatchServer{}, client: &clientv3.Client{Watcher: watcher},
	}}
	clientCtx, cancelClient := context.WithCancel(t.Context())
	defer cancelClient()
	stream := &metaWatchStream{ctx: clientCtx, responses: make(chan *meta_storagepb.WatchResponse)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = service.Watch(&meta_storagepb.WatchRequest{Key: []byte("resource_group/controller")}, stream)
	}()
	select {
	case <-watcher.started:
	case <-time.After(5 * time.Second):
		t.Fatal("watch did not start creating its backend subscription")
	}
	stopServer()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		cancelClient()
		<-done
		t.Fatal("server shutdown did not cancel backend watch creation")
	}
	re.NoError(clientCtx.Err(), "server shutdown must not require caller cancellation")
}
