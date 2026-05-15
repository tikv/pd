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

func TestSyncExitsWhenRegionSyncerStops(t *testing.T) {
	re := require.New(t)
	tempDir := t.TempDir()
	regionStorage, err := storage.NewRegionStorageWithLevelDBBackend(context.Background(), tempDir, nil)
	re.NoError(err)
	defer re.NoError(regionStorage.Close())

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
	defer re.NoError(regionStorage.Close())

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

type mockSyncRegionsServer struct {
	mu      sync.Mutex
	ctx     context.Context
	recvCh  chan *pdpb.SyncRegionRequest
	sendCh  chan *pdpb.SyncRegionResponse
	sendErr error
}

func newMockSyncRegionsServer() *mockSyncRegionsServer {
	return &mockSyncRegionsServer{
		ctx:    context.Background(),
		recvCh: make(chan *pdpb.SyncRegionRequest),
		sendCh: make(chan *pdpb.SyncRegionResponse, 1),
	}
}

func (s *mockSyncRegionsServer) Send(resp *pdpb.SyncRegionResponse) error {
	s.mu.Lock()
	err := s.sendErr
	s.mu.Unlock()
	if err != nil {
		return err
	}
	s.sendCh <- resp
	return nil
}

func (s *mockSyncRegionsServer) setSendErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sendErr = err
}

func (s *mockSyncRegionsServer) Recv() (*pdpb.SyncRegionRequest, error) {
	req, ok := <-s.recvCh
	if !ok {
		return nil, io.EOF
	}
	return req, nil
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
