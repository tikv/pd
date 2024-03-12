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
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/grpcutil"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// For issue https://github.com/tikv/pd/issues/3936
func TestLoadRegion(t *testing.T) {
	re := require.New(t)
	tempDir := t.TempDir()
	rs, err := storage.NewStorageWithLevelDBBackend(context.Background(), tempDir, nil)
	re.NoError(err)

	server := &mockServer{
		ctx:     context.Background(),
		storage: storage.NewCoreStorage(storage.NewStorageWithMemoryBackend(), rs),
		bc:      core.NewBasicCluster(),
	}
	for i := 0; i < 30; i++ {
		rs.SaveRegion(&metapb.Region{Id: uint64(i) + 1})
	}
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/storage/base_backend/slowLoadRegion", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/server/storage/base_backend/slowLoadRegion"))
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
	rs, err := storage.NewStorageWithLevelDBBackend(context.Background(), tempDir, nil)
	re.NoError(err)
	server := &mockServer{
		ctx:     context.Background(),
		storage: storage.NewCoreStorage(storage.NewStorageWithMemoryBackend(), rs),
		bc:      core.NewBasicCluster(),
	}
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

type mockServer struct {
	ctx            context.Context
	member, leader *pdpb.Member
	storage        storage.Storage
	bc             *core.BasicCluster
}

func (s *mockServer) LoopContext() context.Context {
	return s.ctx
}

func (s *mockServer) ClusterID() uint64 {
	return 1
}

func (s *mockServer) GetMemberInfo() *pdpb.Member {
	return s.member
}

func (s *mockServer) GetLeader() *pdpb.Member {
	return s.leader
}

func (s *mockServer) GetStorage() storage.Storage {
	return s.storage
}

func (s *mockServer) Name() string {
	return "mock-server"
}

func (s *mockServer) GetRegions() []*core.RegionInfo {
	return s.bc.GetRegions()
}

func (s *mockServer) GetTLSConfig() *grpcutil.TLSConfig {
	return &grpcutil.TLSConfig{}
}

func (s *mockServer) GetBasicCluster() *core.BasicCluster {
	return s.bc
}
