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
	stderrors "errors"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/member"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/syncer"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/cluster"
	"github.com/tikv/pd/server/config"
)

var errTestFollowerRegionStorage = stderrors.New("test follower region storage error")

func TestResetFollowerRegionCacheRequiresRegionStorage(t *testing.T) {
	re := require.New(t)
	cfg := config.NewConfig()
	cfg.PDServerCfg.UseRegionStorage = false
	s := &Server{persistOptions: config.NewPersistOptions(cfg)}

	re.ErrorContains(s.ResetFollowerRegionCache(), "region storage is disabled")

	cfg.PDServerCfg.UseRegionStorage = true
	s = newTestFollowerRegionResetServer(context.Background())
	s.persistOptions = config.NewPersistOptions(cfg)
	s.member = member.NewMember(nil, nil, 1)
	re.Error(s.ResetFollowerRegionCache())
}

func TestResetFollowerRegionCacheRestartsSyncAfterError(t *testing.T) {
	re := require.New(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	re.NoError(err)
	grpcServer := grpc.NewServer()
	pdpb.RegisterPDServer(grpcServer, &testFollowerRegionSyncServer{})
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- grpcServer.Serve(listener)
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
		err := <-serveDone
		if err != nil {
			re.ErrorIs(err, grpc.ErrServerStopped)
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cfg := config.NewConfig()
	cfg.Name = "pd-follower"
	cfg.PDServerCfg.UseRegionStorage = true
	testStorage := &testFollowerRegionStorage{
		Storage: storage.NewStorageWithMemoryBackend(),
	}
	member := member.NewMember(nil, nil, 2)
	leaderURL := "http://" + listener.Addr().String()
	member.InitMemberInfo(leaderURL, leaderURL, cfg.Name)
	member.PromoteSelf()
	basicCluster := core.NewBasicCluster()
	server := &Server{
		ctx:            ctx,
		serverLoopCtx:  ctx,
		cfg:            cfg,
		persistOptions: config.NewPersistOptions(cfg),
		storage:        testStorage,
		basicCluster:   basicCluster,
		member:         member,
	}
	regionSyncer := syncer.NewRegionSyncer(server)
	re.NotNil(regionSyncer)
	server.cluster = cluster.NewRaftCluster(
		ctx,
		member,
		basicCluster,
		testStorage,
		regionSyncer,
		nil,
		http.DefaultClient,
		nil,
	)
	re.NoError(regionSyncer.MarkHistorySynced())
	regionSyncer.StartSyncWithLeader(leaderURL)
	t.Cleanup(func() {
		regionSyncer.StopSyncWithLeader()
	})
	testutil.Eventually(re, regionSyncer.IsRunning,
		testutil.WithWaitFor(3*time.Second),
		testutil.WithTickInterval(20*time.Millisecond))

	testStorage.failNextRegionScan()
	re.ErrorContains(server.ResetFollowerRegionCache(), "load regions from local storage")
	testutil.Eventually(re, regionSyncer.IsRunning,
		testutil.WithWaitFor(3*time.Second),
		testutil.WithTickInterval(20*time.Millisecond))
}

func TestDeleteFollowerRegion(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(*require.Assertions, *Server) uint64
		errContains string
		check       func(*require.Assertions, *Server, uint64)
	}{
		{
			name: "cached region",
			setup: func(re *require.Assertions, s *Server) uint64 {
				region := newTestFollowerRegionMeta(1)
				re.NoError(s.storage.SaveRegion(region))
				s.basicCluster.PutRegion(core.NewRegionInfo(region, nil, core.SetSource(core.Storage)))
				return region.GetId()
			},
			check: assertTestFollowerRegionDeleted,
		},
		{
			name: "storage-only region",
			setup: func(re *require.Assertions, s *Server) uint64 {
				region := newTestFollowerRegionMeta(2)
				re.NoError(s.storage.SaveRegion(region))
				return region.GetId()
			},
			check: assertTestFollowerRegionDeleted,
		},
		{
			name: "missing region",
			setup: func(*require.Assertions, *Server) uint64 {
				return 3
			},
		},
		{
			name: "load storage error",
			setup: func(_ *require.Assertions, s *Server) uint64 {
				s.storage = &testFollowerRegionStorage{
					Storage:       s.storage,
					loadRegionErr: errTestFollowerRegionStorage,
				}
				return 4
			},
			errContains: "load follower region from local storage",
		},
		{
			name: "delete storage error",
			setup: func(_ *require.Assertions, s *Server) uint64 {
				region := newTestFollowerRegionMeta(5)
				s.basicCluster.PutRegion(core.NewRegionInfo(region, nil, core.SetSource(core.Storage)))
				s.storage = &testFollowerRegionStorage{
					Storage:         s.storage,
					deleteRegionErr: errTestFollowerRegionStorage,
				}
				return region.GetId()
			},
			errContains: "delete follower region from local storage",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			re := require.New(t)
			s := newTestFollowerRegionResetServer(context.Background())
			regionID := test.setup(re, s)

			err := s.deleteFollowerRegion(regionID)
			if test.errContains != "" {
				re.ErrorContains(err, test.errContains)
				return
			}
			re.NoError(err)
			if test.check != nil {
				test.check(re, s, regionID)
			}
		})
	}
}

func newTestFollowerRegionResetServer(ctx context.Context) *Server {
	cfg := config.NewConfig()
	return &Server{
		ctx:          ctx,
		cfg:          cfg,
		storage:      storage.NewStorageWithMemoryBackend(),
		basicCluster: core.NewBasicCluster(),
	}
}

func newTestFollowerRegionMeta(regionID uint64) *metapb.Region {
	return &metapb.Region{
		Id:          regionID,
		StartKey:    []byte{byte(regionID)},
		EndKey:      []byte{byte(regionID + 1)},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
		Peers: []*metapb.Peer{
			{Id: regionID*10 + 1, StoreId: 1},
		},
	}
}

func assertTestFollowerRegionDeleted(re *require.Assertions, s *Server, regionID uint64) {
	region := &metapb.Region{}
	ok, err := s.storage.LoadRegion(regionID, region)
	re.NoError(err)
	re.False(ok)
	re.Nil(s.basicCluster.GetRegion(regionID))
}

type testFollowerRegionStorage struct {
	storage.Storage
	mu              sync.Mutex
	failRegionScan  bool
	loadRegionErr   error
	deleteRegionErr error
}

func (s *testFollowerRegionStorage) failNextRegionScan() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failRegionScan = true
}

func (s *testFollowerRegionStorage) LoadRange(
	key, endKey string,
	limit int,
) (keys, values []string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failRegionScan {
		s.failRegionScan = false
		return nil, nil, errTestFollowerRegionStorage
	}
	return s.Storage.LoadRange(key, endKey, limit)
}

func (s *testFollowerRegionStorage) LoadRegion(regionID uint64, region *metapb.Region) (bool, error) {
	if s.loadRegionErr != nil {
		return false, s.loadRegionErr
	}
	return s.Storage.LoadRegion(regionID, region)
}

func (s *testFollowerRegionStorage) DeleteRegion(region *metapb.Region) error {
	if s.deleteRegionErr != nil {
		return s.deleteRegionErr
	}
	return s.Storage.DeleteRegion(region)
}

type testFollowerRegionSyncServer struct {
	pdpb.UnimplementedPDServer
}

func (*testFollowerRegionSyncServer) SyncRegions(stream pdpb.PD_SyncRegionsServer) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	header := &pdpb.ResponseHeader{ClusterId: keypath.ClusterID()}
	if err := stream.Send(&pdpb.SyncRegionResponse{
		Header:     header,
		StartIndex: 0,
	}); err != nil {
		return err
	}
	if err := stream.Send(&pdpb.SyncRegionResponse{
		Header:     header,
		StartIndex: 1,
	}); err != nil {
		return err
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}
