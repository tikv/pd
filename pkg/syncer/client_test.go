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

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockserver"
	"github.com/tikv/pd/pkg/storage"
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
	syncer := newTestRegionSyncer(t, core.NewBasicCluster())
	syncer.history.resetWithIndex(10)
	syncer.streamingRunning.Store(true)

	state := &regionSyncState{syncingHistory: true}
	handled, err := syncer.handleRegionSyncResponse(context.Background(), &pdpb.SyncRegionResponse{
		Header: &pdpb.ResponseHeader{
			ClusterId: keypath.ClusterID(),
			Error: &pdpb.Error{
				Type:    pdpb.ErrorType_UNKNOWN,
				Message: "server stopped, close the region syncer client",
			},
		},
	}, nil, nil, state)

	re.NoError(err)
	re.False(handled)
	re.Equal(uint64(10), syncer.history.getNextIndex())
	re.False(syncer.IsRunning())
}

func TestResetRegionCacheAndStorage(t *testing.T) {
	re := require.New(t)
	tempDir := t.TempDir()
	rs, err := storage.NewRegionStorageWithLevelDBBackend(context.Background(), tempDir, nil)
	re.NoError(err)
	defer func() {
		re.NoError(rs.Close())
	}()
	storageWithRegionStorage := storage.NewCoreStorage(storage.NewStorageWithMemoryBackend(), rs)
	storage.TrySwitchRegionStorage(storageWithRegionStorage, true)
	bc := core.NewBasicCluster()
	server := mockserver.NewMockServer(
		context.Background(),
		nil,
		nil,
		storageWithRegionStorage,
		bc,
	)
	for i := range 2 {
		region := &metapb.Region{
			Id:       uint64(i + 1),
			StartKey: []byte{byte(i)},
			EndKey:   []byte{byte(i + 1)},
		}
		re.NoError(storageWithRegionStorage.SaveRegion(region))
		bc.PutRegion(core.NewRegionInfo(region, nil))
	}
	re.NoError(storageWithRegionStorage.Flush())

	rc := NewRegionSyncer(server)
	re.NoError(rc.resetRegionCacheAndStorage(context.Background(), bc, storageWithRegionStorage))
	re.Empty(bc.GetRegions())
	for i := range 2 {
		region := &metapb.Region{}
		ok, err := storageWithRegionStorage.LoadRegion(uint64(i+1), region)
		re.NoError(err)
		re.False(ok)
	}
}

func TestHandleFullSyncResponseResetsStaleRegions(t *testing.T) {
	re := require.New(t)
	tempDir := t.TempDir()
	rs, err := storage.NewRegionStorageWithLevelDBBackend(context.Background(), tempDir, nil)
	re.NoError(err)
	defer func() {
		re.NoError(rs.Close())
	}()
	storageWithRegionStorage := storage.NewCoreStorage(storage.NewStorageWithMemoryBackend(), rs)
	storage.TrySwitchRegionStorage(storageWithRegionStorage, true)
	bc := core.NewBasicCluster()
	server := mockserver.NewMockServer(
		context.Background(),
		nil,
		nil,
		storageWithRegionStorage,
		bc,
	)
	staleRegion1 := &metapb.Region{
		Id:       1,
		StartKey: []byte{0},
		EndKey:   []byte{1},
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
	}
	staleRegion2 := &metapb.Region{
		Id:       2,
		StartKey: []byte{1},
		EndKey:   []byte{2},
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
	}
	for _, region := range []*metapb.Region{staleRegion1, staleRegion2} {
		re.NoError(storageWithRegionStorage.SaveRegion(region))
		bc.PutRegion(core.NewRegionInfo(region, nil))
	}

	rc := NewRegionSyncer(server)
	rc.history.resetWithIndex(10)
	state := &regionSyncState{syncingHistory: true}
	latestRegion := &metapb.Region{
		Id:       1,
		StartKey: []byte{0},
		EndKey:   []byte{1},
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 2,
		},
	}
	handled, err := rc.handleRegionSyncResponse(context.Background(), &pdpb.SyncRegionResponse{
		Header:     &pdpb.ResponseHeader{ClusterId: keypath.ClusterID()},
		StartIndex: 0,
		Regions:    []*metapb.Region{latestRegion},
	}, bc, storageWithRegionStorage, state)
	re.NoError(err)
	re.True(handled)
	re.False(rc.IsRunning())
	re.True(state.fullSyncing)
	handled, err = rc.handleRegionSyncResponse(context.Background(), &pdpb.SyncRegionResponse{
		Header:     &pdpb.ResponseHeader{ClusterId: keypath.ClusterID()},
		StartIndex: 20,
	}, bc, storageWithRegionStorage, state)
	re.NoError(err)
	re.True(handled)

	re.True(rc.IsRunning())
	re.False(state.fullSyncing)
	re.False(state.syncingHistory)
	re.Equal(uint64(20), rc.history.getNextIndex())
	historyIndex, err := rs.Load(historyKey)
	re.NoError(err)
	re.Equal("20", historyIndex)
	re.Len(bc.GetRegions(), 1)
	re.Equal(uint64(2), bc.GetRegion(1).GetRegionEpoch().GetVersion())
	storedRegion := &metapb.Region{}
	ok, err := storageWithRegionStorage.LoadRegion(1, storedRegion)
	re.NoError(err)
	re.True(ok)
	re.Equal(uint64(2), storedRegion.GetRegionEpoch().GetVersion())
	ok, err = storageWithRegionStorage.LoadRegion(2, storedRegion)
	re.NoError(err)
	re.False(ok)
}
