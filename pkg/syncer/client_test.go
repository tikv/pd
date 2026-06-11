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
	"strconv"
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
	"github.com/tikv/pd/pkg/storage/kv"
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

// TestHistorySyncedInitFromDurableState verifies that NewRegionSyncer
// seeds historySynced from the persisted history index so a node that
// previously completed a sync can still campaign after a restart followed
// by a leader death mid-sync. A fresh-on-disk node must stay false so it
// is forced through a real catch-up before it can campaign.
func TestHistorySyncedInitFromDurableState(t *testing.T) {
	re := require.New(t)

	newSyncer := func(seed func(kv.Base)) *RegionSyncer {
		tempDir := t.TempDir()
		rs, err := storage.NewRegionStorageWithLevelDBBackend(context.Background(), tempDir, nil)
		re.NoError(err)
		t.Cleanup(func() { re.NoError(rs.Close()) })
		seed(rs)
		server := mockserver.NewMockServer(
			context.Background(),
			nil,
			nil,
			storage.NewCoreStorage(storage.NewStorageWithMemoryBackend(), rs),
			core.NewBasicCluster(),
		)
		return NewRegionSyncer(server)
	}

	// Fresh KV: no persisted historyIndex. Stays false so the node is
	// gated until it actually catches up to a leader.
	rc := newSyncer(func(kv.Base) {})
	re.False(rc.IsHistorySynced(),
		"fresh KV should not be treated as already synced")

	// Persisted historyIndex from a prior successful sync. The index only
	// ever lands on disk after SaveRegion calls succeeded (via commit() or
	// record()'s flushCount path), so a non-zero index is sufficient
	// evidence of durable region state.
	rc = newSyncer(func(b kv.Base) {
		re.NoError(b.Save(historyKey, "42"))
	})
	re.True(rc.IsHistorySynced(),
		"persisted history index must initialize historySynced to true")
	re.Equal(uint64(42), rc.history.getNextIndex(),
		"history index should reload the persisted value")
	// Sanity: a value that round-trips through strconv matches what
	// history_buffer's persist path writes.
	v, err := strconv.ParseUint("42", 10, 64)
	re.NoError(err)
	re.Equal(rc.history.getNextIndex(), v)
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

	handled := syncer.handleRegionSyncResponse(context.Background(), &pdpb.SyncRegionResponse{
		Header: &pdpb.ResponseHeader{
			ClusterId: keypath.ClusterID(),
			Error: &pdpb.Error{
				Type:    pdpb.ErrorType_UNKNOWN,
				Message: "server stopped, close the region syncer client",
			},
		},
	}, nil, nil)

	re.False(handled)
	re.Equal(uint64(10), syncer.history.getNextIndex())
	re.False(syncer.IsRunning())
}
