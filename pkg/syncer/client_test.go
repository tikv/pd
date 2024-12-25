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
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockserver"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// For issue https://github.com/tikv/pd/issues/3936
func TestLoadRegion(t *testing.T) {
	re := require.New(t)
	tempDir := t.TempDir()
	rs, err := storage.NewRegionStorageWithLevelDBBackend(context.Background(), tempDir, nil)
	re.NoError(err)

	server := mockserver.NewMockServer(
		context.Background(),
		nil,
		nil,
		storage.NewCoreStorage(storage.NewStorageWithMemoryBackend(), rs),
		core.NewBasicCluster(),
	)
	for i := range 30 {
		rs.SaveRegion(&metapb.Region{Id: uint64(i) + 1})
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
