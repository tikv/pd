// Copyright 2025 TiKV Project Authors.
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

package gc

import (
	"context"
	"fmt"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/versioninfo/kerneltype"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

// In tests in this file, we only verify that the parameters and results are properly passed. The detailed behavior
// of these APIs are covered elsewhere.

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func setupGCWatchTestCluster(
	t *testing.T,
	serverCount int,
) (context.Context, context.CancelFunc, *tests.TestCluster, *tests.TestServer, pdpb.PDClient, *grpc.ClientConn, *pdpb.RequestHeader) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	cluster, err := tests.NewTestCluster(ctx, serverCount, func(conf *config.Config, _ string) {
		conf.Keyspace.WaitRegionSplit = false
	})
	re.NoError(err)
	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())

	leaderServer := cluster.GetLeaderServer()
	re.NotNil(leaderServer)
	re.NoError(leaderServer.BootstrapCluster())

	grpcPDClient, conn := testutil.MustNewGrpcClient(re, leaderServer.GetAddr())
	header := testutil.NewRequestHeader(leaderServer.GetClusterID())
	return ctx, cancel, cluster, leaderServer, grpcPDClient, conn, header
}

func createGCWatchTestKeyspaces(re *require.Assertions, leaderServer *tests.TestServer, count int) []uint32 {
	createTime := time.Now().Unix()
	keyspaceIDs := make([]uint32, 0, count)
	for i := range count {
		ks, err := leaderServer.GetKeyspaceManager().CreateKeyspace(&keyspace.CreateKeyspaceRequest{
			Name:       fmt.Sprintf("watch-gc-ks-%d", i),
			Config:     map[string]string{keyspace.GCManagementType: keyspace.KeyspaceLevelGC},
			CreateTime: createTime + int64(i),
		})
		re.NoError(err)
		keyspaceIDs = append(keyspaceIDs, ks.Id)
	}
	return keyspaceIDs
}

func getAllKeyspaceGCStateIDs(ctx context.Context, re *require.Assertions, grpcPDClient pdpb.PDClient, header *pdpb.RequestHeader) []uint32 {
	resp, err := grpcPDClient.GetAllKeyspacesGCStates(ctx, &pdpb.GetAllKeyspacesGCStatesRequest{
		Header: header,
	})
	re.NoError(err)
	re.NotNil(resp.GetHeader())
	re.Nil(resp.GetHeader().GetError())

	keyspaceIDs := make([]uint32, 0, len(resp.GetGcStates()))
	for _, gcState := range resp.GetGcStates() {
		keyspaceIDs = append(keyspaceIDs, gcState.GetKeyspaceScope().GetKeyspaceId())
	}
	slices.Sort(keyspaceIDs)
	return keyspaceIDs
}

func recvGCStateWithTimeout[T any](re *require.Assertions, recv func() (*T, error), timeout time.Duration) (*T, error) {
	type recvResult struct {
		msg *T
		err error
	}
	ch := make(chan recvResult, 1)
	go func() {
		msg, err := recv()
		ch <- recvResult{msg: msg, err: err}
	}()

	select {
	case res := <-ch:
		return res.msg, res.err
	case <-time.After(timeout):
		re.FailNow("timed out waiting for watch response")
		return nil, nil
	}
}

func collectWatchGCStatesInitial(
	re *require.Assertions,
	stream pdpb.PD_WatchGCStatesClient,
	expectedCount int,
) (map[uint32]*pdpb.GCState, int) {
	received := make(map[uint32]*pdpb.GCState, expectedCount)
	maxBatchSize := 0
	for len(received) < expectedCount {
		resp, err := recvGCStateWithTimeout(re, stream.Recv, 5*time.Second)
		re.NoError(err)
		re.NotNil(resp.GetHeader())
		re.Nil(resp.GetHeader().GetError())
		maxBatchSize = max(maxBatchSize, len(resp.GetGcStates()))
		for _, gcState := range resp.GetGcStates() {
			received[gcState.GetKeyspaceScope().GetKeyspaceId()] = gcState
		}
	}
	return received, maxBatchSize
}

func collectWatchGCStatesUntilError(
	re *require.Assertions,
	stream pdpb.PD_WatchGCStatesClient,
	timeout time.Duration,
) error {
	deadline := time.Now().Add(timeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			re.FailNow("timed out waiting for WatchGCStates stream to terminate")
		}
		resp, err := recvGCStateWithTimeout(re, stream.Recv, remaining)
		if err != nil {
			return err
		}
		re.NotNil(resp.GetHeader())
		re.Nil(resp.GetHeader().GetError())
	}
}

func collectWatchGCSafePointV2UntilError(
	re *require.Assertions,
	stream pdpb.PD_WatchGCSafePointV2Client,
	timeout time.Duration,
) error {
	deadline := time.Now().Add(timeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			re.FailNow("timed out waiting for WatchGCSafePointV2 stream to terminate")
		}
		resp, err := recvGCStateWithTimeout(re, stream.Recv, remaining)
		if err != nil {
			return err
		}
		re.NotNil(resp.GetHeader())
		re.Nil(resp.GetHeader().GetError())
	}
}

func waitForWatchGCSafePointV2SafePoint(
	re *require.Assertions,
	stream pdpb.PD_WatchGCSafePointV2Client,
	keyspaceID uint32,
	targetSafePoint uint64,
	timeout time.Duration,
) {
	deadline := time.Now().Add(timeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			re.FailNow("timed out waiting for WatchGCSafePointV2 event")
		}

		resp, err := recvGCStateWithTimeout(re, stream.Recv, remaining)
		re.NoError(err)
		re.NotNil(resp.GetHeader())
		re.Nil(resp.GetHeader().GetError())

		for _, event := range resp.GetEvents() {
			re.NotEqual(constant.NullKeyspaceID, event.GetKeyspaceId())
			if event.GetKeyspaceId() == keyspaceID && event.GetSafePoint() == targetSafePoint {
				return
			}
		}
	}
}

func mustSetGCBarrier(
	ctx context.Context,
	re *require.Assertions,
	grpcPDClient pdpb.PDClient,
	header *pdpb.RequestHeader,
	keyspaceID uint32,
	barrierID string,
	barrierTS uint64,
) {
	resp, err := grpcPDClient.SetGCBarrier(ctx, &pdpb.SetGCBarrierRequest{
		Header:        header,
		KeyspaceScope: &pdpb.KeyspaceScope{KeyspaceId: keyspaceID},
		BarrierId:     barrierID,
		BarrierTs:     barrierTS,
		TtlSeconds:    math.MaxInt64,
	})
	re.NoError(err)
	re.NotNil(resp.GetHeader())
	re.Nil(resp.GetHeader().GetError())
}

func mustAdvanceTxnSafePoint(
	ctx context.Context,
	re *require.Assertions,
	grpcPDClient pdpb.PDClient,
	header *pdpb.RequestHeader,
	keyspaceID uint32,
	target uint64,
) {
	resp, err := grpcPDClient.AdvanceTxnSafePoint(ctx, &pdpb.AdvanceTxnSafePointRequest{
		Header:        header,
		KeyspaceScope: &pdpb.KeyspaceScope{KeyspaceId: keyspaceID},
		Target:        target,
	})
	re.NoError(err)
	re.NotNil(resp.GetHeader())
	re.Nil(resp.GetHeader().GetError())
	re.Equal(target, resp.GetNewTxnSafePoint())
}

func mustAdvanceGCSafePoint(
	ctx context.Context,
	re *require.Assertions,
	grpcPDClient pdpb.PDClient,
	header *pdpb.RequestHeader,
	keyspaceID uint32,
	target uint64,
) {
	resp, err := grpcPDClient.AdvanceGCSafePoint(ctx, &pdpb.AdvanceGCSafePointRequest{
		Header:        header,
		KeyspaceScope: &pdpb.KeyspaceScope{KeyspaceId: keyspaceID},
		Target:        target,
	})
	re.NoError(err)
	re.NotNil(resp.GetHeader())
	re.Nil(resp.GetHeader().GetError())
	re.Equal(target, resp.GetNewGcSafePoint())
}

func TestGCOperations(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1, func(conf *config.Config, _ string) {
		conf.Keyspace.WaitRegionSplit = false
	})
	re.NoError(err)
	defer cluster.Destroy()
	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())

	leaderServer := cluster.GetLeaderServer()
	re.NotNil(leaderServer)
	re.NoError(leaderServer.BootstrapCluster())

	ks1, err := leaderServer.GetKeyspaceManager().CreateKeyspace(&keyspace.CreateKeyspaceRequest{
		Name:       "ks1",
		Config:     map[string]string{keyspace.GCManagementType: keyspace.KeyspaceLevelGC},
		CreateTime: time.Now().Unix(),
	})
	re.NoError(err)

	grpcPDClient, conn := testutil.MustNewGrpcClient(re, leaderServer.GetAddr())
	defer conn.Close()
	clusterID := leaderServer.GetClusterID()
	header := testutil.NewRequestHeader(clusterID)

	testInKeyspace := func(keyspaceID uint32) {
		{
			// Successful advancement of txn safe point
			req := &pdpb.AdvanceTxnSafePointRequest{
				Header:        header,
				KeyspaceScope: &pdpb.KeyspaceScope{KeyspaceId: keyspaceID},
				Target:        10,
			}
			resp, err := grpcPDClient.AdvanceTxnSafePoint(ctx, req)
			re.NoError(err)
			re.NotNil(resp.Header)
			re.Nil(resp.Header.Error)
			re.Equal(uint64(10), resp.GetNewTxnSafePoint())
			re.Equal(uint64(0), resp.GetOldTxnSafePoint())
			re.Empty(resp.GetBlockerDescription())

			// Unsuccessful advancement of txn safe point (no backward)
			req.Target = 9
			resp, err = grpcPDClient.AdvanceTxnSafePoint(ctx, req)
			re.NoError(err)
			re.NotNil(resp.Header)
			re.NotNil(resp.Header.Error)
			re.Contains(resp.Header.Error.Message, "ErrDecreasingTxnSafePoint")
		}

		{
			// Successful advancement of GC safe point
			req := &pdpb.AdvanceGCSafePointRequest{
				Header:        header,
				KeyspaceScope: &pdpb.KeyspaceScope{KeyspaceId: keyspaceID},
				Target:        8,
			}
			resp, err := grpcPDClient.AdvanceGCSafePoint(ctx, req)
			re.NoError(err)
			re.NotNil(resp.Header)
			re.Nil(resp.Header.Error)
			re.Equal(uint64(8), resp.GetNewGcSafePoint())
			re.Equal(uint64(0), resp.GetOldGcSafePoint())

			// Unsuccessful advancement of GC safe point (exceeding txn safe point)
			req.Target = 11
			resp, err = grpcPDClient.AdvanceGCSafePoint(ctx, req)
			re.NoError(err)
			re.NotNil(resp.Header)
			re.NotNil(resp.Header.Error)
			re.Contains(resp.Header.Error.Message, "ErrGCSafePointExceedsTxnSafePoint")
		}

		{
			// Successfully sets a GC barrier
			req := &pdpb.SetGCBarrierRequest{
				Header:        header,
				KeyspaceScope: &pdpb.KeyspaceScope{KeyspaceId: keyspaceID},
				BarrierId:     "b1",
				BarrierTs:     15,
				TtlSeconds:    3600,
			}
			resp, err := grpcPDClient.SetGCBarrier(ctx, req)
			re.NoError(err)
			re.NotNil(resp.Header)
			re.Nil(resp.Header.Error)
			re.Equal("b1", resp.GetNewBarrierInfo().GetBarrierId())
			re.Equal(uint64(15), resp.GetNewBarrierInfo().GetBarrierTs())
			re.Greater(resp.GetNewBarrierInfo().GetTtlSeconds(), int64(3599))
			re.Less(resp.GetNewBarrierInfo().GetTtlSeconds(), int64(3601))

			// Successfully sets a GC barrier with infinite ttl.
			req = &pdpb.SetGCBarrierRequest{
				Header:        header,
				KeyspaceScope: &pdpb.KeyspaceScope{KeyspaceId: keyspaceID},
				BarrierId:     "b2",
				BarrierTs:     14,
				TtlSeconds:    math.MaxInt64,
			}
			resp, err = grpcPDClient.SetGCBarrier(ctx, req)
			re.NoError(err)
			re.NotNil(resp.Header)
			re.Nil(resp.Header.Error)
			re.Equal("b2", resp.GetNewBarrierInfo().GetBarrierId())
			re.Equal(uint64(14), resp.GetNewBarrierInfo().GetBarrierTs())
			re.Equal(math.MaxInt64, int(resp.GetNewBarrierInfo().GetTtlSeconds()))

			// Failed to set a GC barrier (below txn safe point)
			req = &pdpb.SetGCBarrierRequest{
				Header:        header,
				KeyspaceScope: &pdpb.KeyspaceScope{KeyspaceId: keyspaceID},
				BarrierId:     "b3",
				BarrierTs:     9,
				TtlSeconds:    3600,
			}
			resp, err = grpcPDClient.SetGCBarrier(ctx, req)
			re.NoError(err)
			re.NotNil(resp.Header)
			re.NotNil(resp.Header.Error)
			re.Contains(resp.Header.Error.Message, "ErrGCBarrierTSBehindTxnSafePoint")
		}

		{
			// Delete a GC barrier
			req := &pdpb.DeleteGCBarrierRequest{
				Header:        header,
				KeyspaceScope: &pdpb.KeyspaceScope{KeyspaceId: keyspaceID},
				BarrierId:     "b2",
			}
			resp, err := grpcPDClient.DeleteGCBarrier(ctx, req)
			re.NoError(err)
			re.NotNil(resp.Header)
			re.Nil(resp.Header.Error)
			re.Equal("b2", resp.GetDeletedBarrierInfo().GetBarrierId())
			re.Equal(uint64(14), resp.GetDeletedBarrierInfo().GetBarrierTs())
			re.Equal(math.MaxInt64, int(resp.GetDeletedBarrierInfo().GetTtlSeconds()))
		}

		{
			// Advance txn safe point reports the reason of being blocked
			req := &pdpb.AdvanceTxnSafePointRequest{
				Header:        header,
				KeyspaceScope: &pdpb.KeyspaceScope{KeyspaceId: keyspaceID},
				Target:        20,
			}
			resp, err := grpcPDClient.AdvanceTxnSafePoint(ctx, req)
			re.NoError(err)
			re.NotNil(resp.Header)
			re.Nil(resp.Header.Error)
			re.Equal(uint64(15), resp.GetNewTxnSafePoint())
			re.Equal(uint64(10), resp.GetOldTxnSafePoint())
			re.Contains(resp.GetBlockerDescription(), "b1")
		}

		{
			// Get GC states
			req := &pdpb.GetGCStateRequest{
				Header:        header,
				KeyspaceScope: &pdpb.KeyspaceScope{KeyspaceId: keyspaceID},
			}
			resp, err := grpcPDClient.GetGCState(ctx, req)
			re.NoError(err)
			re.NotNil(resp.Header)
			re.Nil(resp.Header.Error)
			re.Equal(keyspaceID, resp.GetGcState().KeyspaceScope.KeyspaceId)
			re.Equal(keyspaceID != constant.NullKeyspaceID, resp.GetGcState().GetIsKeyspaceLevelGc())
			re.Equal(uint64(15), resp.GetGcState().GetTxnSafePoint())
			re.Equal(uint64(8), resp.GetGcState().GetGcSafePoint())
			re.Len(resp.GetGcState().GetGcBarriers(), 1)
			re.Equal("b1", resp.GetGcState().GetGcBarriers()[0].GetBarrierId())
			re.Equal(uint64(15), resp.GetGcState().GetGcBarriers()[0].GetBarrierTs())
			re.Greater(resp.GetGcState().GetGcBarriers()[0].GetTtlSeconds(), int64(3500))
			re.Less(resp.GetGcState().GetGcBarriers()[0].GetTtlSeconds(), int64(3601))
		}
	}

	testInKeyspace(constant.NullKeyspaceID)
	testInKeyspace(ks1.Id)

	req := &pdpb.GetAllKeyspacesGCStatesRequest{
		Header: header,
	}
	resp, err := grpcPDClient.GetAllKeyspacesGCStates(ctx, req)
	re.NoError(err)
	re.NotNil(resp.Header)
	re.Nil(resp.Header.Error)
	// The default keyspace will be included, so it has 3.
	re.Len(resp.GetGcStates(), 3)
	receivedKeyspaceIDs := make([]uint32, 0, 3)
	for _, gcState := range resp.GetGcStates() {
		receivedKeyspaceIDs = append(receivedKeyspaceIDs, gcState.KeyspaceScope.KeyspaceId)
	}
	slices.Sort(receivedKeyspaceIDs)

	// Expected keyspace IDs differ between NextGen and Classic
	var expectedKeyspaceIDs []uint32
	if kerneltype.IsNextGen() {
		expectedKeyspaceIDs = []uint32{ks1.Id, constant.SystemKeyspaceID, constant.NullKeyspaceID}
	} else {
		expectedKeyspaceIDs = []uint32{0, ks1.Id, constant.NullKeyspaceID}
	}
	re.Equal(expectedKeyspaceIDs, receivedKeyspaceIDs)

	// As the same test logic was run on the two keyspaces, they should have the same result.
	for _, gcState := range resp.GetGcStates() {
		// Ignore the default keyspace which is not used in this test.
		defaultKeyspaceID := uint32(0)
		if kerneltype.IsNextGen() {
			defaultKeyspaceID = constant.SystemKeyspaceID
		}
		if gcState.KeyspaceScope.KeyspaceId == defaultKeyspaceID {
			continue
		}
		re.Equal(gcState.KeyspaceScope.KeyspaceId != constant.NullKeyspaceID, gcState.GetIsKeyspaceLevelGc())
		re.Equal(uint64(15), gcState.GetTxnSafePoint())
		re.Equal(uint64(8), gcState.GetGcSafePoint())
		re.Len(gcState.GetGcBarriers(), 1)
		re.Equal("b1", gcState.GetGcBarriers()[0].GetBarrierId())
		re.Equal(uint64(15), gcState.GetGcBarriers()[0].GetBarrierTs())
		re.Greater(gcState.GetGcBarriers()[0].GetTtlSeconds(), int64(3500))
		re.Less(gcState.GetGcBarriers()[0].GetTtlSeconds(), int64(3601))
	}

	// Global GC Barrier API
	for _, keyspaceID := range []uint32{constant.NullKeyspaceID, ks1.Id} {
		// Cleanup before test
		req1 := &pdpb.GetGCStateRequest{
			Header:        header,
			KeyspaceScope: &pdpb.KeyspaceScope{KeyspaceId: keyspaceID},
		}
		resp1, err := grpcPDClient.GetGCState(ctx, req1)
		re.NoError(err)
		for _, state := range resp1.GcState.GcBarriers {
			req := &pdpb.DeleteGCBarrierRequest{
				Header:        header,
				KeyspaceScope: &pdpb.KeyspaceScope{KeyspaceId: keyspaceID},
				BarrierId:     state.BarrierId,
			}
			_, err := grpcPDClient.DeleteGCBarrier(ctx, req)
			re.NoError(err)
		}
	}

	{
		// Successfully sets a global GC barrier
		req := &pdpb.SetGlobalGCBarrierRequest{
			Header:     header,
			BarrierId:  "b1",
			BarrierTs:  20,
			TtlSeconds: 3600,
		}
		resp, err := grpcPDClient.SetGlobalGCBarrier(ctx, req)
		re.NoError(err)
		re.NotNil(resp.Header)
		re.Nil(resp.Header.Error)
		re.Equal("b1", resp.GetNewBarrierInfo().GetBarrierId())
		re.Equal(uint64(20), resp.GetNewBarrierInfo().GetBarrierTs())
		re.Greater(resp.GetNewBarrierInfo().GetTtlSeconds(), int64(3599))
		re.Less(resp.GetNewBarrierInfo().GetTtlSeconds(), int64(3601))
	}

	{
		// Successfully sets a global GC barrier with infinite ttl.
		req := &pdpb.SetGlobalGCBarrierRequest{
			Header:     header,
			BarrierId:  "b2",
			BarrierTs:  24,
			TtlSeconds: math.MaxInt64,
		}
		resp, err := grpcPDClient.SetGlobalGCBarrier(ctx, req)
		re.NoError(err)
		re.NotNil(resp.Header)
		re.Nil(resp.Header.Error)
		re.Equal("b2", resp.GetNewBarrierInfo().GetBarrierId())
		re.Equal(uint64(24), resp.GetNewBarrierInfo().GetBarrierTs())
		re.Equal(math.MaxInt64, int(resp.GetNewBarrierInfo().GetTtlSeconds()))
	}

	{
		// Failed to set a global GC barrier (below txn safe point)
		req := &pdpb.SetGlobalGCBarrierRequest{
			Header:     header,
			BarrierId:  "b3",
			BarrierTs:  9,
			TtlSeconds: 3600,
		}
		resp, err := grpcPDClient.SetGlobalGCBarrier(ctx, req)
		re.NoError(err)
		re.NotNil(resp.Header)
		re.NotNil(resp.Header.Error)
		re.Contains(resp.Header.Error.Message, "ErrGlobalGCBarrierTSBehindTxnSafePoint")
	}

	for _, keyspaceID := range []uint32{constant.NullKeyspaceID, ks1.Id} {
		// Advance txn safe point reports the reason of being blocked
		req := &pdpb.AdvanceTxnSafePointRequest{
			Header:        header,
			KeyspaceScope: &pdpb.KeyspaceScope{KeyspaceId: keyspaceID},
			Target:        30,
		}
		resp, err := grpcPDClient.AdvanceTxnSafePoint(ctx, req)
		re.NoError(err)
		re.NotNil(resp.Header)
		re.Nil(resp.Header.Error)
		re.Equal(uint64(20), resp.GetNewTxnSafePoint())
		re.Equal(uint64(15), resp.GetOldTxnSafePoint())
		re.Contains(resp.GetBlockerDescription(), "b1")
	}

	{
		// Delete a global GC barrier
		req := &pdpb.DeleteGlobalGCBarrierRequest{
			Header:    header,
			BarrierId: "b1",
		}
		resp, err := grpcPDClient.DeleteGlobalGCBarrier(ctx, req)
		re.NoError(err)
		re.NotNil(resp.Header)
		re.Nil(resp.Header.Error)
		re.Equal("b1", resp.GetDeletedBarrierInfo().GetBarrierId())
		re.Equal(uint64(20), resp.GetDeletedBarrierInfo().GetBarrierTs())
		// The TTL value decreases over time
		re.Greater(resp.GetDeletedBarrierInfo().GetTtlSeconds(), int64(3595))
		re.LessOrEqual(resp.GetDeletedBarrierInfo().GetTtlSeconds(), int64(3600))
	}

	for _, keyspaceID := range []uint32{constant.NullKeyspaceID, ks1.Id} {
		// Advance txn safe point reports the reason of being blocked
		req := &pdpb.AdvanceTxnSafePointRequest{
			Header:        header,
			KeyspaceScope: &pdpb.KeyspaceScope{KeyspaceId: keyspaceID},
			Target:        30,
		}
		resp, err := grpcPDClient.AdvanceTxnSafePoint(ctx, req)
		re.NoError(err)
		re.NotNil(resp.Header)
		re.Nil(resp.Header.Error)
		re.Equal(uint64(24), resp.GetNewTxnSafePoint())
		re.Equal(uint64(20), resp.GetOldTxnSafePoint())
		re.Contains(resp.GetBlockerDescription(), "b2")
	}
}

func TestWatchGCStatesInitialLoadIncludesNullKeyspace(t *testing.T) {
	re := require.New(t)
	ctx, cancel, cluster, leaderServer, grpcPDClient, conn, header := setupGCWatchTestCluster(t, 1)
	defer cancel()
	defer cluster.Destroy()
	defer conn.Close()

	createGCWatchTestKeyspaces(re, leaderServer, 1100)
	expectedKeyspaceIDs := getAllKeyspaceGCStateIDs(ctx, re, grpcPDClient, header)
	re.Greater(len(expectedKeyspaceIDs), 1024)

	watchCtx, cancelWatch := context.WithTimeout(ctx, 10*time.Second)
	defer cancelWatch()
	stream, err := grpcPDClient.WatchGCStates(watchCtx, &pdpb.WatchGCStatesRequest{
		Header:             header,
		SkipLoadingInitial: false,
		ExcludeGcBarriers:  false,
	})
	re.NoError(err)

	received, _ := collectWatchGCStatesInitial(re, stream, len(expectedKeyspaceIDs))
	receivedKeyspaceIDs := make([]uint32, 0, len(received))
	for keyspaceID := range received {
		receivedKeyspaceIDs = append(receivedKeyspaceIDs, keyspaceID)
	}
	slices.Sort(receivedKeyspaceIDs)
	re.Equal(expectedKeyspaceIDs, receivedKeyspaceIDs)
	re.Contains(received, constant.NullKeyspaceID)
}

func TestWatchGCSafePointV2SkipsInitialLoadAndSkipsNullKeyspace(t *testing.T) {
	re := require.New(t)
	ctx, cancel, cluster, leaderServer, grpcPDClient, conn, header := setupGCWatchTestCluster(t, 1)
	defer cancel()
	defer cluster.Destroy()
	defer conn.Close()

	keyspaceIDs := createGCWatchTestKeyspaces(re, leaderServer, 1)
	targetKeyspaceID := keyspaceIDs[0]

	shortWatchCtx, cancelShortWatch := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancelShortWatch()
	shortStream, err := grpcPDClient.WatchGCSafePointV2(shortWatchCtx, &pdpb.WatchGCSafePointV2Request{
		Header:   header,
		Revision: 0,
	})
	re.NoError(err)

	_, err = shortStream.Recv()
	re.Error(err)
	re.Equal(codes.DeadlineExceeded, status.Code(err))

	longWatchCtx, cancelLongWatch := context.WithTimeout(ctx, 10*time.Second)
	defer cancelLongWatch()
	longStream, err := grpcPDClient.WatchGCSafePointV2(longWatchCtx, &pdpb.WatchGCSafePointV2Request{
		Header:   header,
		Revision: 0,
	})
	re.NoError(err)

	mustAdvanceTxnSafePoint(ctx, re, grpcPDClient, header, constant.NullKeyspaceID, 10)
	mustAdvanceGCSafePoint(ctx, re, grpcPDClient, header, constant.NullKeyspaceID, 8)
	mustAdvanceTxnSafePoint(ctx, re, grpcPDClient, header, targetKeyspaceID, 10)
	mustAdvanceGCSafePoint(ctx, re, grpcPDClient, header, targetKeyspaceID, 8)

	waitForWatchGCSafePointV2SafePoint(re, longStream, targetKeyspaceID, 8, 5*time.Second)
}

func TestWatchGCStatesExcludeGCBarriers(t *testing.T) {
	re := require.New(t)
	ctx, cancel, cluster, leaderServer, grpcPDClient, conn, header := setupGCWatchTestCluster(t, 1)
	defer cancel()
	defer cluster.Destroy()
	defer conn.Close()

	keyspaceIDs := createGCWatchTestKeyspaces(re, leaderServer, 1)
	targetKeyspaceID := keyspaceIDs[0]

	shortWatchCtx, cancelShortWatch := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancelShortWatch()
	shortStream, err := grpcPDClient.WatchGCStates(shortWatchCtx, &pdpb.WatchGCStatesRequest{
		Header:             header,
		SkipLoadingInitial: true,
		ExcludeGcBarriers:  true,
	})
	re.NoError(err)

	mustSetGCBarrier(ctx, re, grpcPDClient, header, targetKeyspaceID, "watch-barrier-only", 20)
	_, err = shortStream.Recv()
	re.Error(err)
	re.Equal(codes.DeadlineExceeded, status.Code(err))

	longWatchCtx, cancelLongWatch := context.WithTimeout(ctx, 10*time.Second)
	defer cancelLongWatch()
	longStream, err := grpcPDClient.WatchGCStates(longWatchCtx, &pdpb.WatchGCStatesRequest{
		Header:             header,
		SkipLoadingInitial: true,
		ExcludeGcBarriers:  true,
	})
	re.NoError(err)

	mustAdvanceTxnSafePoint(ctx, re, grpcPDClient, header, targetKeyspaceID, 10)
	resp, err := recvGCStateWithTimeout(re, longStream.Recv, 5*time.Second)
	re.NoError(err)
	re.Len(resp.GetGcStates(), 1)
	re.Equal(targetKeyspaceID, resp.GetGcStates()[0].GetKeyspaceScope().GetKeyspaceId())
	re.Equal(uint64(10), resp.GetGcStates()[0].GetTxnSafePoint())
	re.Empty(resp.GetGcStates()[0].GetGcBarriers())
}

func TestWatchGCStatesIgnoresUnchangedChanges(t *testing.T) {
	re := require.New(t)
	ctx, cancel, cluster, leaderServer, grpcPDClient, conn, header := setupGCWatchTestCluster(t, 1)
	defer cancel()
	defer cluster.Destroy()
	defer conn.Close()

	keyspaceIDs := createGCWatchTestKeyspaces(re, leaderServer, 1)
	targetKeyspaceID := keyspaceIDs[0]

	mustAdvanceTxnSafePoint(ctx, re, grpcPDClient, header, targetKeyspaceID, 10)
	mustAdvanceGCSafePoint(ctx, re, grpcPDClient, header, targetKeyspaceID, 8)
	mustSetGCBarrier(ctx, re, grpcPDClient, header, targetKeyspaceID, "watch-noop", 10)

	watchCtx, cancelWatch := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancelWatch()
	stream, err := grpcPDClient.WatchGCStates(watchCtx, &pdpb.WatchGCStatesRequest{
		Header:             header,
		SkipLoadingInitial: true,
		ExcludeGcBarriers:  false,
	})
	re.NoError(err)

	resp, err := grpcPDClient.AdvanceTxnSafePoint(ctx, &pdpb.AdvanceTxnSafePointRequest{
		Header:        header,
		KeyspaceScope: &pdpb.KeyspaceScope{KeyspaceId: targetKeyspaceID},
		Target:        30,
	})
	re.NoError(err)
	re.NotNil(resp.GetHeader())
	re.Nil(resp.GetHeader().GetError())
	re.Equal(uint64(10), resp.GetOldTxnSafePoint())
	re.Equal(uint64(10), resp.GetNewTxnSafePoint())
	re.NotEmpty(resp.GetBlockerDescription())

	_, err = stream.Recv()
	re.Error(err)
	re.Equal(codes.DeadlineExceeded, status.Code(err))
}

func TestWatchGCStatesExcludeGCBarriersIgnoresLazyDeleteOnlyChanges(t *testing.T) {
	re := require.New(t)
	ctx, cancel, cluster, leaderServer, grpcPDClient, conn, header := setupGCWatchTestCluster(t, 1)
	defer cancel()
	defer cluster.Destroy()
	defer conn.Close()

	keyspaceIDs := createGCWatchTestKeyspaces(re, leaderServer, 1)
	targetKeyspaceID := keyspaceIDs[0]

	mustAdvanceTxnSafePoint(ctx, re, grpcPDClient, header, targetKeyspaceID, 40)
	mustSetGCBarrier(ctx, re, grpcPDClient, header, targetKeyspaceID, "keep", 50)

	shortBarrierID := "expire-soon"
	setBarrierResp, err := grpcPDClient.SetGCBarrier(ctx, &pdpb.SetGCBarrierRequest{
		Header:        header,
		KeyspaceScope: &pdpb.KeyspaceScope{KeyspaceId: targetKeyspaceID},
		BarrierId:     shortBarrierID,
		BarrierTs:     60,
		TtlSeconds:    1,
	})
	re.NoError(err)
	re.NotNil(setBarrierResp.GetHeader())
	re.Nil(setBarrierResp.GetHeader().GetError())

	includeWatchCtx, cancelIncludeWatch := context.WithTimeout(ctx, 10*time.Second)
	defer cancelIncludeWatch()
	includeStream, err := grpcPDClient.WatchGCStates(includeWatchCtx, &pdpb.WatchGCStatesRequest{
		Header:             header,
		SkipLoadingInitial: true,
		ExcludeGcBarriers:  false,
	})
	re.NoError(err)

	excludeWatchCtx, cancelExcludeWatch := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancelExcludeWatch()
	excludeStream, err := grpcPDClient.WatchGCStates(excludeWatchCtx, &pdpb.WatchGCStatesRequest{
		Header:             header,
		SkipLoadingInitial: true,
		ExcludeGcBarriers:  true,
	})
	re.NoError(err)

	time.Sleep(2 * time.Second)
	advanceResp, err := grpcPDClient.AdvanceTxnSafePoint(ctx, &pdpb.AdvanceTxnSafePointRequest{
		Header:        header,
		KeyspaceScope: &pdpb.KeyspaceScope{KeyspaceId: targetKeyspaceID},
		Target:        40,
	})
	re.NoError(err)
	re.NotNil(advanceResp.GetHeader())
	re.Nil(advanceResp.GetHeader().GetError())
	re.Equal(uint64(40), advanceResp.GetOldTxnSafePoint())
	re.Equal(uint64(40), advanceResp.GetNewTxnSafePoint())

	watchResp, err := recvGCStateWithTimeout(re, includeStream.Recv, 5*time.Second)
	re.NoError(err)
	re.Len(watchResp.GetGcStates(), 1)
	re.Equal(targetKeyspaceID, watchResp.GetGcStates()[0].GetKeyspaceScope().GetKeyspaceId())
	re.Len(watchResp.GetGcStates()[0].GetGcBarriers(), 1)
	re.Equal("keep", watchResp.GetGcStates()[0].GetGcBarriers()[0].GetBarrierId())

	_, err = excludeStream.Recv()
	re.Error(err)
	re.Equal(codes.DeadlineExceeded, status.Code(err))
}

func TestWatchGCSafePointV2IgnoresBarrierOnlyChanges(t *testing.T) {
	re := require.New(t)
	ctx, cancel, cluster, leaderServer, grpcPDClient, conn, header := setupGCWatchTestCluster(t, 1)
	defer cancel()
	defer cluster.Destroy()
	defer conn.Close()

	keyspaceIDs := createGCWatchTestKeyspaces(re, leaderServer, 1)
	targetKeyspaceID := keyspaceIDs[0]

	shortWatchCtx, cancelShortWatch := context.WithTimeout(ctx, time.Second)
	defer cancelShortWatch()
	shortStream, err := grpcPDClient.WatchGCSafePointV2(shortWatchCtx, &pdpb.WatchGCSafePointV2Request{
		Header:   header,
		Revision: 0,
	})
	re.NoError(err)

	mustSetGCBarrier(ctx, re, grpcPDClient, header, targetKeyspaceID, "watch-barrier-only", 20)
	_, err = shortStream.Recv()
	re.Error(err)
	re.Equal(codes.DeadlineExceeded, status.Code(err))

	longWatchCtx, cancelLongWatch := context.WithTimeout(ctx, 10*time.Second)
	defer cancelLongWatch()
	longStream, err := grpcPDClient.WatchGCSafePointV2(longWatchCtx, &pdpb.WatchGCSafePointV2Request{
		Header:   header,
		Revision: 0,
	})
	re.NoError(err)

	mustAdvanceTxnSafePoint(ctx, re, grpcPDClient, header, targetKeyspaceID, 10)
	mustAdvanceGCSafePoint(ctx, re, grpcPDClient, header, targetKeyspaceID, 8)
	waitForWatchGCSafePointV2SafePoint(re, longStream, targetKeyspaceID, 8, 5*time.Second)
}

func TestWatchGCStreamsCloseOnLeaderTransfer(t *testing.T) {
	re := require.New(t)
	ctx, cancel, cluster, leaderServer, grpcPDClient, conn, header := setupGCWatchTestCluster(t, 3)
	defer cancel()
	defer cluster.Destroy()
	defer conn.Close()

	createGCWatchTestKeyspaces(re, leaderServer, 1)

	gcStatesWatchCtx, cancelGCStatesWatch := context.WithTimeout(ctx, 10*time.Second)
	defer cancelGCStatesWatch()
	gcStatesStream, err := grpcPDClient.WatchGCStates(gcStatesWatchCtx, &pdpb.WatchGCStatesRequest{
		Header:             header,
		SkipLoadingInitial: true,
		ExcludeGcBarriers:  false,
	})
	re.NoError(err)

	gcSafePointWatchCtx, cancelGCSafePointWatch := context.WithTimeout(ctx, 10*time.Second)
	defer cancelGCSafePointWatch()
	gcSafePointStream, err := grpcPDClient.WatchGCSafePointV2(gcSafePointWatchCtx, &pdpb.WatchGCSafePointV2Request{
		Header:   header,
		Revision: 0,
	})
	re.NoError(err)

	oldLeaderName := cluster.WaitLeader()
	re.Equal(oldLeaderName, leaderServer.GetServer().Name())
	re.NoError(cluster.GetServer(oldLeaderName).ResignLeader())
	newLeaderName := cluster.WaitLeader()
	re.NotEqual(oldLeaderName, newLeaderName)

	err = collectWatchGCStatesUntilError(re, gcStatesStream, 5*time.Second)
	re.Error(err)
	re.Equal(codes.Unavailable, status.Code(err))
	re.ErrorContains(err, errs.ErrNotLeader.Error())

	err = collectWatchGCSafePointV2UntilError(re, gcSafePointStream, 5*time.Second)
	re.Error(err)
	re.Equal(codes.Unavailable, status.Code(err))
	re.ErrorContains(err, errs.ErrNotLeader.Error())
}

func TestWatchGCStreamsCloseOnClientCancel(t *testing.T) {
	re := require.New(t)
	ctx, cancel, cluster, leaderServer, grpcPDClient, conn, header := setupGCWatchTestCluster(t, 1)
	defer cancel()
	defer cluster.Destroy()
	defer conn.Close()

	createGCWatchTestKeyspaces(re, leaderServer, 1)
	expectedGCStateKeyspaceIDs := getAllKeyspaceGCStateIDs(ctx, re, grpcPDClient, header)

	gcStatesWatchCtx, cancelGCStatesWatch := context.WithCancel(ctx)
	defer cancelGCStatesWatch()
	gcStatesStream, err := grpcPDClient.WatchGCStates(gcStatesWatchCtx, &pdpb.WatchGCStatesRequest{
		Header:             header,
		SkipLoadingInitial: false,
		ExcludeGcBarriers:  false,
	})
	re.NoError(err)
	_, _ = collectWatchGCStatesInitial(re, gcStatesStream, len(expectedGCStateKeyspaceIDs))

	gcSafePointWatchCtx, cancelGCSafePointWatch := context.WithCancel(ctx)
	defer cancelGCSafePointWatch()
	gcSafePointStream, err := grpcPDClient.WatchGCSafePointV2(gcSafePointWatchCtx, &pdpb.WatchGCSafePointV2Request{
		Header:   header,
		Revision: 0,
	})
	re.NoError(err)

	cancelGCStatesWatch()
	cancelGCSafePointWatch()

	err = collectWatchGCStatesUntilError(re, gcStatesStream, 5*time.Second)
	re.Error(err)
	re.Equal(codes.Canceled, status.Code(err))

	err = collectWatchGCSafePointV2UntilError(re, gcSafePointStream, 5*time.Second)
	re.Error(err)
	re.Equal(codes.Canceled, status.Code(err))
}

func TestWatchGCStatesInitialLoadInterruptedByClientCancel(t *testing.T) {
	re := require.New(t)
	ctx, cancel, cluster, leaderServer, grpcPDClient, conn, header := setupGCWatchTestCluster(t, 1)
	defer cancel()
	defer cluster.Destroy()
	defer conn.Close()

	createGCWatchTestKeyspaces(re, leaderServer, 1100)
	expectedKeyspaceIDs := getAllKeyspaceGCStateIDs(ctx, re, grpcPDClient, header)
	re.Greater(len(expectedKeyspaceIDs), 1024)

	watchCtx, cancelWatch := context.WithCancel(ctx)
	defer cancelWatch()
	stream, err := grpcPDClient.WatchGCStates(watchCtx, &pdpb.WatchGCStatesRequest{
		Header:             header,
		SkipLoadingInitial: false,
		ExcludeGcBarriers:  false,
	})
	re.NoError(err)

	resp, err := recvGCStateWithTimeout(re, stream.Recv, 5*time.Second)
	re.NoError(err)
	re.Greater(len(resp.GetGcStates()), 0)
	re.Less(len(resp.GetGcStates()), len(expectedKeyspaceIDs))

	cancelWatch()

	err = collectWatchGCStatesUntilError(re, stream, 5*time.Second)
	re.Error(err)
	re.Equal(codes.Canceled, status.Code(err))
}

func TestWatchGCStatesInitialLoadInterruptedByLeaderTransfer(t *testing.T) {
	re := require.New(t)
	ctx, cancel, cluster, leaderServer, grpcPDClient, conn, header := setupGCWatchTestCluster(t, 3)
	defer cancel()
	defer cluster.Destroy()
	defer conn.Close()

	createGCWatchTestKeyspaces(re, leaderServer, 1100)
	expectedKeyspaceIDs := getAllKeyspaceGCStateIDs(ctx, re, grpcPDClient, header)
	re.Greater(len(expectedKeyspaceIDs), 1024)

	watchCtx, cancelWatch := context.WithTimeout(ctx, 10*time.Second)
	defer cancelWatch()
	stream, err := grpcPDClient.WatchGCStates(watchCtx, &pdpb.WatchGCStatesRequest{
		Header:             header,
		SkipLoadingInitial: false,
		ExcludeGcBarriers:  false,
	})
	re.NoError(err)

	resp, err := recvGCStateWithTimeout(re, stream.Recv, 5*time.Second)
	re.NoError(err)
	re.Greater(len(resp.GetGcStates()), 0)
	re.Less(len(resp.GetGcStates()), len(expectedKeyspaceIDs))

	oldLeaderName := cluster.WaitLeader()
	re.Equal(oldLeaderName, leaderServer.GetServer().Name())
	re.NoError(cluster.GetServer(oldLeaderName).ResignLeader())
	newLeaderName := cluster.WaitLeader()
	re.NotEqual(oldLeaderName, newLeaderName)

	err = collectWatchGCStatesUntilError(re, stream, 5*time.Second)
	re.Error(err)
	re.Equal(codes.Unavailable, status.Code(err))
	re.ErrorContains(err, errs.ErrNotLeader.Error())
}
