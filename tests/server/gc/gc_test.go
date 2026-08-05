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
	"math"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"

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

const (
	postGetGCStateCallFailpoint       = "github.com/tikv/pd/server/postGetGCStateCall"
	getGCStateBeforeSlowPathFailpoint = "github.com/tikv/pd/pkg/gc/getGCStateBeforeSlowPath"
	skipCampaignLeaderCheckFailpoint  = "github.com/tikv/pd/pkg/member/skipCampaignLeaderCheck"
)

func makeKeyspaceScope(keyspaceID uint32) *pdpb.KeyspaceScope {
	return &pdpb.KeyspaceScope{
		Keyspace: &pdpb.KeyspaceScope_KeyspaceId{KeyspaceId: keyspaceID},
	}
}

func newGCStateLeaderTransitionCluster(t *testing.T) (*tests.TestCluster, *pdpb.GetGCStateRequest, func()) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())

	// These tests need two PDs so the request can target a concrete old leader
	// while another node takes over leadership.
	cluster, err := tests.NewTestCluster(ctx, 2, func(conf *config.Config, _ string) {
		conf.Keyspace.WaitRegionSplit = false
	})
	re.NoError(err)
	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())
	re.NoError(failpoint.Enable(skipCampaignLeaderCheckFailpoint, "return(true)"))

	leaderServer := cluster.GetLeaderServer()
	re.NotNil(leaderServer)
	re.NoError(leaderServer.BootstrapCluster())

	req := &pdpb.GetGCStateRequest{
		Header:        testutil.NewRequestHeader(leaderServer.GetClusterID()),
		KeyspaceScope: makeKeyspaceScope(constant.NullKeyspaceID),
	}
	cleanup := func() {
		re.NoError(failpoint.Disable(skipCampaignLeaderCheckFailpoint))
		cancel()
		cluster.Destroy()
	}
	return cluster, req, cleanup
}

func getGCStateFromAddr(ctx context.Context, re *require.Assertions, addr string, req *pdpb.GetGCStateRequest) (*pdpb.GetGCStateResponse, error) {
	grpcPDClient, conn := testutil.MustNewGrpcClient(re, addr)
	defer conn.Close()
	return grpcPDClient.GetGCState(ctx, req)
}

type blockingFailpoint struct {
	name        string
	reached     chan struct{}
	release     chan struct{}
	reachedOnce sync.Once
	releaseOnce sync.Once
	disableOnce sync.Once
}

func enableBlockingFailpoint(re *require.Assertions, name string) *blockingFailpoint {
	point := &blockingFailpoint{
		name:    name,
		reached: make(chan struct{}),
		release: make(chan struct{}),
	}
	re.NoError(failpoint.EnableCall(name, func() {
		point.reachedOnce.Do(func() {
			close(point.reached)
		})
		<-point.release
	}))
	return point
}

func (p *blockingFailpoint) wait(re *require.Assertions, description string) {
	// Always use a bounded wait: if a future refactor moves/removes the target
	// failpoint, the test should fail quickly instead of hanging until the
	// request context times out.
	select {
	case <-p.reached:
	case <-time.After(5 * time.Second):
		re.FailNow("GetGCState did not reach the failpoint", description)
	}
}

func (p *blockingFailpoint) releaseAndDisable(re *require.Assertions) {
	// Tests may release explicitly and still run deferred cleanup. Make both
	// operations idempotent so failure paths do not panic on double close or
	// double disable.
	p.releaseOnce.Do(func() {
		close(p.release)
	})
	p.disableOnce.Do(func() {
		re.NoError(failpoint.Disable(p.name))
	})
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
				KeyspaceScope: makeKeyspaceScope(keyspaceID),
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
				KeyspaceScope: makeKeyspaceScope(keyspaceID),
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
				KeyspaceScope: makeKeyspaceScope(keyspaceID),
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
				KeyspaceScope: makeKeyspaceScope(keyspaceID),
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
				KeyspaceScope: makeKeyspaceScope(keyspaceID),
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
				KeyspaceScope: makeKeyspaceScope(keyspaceID),
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
				KeyspaceScope: makeKeyspaceScope(keyspaceID),
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
				KeyspaceScope: makeKeyspaceScope(keyspaceID),
			}
			resp, err := grpcPDClient.GetGCState(ctx, req)
			re.NoError(err)
			re.NotNil(resp.Header)
			re.Nil(resp.Header.Error)
			re.Equal(keyspaceID, resp.GetGcState().GetKeyspaceScope().GetKeyspaceId())
			re.Equal(keyspaceID != constant.NullKeyspaceID, resp.GetGcState().GetIsKeyspaceLevelGc())
			re.Equal(uint64(15), resp.GetGcState().GetTxnSafePoint())
			re.Equal(uint64(8), resp.GetGcState().GetGcSafePoint())
			re.Len(resp.GetGcState().GetGcBarriers(), 1)
			re.Equal("b1", resp.GetGcState().GetGcBarriers()[0].GetBarrierId())
			re.Equal(uint64(15), resp.GetGcState().GetGcBarriers()[0].GetBarrierTs())
			re.Greater(resp.GetGcState().GetGcBarriers()[0].GetTtlSeconds(), int64(3500))
			re.Less(resp.GetGcState().GetGcBarriers()[0].GetTtlSeconds(), int64(3601))

			req.ExcludeGcBarriers = true
			resp, err = grpcPDClient.GetGCState(ctx, req)
			re.NoError(err)
			re.NotNil(resp.Header)
			re.Nil(resp.Header.Error)
			re.Empty(resp.GetGcState().GetGcBarriers())
		}
	}

	testInKeyspace(constant.NullKeyspaceID)
	testInKeyspace(ks1.GetId())

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
		receivedKeyspaceIDs = append(receivedKeyspaceIDs, gcState.GetKeyspaceScope().GetKeyspaceId())
	}
	slices.Sort(receivedKeyspaceIDs)

	// Expected keyspace IDs differ between NextGen and Classic
	var expectedKeyspaceIDs []uint32
	if kerneltype.IsNextGen() {
		expectedKeyspaceIDs = []uint32{ks1.GetId(), constant.SystemKeyspaceID, constant.NullKeyspaceID}
	} else {
		expectedKeyspaceIDs = []uint32{0, ks1.GetId(), constant.NullKeyspaceID}
	}
	re.Equal(expectedKeyspaceIDs, receivedKeyspaceIDs)

	// As the same test logic was run on the two keyspaces, they should have the same result.
	for _, gcState := range resp.GetGcStates() {
		// Ignore the default keyspace which is not used in this test.
		defaultKeyspaceID := uint32(0)
		if kerneltype.IsNextGen() {
			defaultKeyspaceID = constant.SystemKeyspaceID
		}
		if gcState.GetKeyspaceScope().GetKeyspaceId() == defaultKeyspaceID {
			continue
		}
		re.Equal(gcState.GetKeyspaceScope().GetKeyspaceId() != constant.NullKeyspaceID, gcState.GetIsKeyspaceLevelGc())
		re.Equal(uint64(15), gcState.GetTxnSafePoint())
		re.Equal(uint64(8), gcState.GetGcSafePoint())
		re.Len(gcState.GetGcBarriers(), 1)
		re.Equal("b1", gcState.GetGcBarriers()[0].GetBarrierId())
		re.Equal(uint64(15), gcState.GetGcBarriers()[0].GetBarrierTs())
		re.Greater(gcState.GetGcBarriers()[0].GetTtlSeconds(), int64(3500))
		re.Less(gcState.GetGcBarriers()[0].GetTtlSeconds(), int64(3601))
	}

	req.ExcludeGcBarriers = true
	resp, err = grpcPDClient.GetAllKeyspacesGCStates(ctx, req)
	re.NoError(err)
	re.NotNil(resp.Header)
	re.Nil(resp.Header.Error)
	for _, gcState := range resp.GetGcStates() {
		re.Empty(gcState.GetGcBarriers())
	}

	// Global GC Barrier API
	for _, keyspaceID := range []uint32{constant.NullKeyspaceID, ks1.GetId()} {
		// Cleanup before test
		req1 := &pdpb.GetGCStateRequest{
			Header:        header,
			KeyspaceScope: makeKeyspaceScope(keyspaceID),
		}
		resp1, err := grpcPDClient.GetGCState(ctx, req1)
		re.NoError(err)
		for _, state := range resp1.GcState.GcBarriers {
			req := &pdpb.DeleteGCBarrierRequest{
				Header:        header,
				KeyspaceScope: makeKeyspaceScope(keyspaceID),
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
		req := &pdpb.GetAllKeyspacesGCStatesRequest{
			Header:                  header,
			ExcludeGlobalGcBarriers: true,
		}
		resp, err := grpcPDClient.GetAllKeyspacesGCStates(ctx, req)
		re.NoError(err)
		re.NotNil(resp.Header)
		re.Nil(resp.Header.Error)
		re.Empty(resp.GetGlobalGcBarriers())
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

	for _, keyspaceID := range []uint32{constant.NullKeyspaceID, ks1.GetId()} {
		// Advance txn safe point reports the reason of being blocked
		req := &pdpb.AdvanceTxnSafePointRequest{
			Header:        header,
			KeyspaceScope: makeKeyspaceScope(keyspaceID),
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

	for _, keyspaceID := range []uint32{constant.NullKeyspaceID, ks1.GetId()} {
		// Advance txn safe point reports the reason of being blocked
		req := &pdpb.AdvanceTxnSafePointRequest{
			Header:        header,
			KeyspaceScope: makeKeyspaceScope(keyspaceID),
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

func TestGetGCStateRejectsOldLeaderAfterTransfer(t *testing.T) {
	re := require.New(t)
	cluster, req, cleanup := newGCStateLeaderTransitionCluster(t)
	defer cleanup()

	oldLeader := cluster.GetLeader()
	re.NotEmpty(oldLeader)
	oldLeaderServer := cluster.GetServer(oldLeader)
	re.NotNil(oldLeaderServer)

	// Once the cluster agrees on a new PD leader, the old leader should reject a
	// direct GetGCState request at the normal gRPC role check, regardless of any
	// local GC state cache it may have had.
	re.NoError(oldLeaderServer.ResignLeaderWithRetry())
	newLeader := cluster.WaitLeader()
	re.NotEmpty(newLeader)
	re.NotEqual(oldLeader, newLeader)

	_, err := getGCStateFromAddr(context.Background(), re, oldLeaderServer.GetAddr(), req)
	re.ErrorContains(err, "not leader")
}

func TestGetGCStateReturnsCachedStateIfLeaderLostBeforeReply(t *testing.T) {
	re := require.New(t)
	cluster, req, cleanup := newGCStateLeaderTransitionCluster(t)
	defer cleanup()

	leaderServer := cluster.GetLeaderServer()
	re.NotNil(leaderServer)
	req.ExcludeGcBarriers = true

	// Warm the local cache so the blocked request has already obtained its
	// cached result before the leadership transfer happens.
	_, err := leaderServer.GetServer().GetGCStateManager().AdvanceTxnSafePoint(constant.NullKeyspaceID, 10, time.Now())
	re.NoError(err)
	resp, err := getGCStateFromAddr(context.Background(), re, leaderServer.GetAddr(), req)
	re.NoError(err)
	re.Nil(resp.GetHeader().GetError())
	re.Equal(uint64(10), resp.GetGcState().GetTxnSafePoint())

	point := enableBlockingFailpoint(re, postGetGCStateCallFailpoint)
	defer point.releaseAndDisable(re)

	grpcPDClient, conn := testutil.MustNewGrpcClient(re, leaderServer.GetAddr())
	defer conn.Close()

	type result struct {
		resp *pdpb.GetGCStateResponse
		err  error
	}
	resultCh := make(chan result, 1)
	reqCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	go func() {
		resp, err := grpcPDClient.GetGCState(reqCtx, req)
		resultCh <- result{resp: resp, err: err}
	}()

	point.wait(re, "after GetGCState has returned and before the handler replies")
	oldLeader := leaderServer.GetConfig().Name
	re.NoError(leaderServer.ResignLeaderWithRetry())
	newLeader := cluster.WaitLeader()
	re.NotEmpty(newLeader)
	re.NotEqual(oldLeader, newLeader)

	// Even if another leader advances the persisted state while the request is
	// blocked, this already-read cache-hit request should still return the value
	// it obtained before the transfer.
	_, err = cluster.GetLeaderServer().GetServer().GetGCStateManager().AdvanceTxnSafePoint(constant.NullKeyspaceID, 20, time.Now())
	re.NoError(err)

	point.releaseAndDisable(re)
	res := <-resultCh
	re.NoError(res.err)
	re.Nil(res.resp.GetHeader().GetError())
	re.Equal(uint64(10), res.resp.GetGcState().GetTxnSafePoint())

	freshResp, err := getGCStateFromAddr(context.Background(), re, cluster.GetLeaderServer().GetAddr(), req)
	re.NoError(err)
	re.Nil(freshResp.GetHeader().GetError())
	re.Equal(uint64(20), freshResp.GetGcState().GetTxnSafePoint())
}

func TestGetGCStateReturnsCachedStateAfterLeadershipRecovery(t *testing.T) {
	re := require.New(t)
	cluster, req, cleanup := newGCStateLeaderTransitionCluster(t)
	defer cleanup()

	leaderServer := cluster.GetLeaderServer()
	re.NotNil(leaderServer)
	oldLeader := leaderServer.GetConfig().Name
	req.ExcludeGcBarriers = true

	_, err := leaderServer.GetServer().GetGCStateManager().AdvanceTxnSafePoint(constant.NullKeyspaceID, 10, time.Now())
	re.NoError(err)
	resp, err := getGCStateFromAddr(context.Background(), re, leaderServer.GetAddr(), req)
	re.NoError(err)
	re.Nil(resp.GetHeader().GetError())
	re.Equal(uint64(10), resp.GetGcState().GetTxnSafePoint())

	// This test pins the current cache-hit behavior; it does not claim that this
	// is the ideal long-term contract. Once the request has already obtained the
	// cached GC state and is paused in the handler, leadership churn does not
	// change that already-read value. If GetGCState is intentionally tightened in
	// the future, update this test together with the corresponding semantics
	// documentation.
	point := enableBlockingFailpoint(re, postGetGCStateCallFailpoint)
	defer point.releaseAndDisable(re)

	grpcPDClient, conn := testutil.MustNewGrpcClient(re, leaderServer.GetAddr())
	defer conn.Close()

	type result struct {
		resp *pdpb.GetGCStateResponse
		err  error
	}
	resultCh := make(chan result, 1)
	reqCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	go func() {
		resp, err := grpcPDClient.GetGCState(reqCtx, req)
		resultCh <- result{resp: resp, err: err}
	}()

	point.wait(re, "after GetGCState has returned and before the handler replies")
	re.NoError(leaderServer.ResignLeaderWithRetry())
	newLeader := cluster.WaitLeader()
	re.NotEmpty(newLeader)
	re.NotEqual(oldLeader, newLeader)

	// Let the temporary new leader advance the persisted state. The blocked
	// request should still return its already-read cached value after the old
	// leader regains leadership, while a later fresh request should see this
	// newer value.
	_, err = cluster.GetLeaderServer().GetServer().GetGCStateManager().AdvanceTxnSafePoint(constant.NullKeyspaceID, 20, time.Now())
	re.NoError(err)

	re.NoError(cluster.GetServer(newLeader).ResignLeaderWithRetry())
	re.Equal(oldLeader, cluster.WaitLeader())

	point.releaseAndDisable(re)
	res := <-resultCh
	re.NoError(res.err)
	re.Nil(res.resp.GetHeader().GetError())
	re.Equal(uint64(10), res.resp.GetGcState().GetTxnSafePoint())

	freshResp, err := getGCStateFromAddr(context.Background(), re, cluster.GetServer(oldLeader).GetAddr(), req)
	re.NoError(err)
	re.Nil(freshResp.GetHeader().GetError())
	re.Equal(uint64(20), freshResp.GetGcState().GetTxnSafePoint())
}

func TestGetGCStateSlowPathReadsLatestStateIfLeaderLostBeforeRead(t *testing.T) {
	re := require.New(t)
	cluster, req, cleanup := newGCStateLeaderTransitionCluster(t)
	defer cleanup()

	leaderServer := cluster.GetLeaderServer()
	re.NotNil(leaderServer)
	oldLeader := leaderServer.GetConfig().Name
	req.ExcludeGcBarriers = true

	// Start with no local cache and stop immediately before the slow path. The
	// request has already passed the initial server-side role check, but after
	// the transfer the manager must bypass cache and continue with the IO read.
	point := enableBlockingFailpoint(re, getGCStateBeforeSlowPathFailpoint)
	defer point.releaseAndDisable(re)

	grpcPDClient, conn := testutil.MustNewGrpcClient(re, leaderServer.GetAddr())
	defer conn.Close()

	type result struct {
		resp *pdpb.GetGCStateResponse
		err  error
	}
	resultCh := make(chan result, 1)
	reqCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	go func() {
		resp, err := grpcPDClient.GetGCState(reqCtx, req)
		resultCh <- result{resp: resp, err: err}
	}()

	point.wait(re, "before the no-cache slow path reads from storage")
	re.NoError(leaderServer.ResignLeaderWithRetry())
	newLeader := cluster.WaitLeader()
	re.NotEmpty(newLeader)
	re.NotEqual(oldLeader, newLeader)

	// The new leader advances the persisted state while the old leader's request
	// is blocked. Once released, the in-flight request should still succeed on
	// the old follower by reading the latest state from storage.
	_, err := cluster.GetLeaderServer().GetServer().GetGCStateManager().AdvanceTxnSafePoint(constant.NullKeyspaceID, 20, time.Now())
	re.NoError(err)

	point.releaseAndDisable(re)
	res := <-resultCh
	re.NoError(res.err)
	re.Nil(res.resp.GetHeader().GetError())
	re.Equal(uint64(20), res.resp.GetGcState().GetTxnSafePoint())
}
