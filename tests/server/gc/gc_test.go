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
	"errors"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/ratelimit"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/versioninfo/kerneltype"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

// In tests in this file, we only verify that the parameters and results are properly passed. The detailed behavior
// of these APIs are covered elsewhere.

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(testutil.WaitForEtcdConnections(m), testutil.LeakOptions...)
}

const (
	postGetGCStateCallFailpoint       = "github.com/tikv/pd/server/postGetGCStateCall"
	getGCStateBeforeSlowPathFailpoint = "github.com/tikv/pd/pkg/gc/getGCStateBeforeSlowPath"
	skipCampaignLeaderCheckFailpoint  = "github.com/tikv/pd/pkg/member/skipCampaignLeaderCheck"
	watchGCStatesRegisteredFailpoint  = "github.com/tikv/pd/pkg/gc/watchGCStatesRegistered"
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

type watchGCStatesRegistrationPoint struct {
	registered   chan struct{}
	registerOnce sync.Once
	disableOnce  sync.Once
}

func enableWatchGCStatesRegistrationPoint(t *testing.T) *watchGCStatesRegistrationPoint {
	t.Helper()
	re := require.New(t)
	point := &watchGCStatesRegistrationPoint{registered: make(chan struct{})}
	re.NoError(failpoint.EnableCall(watchGCStatesRegisteredFailpoint, func() {
		point.registerOnce.Do(func() {
			close(point.registered)
		})
	}))
	t.Cleanup(func() {
		point.disable(re)
	})
	return point
}

func (p *watchGCStatesRegistrationPoint) wait(t *testing.T) {
	t.Helper()
	select {
	case <-p.registered:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "WatchGCStates was not registered")
	}
}

func (p *watchGCStatesRegistrationPoint) disable(re *require.Assertions) {
	p.disableOnce.Do(func() {
		re.NoError(failpoint.Disable(watchGCStatesRegisteredFailpoint))
	})
}

func newWatchGCStatesCluster(t *testing.T, serverCount int, bootstrap bool) *tests.TestCluster {
	t.Helper()
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	cluster, err := tests.NewTestCluster(ctx, serverCount, func(conf *config.Config, _ string) {
		conf.Keyspace.WaitRegionSplit = false
	})
	re.NoError(err)
	t.Cleanup(func() {
		cancel()
		cluster.Destroy()
	})
	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())
	if bootstrap {
		re.NoError(cluster.GetLeaderServer().BootstrapCluster())
	}
	return cluster
}

func newWatchGCStatesClient(t *testing.T, addr string) pdpb.PDClient {
	t.Helper()
	re := require.New(t)
	client, conn := testutil.MustNewGrpcClient(re, addr)
	t.Cleanup(func() {
		re.NoError(conn.Close())
	})
	return client
}

func openWatchGCStates(
	t *testing.T,
	client pdpb.PDClient,
	header *pdpb.RequestHeader,
	skipLoadingInitial bool,
) (pdpb.PD_WatchGCStatesClient, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	stream, err := client.WatchGCStates(ctx, &pdpb.WatchGCStatesRequest{
		Header:             header,
		SkipLoadingInitial: skipLoadingInitial,
	})
	require.NoError(t, err)
	return stream, cancel
}

func recvWatchGCStateForKeyspace(t *testing.T, stream pdpb.PD_WatchGCStatesClient, keyspaceID uint32) *pdpb.GCState {
	t.Helper()
	for {
		response, err := stream.Recv()
		require.NoError(t, err)
		require.NotNil(t, response.GetHeader())
		for _, change := range response.GetChanges() {
			if state := change.GetUpsert(); state != nil && state.GetKeyspaceScope().GetKeyspaceId() == keyspaceID {
				return state
			}
		}
	}
}

func advanceWatchGCStatesTxnSafePoint(
	t *testing.T,
	client pdpb.PDClient,
	header *pdpb.RequestHeader,
	keyspaceID uint32,
	target uint64,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	response, err := client.AdvanceTxnSafePoint(ctx, &pdpb.AdvanceTxnSafePointRequest{
		Header:        header,
		KeyspaceScope: makeKeyspaceScope(keyspaceID),
		Target:        target,
	})
	require.NoError(t, err)
	require.NotNil(t, response.GetHeader())
	require.Nil(t, response.GetHeader().GetError())
	require.Equal(t, target, response.GetNewTxnSafePoint())
}

type failingWatchGCStatesServer struct {
	ctx     context.Context
	sendErr error
}

func (s *failingWatchGCStatesServer) Send(*pdpb.WatchGCStatesResponse) error {
	return s.sendErr
}

func (*failingWatchGCStatesServer) SetHeader(metadata.MD) error  { return nil }
func (*failingWatchGCStatesServer) SendHeader(metadata.MD) error { return nil }
func (*failingWatchGCStatesServer) SetTrailer(metadata.MD)       {}

func (s *failingWatchGCStatesServer) Context() context.Context {
	return s.ctx
}

func (*failingWatchGCStatesServer) SendMsg(any) error { return nil }
func (*failingWatchGCStatesServer) RecvMsg(any) error { return nil }

func prometheusMetricValue(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			if len(metric.GetLabel()) != len(labels) {
				continue
			}
			matches := true
			for _, pair := range metric.GetLabel() {
				if labels[pair.GetName()] != pair.GetValue() {
					matches = false
					break
				}
			}
			if !matches {
				continue
			}
			if gauge := metric.GetGauge(); gauge != nil {
				return gauge.GetValue()
			}
			if counter := metric.GetCounter(); counter != nil {
				return counter.GetValue()
			}
			require.FailNow(t, "metric has unsupported type", name)
		}
	}
	require.FailNow(t, "metric not found", name)
	return 0
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

	// This hook is reached only after a successful combined state and global
	// barrier read. It proves that the default RPC remains on its no-global-read
	// path while a request that explicitly opts in executes the combined read.
	var combinedReadCount atomic.Int64
	combinedReadFailpoint := "github.com/tikv/pd/pkg/gc/" +
		"getGCStateWithGlobalGCBarriersAfterRead"
	re.NoError(failpoint.EnableCall(combinedReadFailpoint, func() {
		combinedReadCount.Add(1)
	}))
	defer func() {
		re.NoError(failpoint.Disable(combinedReadFailpoint))
	}()

	defaultResp, err := grpcPDClient.GetGCState(ctx, &pdpb.GetGCStateRequest{
		Header:        header,
		KeyspaceScope: makeKeyspaceScope(constant.NullKeyspaceID),
	})
	re.NoError(err)
	re.Nil(defaultResp.GetGlobalGcBarriers())
	re.Zero(combinedReadCount.Load())

	optedInResp, err := grpcPDClient.GetGCState(ctx, &pdpb.GetGCStateRequest{
		Header:                  header,
		KeyspaceScope:           makeKeyspaceScope(constant.NullKeyspaceID),
		IncludeGlobalGcBarriers: true,
	})
	re.NoError(err)
	re.NotNil(optedInResp.GetGlobalGcBarriers())
	re.Empty(optedInResp.GetGlobalGcBarriers().GetBarriers())
	re.Equal(int64(1), combinedReadCount.Load())

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

	for _, keyspaceID := range []uint32{constant.NullKeyspaceID, ks1.GetId()} {
		resp, err := grpcPDClient.GetGCState(ctx, &pdpb.GetGCStateRequest{
			Header:                  header,
			KeyspaceScope:           makeKeyspaceScope(keyspaceID),
			ExcludeGcBarriers:       true,
			IncludeGlobalGcBarriers: true,
		})
		re.NoError(err)
		re.Len(resp.GetGlobalGcBarriers().GetBarriers(), 2)
		re.Empty(resp.GetGcState().GetGcBarriers())

		barriers := append([]*pdpb.GlobalGCBarrierInfo(nil), resp.GetGlobalGcBarriers().GetBarriers()...)
		slices.SortFunc(barriers, func(a, b *pdpb.GlobalGCBarrierInfo) int {
			if a.GetBarrierId() < b.GetBarrierId() {
				return -1
			}
			if a.GetBarrierId() > b.GetBarrierId() {
				return 1
			}
			return 0
		})
		re.Equal("b1", barriers[0].GetBarrierId())
		re.Equal(uint64(20), barriers[0].GetBarrierTs())
		re.GreaterOrEqual(barriers[0].GetTtlSeconds(), int64(3595))
		re.LessOrEqual(barriers[0].GetTtlSeconds(), int64(3600))
		re.Equal("b2", barriers[1].GetBarrierId())
		re.Equal(uint64(24), barriers[1].GetBarrierTs())
		re.Equal(int64(math.MaxInt64), barriers[1].GetTtlSeconds())
	}

	localObserver, err := grpcPDClient.SetGCBarrier(ctx, &pdpb.SetGCBarrierRequest{
		Header:        header,
		KeyspaceScope: makeKeyspaceScope(constant.NullKeyspaceID),
		BarrierId:     "local-observer",
		BarrierTs:     26,
		TtlSeconds:    math.MaxInt64,
	})
	re.NoError(err)
	re.Nil(localObserver.GetHeader().GetError())
	localAndGlobalResp, err := grpcPDClient.GetGCState(ctx, &pdpb.GetGCStateRequest{
		Header:                  header,
		KeyspaceScope:           makeKeyspaceScope(constant.NullKeyspaceID),
		IncludeGlobalGcBarriers: true,
	})
	re.NoError(err)
	re.Len(localAndGlobalResp.GetGlobalGcBarriers().GetBarriers(), 2)
	re.Len(localAndGlobalResp.GetGcState().GetGcBarriers(), 1)
	re.Equal("local-observer", localAndGlobalResp.GetGcState().GetGcBarriers()[0].GetBarrierId())

	_, err = leaderServer.GetServer().GetGCStateManager().SetGlobalGCBarrier(
		ctx,
		"expired",
		25,
		time.Second,
		time.Now().Add(-2*time.Second),
	)
	re.NoError(err)
	expiredResp, err := grpcPDClient.GetGCState(ctx, &pdpb.GetGCStateRequest{
		Header:                  header,
		KeyspaceScope:           makeKeyspaceScope(constant.NullKeyspaceID),
		ExcludeGcBarriers:       true,
		IncludeGlobalGcBarriers: true,
	})
	re.NoError(err)
	barriersWithExpired := append([]*pdpb.GlobalGCBarrierInfo(nil), expiredResp.GetGlobalGcBarriers().GetBarriers()...)
	slices.SortFunc(barriersWithExpired, func(a, b *pdpb.GlobalGCBarrierInfo) int {
		if a.GetBarrierId() < b.GetBarrierId() {
			return -1
		}
		if a.GetBarrierId() > b.GetBarrierId() {
			return 1
		}
		return 0
	})
	re.Len(barriersWithExpired, 3)
	re.Equal("b1", barriersWithExpired[0].GetBarrierId())
	re.Equal("b2", barriersWithExpired[1].GetBarrierId())
	re.Equal("expired", barriersWithExpired[2].GetBarrierId())
	re.Equal(uint64(25), barriersWithExpired[2].GetBarrierTs())
	re.Zero(barriersWithExpired[2].GetTtlSeconds())

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

func TestGetGCStateGlobalBarriersReturnsReadSnapshot(t *testing.T) {
	re := require.New(t)
	cluster, req, cleanup := newGCStateLeaderTransitionCluster(t)
	defer cleanup()

	leaderServer := cluster.GetLeaderServer()
	re.NotNil(leaderServer)
	req.IncludeGlobalGcBarriers = true
	manager := leaderServer.GetServer().GetGCStateManager()
	_, err := manager.SetGlobalGCBarrier(
		context.Background(),
		"snapshot",
		100,
		time.Duration(math.MaxInt64),
		time.Now(),
	)
	re.NoError(err)

	point := enableBlockingFailpoint(re, postGetGCStateCallFailpoint)
	defer point.releaseAndDisable(re)

	type result struct {
		resp *pdpb.GetGCStateResponse
		err  error
	}
	resultCh := make(chan result, 1)
	go func() {
		resp, err := getGCStateFromAddr(context.Background(), re, leaderServer.GetAddr(), req)
		resultCh <- result{resp: resp, err: err}
	}()

	point.wait(re, "after the combined GetGCState read and before response conversion")
	_, err = manager.SetGlobalGCBarrier(
		context.Background(),
		"snapshot",
		200,
		time.Duration(math.MaxInt64),
		time.Now(),
	)
	re.NoError(err)

	point.releaseAndDisable(re)
	select {
	case res := <-resultCh:
		re.NoError(res.err)
		re.Len(res.resp.GetGlobalGcBarriers().GetBarriers(), 1)
		re.Equal(uint64(100), res.resp.GetGlobalGcBarriers().GetBarriers()[0].GetBarrierTs())
	case <-time.After(5 * time.Second):
		re.FailNow("GetGCState did not return after releasing the failpoint")
	}

	freshResp, err := getGCStateFromAddr(context.Background(), re, leaderServer.GetAddr(), req)
	re.NoError(err)
	re.Len(freshResp.GetGlobalGcBarriers().GetBarriers(), 1)
	re.Equal(uint64(200), freshResp.GetGlobalGcBarriers().GetBarriers()[0].GetBarrierTs())
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

func TestWatchGCStatesInitialAndSkipInitialRegistrationBoundary(t *testing.T) {
	re := require.New(t)
	cluster := newWatchGCStatesCluster(t, 1, true)
	leaderServer := cluster.GetLeaderServer()
	re.NotNil(leaderServer)

	ks, err := leaderServer.GetKeyspaceManager().CreateKeyspace(&keyspace.CreateKeyspaceRequest{
		Name:       "watch-gc-states",
		Config:     map[string]string{keyspace.GCManagementType: keyspace.KeyspaceLevelGC},
		CreateTime: time.Now().Unix(),
	})
	re.NoError(err)

	client := newWatchGCStatesClient(t, leaderServer.GetAddr())
	header := testutil.NewRequestHeader(leaderServer.GetClusterID())
	initialStream, cancelInitial := openWatchGCStates(t, client, header, false)
	initial := recvWatchGCStateForKeyspace(t, initialStream, ks.GetId())
	re.True(initial.GetIsKeyspaceLevelGc())
	re.Zero(initial.GetTxnSafePoint())
	re.Zero(initial.GetGcSafePoint())
	re.Empty(initial.GetGcBarriers())

	advanceWatchGCStatesTxnSafePoint(t, client, header, ks.GetId(), 10)
	live := recvWatchGCStateForKeyspace(t, initialStream, ks.GetId())
	re.True(live.GetIsKeyspaceLevelGc())
	re.Equal(uint64(10), live.GetTxnSafePoint())
	re.Zero(live.GetGcSafePoint())
	re.Empty(live.GetGcBarriers())
	cancelInitial()

	registration := enableWatchGCStatesRegistrationPoint(t)
	skipInitialStream, _ := openWatchGCStates(t, client, header, true)
	registration.wait(t)
	registration.disable(re)

	advanceWatchGCStatesTxnSafePoint(t, client, header, ks.GetId(), 20)
	firstResponse, err := skipInitialStream.Recv()
	re.NoError(err)
	re.NotNil(firstResponse.GetHeader())
	re.Len(firstResponse.GetChanges(), 1)
	firstAfterRegistration := firstResponse.GetChanges()[0].GetUpsert()
	re.NotNil(firstAfterRegistration)
	re.Equal(ks.GetId(), firstAfterRegistration.GetKeyspaceScope().GetKeyspaceId())
	re.True(firstAfterRegistration.GetIsKeyspaceLevelGc())
	re.Equal(uint64(20), firstAfterRegistration.GetTxnSafePoint())
	re.Zero(firstAfterRegistration.GetGcSafePoint())
	re.Empty(firstAfterRegistration.GetGcBarriers())
}

func TestWatchGCStatesUsesIndexOnlyInNextGen(t *testing.T) {
	cluster := newWatchGCStatesCluster(t, 1, true)
	leader := cluster.GetLeaderServer()
	require.NotNil(t, leader)
	const legacyIterator = "github.com/tikv/pd/pkg/gc/iterateAllKeyspacesGCStatesError"
	const legacyError = "legacy keyspace iterator reached"
	require.NoError(t, failpoint.Enable(legacyIterator, `return("legacy keyspace iterator reached")`))
	defer func() { require.NoError(t, failpoint.Disable(legacyIterator)) }()

	client := newWatchGCStatesClient(t, leader.GetAddr())
	stream, _ := openWatchGCStates(t, client, testutil.NewRequestHeader(leader.GetClusterID()), false)
	if kerneltype.IsNextGen() {
		state := recvWatchGCStateForKeyspace(t, stream, constant.NullKeyspaceID)
		require.False(t, state.GetIsKeyspaceLevelGc())
		return
	}
	response, err := stream.Recv()
	require.Nil(t, response)
	require.ErrorContains(t, err, legacyError)
	require.Equal(t, codes.Unavailable, status.Code(err))
}

func TestWatchGCStatesRequestPreflight(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(*testing.T) (string, *pdpb.RequestHeader)
		wantCode codes.Code
	}{
		{
			name: "wrong cluster ID",
			setup: func(t *testing.T) (string, *pdpb.RequestHeader) {
				cluster := newWatchGCStatesCluster(t, 1, true)
				leader := cluster.GetLeaderServer()
				return leader.GetAddr(), testutil.NewRequestHeader(leader.GetClusterID() + 1)
			},
			wantCode: codes.FailedPrecondition,
		},
		{
			name: "direct follower",
			setup: func(t *testing.T) (string, *pdpb.RequestHeader) {
				cluster := newWatchGCStatesCluster(t, 2, true)
				follower := cluster.GetServer(cluster.GetFollower())
				require.NotNil(t, follower)
				return follower.GetAddr(), testutil.NewRequestHeader(follower.GetClusterID())
			},
			wantCode: codes.Unavailable,
		},
		{
			name: "unbootstrapped leader",
			setup: func(t *testing.T) (string, *pdpb.RequestHeader) {
				cluster := newWatchGCStatesCluster(t, 1, false)
				leader := cluster.GetLeaderServer()
				return leader.GetAddr(), testutil.NewRequestHeader(leader.GetClusterID())
			},
			wantCode: codes.Unavailable,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			addr, header := test.setup(t)
			client := newWatchGCStatesClient(t, addr)
			stream, _ := openWatchGCStates(t, client, header, true)
			response, err := stream.Recv()
			require.Nil(t, response)
			require.Equal(t, test.wantCode, status.Code(err))
		})
	}
}

func TestWatchGCStatesSendFailureCleansUpPublicHandler(t *testing.T) {
	re := require.New(t)
	cluster := newWatchGCStatesCluster(t, 1, true)
	leaderServer := cluster.GetLeaderServer()
	re.NotNil(leaderServer)
	pdServer := leaderServer.GetServer()

	limiter := limitWatchGCStatesConcurrency(t, pdServer)

	activeBefore := prometheusMetricValue(t, "pd_gc_watcher_count", nil)
	clientCancelBefore := prometheusMetricValue(t, "pd_gc_watcher_termination_total", map[string]string{"reason": "client_cancel"})
	registration := enableWatchGCStatesRegistrationPoint(t)
	sendErr := errors.New("send failed")
	streamCtx, cancelStream := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelStream()
	stream := &failingWatchGCStatesServer{ctx: streamCtx, sendErr: sendErr}
	handlerDone := make(chan error, 1)
	go func() {
		handlerDone <- (&server.GrpcServer{Server: pdServer}).WatchGCStates(&pdpb.WatchGCStatesRequest{
			Header:             testutil.NewRequestHeader(leaderServer.GetClusterID()),
			SkipLoadingInitial: true,
		}, stream)
	}()

	registration.wait(t)
	registration.disable(re)
	re.Equal(activeBefore+1, prometheusMetricValue(t, "pd_gc_watcher_count", nil))
	limit, current := limiter.GetConcurrencyLimiterStatus("WatchGCStates")
	re.Equal(uint64(1), limit)
	re.Equal(uint64(1), current)

	_, err := pdServer.GetGCStateManager().AdvanceTxnSafePoint(constant.NullKeyspaceID, 10, time.Now())
	re.NoError(err)
	select {
	case err := <-handlerDone:
		re.Same(sendErr, err)
	case <-time.After(5 * time.Second):
		re.FailNow("WatchGCStates handler did not return after the send failure")
	}

	re.Equal(activeBefore, prometheusMetricValue(t, "pd_gc_watcher_count", nil))
	re.Equal(clientCancelBefore+1, prometheusMetricValue(t, "pd_gc_watcher_termination_total", map[string]string{"reason": "client_cancel"}))
	_, current = limiter.GetConcurrencyLimiterStatus("WatchGCStates")
	re.Zero(current)
}

func TestWatchGCStatesHoldsRateLimitTokenForStreamLifetime(t *testing.T) {
	re := require.New(t)
	cluster := newWatchGCStatesCluster(t, 1, true)
	leaderServer := cluster.GetLeaderServer()
	re.NotNil(leaderServer)
	limiter := limitWatchGCStatesConcurrency(t, leaderServer.GetServer())

	client := newWatchGCStatesClient(t, leaderServer.GetAddr())
	header := testutil.NewRequestHeader(leaderServer.GetClusterID())
	firstRegistration := enableWatchGCStatesRegistrationPoint(t)
	_, cancelFirst := openWatchGCStates(t, client, header, true)
	firstRegistration.wait(t)
	firstRegistration.disable(re)
	limit, current := limiter.GetConcurrencyLimiterStatus("WatchGCStates")
	re.Equal(uint64(1), limit)
	re.Equal(uint64(1), current)

	secondStream, _ := openWatchGCStates(t, client, header, true)
	response, err := secondStream.Recv()
	re.Nil(response)
	re.Equal(codes.ResourceExhausted, status.Code(err))

	cancelFirst()
	testutil.Eventually(re, func() bool {
		_, current := limiter.GetConcurrencyLimiterStatus("WatchGCStates")
		return current == 0
	}, testutil.WithWaitFor(5*time.Second), testutil.WithTickInterval(10*time.Millisecond))

	thirdRegistration := enableWatchGCStatesRegistrationPoint(t)
	thirdStream, _ := openWatchGCStates(t, client, header, true)
	thirdRegistration.wait(t)
	thirdRegistration.disable(re)
	advanceWatchGCStatesTxnSafePoint(t, client, header, constant.NullKeyspaceID, 10)
	state := recvWatchGCStateForKeyspace(t, thirdStream, constant.NullKeyspaceID)
	re.Equal(uint64(10), state.GetTxnSafePoint())
}

func TestWatchGCStatesTerminatesOnLeaderTransferAndReinitializes(t *testing.T) {
	re := require.New(t)
	cluster, req, cleanup := newGCStateLeaderTransitionCluster(t)
	t.Cleanup(cleanup)

	oldLeader := cluster.GetLeader()
	re.NotEmpty(oldLeader)
	oldLeaderServer := cluster.GetServer(oldLeader)
	re.NotNil(oldLeaderServer)
	oldClient := newWatchGCStatesClient(t, oldLeaderServer.GetAddr())
	oldStream, _ := openWatchGCStates(t, oldClient, req.GetHeader(), false)
	initial := recvWatchGCStateForKeyspace(t, oldStream, constant.NullKeyspaceID)
	re.False(initial.GetIsKeyspaceLevelGc())
	re.Zero(initial.GetTxnSafePoint())
	re.Zero(initial.GetGcSafePoint())
	re.Empty(initial.GetGcBarriers())

	re.NoError(oldLeaderServer.ResignLeaderWithRetry())
	newLeader := cluster.WaitLeader()
	re.NotEmpty(newLeader)
	re.NotEqual(oldLeader, newLeader)
	for {
		response, err := oldStream.Recv()
		if err != nil {
			re.Nil(response)
			re.Equal(codes.Unavailable, status.Code(err))
			break
		}
		re.NotNil(response)
	}

	newLeaderServer := cluster.GetServer(newLeader)
	re.NotNil(newLeaderServer)
	newClient := newWatchGCStatesClient(t, newLeaderServer.GetAddr())
	advanceWatchGCStatesTxnSafePoint(t, newClient, req.GetHeader(), constant.NullKeyspaceID, 10)
	newStream, _ := openWatchGCStates(t, newClient, req.GetHeader(), false)
	reinitialized := recvWatchGCStateForKeyspace(t, newStream, constant.NullKeyspaceID)
	re.False(reinitialized.GetIsKeyspaceLevelGc())
	re.Equal(uint64(10), reinitialized.GetTxnSafePoint())
	re.Zero(reinitialized.GetGcSafePoint())
	re.Empty(reinitialized.GetGcBarriers())
}

func limitWatchGCStatesConcurrency(t *testing.T, pdServer *server.Server) *ratelimit.Controller {
	t.Helper()
	options := pdServer.GetServiceMiddlewarePersistOptions()
	previousConfig := options.GetGRPCRateLimitConfig().Clone()
	enabledConfig := previousConfig.Clone()
	enabledConfig.EnableRateLimit = true
	options.SetGRPCRateLimitConfig(enabledConfig)
	limiter := pdServer.GetGRPCRateLimiter()
	limiter.Update("WatchGCStates", ratelimit.UpdateConcurrencyLimiter(1))
	t.Cleanup(func() {
		limiter.Update("WatchGCStates", ratelimit.UpdateConcurrencyLimiter(0))
		options.SetGRPCRateLimitConfig(previousConfig)
	})
	return limiter
}

type blockedWatchGCStatesServer struct {
	failingWatchGCStatesServer
	sendStarted chan struct{}
	sendExited  chan struct{}
}

func (s *blockedWatchGCStatesServer) Send(*pdpb.WatchGCStatesResponse) error {
	close(s.sendStarted)
	defer close(s.sendExited)
	<-s.ctx.Done()
	return s.ctx.Err()
}

func waitWatchGCStatesSignal(t *testing.T, signal <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		require.FailNow(t, message)
	}
}

func TestWatchGCStatesBlockedSendCleansUpPublicHandler(t *testing.T) {
	for _, tc := range []struct {
		name   string
		reason string
		code   codes.Code
	}{
		{"leader loss", "leader_lost", codes.Unavailable},
		{"slow consumer", "slow_consumer", codes.ResourceExhausted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			cluster := newWatchGCStatesCluster(t, 1, true)
			leaderServer := cluster.GetLeaderServer()
			re.NotNil(leaderServer)
			pdServer := leaderServer.GetServer()
			manager := pdServer.GetGCStateManager()
			limiter := limitWatchGCStatesConcurrency(t, pdServer)
			activeBefore := prometheusMetricValue(t, "pd_gc_watcher_count", nil)
			labels := map[string]string{"reason": tc.reason}
			terminatedBefore := prometheusMetricValue(t, "pd_gc_watcher_termination_total", labels)
			registration := enableWatchGCStatesRegistrationPoint(t)
			streamCtx, cancelStream := context.WithTimeout(context.Background(), 30*time.Second)
			stream := &blockedWatchGCStatesServer{
				failingWatchGCStatesServer: failingWatchGCStatesServer{ctx: streamCtx},
				sendStarted:                make(chan struct{}), sendExited: make(chan struct{}),
			}
			handlerDone := make(chan struct{})
			var handlerErr error
			t.Cleanup(func() {
				cancelStream()
				waitWatchGCStatesSignal(t, handlerDone, "public handler did not clean up")
				select {
				case <-stream.sendStarted:
					waitWatchGCStatesSignal(t, stream.sendExited, "send did not clean up")
				default:
				}
			})
			go func() {
				defer close(handlerDone)
				handlerErr = (&server.GrpcServer{Server: pdServer}).WatchGCStates(&pdpb.WatchGCStatesRequest{
					Header: testutil.NewRequestHeader(leaderServer.GetClusterID()), SkipLoadingInitial: true,
				}, stream)
			}()
			registration.wait(t)
			registration.disable(re)
			re.Equal(activeBefore+1, prometheusMetricValue(t, "pd_gc_watcher_count", nil))
			_, current := limiter.GetConcurrencyLimiterStatus("WatchGCStates")
			re.Equal(uint64(1), current)
			res, err := manager.AdvanceTxnSafePoint(constant.NullKeyspaceID, 10, time.Now())
			re.NoError(err)
			re.Equal(uint64(10), res.NewTxnSafePoint)
			waitWatchGCStatesSignal(t, stream.sendStarted, "send did not start")
			if tc.reason == "leader_lost" {
				stop := manager.OnNodeBecomesLeader()
				t.Cleanup(stop)
			} else {
				// The sending worker cannot receive these 1025 updates, overflowing
				// the default live queue's 1024 slots.
				for target := uint64(11); target <= 1035; target++ {
					res, err := manager.AdvanceTxnSafePoint(constant.NullKeyspaceID, target, time.Now())
					re.NoError(err)
					re.Equal(target, res.NewTxnSafePoint)
				}
			}
			waitWatchGCStatesSignal(t, handlerDone, "public handler did not return while send was blocked")
			re.Equal(tc.code, status.Code(handlerErr))
			re.NoError(streamCtx.Err())
			select {
			case <-stream.sendExited:
				re.FailNow("send exited before transport teardown")
			default:
			}
			re.Equal(activeBefore, prometheusMetricValue(t, "pd_gc_watcher_count", nil))
			re.Equal(terminatedBefore+1, prometheusMetricValue(t, "pd_gc_watcher_termination_total", labels))
			_, current = limiter.GetConcurrencyLimiterStatus("WatchGCStates")
			re.Zero(current)

			// Admission while the old send is still blocked proves the public
			// handler released its token independently of transport progress.
			nextRegistration := enableWatchGCStatesRegistrationPoint(t)
			client := newWatchGCStatesClient(t, leaderServer.GetAddr())
			nextStream, cancelNext := openWatchGCStates(t, client, testutil.NewRequestHeader(leaderServer.GetClusterID()), true)
			nextRegistration.wait(t)
			nextRegistration.disable(re)
			_, current = limiter.GetConcurrencyLimiterStatus("WatchGCStates")
			re.Equal(uint64(1), current)
			cancelStream()
			waitWatchGCStatesSignal(t, stream.sendExited, "send did not exit after transport teardown")
			cancelNext()
			_, err = nextStream.Recv()
			re.Equal(codes.Canceled, status.Code(err))
			testutil.Eventually(re, func() bool {
				_, current := limiter.GetConcurrencyLimiterStatus("WatchGCStates")
				return current == 0 && prometheusMetricValue(t, "pd_gc_watcher_count", nil) == activeBefore
			})
			re.Equal(terminatedBefore+1, prometheusMetricValue(t, "pd_gc_watcher_termination_total", labels))
		})
	}
}
