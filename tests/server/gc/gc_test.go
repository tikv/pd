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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

// In tests in this file, we only verify that the parameters and results are properly passed. The detailed behavior
// of these APIs are covered elsewhere.

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
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

	grpcPDClient := testutil.MustNewGrpcClient(re, leaderServer.GetAddr())
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
	// The default keyspace (keyspaceID == 0) will be included, so it has 3.
	re.Len(resp.GetGcStates(), 3)
	receivedKeyspaceIDs := make([]uint32, 0, 2)
	for _, gcState := range resp.GetGcStates() {
		receivedKeyspaceIDs = append(receivedKeyspaceIDs, gcState.KeyspaceScope.KeyspaceId)
	}
	slices.Sort(receivedKeyspaceIDs)
	re.Equal([]uint32{0, ks1.Id, constant.NullKeyspaceID}, receivedKeyspaceIDs)
	// As the same test logic was run on the two keyspaces, they should have the same result.
	for _, gcState := range resp.GetGcStates() {
		// Ignore the default keyspace (keyspaceID == 0) which is not used in this test.
		if gcState.KeyspaceScope.KeyspaceId == 0 {
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
}
