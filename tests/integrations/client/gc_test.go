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

package client_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/pdpb"

	pd "github.com/tikv/pd/client"
	clientgc "github.com/tikv/pd/client/clients/gc"
	"github.com/tikv/pd/client/pkg/caller"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

func TestServiceSafePointV2Operations(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()
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
	header := testutil.NewRequestHeader(leaderServer.GetClusterID())

	advanceTxnSafePointReq := &pdpb.AdvanceTxnSafePointRequest{
		Header:        header,
		KeyspaceScope: &pdpb.KeyspaceScope{Keyspace: &pdpb.KeyspaceScope_KeyspaceId{KeyspaceId: ks1.GetId()}},
		Target:        10,
	}
	advanceTxnSafePointResp, err := grpcPDClient.AdvanceTxnSafePoint(ctx, advanceTxnSafePointReq)
	re.NoError(err)
	re.NotNil(advanceTxnSafePointResp.Header)
	re.Nil(advanceTxnSafePointResp.Header.Error)
	re.Equal(uint64(10), advanceTxnSafePointResp.GetNewTxnSafePoint())

	cli, err := pd.NewClientWithContext(ctx, caller.TestComponent, []string{leaderServer.GetAddr()}, pd.SecurityOption{})
	re.NoError(err)
	defer cli.Close()
	legacyClientV2, ok := cli.(clientgc.LegacyClientV2)
	re.True(ok)
	gcStatesClient := cli.GetGCStatesClient(ks1.GetId())
	getGCBarriers := func() []*clientgc.GCBarrierInfo {
		gcState, err := gcStatesClient.GetGCState(ctx, clientgc.ExcludeGCBarriers(false))
		re.NoError(err)
		re.Equal(ks1.GetId(), gcState.KeyspaceID)
		gcBarriers, err := gcState.GetGCBarriers()
		re.NoError(err)
		return gcBarriers
	}

	minSafePoint, err := legacyClientV2.GetMinServiceSafePointV2(ctx, ks1.GetId())
	re.NoError(err)
	re.Equal(uint64(10), minSafePoint)
	re.Empty(getGCBarriers())

	for _, ttl := range []int64{0, -1} {
		minSafePoint, err = legacyClientV2.SetServiceSafePointV2(ctx, ks1.GetId(), "v2-service-safe-point", ttl, 20)
		re.Error(err)
		re.ErrorContains(err, "invalid ttl")
		re.Equal(uint64(0), minSafePoint)
	}
	re.Empty(getGCBarriers())

	minSafePoint, err = legacyClientV2.SetServiceSafePointV2(ctx, ks1.GetId(), "_reserved_get_min_ssp", 3600, 20)
	re.Error(err)
	re.ErrorContains(err, "reserved for GetMinServiceSafePointV2")
	re.Equal(uint64(0), minSafePoint)
	re.Empty(getGCBarriers())

	// The safePoint < minSafePoint is rejected, and the returned minSafePoint is still 10.
	minSafePoint, err = legacyClientV2.SetServiceSafePointV2(ctx, ks1.GetId(), "v2-service-safe-point", 3600, 5)
	re.NoError(err)
	re.Equal(uint64(10), minSafePoint)
	gcBarriers := getGCBarriers()
	re.Empty(gcBarriers)

	minSafePoint, err = legacyClientV2.SetServiceSafePointV2(ctx, ks1.GetId(), "v2-service-safe-point", 3600, 20)
	re.NoError(err)
	re.Equal(uint64(10), minSafePoint)
	gcBarriers = getGCBarriers()
	re.Len(gcBarriers, 1)
	re.Equal("v2-service-safe-point", gcBarriers[0].BarrierID)
	re.Equal(uint64(20), gcBarriers[0].BarrierTS)
	re.Greater(gcBarriers[0].TTL, 3595*time.Second)
	re.LessOrEqual(gcBarriers[0].TTL, 3600*time.Second)

	minSafePoint, err = legacyClientV2.DeleteServiceSafePointV2(ctx, ks1.GetId(), "v2-service-safe-point")
	re.NoError(err)
	re.Equal(uint64(10), minSafePoint)
	re.Empty(getGCBarriers())
}
