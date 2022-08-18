// Copyright 2020 TiKV Project Authors.
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

//go:build tso_full_test || tso_function_test
// +build tso_full_test tso_function_test

package tso_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/grpcutil"
	"github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server/tso"
	"github.com/tikv/pd/tests"
)

// There are three kinds of ways to generate a TSO:
// 1. Normal Global TSO, the normal way to get a global TSO from the PD leader,
//    a.k.a the single Global TSO Allocator.
// 2. Normal Local TSO, the new way to get a local TSO may from any of PD servers,
//    a.k.a the Local TSO Allocator leader.
// 3. Synchronized global TSO, the new way to get a global TSO from the PD leader,
//    which will coordinate and synchronize a TSO with other Local TSO Allocator
//    leaders.

func TestConcurrentlyReset(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	defer cluster.Destroy()
	re.NoError(err)

	re.NoError(cluster.RunInitialServers())

	cluster.WaitLeader()
	leader := cluster.GetServer(cluster.GetLeader())
	re.NotNil(leader)

	var wg sync.WaitGroup
	wg.Add(2)
	now := time.Now()
	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()
			for i := 0; i <= 100; i++ {
				physical := now.Add(time.Duration(2*i)*time.Minute).UnixNano() / int64(time.Millisecond)
				ts := uint64(physical << 18)
				leader.GetServer().GetHandler().ResetTS(ts, false, false)
			}
		}()
	}
	wg.Wait()
}

func TestZeroTSOCount(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	defer cluster.Destroy()
	re.NoError(err)
	re.NoError(cluster.RunInitialServers())
	cluster.WaitLeader()

	leaderServer := cluster.GetServer(cluster.GetLeader())
	grpcPDClient := testutil.MustNewGrpcClient(re, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()

	req := &pdpb.TsoRequest{
		Header:     testutil.NewRequestHeader(clusterID),
		DcLocation: tso.GlobalDCLocation,
	}
	tsoClient, err := grpcPDClient.Tso(ctx)
	re.NoError(err)
	defer tsoClient.CloseSend()
	re.NoError(tsoClient.Send(req))
	_, err = tsoClient.Recv()
	re.Error(err)
}

func TestRequestFollower(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 2)
	re.NoError(err)
	defer cluster.Destroy()

	re.NoError(cluster.RunInitialServers())
	cluster.WaitLeader()

	var followerServer *tests.TestServer
	for _, s := range cluster.GetServers() {
		if s.GetConfig().Name != cluster.GetLeader() {
			followerServer = s
		}
	}
	re.NotNil(followerServer)

	grpcPDClient := testutil.MustNewGrpcClient(re, followerServer.GetAddr())
	clusterID := followerServer.GetClusterID()
	req := &pdpb.TsoRequest{
		Header:     testutil.NewRequestHeader(clusterID),
		Count:      1,
		DcLocation: tso.GlobalDCLocation,
	}
	ctx = grpcutil.BuildForwardContext(ctx, followerServer.GetAddr())
	tsoClient, err := grpcPDClient.Tso(ctx)
	re.NoError(err)
	defer tsoClient.CloseSend()

	start := time.Now()
	re.NoError(tsoClient.Send(req))
	_, err = tsoClient.Recv()
	re.Error(err)
	re.Contains(err.Error(), "generate timestamp failed")

	// Requesting follower should fail fast, or the unavailable time will be
	// too long.
	re.Less(time.Since(start), time.Second)
}

// In some cases, when a TSO request arrives, the SyncTimestamp may not finish yet.
// This test is used to simulate this situation and verify that the retry mechanism.
func TestDelaySyncTimestamp(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 2)
	re.NoError(err)
	defer cluster.Destroy()
	re.NoError(cluster.RunInitialServers())
	cluster.WaitLeader()

	var leaderServer, nextLeaderServer *tests.TestServer
	leaderServer = cluster.GetServer(cluster.GetLeader())
	re.NotNil(leaderServer)
	for _, s := range cluster.GetServers() {
		if s.GetConfig().Name != cluster.GetLeader() {
			nextLeaderServer = s
		}
	}
	re.NotNil(nextLeaderServer)

	grpcPDClient := testutil.MustNewGrpcClient(re, nextLeaderServer.GetAddr())
	clusterID := nextLeaderServer.GetClusterID()
	req := &pdpb.TsoRequest{
		Header:     testutil.NewRequestHeader(clusterID),
		Count:      1,
		DcLocation: tso.GlobalDCLocation,
	}

	re.NoError(failpoint.Enable("github.com/tikv/pd/server/tso/delaySyncTimestamp", `return(true)`))

	// Make the old leader resign and wait for the new leader to get a lease
	leaderServer.ResignLeader()
	re.True(nextLeaderServer.WaitLeader())

	ctx = grpcutil.BuildForwardContext(ctx, nextLeaderServer.GetAddr())
	tsoClient, err := grpcPDClient.Tso(ctx)
	re.NoError(err)
	defer tsoClient.CloseSend()
	re.NoError(tsoClient.Send(req))
	resp, err := tsoClient.Recv()
	re.NoError(err)
	re.NotNil(checkAndReturnTimestampResponse(re, req, resp))
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/tso/delaySyncTimestamp"))
}
