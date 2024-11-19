// Copyright 2024 TiKV Project Authors.
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

package tso_test

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/tso"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func TestRequestFollower(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 2)
	re.NoError(err)
	defer cluster.Destroy()

	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())

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
	re.NotEmpty(cluster.WaitLeader())

	var leaderServer, nextLeaderServer *tests.TestServer
	leaderServer = cluster.GetLeaderServer()
	re.NotNil(leaderServer)
	leaderServer.BootstrapCluster()
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

	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/delaySyncTimestamp", `return(true)`))

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
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/tso/delaySyncTimestamp"))
}

func checkAndReturnTimestampResponse(re *require.Assertions, req *pdpb.TsoRequest, resp *pdpb.TsoResponse) *pdpb.Timestamp {
	re.Equal(req.GetCount(), resp.GetCount())
	timestamp := resp.GetTimestamp()
	re.Positive(timestamp.GetPhysical())
	re.GreaterOrEqual(uint32(timestamp.GetLogical())>>timestamp.GetSuffixBits(), req.GetCount())
	return timestamp
}
func TestLogicalOverflow(t *testing.T) {
	re := require.New(t)

	runCase := func(updateInterval time.Duration) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		cluster, err := tests.NewTestCluster(ctx, 1, func(conf *config.Config, _ string) {
			conf.TSOUpdatePhysicalInterval = typeutil.Duration{Duration: updateInterval}
		})
		defer cluster.Destroy()
		re.NoError(err)
		re.NoError(cluster.RunInitialServers())
		re.NotEmpty(cluster.WaitLeader())

		leaderServer := cluster.GetLeaderServer()
		re.NotNil(leaderServer)
		leaderServer.BootstrapCluster()
		grpcPDClient := testutil.MustNewGrpcClient(re, leaderServer.GetAddr())
		clusterID := leaderServer.GetClusterID()

		tsoClient, err := grpcPDClient.Tso(ctx)
		re.NoError(err)
		defer tsoClient.CloseSend()

		begin := time.Now()
		for i := range 3 {
			req := &pdpb.TsoRequest{
				Header:     testutil.NewRequestHeader(clusterID),
				Count:      150000,
				DcLocation: tso.GlobalDCLocation,
			}
			re.NoError(tsoClient.Send(req))
			_, err = tsoClient.Recv()
			re.NoError(err)
			if i == 1 {
				// the 2nd request may (but not must) overflow, as max logical interval is 262144
				re.Less(time.Since(begin), updateInterval+50*time.Millisecond) // additional 50ms for gRPC latency
			}
		}
		// the 3rd request must overflow
		re.GreaterOrEqual(time.Since(begin), updateInterval)
	}

	for _, updateInterval := range []int{1, 5, 30, 50} {
		runCase(time.Duration(updateInterval) * time.Millisecond)
	}
}
