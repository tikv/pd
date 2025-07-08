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

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/tso"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
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
	defer func() {
		err = tsoClient.CloseSend()
		re.NoError(err)
	}()

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
	err = leaderServer.BootstrapCluster()
	re.NoError(err)
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
	err = leaderServer.ResignLeader()
	re.NoError(err)
	re.True(nextLeaderServer.WaitLeader())

	ctx = grpcutil.BuildForwardContext(ctx, nextLeaderServer.GetAddr())
	tsoClient, err := grpcPDClient.Tso(ctx)
	re.NoError(err)
	defer func() {
		err = tsoClient.CloseSend()
		re.NoError(err)
	}()
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
	re.GreaterOrEqual(uint32(timestamp.GetLogical()), req.GetCount())
	return timestamp
}

func TestLogicalOverflow(t *testing.T) {
	re := require.New(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Set to max update interval so we can drain the logical part easily later.
	updateInterval := config.MaxTSOUpdatePhysicalInterval
	cluster, err := tests.NewTestCluster(ctx, 1, func(conf *config.Config, _ string) {
		conf.TSOUpdatePhysicalInterval = typeutil.Duration{Duration: updateInterval}
	})
	defer cluster.Destroy()
	re.NoError(err)
	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())

	leaderServer := cluster.GetLeaderServer()
	re.NotNil(leaderServer)
	err = leaderServer.BootstrapCluster()
	re.NoError(err)
	grpcPDClient := testutil.MustNewGrpcClient(re, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()

	tsoClient, err := grpcPDClient.Tso(ctx)
	re.NoError(err)
	defer func() {
		err = tsoClient.CloseSend()
		re.NoError(err)
	}()

	var (
		maxDuration   time.Duration
		lastTimestamp *pdpb.Timestamp
	)
	// Since the max logical count is 2 << 18 (262144), we request 20 times with 26214 count each time.
	// This ensures that the logical part will definitely overflow once within the `updateInterval`.
	count := (1 << 18) / 10
	for range 20 {
		begin := time.Now()
		req := &pdpb.TsoRequest{
			Header:     testutil.NewRequestHeader(clusterID),
			Count:      uint32(count),
			DcLocation: tso.GlobalDCLocation,
		}
		re.NoError(tsoClient.Send(req))
		resp, err := tsoClient.Recv()
		re.NoError(err)
		// Record the max duration to validate whether the overflow is triggered later.
		duration := time.Since(begin)
		if duration > maxDuration {
			maxDuration = duration
		}
		// Check the monotonicity of the timestamp.
		timestamp := checkAndReturnTimestampResponse(re, req, resp)
		re.NotNil(timestamp)
		if lastTimestamp != nil {
			lastPhysical, curPhysical := lastTimestamp.GetPhysical(), timestamp.GetPhysical()
			re.GreaterOrEqual(curPhysical, lastPhysical)
			// If the physical time is the same, the logical time must be strictly increasing.
			if curPhysical == lastPhysical {
				re.Greater(timestamp.GetLogical(), lastTimestamp.GetLogical())
			}
		}
		lastTimestamp = timestamp
	}
	// Due to the overflow triggered, there at least one request duration greater than the `updateInterval`.
	re.Greater(maxDuration, updateInterval)
}
