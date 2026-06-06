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
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/response"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

type tsoTestSuite struct {
	suite.Suite
	env            *tests.SchedulingTestEnvironment
	updateInterval time.Duration
}

func TestTSOSuite(t *testing.T) {
	suite.Run(t, new(tsoTestSuite))
}

func (s *tsoTestSuite) SetupSuite() {
	// Set to max update interval so we can drain the logical part easily later.
	s.updateInterval = config.MaxTSOUpdatePhysicalInterval
	s.env = tests.NewSchedulingTestEnvironment(s.T(), func(conf *config.Config, _ string) {
		conf.TSOUpdatePhysicalInterval = typeutil.Duration{Duration: s.updateInterval}
		conf.PDServerCfg.UseRegionStorage = false
	})
	s.env.PDCount = 2
}

func (s *tsoTestSuite) TearDownSuite() {
	s.env.Cleanup()
}

func (s *tsoTestSuite) TearDownTest() {
	s.env.Reset(s.Require())
}

func (s *tsoTestSuite) TestRequestFollower() {
	s.env.RunTestInNonMicroserviceEnv(s.checkRequestFollower)
}

func (s *tsoTestSuite) checkRequestFollower(cluster *tests.TestCluster) {
	re := s.Require()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var followerServer *tests.TestServer
	for _, s := range cluster.GetServers() {
		if s.GetConfig().Name != cluster.GetLeader() {
			followerServer = s
		}
	}
	re.NotNil(followerServer)

	grpcPDClient, conn := testutil.MustNewGrpcClient(re, followerServer.GetAddr())
	defer conn.Close()
	clusterID := followerServer.GetClusterID()
	req := &pdpb.TsoRequest{
		Header: testutil.NewRequestHeader(clusterID),
		Count:  1,
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
func (s *tsoTestSuite) TestDelaySyncTimestamp() {
	s.env.RunTestInNonMicroserviceEnv(s.checkDelaySyncTimestamp)
}

func (s *tsoTestSuite) TestServeBeforeRaftClusterLoaded() {
	s.env.RunTestInNonMicroserviceEnv(s.checkServeBeforeRaftClusterLoaded)
}

func (s *tsoTestSuite) checkServeBeforeRaftClusterLoaded(cluster *tests.TestCluster) {
	re := s.Require()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	leaderServer := cluster.GetLeaderServer()
	var nextLeaderServer *tests.TestServer
	for _, s := range cluster.GetServers() {
		if s.GetConfig().Name != cluster.GetLeader() {
			nextLeaderServer = s
		}
	}
	re.NotNil(nextLeaderServer)

	regionID := uint64(10)
	region := tests.MustPutRegion(re, cluster, regionID, 1, []byte("a"), []byte("z"))
	testutil.Eventually(re, func() bool {
		return leaderServer.GetRaftCluster().GetRegion(regionID) != nil
	})
	testutil.Eventually(re, func() bool {
		loadedRegion := &metapb.Region{}
		ok, err := leaderServer.GetServer().GetStorage().LoadRegion(regionID, loadedRegion)
		re.NoError(err)
		return ok && loadedRegion.GetId() == regionID
	})

	regionReadReadyPaused := make(chan struct{})
	resumeAfterRegionReadReady := make(chan struct{})
	var pauseOnce sync.Once
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/delayCreateRaftCluster", `return(1000)`))
	re.NoError(failpoint.EnableCall("github.com/tikv/pd/server/cluster/pauseAfterRegionReadReady", func() {
		pauseOnce.Do(func() {
			close(regionReadReadyPaused)
		})
		<-resumeAfterRegionReadReady
	}))
	defer func() {
		select {
		case <-resumeAfterRegionReadReady:
		default:
			close(resumeAfterRegionReadReady)
		}
		re.NoError(failpoint.Disable("github.com/tikv/pd/server/cluster/pauseAfterRegionReadReady"))
		re.NoError(failpoint.Disable("github.com/tikv/pd/server/delayCreateRaftCluster"))
	}()

	re.NoError(leaderServer.ResignLeaderWithRetry())
	re.True(nextLeaderServer.WaitLeader())
	grpcPDClient, conn := testutil.MustNewGrpcClient(re, nextLeaderServer.GetAddr())
	defer conn.Close()
	req := &pdpb.TsoRequest{
		Header: testutil.NewRequestHeader(nextLeaderServer.GetClusterID()),
		Count:  1,
	}
	rpcCtx, rpcCancel := context.WithTimeout(ctx, 10*time.Second)
	defer rpcCancel()
	tsoClient, err := grpcPDClient.Tso(rpcCtx)
	re.NoError(err)
	defer func() {
		err = tsoClient.CloseSend()
		re.NoError(err)
	}()
	re.NoError(tsoClient.Send(req))
	resp, err := tsoClient.Recv()
	re.NoError(err)
	re.NotNil(checkAndReturnTimestampResponse(re, req, resp))

	httpClient := &http.Client{Timeout: time.Second}
	select {
	case <-regionReadReadyPaused:
	case <-time.After(20 * time.Second):
		re.FailNow("timed out waiting for region read readiness pause")
	}

	rc := nextLeaderServer.GetServer().DirectlyGetRaftCluster()
	re.True(rc.IsRegionReadReady())

	regionRPCContext, regionRPCCancel := context.WithTimeout(ctx, time.Second)
	defer regionRPCCancel()
	regionByIDResp, err := grpcPDClient.GetRegionByID(regionRPCContext, &pdpb.GetRegionByIDRequest{
		Header:      testutil.NewRequestHeader(nextLeaderServer.GetClusterID()),
		RegionId:    regionID,
		NeedBuckets: true,
	})
	re.NoError(err)
	re.Nil(regionByIDResp.GetHeader().GetError())
	re.Equal(regionID, regionByIDResp.GetRegion().GetId())

	scanContext, scanCancel := context.WithTimeout(ctx, time.Second)
	defer scanCancel()
	scanResp, err := grpcPDClient.ScanRegions(scanContext, &pdpb.ScanRegionsRequest{
		Header:   testutil.NewRequestHeader(nextLeaderServer.GetClusterID()),
		StartKey: region.GetStartKey(),
		EndKey:   region.GetEndKey(),
		Limit:    1,
	})
	re.NoError(err)
	re.Nil(scanResp.GetHeader().GetError())
	re.Len(scanResp.GetRegions(), 1)
	re.Equal(regionID, scanResp.GetRegions()[0].GetRegion().GetId())

	var httpRegion response.RegionInfo
	re.NoError(testutil.ReadGetJSON(re, httpClient,
		fmt.Sprintf("%s/pd/api/v1/region/id/%d", nextLeaderServer.GetAddr(), regionID),
		&httpRegion))
	re.Equal(regionID, httpRegion.ID)

	var scannedRegions response.RegionsInfo
	re.NoError(testutil.ReadGetJSON(re, httpClient,
		fmt.Sprintf("%s/pd/api/v1/regions/key?key=a&end_key=z&limit=1", nextLeaderServer.GetAddr()),
		&scannedRegions))
	re.Equal(1, scannedRegions.Count)
	re.Len(scannedRegions.Regions, 1)
	re.Equal(regionID, scannedRegions.Regions[0].ID)

	var regionCount response.RegionsInfo
	re.NoError(testutil.ReadGetJSON(re, httpClient,
		nextLeaderServer.GetAddr()+"/pd/api/v1/regions/count",
		&regionCount))
	re.Equal(rc.GetTotalRegionCount(), regionCount.Count)

	close(resumeAfterRegionReadReady)
	testutil.Eventually(re, func() bool {
		return nextLeaderServer.GetRaftCluster() != nil
	}, testutil.WithWaitFor(20*time.Second), testutil.WithTickInterval(50*time.Millisecond))
}

func (s *tsoTestSuite) checkDelaySyncTimestamp(cluster *tests.TestCluster) {
	re := s.Require()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var leaderServer, nextLeaderServer *tests.TestServer
	leaderServer = cluster.GetLeaderServer()
	for _, s := range cluster.GetServers() {
		if s.GetConfig().Name != cluster.GetLeader() {
			nextLeaderServer = s
		}
	}
	re.NotNil(nextLeaderServer)

	grpcPDClient, conn := testutil.MustNewGrpcClient(re, nextLeaderServer.GetAddr())
	defer conn.Close()
	clusterID := nextLeaderServer.GetClusterID()
	req := &pdpb.TsoRequest{
		Header: testutil.NewRequestHeader(clusterID),
		Count:  1,
	}

	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/delaySyncTimestamp", `return(true)`))

	// Make the old leader resign and wait for the new leader to get a lease
	err := leaderServer.ResignLeaderWithRetry()
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

func (s *tsoTestSuite) TestLogicalOverflow() {
	s.env.RunTestInNonMicroserviceEnv(s.checkLogicalOverflow)
}

func (s *tsoTestSuite) checkLogicalOverflow(cluster *tests.TestCluster) {
	re := s.Require()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	leaderServer := cluster.GetLeaderServer()
	re.NotNil(leaderServer)
	grpcPDClient, conn := testutil.MustNewGrpcClient(re, leaderServer.GetAddr())
	defer conn.Close()
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
			Header: testutil.NewRequestHeader(clusterID),
			Count:  uint32(count),
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
	re.Greater(maxDuration, s.updateInterval)
}
