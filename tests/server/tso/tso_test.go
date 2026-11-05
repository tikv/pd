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
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/tsoutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(testutil.WaitForEtcdConnections(m), testutil.LeakOptions...)
}

type tsoTestSuite struct {
	suite.Suite
	env *tests.SchedulingTestEnvironment
}

func TestTSOSuite(t *testing.T) {
	suite.Run(t, new(tsoTestSuite))
}

func (s *tsoTestSuite) SetupSuite() {
	// Keep periodic updates infrequent while testing consecutive requests at the logical boundary.
	s.env = tests.NewSchedulingTestEnvironment(s.T(), func(conf *config.Config, _ string) {
		conf.TSOUpdatePhysicalInterval = typeutil.Duration{Duration: config.MaxTSOUpdatePhysicalInterval}
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
	// Connect directly without forwarding metadata to verify the original
	// follower request behavior.
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

func (s *tsoTestSuite) TestForwardedInvalidCountIsolation() {
	s.env.RunTestInNonMicroserviceEnv(func(cluster *tests.TestCluster) {
		re := s.Require()
		leader := cluster.GetLeaderServer()
		var follower *tests.TestServer
		for name, server := range cluster.GetServers() {
			if name != cluster.GetLeader() {
				follower = server
				break
			}
		}
		re.NotNil(follower)
		client, conn := testutil.MustNewGrpcClient(re, follower.GetAddr())
		defer conn.Close()
		ctx, cancel := context.WithTimeout(s.T().Context(), 10*time.Second)
		defer cancel()
		ctx = grpcutil.BuildForwardContext(ctx, leader.GetAddr())
		valid, err := client.Tso(ctx)
		re.NoError(err)
		defer func() { re.NoError(valid.CloseSend()) }()
		request := &pdpb.TsoRequest{Header: testutil.NewRequestHeader(leader.GetClusterID()), Count: 1}
		re.NoError(valid.Send(request))
		previous, err := valid.Recv()
		re.NoError(err)
		checkAndReturnTimestampResponse(re, request, previous)

		for _, count := range []uint32{0, 1 << 18, math.MaxUint32} {
			invalidCtx, invalidCancel := context.WithCancel(ctx)
			invalid, err := client.Tso(invalidCtx)
			re.NoError(err)
			re.NoError(invalid.Send(&pdpb.TsoRequest{Header: request.Header, Count: count}))
			_, err = invalid.Recv()
			invalidCancel()
			re.ErrorContains(err, "tso count")

			// Keep using the same valid stream after the other stream is rejected.
			re.NoError(valid.Send(request))
			response, err := valid.Recv()
			re.NoError(err)
			checkAndReturnTimestampResponse(re, request, response)
			re.Greater(tsoutil.GenerateTS(response.GetTimestamp()), tsoutil.GenerateTS(previous.GetTimestamp()))
			previous = response
		}
	})
}

func (s *tsoTestSuite) TestInvalidCount() {
	s.env.RunTest(func(cluster *tests.TestCluster) {
		re := s.Require()
		leader := cluster.GetLeaderServer()
		if leader.GetServer().IsServiceIndependent(constant.TSOServiceName) {
			tsoCluster, err := tests.NewTestTSOCluster(s.T().Context(), 1, leader.GetAddr())
			re.NoError(err)
			defer tsoCluster.Destroy()
			tsoCluster.WaitForDefaultPrimaryServing(re)
			testutil.Eventually(re, func() bool {
				_, ready := leader.GetServer().GetServicePrimaryAddr(s.T().Context(), constant.TSOServiceName)
				return ready
			})
		}
		var invalidAllocations atomic.Int64
		const beforeGenerate = "github.com/tikv/pd/pkg/tso/beforeGenerateTSO"
		re.NoError(failpoint.EnableCall(beforeGenerate, func(_ int64, _ *int64, count int64) {
			if count >= 1<<18 {
				invalidAllocations.Add(1)
			}
		}))
		defer func() { re.NoError(failpoint.Disable(beforeGenerate)) }()
		client, conn := testutil.MustNewGrpcClient(re, leader.GetAddr())
		defer conn.Close()
		for _, count := range []uint32{0, 1 << 18, 1<<18 + 1, math.MaxUint32} {
			ctx, cancel := context.WithTimeout(s.T().Context(), 10*time.Second)
			stream, err := client.Tso(ctx)
			re.NoError(err)
			re.NoError(stream.Send(&pdpb.TsoRequest{Header: testutil.NewRequestHeader(leader.GetClusterID()), Count: count}))
			_, err = stream.Recv()
			re.Error(err)
			re.Contains(err.Error(), "tso count")
			cancel()
		}
		re.Zero(invalidAllocations.Load())
		stream, err := client.Tso(s.T().Context())
		re.NoError(err)
		re.NoError(stream.Send(&pdpb.TsoRequest{Header: testutil.NewRequestHeader(leader.GetClusterID()), Count: 1}))
		resp, err := stream.Recv()
		re.NoError(err)
		re.Equal(uint32(1), resp.GetCount())
		re.Positive(resp.GetTimestamp().GetLogical())
		re.NoError(stream.CloseSend())
	})
}

func (s *tsoTestSuite) checkLogicalOverflow(cluster *tests.TestCluster) {
	re := s.Require()
	const (
		maxLogical = 1 << 18
		count      = maxLogical / 10
	)
	var (
		overflowPhysical atomic.Int64
		overflowed       atomic.Bool
	)
	// Exhaust logical capacity on the first matching request while holding the
	// allocation lock, so a periodic update cannot clear it before allocation.
	const beforeGenerateTSOFailpoint = "github.com/tikv/pd/pkg/tso/beforeGenerateTSO"
	re.NoError(failpoint.EnableCall(beforeGenerateTSOFailpoint, func(physical int64, logical *int64, batchCount int64) {
		if batchCount == count && overflowPhysical.CompareAndSwap(0, physical) {
			*logical = maxLogical - 1
		}
	}))
	defer func() {
		re.NoError(failpoint.Disable(beforeGenerateTSOFailpoint))
	}()
	const onLogicalOverflowFailpoint = "github.com/tikv/pd/pkg/tso/onLogicalOverflow"
	re.NoError(failpoint.EnableCall(onLogicalOverflowFailpoint, func(batchCount uint32) {
		if batchCount == count {
			overflowed.Store(true)
		}
	}))
	defer func() {
		re.NoError(failpoint.Disable(onLogicalOverflowFailpoint))
	}()

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
		firstPhysical int64
		lastTimestamp *pdpb.Timestamp
	)
	// Keep allocating after the forced overflow to check recovery and monotonicity.
	for range 20 {
		req := &pdpb.TsoRequest{
			Header: testutil.NewRequestHeader(clusterID),
			Count:  count,
		}
		re.NoError(tsoClient.Send(req))
		resp, err := tsoClient.Recv()
		re.NoError(err)
		timestamp := checkAndReturnTimestampResponse(re, req, resp)
		re.NotNil(timestamp)
		re.Less(timestamp.GetLogical(), int64(maxLogical))
		if lastTimestamp == nil {
			firstPhysical = timestamp.GetPhysical()
		} else {
			lastPhysical, curPhysical := lastTimestamp.GetPhysical(), timestamp.GetPhysical()
			re.GreaterOrEqual(curPhysical, lastPhysical)
			// If the physical time is the same, the logical time must be strictly increasing.
			if curPhysical == lastPhysical {
				re.Greater(timestamp.GetLogical(), lastTimestamp.GetLogical())
			}
		}
		lastTimestamp = timestamp
	}
	re.Positive(overflowPhysical.Load(), "the logical capacity must be exhausted")
	re.True(overflowed.Load(), "the logical overflow branch must be reached")
	re.Greater(firstPhysical, overflowPhysical.Load())
}
