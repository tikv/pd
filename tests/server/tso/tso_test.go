// Copyright 2021 TiKV Project Authors.
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
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"
<<<<<<< HEAD
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/tso"
=======

>>>>>>> 7fa31c663 (metrics: clean up metrics (#9535))
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

func TestLoadTimestamp(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dcLocationConfig := map[string]string{
		"pd1": "dc-1",
		"pd2": "dc-2",
		"pd3": "dc-3",
	}
	dcLocationNum := len(dcLocationConfig)
	cluster, err := tests.NewTestCluster(ctx, dcLocationNum, func(conf *config.Config, serverName string) {
		conf.EnableLocalTSO = true
		conf.Labels[config.ZoneLabel] = dcLocationConfig[serverName]
	})
<<<<<<< HEAD
	defer cluster.Destroy()
=======
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

	grpcPDClient := testutil.MustNewGrpcClient(re, followerServer.GetAddr())
	clusterID := followerServer.GetClusterID()
	req := &pdpb.TsoRequest{
		Header: testutil.NewRequestHeader(clusterID),
		Count:  1,
	}
	ctx = grpcutil.BuildForwardContext(ctx, followerServer.GetAddr())
	tsoClient, err := grpcPDClient.Tso(ctx)
>>>>>>> 7fa31c663 (metrics: clean up metrics (#9535))
	re.NoError(err)
	re.NoError(cluster.RunInitialServers())

	cluster.WaitAllLeaders(re, dcLocationConfig)

	lastTSMap := requestLocalTSOs(re, cluster, dcLocationConfig)

	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/systemTimeSlow", `return(true)`))

	// Reboot the cluster.
	re.NoError(cluster.StopAll())
	re.NoError(cluster.RunInitialServers())

	cluster.WaitAllLeaders(re, dcLocationConfig)

	// Re-request the Local TSOs.
	newTSMap := requestLocalTSOs(re, cluster, dcLocationConfig)
	for dcLocation, newTS := range newTSMap {
		lastTS, ok := lastTSMap[dcLocation]
		re.True(ok)
		// The new physical time of TSO should be larger even if the system time is slow.
		re.Greater(newTS.GetPhysical()-lastTS.GetPhysical(), int64(0))
	}

	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/tso/systemTimeSlow"))
}

func requestLocalTSOs(re *require.Assertions, cluster *tests.TestCluster, dcLocationConfig map[string]string) map[string]*pdpb.Timestamp {
	dcClientMap := make(map[string]pdpb.PDClient)
	tsMap := make(map[string]*pdpb.Timestamp)
	leaderServer := cluster.GetLeaderServer()
	for _, dcLocation := range dcLocationConfig {
		pdName := leaderServer.GetAllocatorLeader(dcLocation).GetName()
		dcClientMap[dcLocation] = testutil.MustNewGrpcClient(re, cluster.GetServer(pdName).GetAddr())
	}
	for _, dcLocation := range dcLocationConfig {
		req := &pdpb.TsoRequest{
			Header:     testutil.NewRequestHeader(leaderServer.GetClusterID()),
			Count:      tsoCount,
			DcLocation: dcLocation,
		}
		ctx, cancel := context.WithCancel(context.Background())
		ctx = grpcutil.BuildForwardContext(ctx, cluster.GetServer(leaderServer.GetAllocatorLeader(dcLocation).GetName()).GetAddr())
		tsMap[dcLocation] = testGetTimestamp(re, ctx, dcClientMap[dcLocation], req)
		cancel()
	}
	return tsMap
}

func TestDisableLocalTSOAfterEnabling(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dcLocationConfig := map[string]string{
		"pd1": "dc-1",
		"pd2": "dc-2",
		"pd3": "dc-3",
	}
	dcLocationNum := len(dcLocationConfig)
	cluster, err := tests.NewTestCluster(ctx, dcLocationNum, func(conf *config.Config, serverName string) {
		conf.EnableLocalTSO = true
		conf.Labels[config.ZoneLabel] = dcLocationConfig[serverName]
	})
	defer cluster.Destroy()
	re.NoError(err)
	re.NoError(cluster.RunInitialServers())

	cluster.WaitAllLeaders(re, dcLocationConfig)
	leaderServer := cluster.GetLeaderServer()
	leaderServer.BootstrapCluster()
	requestLocalTSOs(re, cluster, dcLocationConfig)

	// Reboot the cluster.
	re.NoError(cluster.StopAll())
	for _, server := range cluster.GetServers() {
		server.SetEnableLocalTSO(false)
	}
	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())

	// Re-request the global TSOs.
	leaderServer = cluster.GetLeaderServer()
	grpcPDClient := testutil.MustNewGrpcClient(re, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()
	req := &pdpb.TsoRequest{
		Header: testutil.NewRequestHeader(clusterID),
		Count:  1,
	}

	ctx = grpcutil.BuildForwardContext(ctx, leaderServer.GetAddr())
	tsoClient, err := grpcPDClient.Tso(ctx)
	re.NoError(err)
	defer tsoClient.CloseSend()
	re.NoError(tsoClient.Send(req))
	resp, err := tsoClient.Recv()
	re.NoError(err)
	re.NotNil(checkAndReturnTimestampResponse(re, req, resp))
	// Test whether the number of existing DCs is as expected.
	dcLocations, err := leaderServer.GetTSOAllocatorManager().GetClusterDCLocationsFromEtcd()
	re.NoError(err)
<<<<<<< HEAD
	re.Equal(0, len(dcLocations))
=======
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
>>>>>>> 7fa31c663 (metrics: clean up metrics (#9535))
}
