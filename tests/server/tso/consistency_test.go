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

//go:build tso_full_test || tso_consistency_test
// +build tso_full_test tso_consistency_test

package tso_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/tso"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/tsoutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

type tsoConsistencyTestSuite struct {
	suite.Suite
	ctx    context.Context
	cancel context.CancelFunc

	leaderServer *tests.TestServer
	dcClientMap  map[string]pdpb.PDClient

	tsPoolMutex syncutil.Mutex
	tsPool      map[uint64]struct{}
}

func TestTSOConsistencyTestSuite(t *testing.T) {
	suite.Run(t, new(tsoConsistencyTestSuite))
}

func (suite *tsoConsistencyTestSuite) SetupSuite() {
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.dcClientMap = make(map[string]pdpb.PDClient)
	suite.tsPool = make(map[uint64]struct{})
}

func (suite *tsoConsistencyTestSuite) TearDownSuite() {
	suite.cancel()
}

// TestSynchronizedGlobalTSO is used to test the synchronized way of global TSO generation.
func (suite *tsoConsistencyTestSuite) TestSynchronizedGlobalTSO() {
	re := suite.Require()
	dcLocationConfig := map[string]string{
		"pd1": "dc-1",
		"pd2": "dc-2",
		"pd3": "dc-3",
	}
	dcLocationNum := len(dcLocationConfig)
	cluster, err := tests.NewTestCluster(suite.ctx, dcLocationNum, func(conf *config.Config, serverName string) {
		conf.EnableLocalTSO = true
		conf.Labels[config.ZoneLabel] = dcLocationConfig[serverName]
	})
	defer cluster.Destroy()
	re.NoError(err)
	re.NoError(cluster.RunInitialServers())

	cluster.WaitAllLeaders(re, dcLocationConfig)

	suite.leaderServer = cluster.GetLeaderServer()
	re.NotNil(suite.leaderServer)
	suite.dcClientMap[tso.GlobalDCLocation] = testutil.MustNewGrpcClient(re, suite.leaderServer.GetAddr())
	for _, dcLocation := range dcLocationConfig {
		pdName := suite.leaderServer.GetAllocatorLeader(dcLocation).GetName()
		suite.dcClientMap[dcLocation] = testutil.MustNewGrpcClient(re, cluster.GetServer(pdName).GetAddr())
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	maxGlobalTSO := &pdpb.Timestamp{}
	for range tsoRequestRound {
		// Get some local TSOs first
		oldLocalTSOs := make([]*pdpb.Timestamp, 0, dcLocationNum)
		for _, dcLocation := range dcLocationConfig {
			localTSO := suite.getTimestampByDC(ctx, re, cluster, dcLocation)
			oldLocalTSOs = append(oldLocalTSOs, localTSO)
			re.Equal(-1, tsoutil.CompareTimestamp(maxGlobalTSO, localTSO))
		}
		// Get a global TSO then
		globalTSO := suite.getTimestampByDC(ctx, re, cluster, tso.GlobalDCLocation)
		for _, oldLocalTSO := range oldLocalTSOs {
			re.Equal(1, tsoutil.CompareTimestamp(globalTSO, oldLocalTSO))
		}
		if tsoutil.CompareTimestamp(maxGlobalTSO, globalTSO) < 0 {
			maxGlobalTSO = globalTSO
		}
		// Get some local TSOs again
		newLocalTSOs := make([]*pdpb.Timestamp, 0, dcLocationNum)
		for _, dcLocation := range dcLocationConfig {
			newLocalTSOs = append(newLocalTSOs, suite.getTimestampByDC(ctx, re, cluster, dcLocation))
		}
		for _, newLocalTSO := range newLocalTSOs {
			re.Equal(-1, tsoutil.CompareTimestamp(maxGlobalTSO, newLocalTSO))
		}
	}
}

func (suite *tsoConsistencyTestSuite) getTimestampByDC(
	ctx context.Context,
	re *require.Assertions,
	cluster *tests.TestCluster,
	dcLocation string,
) *pdpb.Timestamp {
	req := &pdpb.TsoRequest{
		Header:     testutil.NewRequestHeader(suite.leaderServer.GetClusterID()),
		Count:      tsoCount,
		DcLocation: dcLocation,
	}
	pdClient, ok := suite.dcClientMap[dcLocation]
	re.True(ok)
	forwardedHost := cluster.GetServer(suite.leaderServer.GetAllocatorLeader(dcLocation).GetName()).GetAddr()
	ctx = grpcutil.BuildForwardContext(ctx, forwardedHost)
	tsoClient, err := pdClient.Tso(ctx)
	re.NoError(err)
	defer tsoClient.CloseSend()
	re.NoError(tsoClient.Send(req))
	resp, err := tsoClient.Recv()
	re.NoError(err)
	return checkAndReturnTimestampResponse(re, req, resp)
}

func (suite *tsoConsistencyTestSuite) TestSynchronizedGlobalTSOOverflow() {
	re := suite.Require()
	dcLocationConfig := map[string]string{
		"pd1": "dc-1",
		"pd2": "dc-2",
		"pd3": "dc-3",
	}
	dcLocationNum := len(dcLocationConfig)
	cluster, err := tests.NewTestCluster(suite.ctx, dcLocationNum, func(conf *config.Config, serverName string) {
		conf.EnableLocalTSO = true
		conf.Labels[config.ZoneLabel] = dcLocationConfig[serverName]
	})
	defer cluster.Destroy()
	re.NoError(err)
	re.NoError(cluster.RunInitialServers())

	cluster.WaitAllLeaders(re, dcLocationConfig)

	suite.leaderServer = cluster.GetLeaderServer()
	re.NotNil(suite.leaderServer)
	suite.dcClientMap[tso.GlobalDCLocation] = testutil.MustNewGrpcClient(re, suite.leaderServer.GetAddr())
	for _, dcLocation := range dcLocationConfig {
		pdName := suite.leaderServer.GetAllocatorLeader(dcLocation).GetName()
		suite.dcClientMap[dcLocation] = testutil.MustNewGrpcClient(re, cluster.GetServer(pdName).GetAddr())
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/globalTSOOverflow", `return(true)`))
	suite.getTimestampByDC(ctx, re, cluster, tso.GlobalDCLocation)
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/tso/globalTSOOverflow"))
}

func (suite *tsoConsistencyTestSuite) TestLocalAllocatorLeaderChange() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/mockLocalAllocatorLeaderChange", `return(true)`))
	dcLocationConfig := map[string]string{
		"pd1": "dc-1",
	}
	dcLocationNum := len(dcLocationConfig)
	cluster, err := tests.NewTestCluster(suite.ctx, dcLocationNum, func(conf *config.Config, serverName string) {
		conf.EnableLocalTSO = true
		conf.Labels[config.ZoneLabel] = dcLocationConfig[serverName]
	})
	defer cluster.Destroy()
	re.NoError(err)
	re.NoError(cluster.RunInitialServers())

	cluster.WaitAllLeaders(re, dcLocationConfig)

	suite.leaderServer = cluster.GetLeaderServer()
	re.NotNil(suite.leaderServer)
	suite.dcClientMap[tso.GlobalDCLocation] = testutil.MustNewGrpcClient(re, suite.leaderServer.GetAddr())
	for _, dcLocation := range dcLocationConfig {
		pdName := suite.leaderServer.GetAllocatorLeader(dcLocation).GetName()
		suite.dcClientMap[dcLocation] = testutil.MustNewGrpcClient(re, cluster.GetServer(pdName).GetAddr())
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	suite.getTimestampByDC(ctx, re, cluster, tso.GlobalDCLocation)
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/mockLocalAllocatorLeaderChange"))
}

func (suite *tsoConsistencyTestSuite) TestLocalTSO() {
	re := suite.Require()
	dcLocationConfig := map[string]string{
		"pd1": "dc-1",
		"pd2": "dc-2",
		"pd3": "dc-3",
	}
	dcLocationNum := len(dcLocationConfig)
	cluster, err := tests.NewTestCluster(suite.ctx, dcLocationNum, func(conf *config.Config, serverName string) {
		conf.EnableLocalTSO = true
		conf.Labels[config.ZoneLabel] = dcLocationConfig[serverName]
	})
	defer cluster.Destroy()
	re.NoError(err)
	re.NoError(cluster.RunInitialServers())

	cluster.WaitAllLeaders(re, dcLocationConfig)
	suite.testTSO(cluster, dcLocationConfig, nil)
}

func (suite *tsoConsistencyTestSuite) checkTSOUnique(tso *pdpb.Timestamp) bool {
	suite.tsPoolMutex.Lock()
	defer suite.tsPoolMutex.Unlock()
	ts := tsoutil.GenerateTS(tso)
	if _, exist := suite.tsPool[ts]; exist {
		return false
	}
	suite.tsPool[ts] = struct{}{}
	return true
}

func (suite *tsoConsistencyTestSuite) TestLocalTSOAfterMemberChanged() {
	re := suite.Require()
	dcLocationConfig := map[string]string{
		"pd1": "dc-1",
		"pd2": "dc-2",
		"pd3": "dc-3",
	}
	dcLocationNum := len(dcLocationConfig)
	cluster, err := tests.NewTestCluster(suite.ctx, dcLocationNum, func(conf *config.Config, serverName string) {
		conf.EnableLocalTSO = true
		conf.Labels[config.ZoneLabel] = dcLocationConfig[serverName]
	})
	defer cluster.Destroy()
	re.NoError(err)
	re.NoError(cluster.RunInitialServers())

	cluster.WaitAllLeaders(re, dcLocationConfig)

	leaderServer := cluster.GetLeaderServer()
	leaderCli := testutil.MustNewGrpcClient(re, leaderServer.GetAddr())
	req := &pdpb.TsoRequest{
		Header:     testutil.NewRequestHeader(cluster.GetCluster().GetId()),
		Count:      tsoCount,
		DcLocation: tso.GlobalDCLocation,
	}
	ctx, cancel := context.WithCancel(context.Background())
	ctx = grpcutil.BuildForwardContext(ctx, leaderServer.GetAddr())
	previousTS := testGetTimestamp(re, ctx, leaderCli, req)
	cancel()

	// Wait for all nodes becoming healthy.
	time.Sleep(time.Second * 5)

	// Mock the situation that the system time of PD nodes in dc-4 is slower than others.
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/systemTimeSlow", `return(true)`))

	// Join a new dc-location
	pd4, err := cluster.Join(suite.ctx, func(conf *config.Config, _ string) {
		conf.EnableLocalTSO = true
		conf.Labels[config.ZoneLabel] = "dc-4"
	})
	re.NoError(err)
	re.NoError(pd4.Run())
	dcLocationConfig["pd4"] = "dc-4"
	cluster.CheckClusterDCLocation()
	re.NotEqual("", cluster.WaitAllocatorLeader(
		"dc-4",
		tests.WithRetryTimes(90), tests.WithWaitInterval(time.Second),
	))
	suite.testTSO(cluster, dcLocationConfig, previousTS)

	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/tso/systemTimeSlow"))
}

func (suite *tsoConsistencyTestSuite) testTSO(cluster *tests.TestCluster, dcLocationConfig map[string]string, previousTS *pdpb.Timestamp) {
	re := suite.Require()
	leaderServer := cluster.GetLeaderServer()
	dcClientMap := make(map[string]pdpb.PDClient)
	for _, dcLocation := range dcLocationConfig {
		pdName := leaderServer.GetAllocatorLeader(dcLocation).GetName()
		dcClientMap[dcLocation] = testutil.MustNewGrpcClient(re, cluster.GetServer(pdName).GetAddr())
	}

	var wg sync.WaitGroup
	wg.Add(tsoRequestConcurrencyNumber)
	for range tsoRequestConcurrencyNumber {
		go func() {
			defer wg.Done()
			lastList := make(map[string]*pdpb.Timestamp)
			for _, dcLocation := range dcLocationConfig {
				lastList[dcLocation] = &pdpb.Timestamp{
					Physical: 0,
					Logical:  0,
				}
			}
			for range tsoRequestRound {
				for _, dcLocation := range dcLocationConfig {
					req := &pdpb.TsoRequest{
						Header:     testutil.NewRequestHeader(leaderServer.GetClusterID()),
						Count:      tsoCount,
						DcLocation: dcLocation,
					}
					ctx, cancel := context.WithCancel(context.Background())
					ctx = grpcutil.BuildForwardContext(ctx, cluster.GetServer(leaderServer.GetAllocatorLeader(dcLocation).GetName()).GetAddr())
					ts := testGetTimestamp(re, ctx, dcClientMap[dcLocation], req)
					cancel()
					lastTS := lastList[dcLocation]
					// Check whether the TSO fallbacks
					re.Equal(1, tsoutil.CompareTimestamp(ts, lastTS))
					if previousTS != nil {
						// Because we have a Global TSO synchronization, even though the system time
						// of the PD nodes in dc-4 is slower, its TSO will still be big enough.
						re.Equal(1, tsoutil.CompareTimestamp(ts, previousTS))
					}
					lastList[dcLocation] = ts
					// Check whether the TSO is not unique
					re.True(suite.checkTSOUnique(ts))
				}
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}
	wg.Wait()
}
