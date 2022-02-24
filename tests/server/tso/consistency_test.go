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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/pkg/grpcutil"
	"github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/pkg/tsoutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/tso"
	"github.com/tikv/pd/tests"
)

var _ = Suite(&testTSOConsistencySuite{})

type testTSOConsistencySuite struct {
	ctx    context.Context
	cancel context.CancelFunc

	leaderServer *tests.TestServer
	dcClientMap  map[string]pdpb.PDClient

	tsPoolMutex sync.Mutex
	tsPool      map[uint64]struct{}
}

func (s *testTSOConsistencySuite) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.dcClientMap = make(map[string]pdpb.PDClient)
	s.tsPool = make(map[uint64]struct{})
	server.EnableZap = true
}

func (s *testTSOConsistencySuite) TearDownSuite(c *C) {
	s.cancel()
}

// TestNormalGlobalTSO is used to test the normal way of global TSO generation.
func (s *testTSOConsistencySuite) TestNormalGlobalTSO(c *C) {
	cluster, err := tests.NewTestCluster(s.ctx, 1)
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()

	leaderServer := cluster.GetServer(cluster.GetLeader())
	grpcPDClient := testutil.MustNewGrpcClient(c, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()
	req := &pdpb.TsoRequest{
		Header:     testutil.NewRequestHeader(clusterID),
		Count:      uint32(tsoCount),
		DcLocation: tso.GlobalDCLocation,
	}
	s.requestGlobalTSOConcurrently(c, grpcPDClient, req)
	// Test Global TSO after the leader change
	leaderServer.GetServer().GetMember().ResetLeader()
	cluster.WaitLeader()
	s.requestGlobalTSOConcurrently(c, grpcPDClient, req)
}

func (s *testTSOConsistencySuite) requestGlobalTSOConcurrently(c *C, grpcPDClient pdpb.PDClient, req *pdpb.TsoRequest) {
	var wg sync.WaitGroup
	wg.Add(tsoRequestConcurrencyNumber)
	for i := 0; i < tsoRequestConcurrencyNumber; i++ {
		go func() {
			defer wg.Done()
			last := &pdpb.Timestamp{
				Physical: 0,
				Logical:  0,
			}
			for j := 0; j < tsoRequestRound; j++ {
				ts := s.testGetNormalGlobalTimestamp(c, grpcPDClient, req)
				// Check whether the TSO fallbacks
				c.Assert(tsoutil.CompareTimestamp(ts, last), Equals, 1)
				last = ts
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}
	wg.Wait()
}

func (s *testTSOConsistencySuite) testGetNormalGlobalTimestamp(c *C, pdCli pdpb.PDClient, req *pdpb.TsoRequest) *pdpb.Timestamp {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tsoClient, err := pdCli.Tso(ctx)
	c.Assert(err, IsNil)
	defer tsoClient.CloseSend()
	err = tsoClient.Send(req)
	c.Assert(err, IsNil)
	resp, err := tsoClient.Recv()
	c.Assert(err, IsNil)
	c.Assert(resp.GetCount(), Equals, req.GetCount())
	res := resp.GetTimestamp()
	c.Assert(res.GetPhysical(), Greater, int64(0))
	c.Assert(uint32(res.GetLogical())>>res.GetSuffixBits(), GreaterEqual, req.GetCount())
	return res
}

// TestSynchronizedGlobalTSO is used to test the synchronized way of global TSO generation.
func (s *testTSOConsistencySuite) TestSynchronizedGlobalTSO(c *C) {
	dcLocationConfig := map[string]string{
		"pd1": "dc-1",
		"pd2": "dc-2",
		"pd3": "dc-3",
	}
	dcLocationNum := len(dcLocationConfig)
	cluster, err := tests.NewTestCluster(s.ctx, dcLocationNum, func(conf *config.Config, serverName string) {
		conf.EnableLocalTSO = true
		conf.Labels[config.ZoneLabel] = dcLocationConfig[serverName]
	})
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)

	cluster.WaitAllLeaders(c, dcLocationConfig)

	s.leaderServer = cluster.GetServer(cluster.GetLeader())
	c.Assert(s.leaderServer, NotNil)
	s.dcClientMap[tso.GlobalDCLocation] = testutil.MustNewGrpcClient(c, s.leaderServer.GetAddr())
	for _, dcLocation := range dcLocationConfig {
		pdName := s.leaderServer.GetAllocatorLeader(dcLocation).GetName()
		s.dcClientMap[dcLocation] = testutil.MustNewGrpcClient(c, cluster.GetServer(pdName).GetAddr())
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	maxGlobalTSO := &pdpb.Timestamp{}
	for i := 0; i < tsoRequestRound; i++ {
		// Get some local TSOs first
		oldLocalTSOs := make([]*pdpb.Timestamp, 0, dcLocationNum)
		for _, dcLocation := range dcLocationConfig {
			localTSO := s.getTimestampByDC(ctx, c, cluster, tsoCount, dcLocation)
			oldLocalTSOs = append(oldLocalTSOs, localTSO)
			c.Assert(tsoutil.CompareTimestamp(maxGlobalTSO, localTSO), Equals, -1)
		}
		// Get a global TSO then
		globalTSO := s.getTimestampByDC(ctx, c, cluster, tsoCount, tso.GlobalDCLocation)
		for _, oldLocalTSO := range oldLocalTSOs {
			c.Assert(tsoutil.CompareTimestamp(globalTSO, oldLocalTSO), Equals, 1)
		}
		if tsoutil.CompareTimestamp(maxGlobalTSO, globalTSO) < 0 {
			maxGlobalTSO = globalTSO
		}
		// Get some local TSOs again
		newLocalTSOs := make([]*pdpb.Timestamp, 0, dcLocationNum)
		for _, dcLocation := range dcLocationConfig {
			newLocalTSOs = append(newLocalTSOs, s.getTimestampByDC(ctx, c, cluster, tsoCount, dcLocation))
		}
		for _, newLocalTSO := range newLocalTSOs {
			c.Assert(tsoutil.CompareTimestamp(maxGlobalTSO, newLocalTSO), Equals, -1)
		}
	}
}

func (s *testTSOConsistencySuite) getTimestampByDC(ctx context.Context, c *C, cluster *tests.TestCluster, n uint32, dcLocation string) *pdpb.Timestamp {
	req := &pdpb.TsoRequest{
		Header:     testutil.NewRequestHeader(s.leaderServer.GetClusterID()),
		Count:      n,
		DcLocation: dcLocation,
	}
	pdClient, ok := s.dcClientMap[dcLocation]
	c.Assert(ok, IsTrue)
	forwardedHost := cluster.GetServer(s.leaderServer.GetAllocatorLeader(dcLocation).GetName()).GetAddr()
	ctx = grpcutil.BuildForwardContext(ctx, forwardedHost)
	tsoClient, err := pdClient.Tso(ctx)
	c.Assert(err, IsNil)
	defer tsoClient.CloseSend()
	err = tsoClient.Send(req)
	c.Assert(err, IsNil)
	resp, err := tsoClient.Recv()
	c.Assert(err, IsNil)
	return checkAndReturnTimestampResponse(c, req, resp)
}

func (s *testTSOConsistencySuite) TestSynchronizedGlobalTSOOverflow(c *C) {
	dcLocationConfig := map[string]string{
		"pd1": "dc-1",
		"pd2": "dc-2",
		"pd3": "dc-3",
	}
	dcLocationNum := len(dcLocationConfig)
	cluster, err := tests.NewTestCluster(s.ctx, dcLocationNum, func(conf *config.Config, serverName string) {
		conf.EnableLocalTSO = true
		conf.Labels[config.ZoneLabel] = dcLocationConfig[serverName]
	})
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)

	cluster.WaitAllLeaders(c, dcLocationConfig)

	s.leaderServer = cluster.GetServer(cluster.GetLeader())
	c.Assert(s.leaderServer, NotNil)
	s.dcClientMap[tso.GlobalDCLocation] = testutil.MustNewGrpcClient(c, s.leaderServer.GetAddr())
	for _, dcLocation := range dcLocationConfig {
		pdName := s.leaderServer.GetAllocatorLeader(dcLocation).GetName()
		s.dcClientMap[dcLocation] = testutil.MustNewGrpcClient(c, cluster.GetServer(pdName).GetAddr())
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Assert(failpoint.Enable("github.com/tikv/pd/server/tso/globalTSOOverflow", `return(true)`), IsNil)
	s.getTimestampByDC(ctx, c, cluster, tsoCount, tso.GlobalDCLocation)
	failpoint.Disable("github.com/tikv/pd/server/tso/globalTSOOverflow")
}

func (s *testTSOConsistencySuite) TestLocalAllocatorLeaderChange(c *C) {
	c.Assert(failpoint.Enable("github.com/tikv/pd/server/mockLocalAllocatorLeaderChange", `return(true)`), IsNil)
	defer failpoint.Disable("github.com/tikv/pd/server/mockLocalAllocatorLeaderChange")
	dcLocationConfig := map[string]string{
		"pd1": "dc-1",
	}
	dcLocationNum := len(dcLocationConfig)
	cluster, err := tests.NewTestCluster(s.ctx, dcLocationNum, func(conf *config.Config, serverName string) {
		conf.EnableLocalTSO = true
		conf.Labels[config.ZoneLabel] = dcLocationConfig[serverName]
	})
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)

	cluster.WaitAllLeaders(c, dcLocationConfig)

	s.leaderServer = cluster.GetServer(cluster.GetLeader())
	c.Assert(s.leaderServer, NotNil)
	s.dcClientMap[tso.GlobalDCLocation] = testutil.MustNewGrpcClient(c, s.leaderServer.GetAddr())
	for _, dcLocation := range dcLocationConfig {
		pdName := s.leaderServer.GetAllocatorLeader(dcLocation).GetName()
		s.dcClientMap[dcLocation] = testutil.MustNewGrpcClient(c, cluster.GetServer(pdName).GetAddr())
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.getTimestampByDC(ctx, c, cluster, tsoCount, tso.GlobalDCLocation)
}

func (s *testTSOConsistencySuite) TestLocalTSO(c *C) {
	dcLocationConfig := map[string]string{
		"pd1": "dc-1",
		"pd2": "dc-2",
		"pd3": "dc-3",
	}
	dcLocationNum := len(dcLocationConfig)
	cluster, err := tests.NewTestCluster(s.ctx, dcLocationNum, func(conf *config.Config, serverName string) {
		conf.EnableLocalTSO = true
		conf.Labels[config.ZoneLabel] = dcLocationConfig[serverName]
	})
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)

	cluster.WaitAllLeaders(c, dcLocationConfig)
	s.testTSO(c, cluster, dcLocationConfig, nil)
}

func (s *testTSOConsistencySuite) checkTSOUnique(tso *pdpb.Timestamp) bool {
	s.tsPoolMutex.Lock()
	defer s.tsPoolMutex.Unlock()
	ts := tsoutil.GenerateTS(tso)
	if _, exist := s.tsPool[ts]; exist {
		return false
	}
	s.tsPool[ts] = struct{}{}
	return true
}

func (s *testTSOConsistencySuite) TestLocalTSOAfterMemberChanged(c *C) {
	dcLocationConfig := map[string]string{
		"pd1": "dc-1",
		"pd2": "dc-2",
		"pd3": "dc-3",
	}
	dcLocationNum := len(dcLocationConfig)
	cluster, err := tests.NewTestCluster(s.ctx, dcLocationNum, func(conf *config.Config, serverName string) {
		conf.EnableLocalTSO = true
		conf.Labels[config.ZoneLabel] = dcLocationConfig[serverName]
	})
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)

	cluster.WaitAllLeaders(c, dcLocationConfig)

	leaderServer := cluster.GetServer(cluster.GetLeader())
	leaderCli := testutil.MustNewGrpcClient(c, leaderServer.GetAddr())
	req := &pdpb.TsoRequest{
		Header:     testutil.NewRequestHeader(cluster.GetCluster().GetId()),
		Count:      tsoCount,
		DcLocation: tso.GlobalDCLocation,
	}
	ctx, cancel := context.WithCancel(context.Background())
	ctx = grpcutil.BuildForwardContext(ctx, leaderServer.GetAddr())
	previousTS := testGetTimestamp(c, ctx, leaderCli, req)
	cancel()

	// Wait for all nodes becoming healthy.
	time.Sleep(time.Second * 5)

	// Mock the situation that the system time of PD nodes in dc-4 is slower than others.
	c.Assert(failpoint.Enable("github.com/tikv/pd/server/tso/systemTimeSlow", `return(true)`), IsNil)

	// Join a new dc-location
	pd4, err := cluster.Join(s.ctx, func(conf *config.Config, serverName string) {
		conf.EnableLocalTSO = true
		conf.Labels[config.ZoneLabel] = "dc-4"
	})
	c.Assert(err, IsNil)
	err = pd4.Run()
	c.Assert(err, IsNil)
	dcLocationConfig["pd4"] = "dc-4"
	cluster.CheckClusterDCLocation()
	testutil.WaitUntil(c, func() bool {
		leaderName := cluster.WaitAllocatorLeader("dc-4")
		return leaderName != ""
	})
	s.testTSO(c, cluster, dcLocationConfig, previousTS)

	failpoint.Disable("github.com/tikv/pd/server/tso/systemTimeSlow")
}

func (s *testTSOConsistencySuite) testTSO(c *C, cluster *tests.TestCluster, dcLocationConfig map[string]string, previousTS *pdpb.Timestamp) {
	leaderServer := cluster.GetServer(cluster.GetLeader())
	dcClientMap := make(map[string]pdpb.PDClient)
	for _, dcLocation := range dcLocationConfig {
		pdName := leaderServer.GetAllocatorLeader(dcLocation).GetName()
		dcClientMap[dcLocation] = testutil.MustNewGrpcClient(c, cluster.GetServer(pdName).GetAddr())
	}

	var wg sync.WaitGroup
	wg.Add(tsoRequestConcurrencyNumber)
	for i := 0; i < tsoRequestConcurrencyNumber; i++ {
		go func() {
			defer wg.Done()
			lastList := make(map[string]*pdpb.Timestamp)
			for _, dcLocation := range dcLocationConfig {
				lastList[dcLocation] = &pdpb.Timestamp{
					Physical: 0,
					Logical:  0,
				}
			}
			for j := 0; j < tsoRequestRound; j++ {
				for _, dcLocation := range dcLocationConfig {
					req := &pdpb.TsoRequest{
						Header:     testutil.NewRequestHeader(leaderServer.GetClusterID()),
						Count:      tsoCount,
						DcLocation: dcLocation,
					}
					ctx, cancel := context.WithCancel(context.Background())
					ctx = grpcutil.BuildForwardContext(ctx, cluster.GetServer(leaderServer.GetAllocatorLeader(dcLocation).GetName()).GetAddr())
					ts := testGetTimestamp(c, ctx, dcClientMap[dcLocation], req)
					cancel()
					lastTS := lastList[dcLocation]
					// Check whether the TSO fallbacks
					c.Assert(tsoutil.CompareTimestamp(ts, lastTS), Equals, 1)
					if previousTS != nil {
						// Because we have a Global TSO synchronization, even though the system time
						// of the PD nodes in dc-4 is slower, its TSO will still be big enough.
						c.Assert(tsoutil.CompareTimestamp(ts, previousTS), Equals, 1)
					}
					lastList[dcLocation] = ts
					// Check whether the TSO is not unique
					c.Assert(s.checkTSOUnique(ts), IsTrue)
				}
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}
	wg.Wait()

	failpoint.Disable("github.com/tikv/pd/server/tso/systemTimeSlow")
}

var _ = Suite(&testFallbackTSOConsistencySuite{})

type testFallbackTSOConsistencySuite struct {
	ctx          context.Context
	cancel       context.CancelFunc
	cluster      *tests.TestCluster
	grpcPDClient pdpb.PDClient
	server       *tests.TestServer
}

func (s *testFallbackTSOConsistencySuite) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	c.Assert(failpoint.Enable("github.com/tikv/pd/server/tso/fallBackSync", `return(true)`), IsNil)
	c.Assert(failpoint.Enable("github.com/tikv/pd/server/tso/fallBackUpdate", `return(true)`), IsNil)
	var err error
	s.cluster, err = tests.NewTestCluster(s.ctx, 1)
	c.Assert(err, IsNil)

	err = s.cluster.RunInitialServers()
	c.Assert(err, IsNil)
	s.cluster.WaitLeader()

	s.server = s.cluster.GetServer(s.cluster.GetLeader())
	s.grpcPDClient = testutil.MustNewGrpcClient(c, s.server.GetAddr())
	svr := s.server.GetServer()
	svr.Close()
	failpoint.Disable("github.com/tikv/pd/server/tso/fallBackSync")
	failpoint.Disable("github.com/tikv/pd/server/tso/fallBackUpdate")
	err = svr.Run()
	c.Assert(err, IsNil)
	s.cluster.WaitLeader()
}

func (s *testFallbackTSOConsistencySuite) TearDownSuite(c *C) {
	s.cancel()
	s.cluster.Destroy()
}

func (s *testFallbackTSOConsistencySuite) TestFallbackTSOConsistency(c *C) {
	var wg sync.WaitGroup
	wg.Add(tsoRequestConcurrencyNumber)
	for i := 0; i < tsoRequestConcurrencyNumber; i++ {
		go func() {
			defer wg.Done()
			last := &pdpb.Timestamp{
				Physical: 0,
				Logical:  0,
			}
			for j := 0; j < tsoRequestRound; j++ {
				ts := s.testGetTSO(c, 10)
				c.Assert(tsoutil.CompareTimestamp(ts, last), Equals, 1)
				last = ts
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}
	wg.Wait()
}

func (s *testFallbackTSOConsistencySuite) testGetTSO(c *C, n uint32) *pdpb.Timestamp {
	clusterID := s.server.GetClusterID()
	req := &pdpb.TsoRequest{
		Header:     testutil.NewRequestHeader(clusterID),
		Count:      n,
		DcLocation: tso.GlobalDCLocation,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tsoClient, err := s.grpcPDClient.Tso(ctx)
	c.Assert(err, IsNil)
	defer tsoClient.CloseSend()
	err = tsoClient.Send(req)
	c.Assert(err, IsNil)
	resp, err := tsoClient.Recv()
	c.Assert(err, IsNil)
	return checkAndReturnTimestampResponse(c, req, resp)
}
