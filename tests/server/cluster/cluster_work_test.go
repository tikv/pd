// Copyright 2016 TiKV Project Authors.
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

package cluster_test

import (
	"context"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/versioninfo"
	"github.com/tikv/pd/tests"
)

type raftClusterTestSuite struct {
	suite.Suite
	env *tests.SchedulingTestEnvironment
}

func TestRaftClusterTestSuite(t *testing.T) {
	suite.Run(t, new(raftClusterTestSuite))
}

func (s *raftClusterTestSuite) SetupSuite() {
	s.env = tests.NewSchedulingTestEnvironment(s.T())
}

func (s *raftClusterTestSuite) TearDownSuite() {
	s.env.Cleanup()
}

func (s *raftClusterTestSuite) TearDownTest() {
	re := s.Require()
	s.env.Reset(re)
}

func (s *raftClusterTestSuite) TestValidRequestRegion() {
	s.env.RunTestInNonMicroserviceEnv(s.checkValidRequestRegion)
}

func (s *raftClusterTestSuite) checkValidRequestRegion(cluster *tests.TestCluster) {
	re := s.Require()
	rc := cluster.GetLeaderServer().GetRaftCluster()
	r1 := core.NewRegionInfo(&metapb.Region{
		Id:       1,
		StartKey: []byte(""),
		EndKey:   []byte("a"),
		Peers: []*metapb.Peer{{
			Id:      1,
			StoreId: 1,
		}},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 2, Version: 2},
	}, &metapb.Peer{
		Id:      1,
		StoreId: 1,
	})
	err := rc.HandleRegionHeartbeat(r1)
	re.NoError(err)
	r2 := &metapb.Region{Id: 2, StartKey: []byte("a"), EndKey: []byte("b")}
	re.Error(rc.ValidRegion(r2))
	r3 := &metapb.Region{Id: 1, StartKey: []byte(""), EndKey: []byte("a"), RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 2}}
	re.Error(rc.ValidRegion(r3))
	r4 := &metapb.Region{Id: 1, StartKey: []byte(""), EndKey: []byte("a"), RegionEpoch: &metapb.RegionEpoch{ConfVer: 2, Version: 1}}
	re.Error(rc.ValidRegion(r4))
	r5 := &metapb.Region{Id: 1, StartKey: []byte(""), EndKey: []byte("a"), RegionEpoch: &metapb.RegionEpoch{ConfVer: 2, Version: 2}}
	re.NoError(rc.ValidRegion(r5))
}

func (s *raftClusterTestSuite) TestAskSplit() {
	s.env.RunTestInNonMicroserviceEnv(s.checkAskSplit)
}

func (s *raftClusterTestSuite) TestAskBatchSplitCountAdmission() {
	env := tests.NewSchedulingTestEnvironment(s.T())
	defer env.Cleanup()
	env.RunTest(func(cluster *tests.TestCluster) {
		re := s.Require()
		leader := cluster.GetLeaderServer()
		re.NoError(leader.GetServer().SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.BatchSplit).String()))
		regions := leader.GetRegions()
		re.NotEmpty(regions)
		region := regions[0].GetMeta()
		if primary := cluster.GetSchedulingPrimaryServer(); primary != nil {
			primary.GetCluster().PutRegion(regions[0])
		}
		client, conn := testutil.MustNewGrpcClient(re, leader.GetAddr())
		defer conn.Close()
		request := &pdpb.AskBatchSplitRequest{
			Header: testutil.NewRequestHeader(leader.GetClusterID()),
			Region: region,
		}
		for _, count := range []uint32{131073, math.MaxUint32} {
			request.SplitCount = count
			ctx, cancel := context.WithTimeout(s.T().Context(), 10*time.Second)
			resp, err := client.AskBatchSplit(ctx, request)
			cancel()
			re.NoError(err)
			re.Contains(resp.GetHeader().GetError().GetMessage(), "exceeds maximum")
			re.Empty(resp.GetIds())
		}
		// The boundary batch exercises the monolithic handler's existing per-ID
		// allocation under race instrumentation, so allow a longer RPC deadline.
		for _, count := range []uint32{0, 100000, 131072, 1} {
			request.SplitCount = count
			ctx, cancel := context.WithTimeout(s.T().Context(), 30*time.Second)
			resp, err := client.AskBatchSplit(ctx, request)
			cancel()
			re.NoError(err)
			re.Nil(resp.GetHeader().GetError())
			re.Len(resp.GetIds(), int(count))
			var previous uint64
			for _, split := range resp.GetIds() {
				re.Greater(split.GetNewRegionId(), previous)
				previous = split.GetNewRegionId()
				re.Len(split.GetNewPeerIds(), len(region.GetPeers()))
				for _, peer := range split.GetNewPeerIds() {
					re.Greater(peer, previous)
					previous = peer
				}
			}
		}
	})
}

func (s *raftClusterTestSuite) checkAskSplit(cluster *tests.TestCluster) {
	re := s.Require()
	leaderServer := cluster.GetLeaderServer()
	rc := leaderServer.GetRaftCluster()
	clusterID := leaderServer.GetClusterID()
	r1 := core.NewRegionInfo(&metapb.Region{
		Id:       1,
		StartKey: []byte(""),
		EndKey:   []byte("a"),
		Peers: []*metapb.Peer{{
			Id:      1,
			StoreId: 1,
		}},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 2, Version: 2},
	}, &metapb.Peer{
		Id:      1,
		StoreId: 1,
	})
	err := rc.HandleRegionHeartbeat(r1)
	re.NoError(err)
	regions := rc.GetRegions()

	req := &pdpb.AskSplitRequest{
		Header: &pdpb.RequestHeader{
			ClusterId: clusterID,
		},
		Region: regions[0].GetMeta(),
	}

	req1 := &pdpb.AskBatchSplitRequest{
		Header: &pdpb.RequestHeader{
			ClusterId: clusterID,
		},
		Region:     regions[0].GetMeta(),
		SplitCount: 10,
	}

	re.NoError(leaderServer.GetServer().SaveTTLConfig(map[string]any{"schedule.enable-tikv-split-region": 0}, time.Minute))
	_, err = rc.HandleAskSplit(req)
	re.ErrorIs(err, errs.ErrSchedulerTiKVSplitDisabled)
	_, err = rc.HandleAskBatchSplit(req1)
	re.ErrorIs(err, errs.ErrSchedulerTiKVSplitDisabled)
	re.NoError(leaderServer.GetServer().SaveTTLConfig(map[string]any{"schedule.enable-tikv-split-region": 0}, 0))
	testutil.Eventually(re, leaderServer.GetPersistOptions().IsTikvRegionSplitEnabled)

	_, err = rc.HandleAskSplit(req)
	re.NoError(err)

	_, err = rc.HandleAskBatchSplit(req1)
	re.NoError(err)
	// test region id whether valid
	opt := rc.GetOpts()
	opt.SetSplitMergeInterval(time.Duration(0))
	defer func() {
		// reset to default value to avoid affecting other tests
		opt.SetSplitMergeInterval(time.Hour)
	}()
	mergeChecker := rc.GetMergeChecker()
	mergeChecker.Check(regions[0])
	re.NoError(err)
}

func (s *raftClusterTestSuite) TestPendingProcessedRegions() {
	re := s.Require()
	// Keep the batch-split entries pending until the assertion. Use a fresh
	// environment so no patrol pass is already in flight when it is disabled.
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/checker/skipPatrolRegions", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/checker/skipPatrolRegions"))
	}()
	env := tests.NewSchedulingTestEnvironment(s.T())
	defer env.Cleanup()
	env.RunTestInNonMicroserviceEnv(s.checkPendingProcessedRegions)
}

func (s *raftClusterTestSuite) checkPendingProcessedRegions(cluster *tests.TestCluster) {
	re := s.Require()
	leaderServer := cluster.GetLeaderServer()
	clusterID := leaderServer.GetClusterID()
	rc := leaderServer.GetRaftCluster()
	r1 := core.NewRegionInfo(&metapb.Region{
		Id:       223,
		StartKey: []byte(""),
		EndKey:   []byte(""),
		Peers: []*metapb.Peer{{
			Id:      224,
			StoreId: 1,
		}},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 2, Version: 2},
	}, &metapb.Peer{
		Id:      224,
		StoreId: 1,
	})
	err := rc.HandleRegionHeartbeat(r1)
	re.NoError(err)
	regions := rc.GetRegions()

	req := &pdpb.AskBatchSplitRequest{
		Header: &pdpb.RequestHeader{
			ClusterId: clusterID,
		},
		Region:     regions[0].GetMeta(),
		SplitCount: 2,
	}
	res, err := rc.HandleAskBatchSplit(req)
	re.NoError(err)
	ids := []uint64{regions[0].GetMeta().GetId(), res.Ids[0].NewRegionId, res.Ids[1].NewRegionId}
	slices.Sort(ids)
	pendingProcessedRegions := rc.GetPendingProcessedRegions()
	slices.Sort(pendingProcessedRegions)
	re.Equal(ids, pendingProcessedRegions)
}
