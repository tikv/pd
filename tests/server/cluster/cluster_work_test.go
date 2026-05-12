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
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
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
	// wait ttl config takes effect
	time.Sleep(time.Second)

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
	s.env.RunTestInNonMicroserviceEnv(s.checkPendingProcessedRegions)
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
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	pendingProcessedRegions := rc.GetPendingProcessedRegions()
	sort.Slice(pendingProcessedRegions, func(i, j int) bool { return pendingProcessedRegions[i] < pendingProcessedRegions[j] })
	re.Equal(ids, pendingProcessedRegions)
}
