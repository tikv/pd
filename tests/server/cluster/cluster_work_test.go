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
// See the License for the specific language governing permissions and
// limitations under the License.

package cluster_test

import (
	"context"
	"sort"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/tests"
)

var _ = Suite(&clusterWorkerTestSuite{})

type clusterWorkerTestSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *clusterWorkerTestSuite) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	server.EnableZap = true
}

func (s *clusterWorkerTestSuite) TearDownSuite(c *C) {
	s.cancel()
}

func (s *clusterWorkerTestSuite) TestValidRequestRegion(c *C) {
	cluster, err := tests.NewTestCluster(s.ctx, 1)
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)

	cluster.WaitLeader()
	leaderServer := cluster.GetServer(cluster.GetLeader())
	grpcPDClient := testutil.MustNewGrpcClient(c, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()
	bootstrapCluster(c, clusterID, grpcPDClient, "127.0.0.1:0")
	rc := leaderServer.GetRaftCluster()

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
	err = rc.HandleRegionHeartbeat(r1)
	c.Assert(err, IsNil)
	r2 := &metapb.Region{Id: 2, StartKey: []byte("a"), EndKey: []byte("b")}
	c.Assert(rc.ValidRequestRegion(r2), NotNil)
	r3 := &metapb.Region{Id: 1, StartKey: []byte(""), EndKey: []byte("a"), RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 2}}
	c.Assert(rc.ValidRequestRegion(r3), NotNil)
	r4 := &metapb.Region{Id: 1, StartKey: []byte(""), EndKey: []byte("a"), RegionEpoch: &metapb.RegionEpoch{ConfVer: 2, Version: 1}}
	c.Assert(rc.ValidRequestRegion(r4), NotNil)
	r5 := &metapb.Region{Id: 1, StartKey: []byte(""), EndKey: []byte("a"), RegionEpoch: &metapb.RegionEpoch{ConfVer: 2, Version: 2}}
	c.Assert(rc.ValidRequestRegion(r5), IsNil)
	rc.Stop()
}

func (s *clusterWorkerTestSuite) TestAskSplit(c *C) {
	cluster, err := tests.NewTestCluster(s.ctx, 1)
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)

	cluster.WaitLeader()
	leaderServer := cluster.GetServer(cluster.GetLeader())
	grpcPDClient := testutil.MustNewGrpcClient(c, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()
	bootstrapCluster(c, clusterID, grpcPDClient, "127.0.0.1:0")
	rc := leaderServer.GetRaftCluster()
	opt := rc.GetOpts()
	opt.SetSplitMergeInterval(time.Hour)
	regions := rc.GetRegions()

	req := &pdpb.AskSplitRequest{
		Header: &pdpb.RequestHeader{
			ClusterId: clusterID,
		},
		Region: regions[0].GetMeta(),
	}

	_, err = rc.HandleAskSplit(req)
	c.Assert(err, IsNil)

	req1 := &pdpb.AskBatchSplitRequest{
		Header: &pdpb.RequestHeader{
			ClusterId: clusterID,
		},
		Region:     regions[0].GetMeta(),
		SplitCount: 10,
	}

	_, err = rc.HandleAskBatchSplit(req1)
	c.Assert(err, IsNil)
	// test region id whether valid
	opt.SetSplitMergeInterval(time.Duration(0))
	mergeChecker := rc.GetMergeChecker()
	mergeChecker.Check(regions[0])
	c.Assert(err, IsNil)
}

func (s *clusterWorkerTestSuite) TestSuspectRegions(c *C) {
	cluster, err := tests.NewTestCluster(s.ctx, 1)
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)

	cluster.WaitLeader()
	leaderServer := cluster.GetServer(cluster.GetLeader())
	grpcPDClient := testutil.MustNewGrpcClient(c, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()
	bootstrapCluster(c, clusterID, grpcPDClient, "127.0.0.1:0")
	rc := leaderServer.GetRaftCluster()
	opt := rc.GetOpts()
	opt.SetSplitMergeInterval(time.Hour)
	regions := rc.GetRegions()

	req := &pdpb.AskBatchSplitRequest{
		Header: &pdpb.RequestHeader{
			ClusterId: clusterID,
		},
		Region:     regions[0].GetMeta(),
		SplitCount: 2,
	}
	res, err := rc.HandleAskBatchSplit(req)
	c.Assert(err, IsNil)
	ids := []uint64{regions[0].GetMeta().GetId(), res.Ids[0].NewRegionId, res.Ids[1].NewRegionId}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	suspects := rc.GetSuspectRegions()
	sort.Slice(suspects, func(i, j int) bool { return suspects[i] < suspects[j] })
	c.Assert(suspects, DeepEquals, ids)
}
