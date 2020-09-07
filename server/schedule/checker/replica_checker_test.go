// Copyright 2019 TiKV Project Authors.
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

package checker

import (
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/opt"
	"github.com/tikv/pd/server/versioninfo"
)

const (
	KB = 1024
	MB = 1024 * KB
)

var _ = Suite(&testReplicaCheckerSuite{})

type testReplicaCheckerSuite struct {
	cluster *mockcluster.Cluster
	rc      *ReplicaChecker
}

func (s *testReplicaCheckerSuite) SetUpTest(c *C) {
	cfg := config.NewTestOptions()
	s.cluster = mockcluster.NewCluster(cfg)
	s.cluster.DisableFeature(versioninfo.JointConsensus)
	s.rc = NewReplicaChecker(s.cluster)
	stats := &pdpb.StoreStats{
		Capacity:  100,
		Available: 100,
	}
	stores := []*core.StoreInfo{
		core.NewStoreInfo(
			&metapb.Store{
				Id:    1,
				State: metapb.StoreState_Offline,
			},
			core.SetStoreStats(stats),
			core.SetLastHeartbeatTS(time.Now()),
		),
		core.NewStoreInfo(
			&metapb.Store{
				Id:    3,
				State: metapb.StoreState_Up,
			},
			core.SetStoreStats(stats),
			core.SetLastHeartbeatTS(time.Now()),
		),
		core.NewStoreInfo(
			&metapb.Store{
				Id:    4,
				State: metapb.StoreState_Up,
			}, core.SetStoreStats(stats),
			core.SetLastHeartbeatTS(time.Now()),
		),
	}
	for _, store := range stores {
		s.cluster.PutStore(store)
	}
	s.cluster.AddLabelsStore(2, 1, map[string]string{"noleader": "true"})
}

func (s *testReplicaCheckerSuite) TestReplacePendingPeer(c *C) {
	peers := []*metapb.Peer{
		{
			Id:      2,
			StoreId: 1,
		},
		{
			Id:      3,
			StoreId: 2,
		},
		{
			Id:      4,
			StoreId: 3,
		},
	}
	r := core.NewRegionInfo(&metapb.Region{Id: 1, Peers: peers}, peers[1], core.WithPendingPeers(peers[0:1]))
	s.cluster.PutRegion(r)
	op := s.rc.Check(r)
	c.Assert(op, NotNil)
	c.Assert(op.Step(0).(operator.AddLearner).ToStore, Equals, uint64(4))
	c.Assert(op.Step(1).(operator.PromoteLearner).ToStore, Equals, uint64(4))
	c.Assert(op.Step(2).(operator.RemovePeer).FromStore, Equals, uint64(1))
}

func (s *testReplicaCheckerSuite) TestReplaceOfflinePeer(c *C) {
	s.cluster.SetLabelPropertyConfig(config.LabelPropertyConfig{
		opt.RejectLeader: {{Key: "noleader", Value: "true"}},
	})
	peers := []*metapb.Peer{
		{
			Id:      4,
			StoreId: 1,
		},
		{
			Id:      5,
			StoreId: 2,
		},
		{
			Id:      6,
			StoreId: 3,
		},
	}
	r := core.NewRegionInfo(&metapb.Region{Id: 2, Peers: peers}, peers[0])
	s.cluster.PutRegion(r)
	op := s.rc.Check(r)
	c.Assert(op, NotNil)
	c.Assert(op.Step(0).(operator.TransferLeader).ToStore, Equals, uint64(3))
	c.Assert(op.Step(1).(operator.AddLearner).ToStore, Equals, uint64(4))
	c.Assert(op.Step(2).(operator.PromoteLearner).ToStore, Equals, uint64(4))
	c.Assert(op.Step(3).(operator.RemovePeer).FromStore, Equals, uint64(1))
}

func (s *testReplicaCheckerSuite) TestOfflineWithOneReplica(c *C) {
	s.cluster.SetMaxReplicas(1)
	peers := []*metapb.Peer{
		{
			Id:      4,
			StoreId: 1,
		},
	}
	r := core.NewRegionInfo(&metapb.Region{Id: 2, Peers: peers}, peers[0])
	s.cluster.PutRegion(r)
	op := s.rc.Check(r)
	c.Assert(op, NotNil)
	c.Assert(op.Desc(), Equals, "replace-offline-replica")
}

func (s *testReplicaCheckerSuite) TestBasic(c *C) {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.SetMaxSnapshotCount(2)
	tc.DisableFeature(versioninfo.JointConsensus)
	rc := NewReplicaChecker(tc)

	// Add stores 1,2,3,4.
	tc.AddRegionStore(1, 4)
	tc.AddRegionStore(2, 3)
	tc.AddRegionStore(3, 2)
	tc.AddRegionStore(4, 1)
	// Add region 1 with leader in store 1 and follower in store 2.
	tc.AddLeaderRegion(1, 1, 2)

	// Region has 2 peers, we need to add a new peer.
	region := tc.GetRegion(1)
	testutil.CheckAddPeer(c, rc.Check(region), operator.OpReplica, 4)

	// Disable make up replica feature.
	tc.SetEnableMakeUpReplica(false)
	c.Assert(rc.Check(region), IsNil)
	tc.SetEnableMakeUpReplica(true)

	// Test healthFilter.
	// If store 4 is down, we add to store 3.
	tc.SetStoreDown(4)
	testutil.CheckAddPeer(c, rc.Check(region), operator.OpReplica, 3)
	tc.SetStoreUp(4)
	testutil.CheckAddPeer(c, rc.Check(region), operator.OpReplica, 4)

	// Test snapshotCountFilter.
	// If snapshotCount > MaxSnapshotCount, we add to store 3.
	tc.UpdateSnapshotCount(4, 3)
	testutil.CheckAddPeer(c, rc.Check(region), operator.OpReplica, 3)
	// If snapshotCount < MaxSnapshotCount, we can add peer again.
	tc.UpdateSnapshotCount(4, 1)
	testutil.CheckAddPeer(c, rc.Check(region), operator.OpReplica, 4)

	// Add peer in store 4, and we have enough replicas.
	peer4, _ := tc.AllocPeer(4)
	region = region.Clone(core.WithAddPeer(peer4))
	c.Assert(rc.Check(region), IsNil)

	// Add peer in store 3, and we have redundant replicas.
	peer3, _ := tc.AllocPeer(3)
	region = region.Clone(core.WithAddPeer(peer3))
	testutil.CheckRemovePeer(c, rc.Check(region), 1)

	// Disable remove extra replica feature.
	tc.SetEnableRemoveExtraReplica(false)
	c.Assert(rc.Check(region), IsNil)
	tc.SetEnableRemoveExtraReplica(true)

	region = region.Clone(core.WithRemoveStorePeer(1), core.WithLeader(region.GetStorePeer(3)))

	// Peer in store 2 is down, remove it.
	tc.SetStoreDown(2)
	downPeer := &pdpb.PeerStats{
		Peer:        region.GetStorePeer(2),
		DownSeconds: 24 * 60 * 60,
	}

	region = region.Clone(core.WithDownPeers(append(region.GetDownPeers(), downPeer)))
	testutil.CheckTransferPeer(c, rc.Check(region), operator.OpReplica, 2, 1)
	region = region.Clone(core.WithDownPeers(nil))
	c.Assert(rc.Check(region), IsNil)

	// Peer in store 3 is offline, transfer peer to store 1.
	tc.SetStoreOffline(3)
	testutil.CheckTransferPeer(c, rc.Check(region), operator.OpReplica, 3, 1)
}

func (s *testReplicaCheckerSuite) TestLostStore(c *C) {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)

	tc.DisableFeature(versioninfo.JointConsensus)
	tc.AddRegionStore(1, 1)
	tc.AddRegionStore(2, 1)

	rc := NewReplicaChecker(tc)

	// now region peer in store 1,2,3.but we just have store 1,2
	// This happens only in recovering the PD tc
	// should not panic
	tc.AddLeaderRegion(1, 1, 2, 3)
	region := tc.GetRegion(1)
	op := rc.Check(region)
	c.Assert(op, IsNil)
}

func (s *testReplicaCheckerSuite) TestOffline(c *C) {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.DisableFeature(versioninfo.JointConsensus)
	tc.SetMaxReplicas(3)
	tc.SetLocationLabels([]string{"zone", "rack", "host"})

	rc := NewReplicaChecker(tc)

	tc.AddLabelsStore(1, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	tc.AddLabelsStore(2, 2, map[string]string{"zone": "z2", "rack": "r1", "host": "h1"})
	tc.AddLabelsStore(3, 3, map[string]string{"zone": "z3", "rack": "r1", "host": "h1"})
	tc.AddLabelsStore(4, 4, map[string]string{"zone": "z3", "rack": "r2", "host": "h1"})

	tc.AddLeaderRegion(1, 1)
	region := tc.GetRegion(1)

	// Store 2 has different zone and smallest region score.
	testutil.CheckAddPeer(c, rc.Check(region), operator.OpReplica, 2)
	peer2, _ := tc.AllocPeer(2)
	region = region.Clone(core.WithAddPeer(peer2))

	// Store 3 has different zone and smallest region score.
	testutil.CheckAddPeer(c, rc.Check(region), operator.OpReplica, 3)
	peer3, _ := tc.AllocPeer(3)
	region = region.Clone(core.WithAddPeer(peer3))

	// Store 4 has the same zone with store 3 and larger region score.
	peer4, _ := tc.AllocPeer(4)
	region = region.Clone(core.WithAddPeer(peer4))
	testutil.CheckRemovePeer(c, rc.Check(region), 4)

	// Test offline
	// the number of region peers more than the maxReplicas
	// remove the peer
	tc.SetStoreOffline(3)
	testutil.CheckRemovePeer(c, rc.Check(region), 3)
	region = region.Clone(core.WithRemoveStorePeer(4))
	// the number of region peers equals the maxReplicas
	// Transfer peer to store 4.
	testutil.CheckTransferPeer(c, rc.Check(region), operator.OpReplica, 3, 4)

	// Store 5 has a same label score with store 4,but the region score smaller than store 4, we will choose store 5.
	tc.AddLabelsStore(5, 3, map[string]string{"zone": "z4", "rack": "r1", "host": "h1"})
	testutil.CheckTransferPeer(c, rc.Check(region), operator.OpReplica, 3, 5)
	// Store 5 has too many snapshots, choose store 4
	tc.UpdateSnapshotCount(5, 10)
	testutil.CheckTransferPeer(c, rc.Check(region), operator.OpReplica, 3, 4)
	tc.UpdatePendingPeerCount(4, 30)
	c.Assert(rc.Check(region), IsNil)
}

func (s *testReplicaCheckerSuite) TestDistinctScore(c *C) {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.DisableFeature(versioninfo.JointConsensus)
	tc.SetMaxReplicas(3)
	tc.SetLocationLabels([]string{"zone", "rack", "host"})

	rc := NewReplicaChecker(tc)

	tc.AddLabelsStore(1, 9, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	tc.AddLabelsStore(2, 8, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})

	// We need 3 replicas.
	tc.AddLeaderRegion(1, 1)
	region := tc.GetRegion(1)
	testutil.CheckAddPeer(c, rc.Check(region), operator.OpReplica, 2)
	peer2, _ := tc.AllocPeer(2)
	region = region.Clone(core.WithAddPeer(peer2))

	// Store 1,2,3 have the same zone, rack, and host.
	tc.AddLabelsStore(3, 5, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	testutil.CheckAddPeer(c, rc.Check(region), operator.OpReplica, 3)

	// Store 4 has smaller region score.
	tc.AddLabelsStore(4, 4, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	testutil.CheckAddPeer(c, rc.Check(region), operator.OpReplica, 4)

	// Store 5 has a different host.
	tc.AddLabelsStore(5, 5, map[string]string{"zone": "z1", "rack": "r1", "host": "h2"})
	testutil.CheckAddPeer(c, rc.Check(region), operator.OpReplica, 5)

	// Store 6 has a different rack.
	tc.AddLabelsStore(6, 6, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"})
	testutil.CheckAddPeer(c, rc.Check(region), operator.OpReplica, 6)

	// Store 7 has a different zone.
	tc.AddLabelsStore(7, 7, map[string]string{"zone": "z2", "rack": "r1", "host": "h1"})
	testutil.CheckAddPeer(c, rc.Check(region), operator.OpReplica, 7)

	// Test stateFilter.
	tc.SetStoreOffline(7)
	testutil.CheckAddPeer(c, rc.Check(region), operator.OpReplica, 6)
	tc.SetStoreUp(7)
	testutil.CheckAddPeer(c, rc.Check(region), operator.OpReplica, 7)

	// Add peer to store 7.
	peer7, _ := tc.AllocPeer(7)
	region = region.Clone(core.WithAddPeer(peer7))

	// Replace peer in store 1 with store 6 because it has a different rack.
	testutil.CheckTransferPeer(c, rc.Check(region), operator.OpReplica, 1, 6)
	// Disable locationReplacement feature.
	tc.SetEnableLocationReplacement(false)
	c.Assert(rc.Check(region), IsNil)
	tc.SetEnableLocationReplacement(true)
	peer6, _ := tc.AllocPeer(6)
	region = region.Clone(core.WithAddPeer(peer6))
	testutil.CheckRemovePeer(c, rc.Check(region), 1)
	region = region.Clone(core.WithRemoveStorePeer(1), core.WithLeader(region.GetStorePeer(2)))
	c.Assert(rc.Check(region), IsNil)

	// Store 8 has the same zone and different rack with store 7.
	// Store 1 has the same zone and different rack with store 6.
	// So store 8 and store 1 are equivalent.
	tc.AddLabelsStore(8, 1, map[string]string{"zone": "z2", "rack": "r2", "host": "h1"})
	c.Assert(rc.Check(region), IsNil)

	// Store 10 has a different zone.
	// Store 2 and 6 have the same distinct score, but store 2 has larger region score.
	// So replace peer in store 2 with store 10.
	tc.AddLabelsStore(10, 1, map[string]string{"zone": "z3", "rack": "r1", "host": "h1"})
	testutil.CheckTransferPeer(c, rc.Check(region), operator.OpReplica, 2, 10)
	peer10, _ := tc.AllocPeer(10)
	region = region.Clone(core.WithAddPeer(peer10))
	testutil.CheckRemovePeer(c, rc.Check(region), 2)
	region = region.Clone(core.WithRemoveStorePeer(2))
	c.Assert(rc.Check(region), IsNil)
}

func (s *testReplicaCheckerSuite) TestDistinctScore2(c *C) {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.DisableFeature(versioninfo.JointConsensus)
	tc.SetMaxReplicas(5)
	tc.SetLocationLabels([]string{"zone", "host"})

	rc := NewReplicaChecker(tc)

	tc.AddLabelsStore(1, 1, map[string]string{"zone": "z1", "host": "h1"})
	tc.AddLabelsStore(2, 1, map[string]string{"zone": "z1", "host": "h2"})
	tc.AddLabelsStore(3, 1, map[string]string{"zone": "z1", "host": "h3"})
	tc.AddLabelsStore(4, 1, map[string]string{"zone": "z2", "host": "h1"})
	tc.AddLabelsStore(5, 1, map[string]string{"zone": "z2", "host": "h2"})
	tc.AddLabelsStore(6, 1, map[string]string{"zone": "z3", "host": "h1"})

	tc.AddLeaderRegion(1, 1, 2, 4)
	region := tc.GetRegion(1)

	testutil.CheckAddPeer(c, rc.Check(region), operator.OpReplica, 6)
	peer6, _ := tc.AllocPeer(6)
	region = region.Clone(core.WithAddPeer(peer6))

	testutil.CheckAddPeer(c, rc.Check(region), operator.OpReplica, 5)
	peer5, _ := tc.AllocPeer(5)
	region = region.Clone(core.WithAddPeer(peer5))

	c.Assert(rc.Check(region), IsNil)
}

func (s *testReplicaCheckerSuite) TestStorageThreshold(c *C) {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.SetLocationLabels([]string{"zone"})
	tc.DisableFeature(versioninfo.JointConsensus)
	rc := NewReplicaChecker(tc)

	tc.AddLabelsStore(1, 1, map[string]string{"zone": "z1"})
	tc.UpdateStorageRatio(1, 0.5, 0.5)
	tc.UpdateStoreRegionSize(1, 500*MB)
	tc.AddLabelsStore(2, 1, map[string]string{"zone": "z1"})
	tc.UpdateStorageRatio(2, 0.1, 0.9)
	tc.UpdateStoreRegionSize(2, 100*MB)
	tc.AddLabelsStore(3, 1, map[string]string{"zone": "z2"})
	tc.AddLabelsStore(4, 0, map[string]string{"zone": "z3"})

	tc.AddLeaderRegion(1, 1, 2, 3)
	region := tc.GetRegion(1)

	// Move peer to better location.
	tc.UpdateStorageRatio(4, 0, 1)
	testutil.CheckTransferPeer(c, rc.Check(region), operator.OpReplica, 1, 4)
	// If store4 is almost full, do not add peer on it.
	tc.UpdateStorageRatio(4, 0.9, 0.1)
	c.Assert(rc.Check(region), IsNil)

	tc.AddLeaderRegion(2, 1, 3)
	region = tc.GetRegion(2)
	// Add peer on store4.
	tc.UpdateStorageRatio(4, 0, 1)
	testutil.CheckAddPeer(c, rc.Check(region), operator.OpReplica, 4)
	// If store4 is almost full, do not add peer on it.
	tc.UpdateStorageRatio(4, 0.8, 0)
	testutil.CheckAddPeer(c, rc.Check(region), operator.OpReplica, 2)
}

func (s *testReplicaCheckerSuite) TestOpts(c *C) {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.DisableFeature(versioninfo.JointConsensus)
	rc := NewReplicaChecker(tc)

	tc.AddRegionStore(1, 100)
	tc.AddRegionStore(2, 100)
	tc.AddRegionStore(3, 100)
	tc.AddRegionStore(4, 100)
	tc.AddLeaderRegion(1, 1, 2, 3)

	region := tc.GetRegion(1)
	// Test remove down replica and replace offline replica.
	tc.SetStoreDown(1)
	region = region.Clone(core.WithDownPeers([]*pdpb.PeerStats{
		{
			Peer:        region.GetStorePeer(1),
			DownSeconds: 24 * 60 * 60,
		},
	}))
	tc.SetStoreOffline(2)
	// RemoveDownReplica has higher priority than replaceOfflineReplica.
	testutil.CheckTransferPeer(c, rc.Check(region), operator.OpReplica, 1, 4)
	tc.SetEnableRemoveDownReplica(false)
	testutil.CheckTransferPeer(c, rc.Check(region), operator.OpReplica, 2, 4)
	tc.SetEnableReplaceOfflineReplica(false)
	c.Assert(rc.Check(region), IsNil)
}
