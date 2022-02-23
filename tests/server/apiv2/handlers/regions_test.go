package handlers_test

import (
	"context"
	"encoding/json"
	"io"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/apiv2/handlers"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/tests"
)

const regionsPrefix = "/pd/api/v2/meta/regions"

var _ = Suite(&testRegionsAPISuite{})

type testRegionsAPISuite struct {
	cleanup      context.CancelFunc
	cluster      *tests.TestCluster
	leaderServer *tests.TestServer
}

func (s *testRegionsAPISuite) SetUpSuite(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	server.EnableZap = true
	s.cleanup = cancel
	cluster, err := tests.NewTestCluster(ctx, 1)
	c.Assert(err, IsNil)
	c.Assert(cluster.RunInitialServers(), IsNil)
	c.Assert(cluster.WaitLeader(), Not(HasLen), 0)
	s.leaderServer = cluster.GetServer(cluster.GetLeader())
	c.Assert(s.leaderServer.BootstrapCluster(), IsNil)
	s.putRegion(1, 1, []byte(""), []byte("a"))
	s.putRegion(2, 1, []byte("a"), []byte("b"))
	s.putRegion(3, 1, []byte("b"), []byte(""))
	s.cluster = cluster
}

func (s *testRegionsAPISuite) TearDownSuite(c *C) {
	s.cleanup()
	s.cluster.Destroy()
}

func (s *testRegionsAPISuite) putRegion(regionID, storeID uint64, start, end []byte, opts ...core.RegionCreateOption) {
	leader := &metapb.Peer{
		Id:      regionID,
		StoreId: storeID,
	}
	metaRegion := &metapb.Region{
		Id:          regionID,
		StartKey:    start,
		EndKey:      end,
		Peers:       []*metapb.Peer{leader},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
	}
	newOpts := []core.RegionCreateOption{
		core.SetApproximateKeys(1000),
		core.SetApproximateSize(10),
		core.SetWrittenBytes(100 * 1024 * 1024),
		core.SetWrittenKeys(1 * 1024 * 1024),
		core.SetReadBytes(200 * 1024 * 1024),
		core.SetReadKeys(2 * 1024 * 1024),
	}
	newOpts = append(newOpts, opts...)
	region := core.NewRegionInfo(metaRegion, leader, newOpts...)
	s.leaderServer.GetRaftCluster().HandleRegionHeartbeat(region)
}

func (s *testRegionsAPISuite) TestRegionGet(c *C) {
	resp, err := dialClient.Get(s.leaderServer.GetServer().GetAddr() + regionsPrefix + "/1")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	buf, err := io.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	got := &handlers.RegionInfo{}
	c.Assert(json.Unmarshal(buf, &got), IsNil)
	checkRegionsInfo(c, []*handlers.RegionInfo{got}, []*metapb.Region{s.leaderServer.GetRegionInfoByID(1).GetMeta()})
	c.Assert(got.ApproximateKeys, Equals, uint64(1000))
	c.Assert(got.ApproximateSize, Equals, uint64(10))
	c.Assert(got.WrittenBytes, Equals, uint64(100*1024*1024))
	c.Assert(got.WrittenKeys, Equals, uint64(1*1024*1024))
	c.Assert(got.ReadBytes, Equals, uint64(200*1024*1024))
	c.Assert(got.ReadKeys, Equals, uint64(2*1024*1024))
}

func (s *testRegionsAPISuite) TestRegionsGet(c *C) {
	resp, err := dialClient.Get(s.leaderServer.GetServer().GetAddr() + regionsPrefix)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	buf, err := io.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	got := &handlers.RegionsInfo{}
	c.Assert(json.Unmarshal(buf, &got), IsNil)
	checkRegionsInfo(c, got.Regions, []*metapb.Region{
		s.leaderServer.GetRegionInfoByID(1).GetMeta(),
		s.leaderServer.GetRegionInfoByID(2).GetMeta(),
		s.leaderServer.GetRegionInfoByID(3).GetMeta(),
	})
}

func checkRegionsInfo(c *C, rs []*handlers.RegionInfo, want []*metapb.Region) {
	c.Assert(len(rs), Equals, len(want))
	mapWant := make(map[uint64]*metapb.Region)
	for _, r := range want {
		if _, ok := mapWant[r.Id]; !ok {
			mapWant[r.Id] = r
		}
	}
	for _, r := range rs {
		obtained := r
		expected := mapWant[obtained.ID]
		c.Assert(obtained.ID, DeepEquals, expected.GetId())
		c.Assert(obtained.StartKey, DeepEquals, core.HexRegionKeyStr(expected.GetStartKey()))
		c.Assert(obtained.EndKey, DeepEquals, core.HexRegionKeyStr(expected.GetEndKey()))
		c.Assert(obtained.RegionEpoch, DeepEquals, expected.GetRegionEpoch())
		c.Assert(obtained.Peers[0].ID, DeepEquals, expected.GetPeers()[0].Id)
		c.Assert(obtained.Peers[0].StoreID, DeepEquals, expected.GetPeers()[0].StoreId)
	}
}
