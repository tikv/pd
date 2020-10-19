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

package region_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/api"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/pdctl"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&regionTestSuite{})

type regionTestSuite struct{}

func (s *regionTestSuite) SetUpSuite(c *C) {
	server.EnableZap = true
}

func (s *regionTestSuite) TestRegionKeyFormat(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	url := cluster.GetConfig().GetClientURL()
	store := metapb.Store{
		Id:    1,
		State: metapb.StoreState_Up,
	}
	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)
	pdctl.MustPutStore(c, leaderServer.GetServer(), store.Id, store.State, store.Labels)

	echo := pdctl.GetEcho([]string{"-u", url, "region", "key", "--format=raw", " "})
	c.Assert(strings.Contains(echo, "unknown flag"), IsFalse)
}

func (s *regionTestSuite) TestRegion(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	pdAddr := cluster.GetConfig().GetClientURL()
	cmd := pdctl.InitCommand()

	store := metapb.Store{
		Id:    1,
		State: metapb.StoreState_Up,
	}
	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)
	pdctl.MustPutStore(c, leaderServer.GetServer(), store.Id, store.State, store.Labels)

	downPeer := &metapb.Peer{Id: 8, StoreId: 3}
	r1 := pdctl.MustPutRegion(c, cluster, 1, 1, []byte("a"), []byte("b"),
		core.SetWrittenBytes(1000), core.SetReadBytes(1000), core.SetRegionConfVer(1), core.SetRegionVersion(1), core.SetApproximateSize(10),
		core.SetPeers([]*metapb.Peer{
			{Id: 1, StoreId: 1},
			{Id: 5, StoreId: 2},
			{Id: 6, StoreId: 3},
			{Id: 7, StoreId: 4},
		}))
	r2 := pdctl.MustPutRegion(c, cluster, 2, 1, []byte("b"), []byte("c"),
		core.SetWrittenBytes(2000), core.SetReadBytes(0), core.SetRegionConfVer(2), core.SetRegionVersion(3), core.SetApproximateSize(20))
	r3 := pdctl.MustPutRegion(c, cluster, 3, 1, []byte("c"), []byte("d"),
		core.SetWrittenBytes(500), core.SetReadBytes(800), core.SetRegionConfVer(3), core.SetRegionVersion(2), core.SetApproximateSize(30),
		core.WithDownPeers([]*pdpb.PeerStats{{Peer: downPeer, DownSeconds: 3600}}),
		core.WithPendingPeers([]*metapb.Peer{downPeer}), core.WithLearners([]*metapb.Peer{{Id: 3, StoreId: 1}}))
	r4 := pdctl.MustPutRegion(c, cluster, 4, 1, []byte("d"), []byte("e"),
		core.SetWrittenBytes(100), core.SetReadBytes(100), core.SetRegionConfVer(1), core.SetRegionVersion(1), core.SetApproximateSize(10))
	defer cluster.Destroy()

	var testRegionsCases = []struct {
		args   []string
		expect []*core.RegionInfo
	}{
		// region command
		{[]string{"region"}, leaderServer.GetRegions()},
		// region sibling <region_id> command
		{[]string{"region", "sibling", "2"}, leaderServer.GetAdjacentRegions(leaderServer.GetRegionInfoByID(2))},
		// region store <store_id> command
		{[]string{"region", "store", "1"}, leaderServer.GetStoreRegions(1)},
		{[]string{"region", "store", "1"}, []*core.RegionInfo{r1, r2, r3, r4}},
		// region topread [limit] command
		{[]string{"region", "topread", "2"}, api.TopNRegions(leaderServer.GetRegions(), func(a, b *core.RegionInfo) bool { return a.GetBytesRead() < b.GetBytesRead() }, 2)},
		// region topwrite [limit] command
		{[]string{"region", "topwrite", "2"}, api.TopNRegions(leaderServer.GetRegions(), func(a, b *core.RegionInfo) bool { return a.GetBytesWritten() < b.GetBytesWritten() }, 2)},
		// region topconfver [limit] command
		{[]string{"region", "topconfver", "2"}, api.TopNRegions(leaderServer.GetRegions(), func(a, b *core.RegionInfo) bool {
			return a.GetMeta().GetRegionEpoch().GetConfVer() < b.GetMeta().GetRegionEpoch().GetConfVer()
		}, 2)},
		// region topversion [limit] command
		{[]string{"region", "topversion", "2"}, api.TopNRegions(leaderServer.GetRegions(), func(a, b *core.RegionInfo) bool {
			return a.GetMeta().GetRegionEpoch().GetVersion() < b.GetMeta().GetRegionEpoch().GetVersion()
		}, 2)},
		// region topsize [limit] command
		{[]string{"region", "topsize", "2"}, api.TopNRegions(leaderServer.GetRegions(), func(a, b *core.RegionInfo) bool {
			return a.GetApproximateSize() < b.GetApproximateSize()
		}, 2)},
		// region check extra-peer command
		{[]string{"region", "check", "extra-peer"}, []*core.RegionInfo{r1}},
		// region check miss-peer command
		{[]string{"region", "check", "miss-peer"}, []*core.RegionInfo{r2, r3, r4}},
		// region check pending-peer command
		{[]string{"region", "check", "pending-peer"}, []*core.RegionInfo{r3}},
		// region check down-peer command
		{[]string{"region", "check", "down-peer"}, []*core.RegionInfo{r3}},
		// region check learner-peer command
		{[]string{"region", "check", "learner-peer"}, []*core.RegionInfo{r3}},
		// region startkey --format=raw <key> command
		{[]string{"region", "startkey", "--format=raw", "b", "2"}, []*core.RegionInfo{r2, r3}},
		// region startkey --format=hex <key> command
		{[]string{"region", "startkey", "--format=hex", "63", "2"}, []*core.RegionInfo{r3, r4}},
	}

	for _, testCase := range testRegionsCases {
		args := append([]string{"-u", pdAddr}, testCase.args...)
		_, output, e := pdctl.ExecuteCommandC(cmd, args...)
		c.Assert(e, IsNil)
		regionsInfo := api.RegionsInfo{}
		c.Assert(json.Unmarshal(output, &regionsInfo), IsNil)
		pdctl.CheckRegionsInfo(c, regionsInfo, testCase.expect)
	}

	var testRegionCases = []struct {
		args   []string
		expect *api.RegionInfo
	}{
		// region <region_id> command
		{[]string{"region", "1"}, api.NewRegionInfo(leaderServer.GetRegionInfoByID(1))},
		// region key --format=raw <key> command
		{[]string{"region", "key", "--format=raw", "b"}, api.NewRegionInfo(r2)},
		// region key --format=hex <key> command
		{[]string{"region", "key", "--format=hex", "62"}, api.NewRegionInfo(r2)},
		// issue #2351
		{[]string{"region", "key", "--format=hex", "622f62"}, api.NewRegionInfo(r2)},
	}

	for _, testCase := range testRegionCases {
		args := append([]string{"-u", pdAddr}, testCase.args...)
		_, output, e := pdctl.ExecuteCommandC(cmd, args...)
		c.Assert(e, IsNil)
		regionInfo := api.RegionInfo{}
		c.Assert(json.Unmarshal(output, &regionInfo), IsNil)
		c.Assert(&regionInfo, DeepEquals, testCase.expect)
	}
}
