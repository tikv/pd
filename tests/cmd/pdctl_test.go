// Copyright 2018 PingCAP, Inc.
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

package cmd_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/gogo/protobuf/proto"
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/pkg/etcdutil"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/api"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/tests"
	"github.com/pingcap/pd/tools/pd-ctl/pdctl"
	"github.com/pingcap/pd/tools/pd-ctl/pdctl/command"
	"github.com/spf13/cobra"

	// Register schedulers.
	_ "github.com/pingcap/pd/server/schedulers"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&cmdTestSuite{})

type cmdTestSuite struct{}

func (s *cmdTestSuite) SetUpSuite(c *C) {
	server.EnableZap = true
}

func (s *cmdTestSuite) TestClusterAndPing(c *C) {
	c.Parallel()

	cluster, err := tests.NewTestCluster(1)
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	pdAddr := cluster.GetConfig().GetClientURLs()
	cmd := initCommand()
	defer cluster.Destroy()

	// cluster
	args := []string{"-u", pdAddr, "cluster"}
	_, output, err := executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	ci := &metapb.Cluster{}
	c.Assert(json.Unmarshal(output, ci), IsNil)
	c.Assert(ci, DeepEquals, cluster.GetCluster())

	// cluster status
	args = []string{"-u", pdAddr, "cluster", "status"}
	_, output, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	ci = &metapb.Cluster{}
	c.Assert(json.Unmarshal(output, ci), IsNil)
	c.Assert(ci, DeepEquals, cluster.GetCluster())

	// ping
	args = []string{"-u", pdAddr, "ping"}
	_, output, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(output, NotNil)
}

func (s *cmdTestSuite) TestHealth(c *C) {
	c.Parallel()

	cluster, err := tests.NewTestCluster(3)
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	pdAddr := cluster.GetConfig().GetClientURLs()
	cmd := initCommand()
	defer cluster.Destroy()

	client := cluster.GetEtcdClient()
	members, err := server.GetMembers(client)
	c.Assert(err, IsNil)
	unhealthMembers := cluster.CheckHealth(members)
	healths := []api.Health{}
	for _, member := range members {
		h := api.Health{
			Name:       member.Name,
			MemberID:   member.MemberId,
			ClientUrls: member.ClientUrls,
			Health:     true,
		}
		if _, ok := unhealthMembers[member.GetMemberId()]; ok {
			h.Health = false
		}
		healths = append(healths, h)
	}

	// health command
	args := []string{"-u", pdAddr, "health"}
	_, output, err := executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	h := make([]api.Health, len(healths))
	c.Assert(json.Unmarshal(output, &h), IsNil)
	c.Assert(err, IsNil)
	c.Assert(h, DeepEquals, healths)
}

func (s *cmdTestSuite) TestStore(c *C) {
	c.Parallel()

	cluster, err := tests.NewTestCluster(1)
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	pdAddr := cluster.GetConfig().GetClientURLs()
	cmd := initCommand()

	stores := []*metapb.Store{
		{
			Id:      1,
			Address: "tikv1",
			State:   metapb.StoreState_Up,
			Version: "2.0.0",
		},
		{
			Id:      2,
			Address: "tikv3",
			State:   metapb.StoreState_Tombstone,
			Version: "2.0.0",
		},
	}

	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)

	for _, store := range stores {
		mustPutStore(c, leaderServer.GetServer(), store.Id, store.State, store.Labels)
	}
	defer cluster.Destroy()

	// store command
	args := []string{"-u", pdAddr, "store"}
	_, output, err := executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	storesInfo := new(api.StoresInfo)
	c.Assert(json.Unmarshal(output, &storesInfo), IsNil)
	checkStoresInfo(c, storesInfo.Stores, stores[:1])

	// store <store_id> command
	args = []string{"-u", pdAddr, "store", "1"}
	_, output, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	storeInfo := new(api.StoreInfo)
	c.Assert(json.Unmarshal(output, &storeInfo), IsNil)
	checkStoresInfo(c, []*api.StoreInfo{storeInfo}, stores[:1])

	// store label <store_id> <key> <value> command
	c.Assert(storeInfo.Store.Labels, IsNil)
	args = []string{"-u", pdAddr, "store", "label", "1", "zone", "cn"}
	_, _, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args = []string{"-u", pdAddr, "store", "1"}
	_, output, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(json.Unmarshal(output, &storeInfo), IsNil)
	label := storeInfo.Store.Labels[0]
	c.Assert(label.Key, Equals, "zone")
	c.Assert(label.Value, Equals, "cn")

	// store weight <store_id> <leader_weight> <region_weight> command
	c.Assert(storeInfo.Status.LeaderWeight, Equals, float64(1))
	c.Assert(storeInfo.Status.RegionWeight, Equals, float64(1))
	args = []string{"-u", pdAddr, "store", "weight", "1", "5", "10"}
	_, _, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args = []string{"-u", pdAddr, "store", "1"}
	_, output, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(json.Unmarshal(output, &storeInfo), IsNil)
	c.Assert(storeInfo.Status.LeaderWeight, Equals, float64(5))
	c.Assert(storeInfo.Status.RegionWeight, Equals, float64(10))

	// store delete <store_id> command
	c.Assert(storeInfo.Store.State, Equals, metapb.StoreState_Up)
	args = []string{"-u", pdAddr, "store", "delete", "1"}
	_, _, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args = []string{"-u", pdAddr, "store", "1"}
	_, output, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(json.Unmarshal(output, &storeInfo), IsNil)
	c.Assert(storeInfo.Store.State, Equals, metapb.StoreState_Offline)

	args = []string{"-u", pdAddr, "stores", "remove-tombstone"}
	_, _, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args = []string{"-u", pdAddr, "store"}
	_, output, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	storesInfo = new(api.StoresInfo)
	c.Assert(json.Unmarshal(output, &storesInfo), IsNil)
	c.Assert(len([]*api.StoreInfo{storeInfo}), Equals, 1)
}

func (s *cmdTestSuite) TestLabel(c *C) {
	c.Parallel()

	cluster, err := tests.NewTestCluster(1)
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	pdAddr := cluster.GetConfig().GetClientURLs()
	cmd := initCommand()

	stores := []*metapb.Store{
		{
			Id:      1,
			Address: "tikv1",
			State:   metapb.StoreState_Up,
			Labels: []*metapb.StoreLabel{
				{
					Key:   "zone",
					Value: "us-west",
				},
			},
			Version: "2.0.0",
		},
		{
			Id:      2,
			Address: "tikv2",
			State:   metapb.StoreState_Up,
			Labels: []*metapb.StoreLabel{
				{
					Key:   "zone",
					Value: "us-east",
				},
			},
			Version: "2.0.0",
		},
		{
			Id:      3,
			Address: "tikv3",
			State:   metapb.StoreState_Up,
			Labels: []*metapb.StoreLabel{
				{
					Key:   "zone",
					Value: "us-west",
				},
			},
			Version: "2.0.0",
		},
	}

	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)

	for _, store := range stores {
		mustPutStore(c, leaderServer.GetServer(), store.Id, store.State, store.Labels)
	}
	defer cluster.Destroy()

	// label command
	args := []string{"-u", pdAddr, "label"}
	_, output, err := executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	labels := make([]*metapb.StoreLabel, 0, len(stores))
	c.Assert(json.Unmarshal(output, &labels), IsNil)
	got := make(map[string]struct{})
	for _, l := range labels {
		if _, ok := got[strings.ToLower(l.Key+l.Value)]; !ok {
			got[strings.ToLower(l.Key+l.Value)] = struct{}{}
		}
	}
	expected := make(map[string]struct{})
	ss := leaderServer.GetStores()
	for _, s := range ss {
		ls := s.GetLabels()
		for _, l := range ls {
			if _, ok := expected[strings.ToLower(l.Key+l.Value)]; !ok {
				expected[strings.ToLower(l.Key+l.Value)] = struct{}{}
			}
		}
	}
	c.Assert(got, DeepEquals, expected)

	// label store <name> command
	args = []string{"-u", pdAddr, "label", "store", "zone", "us-west"}
	_, output, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	storesInfo := new(api.StoresInfo)
	c.Assert(json.Unmarshal(output, &storesInfo), IsNil)
	ss = []*metapb.Store{stores[0], stores[2]}
	checkStoresInfo(c, storesInfo.Stores, ss)
}

func (s *cmdTestSuite) TestTSO(c *C) {
	c.Parallel()
	cmd := initCommand()

	const (
		physicalShiftBits = 18
		logicalBits       = 0x3FFFF
	)

	// tso command
	ts := "395181938313123110"
	args := []string{"-u", "127.0.0.1", "tso", ts}
	_, output, err := executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	t, e := strconv.ParseUint(ts, 10, 64)
	c.Assert(e, IsNil)
	c.Assert(err, IsNil)
	logicalTime := t & logicalBits
	physical := t >> physicalShiftBits
	physicalTime := time.Unix(int64(physical/1000), int64(physical%1000)*time.Millisecond.Nanoseconds())
	str := fmt.Sprintln("system: ", physicalTime) + fmt.Sprintln("logic: ", logicalTime)
	c.Assert(str, Equals, string(output))
}

func (s *cmdTestSuite) TestScheduler(c *C) {
	c.Parallel()

	cluster, err := tests.NewTestCluster(1)
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	pdAddr := cluster.GetConfig().GetClientURLs()
	cmd := initCommand()

	stores := []*metapb.Store{
		{
			Id:    1,
			State: metapb.StoreState_Up,
		},
		{
			Id:    2,
			State: metapb.StoreState_Up,
		},
		{
			Id:    3,
			State: metapb.StoreState_Up,
		},
		{
			Id:    4,
			State: metapb.StoreState_Up,
		},
	}

	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)
	for _, store := range stores {
		mustPutStore(c, leaderServer.GetServer(), store.Id, store.State, store.Labels)
	}

	mustPutRegion(c, cluster, 1, 1, []byte("a"), []byte("b"))
	defer cluster.Destroy()

	time.Sleep(3 * time.Second)
	// scheduler show command
	args := []string{"-u", pdAddr, "scheduler", "show"}
	_, output, err := executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	var schedulers []string
	c.Assert(json.Unmarshal(output, &schedulers), IsNil)
	expected := map[string]bool{
		"balance-region-scheduler":     true,
		"balance-leader-scheduler":     true,
		"balance-hot-region-scheduler": true,
		"label-scheduler":              true,
	}
	for _, scheduler := range schedulers {
		c.Assert(expected[scheduler], Equals, true)
	}

	// scheduler add command
	args = []string{"-u", pdAddr, "scheduler", "add", "grant-leader-scheduler", "1"}
	_, _, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args = []string{"-u", pdAddr, "scheduler", "show"}
	_, output, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	schedulers = schedulers[:0]
	c.Assert(json.Unmarshal(output, &schedulers), IsNil)
	expected = map[string]bool{
		"balance-region-scheduler":     true,
		"balance-leader-scheduler":     true,
		"balance-hot-region-scheduler": true,
		"label-scheduler":              true,
		"grant-leader-scheduler-1":     true,
	}
	for _, scheduler := range schedulers {
		c.Assert(expected[scheduler], Equals, true)
	}

	// scheduler delete command
	args = []string{"-u", pdAddr, "scheduler", "remove", "balance-region-scheduler"}
	_, _, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args = []string{"-u", pdAddr, "scheduler", "show"}
	_, output, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	schedulers = schedulers[:0]
	c.Assert(json.Unmarshal(output, &schedulers), IsNil)
	expected = map[string]bool{
		"balance-leader-scheduler":     true,
		"balance-hot-region-scheduler": true,
		"label-scheduler":              true,
		"grant-leader-scheduler-1":     true,
	}
	for _, scheduler := range schedulers {
		c.Assert(expected[scheduler], Equals, true)
	}
}

func (s *cmdTestSuite) TestRegion(c *C) {
	c.Parallel()

	cluster, err := tests.NewTestCluster(1)
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	pdAddr := cluster.GetConfig().GetClientURLs()
	cmd := initCommand()

	store := metapb.Store{
		Id:    1,
		State: metapb.StoreState_Up,
	}
	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)
	mustPutStore(c, leaderServer.GetServer(), store.Id, store.State, store.Labels)

	downPeer := &metapb.Peer{Id: 8, StoreId: 3}
	r1 := mustPutRegion(c, cluster, 1, 1, []byte("a"), []byte("b"),
		core.SetWrittenBytes(1000), core.SetReadBytes(1000), core.SetRegionConfVer(1), core.SetRegionVersion(1), core.SetApproximateSize(10),
		core.SetPeers([]*metapb.Peer{
			{Id: 1, StoreId: 1},
			{Id: 5, StoreId: 2},
			{Id: 6, StoreId: 3},
			{Id: 7, StoreId: 4},
		}))
	r2 := mustPutRegion(c, cluster, 2, 1, []byte("b"), []byte("c"),
		core.SetWrittenBytes(2000), core.SetReadBytes(0), core.SetRegionConfVer(2), core.SetRegionVersion(3), core.SetApproximateSize(20))
	r3 := mustPutRegion(c, cluster, 3, 1, []byte("c"), []byte("d"),
		core.SetWrittenBytes(500), core.SetReadBytes(800), core.SetRegionConfVer(3), core.SetRegionVersion(2), core.SetApproximateSize(30),
		core.WithDownPeers([]*pdpb.PeerStats{{Peer: downPeer, DownSeconds: 3600}}),
		core.WithPendingPeers([]*metapb.Peer{downPeer}))
	r4 := mustPutRegion(c, cluster, 4, 1, []byte("d"), []byte("e"),
		core.SetWrittenBytes(100), core.SetReadBytes(100), core.SetRegionConfVer(1), core.SetRegionVersion(1), core.SetApproximateSize(10))
	defer cluster.Destroy()

	// region command
	args := []string{"-u", pdAddr, "region"}
	_, output, err := executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	regionsInfo := api.RegionsInfo{}
	c.Assert(json.Unmarshal(output, &regionsInfo), IsNil)
	regions := leaderServer.GetRegions()
	checkRegionsInfo(c, regionsInfo, regions)

	// region <region_id> command
	args = []string{"-u", pdAddr, "region", "1"}
	_, output, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	regionInfo := api.RegionInfo{}
	c.Assert(json.Unmarshal(output, &regionInfo), IsNil)
	region := leaderServer.GetRegionInfoByID(1)
	c.Assert(api.NewRegionInfo(region), DeepEquals, &regionInfo)

	// region sibling <region_id> command
	args = []string{"-u", pdAddr, "region", "sibling", "2"}
	_, output, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	regionsInfo = api.RegionsInfo{}
	c.Assert(json.Unmarshal(output, &regionsInfo), IsNil)
	region = leaderServer.GetRegionInfoByID(2)
	regions = leaderServer.GetAdjacentRegions(region)
	checkRegionsInfo(c, regionsInfo, regions)

	// region store <store_id> command
	args = []string{"-u", pdAddr, "region", "store", "1"}
	_, output, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	regionsInfo = api.RegionsInfo{}
	c.Assert(json.Unmarshal(output, &regionsInfo), IsNil)
	regions = leaderServer.GetStoreRegions(1)
	checkRegionsInfo(c, regionsInfo, regions)

	// region topread [limit] command
	args = []string{"-u", pdAddr, "region", "topread", "2"}
	_, output, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	regionsInfo = api.RegionsInfo{}
	c.Assert(json.Unmarshal(output, &regionsInfo), IsNil)
	regions = api.TopNRegions(leaderServer.GetRegions(), func(a, b *core.RegionInfo) bool { return a.GetBytesRead() < b.GetBytesRead() }, 2)
	checkRegionsInfo(c, regionsInfo, regions)

	// region topwrite [limit] command
	args = []string{"-u", pdAddr, "region", "topwrite", "2"}
	_, output, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	regionsInfo = api.RegionsInfo{}
	c.Assert(json.Unmarshal(output, &regionsInfo), IsNil)
	regions = api.TopNRegions(leaderServer.GetRegions(), func(a, b *core.RegionInfo) bool { return a.GetBytesWritten() < b.GetBytesWritten() }, 2)
	checkRegionsInfo(c, regionsInfo, regions)

	// region topconfver [limit] command
	args = []string{"-u", pdAddr, "region", "topconfver", "2"}
	_, output, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	regionsInfo = api.RegionsInfo{}
	c.Assert(json.Unmarshal(output, &regionsInfo), IsNil)
	regions = api.TopNRegions(leaderServer.GetRegions(), func(a, b *core.RegionInfo) bool {
		return a.GetMeta().GetRegionEpoch().GetConfVer() < b.GetMeta().GetRegionEpoch().GetConfVer()
	}, 2)
	checkRegionsInfo(c, regionsInfo, regions)

	// region topversion [limit] command
	args = []string{"-u", pdAddr, "region", "topversion", "2"}
	_, output, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	regionsInfo = api.RegionsInfo{}
	c.Assert(json.Unmarshal(output, &regionsInfo), IsNil)
	regions = api.TopNRegions(leaderServer.GetRegions(), func(a, b *core.RegionInfo) bool {
		return a.GetMeta().GetRegionEpoch().GetVersion() < b.GetMeta().GetRegionEpoch().GetVersion()
	}, 2)
	checkRegionsInfo(c, regionsInfo, regions)

	// region topsize [limit] command
	args = []string{"-u", pdAddr, "region", "topsize", "2"}
	_, output, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	regionsInfo = api.RegionsInfo{}
	c.Assert(json.Unmarshal(output, &regionsInfo), IsNil)
	regions = api.TopNRegions(leaderServer.GetRegions(), func(a, b *core.RegionInfo) bool {
		return a.GetApproximateSize() < b.GetApproximateSize()
	}, 2)
	checkRegionsInfo(c, regionsInfo, regions)

	// region check extra-peer command
	args = []string{"-u", pdAddr, "region", "check", "extra-peer"}
	_, output, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	regionsInfo = api.RegionsInfo{}
	c.Assert(json.Unmarshal(output, &regionsInfo), IsNil)
	checkRegionsInfo(c, regionsInfo, []*core.RegionInfo{r1})

	// region check miss-peer command
	args = []string{"-u", pdAddr, "region", "check", "miss-peer"}
	_, output, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	regionsInfo = api.RegionsInfo{}
	c.Assert(json.Unmarshal(output, &regionsInfo), IsNil)
	checkRegionsInfo(c, regionsInfo, []*core.RegionInfo{r2, r3, r4})

	// region check pending-peer command
	args = []string{"-u", pdAddr, "region", "check", "pending-peer"}
	_, output, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	regionsInfo = api.RegionsInfo{}
	c.Assert(json.Unmarshal(output, &regionsInfo), IsNil)
	checkRegionsInfo(c, regionsInfo, []*core.RegionInfo{r3})

	// region check down-peer command
	args = []string{"-u", pdAddr, "region", "check", "down-peer"}
	_, output, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	regionsInfo = api.RegionsInfo{}
	c.Assert(json.Unmarshal(output, &regionsInfo), IsNil)
	checkRegionsInfo(c, regionsInfo, []*core.RegionInfo{r3})

	// region key --format=raw <key> command
	args = []string{"-u", pdAddr, "region", "key", "--format=raw", "b"}
	_, output, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	regionInfo = api.RegionInfo{}
	c.Assert(json.Unmarshal(output, &regionInfo), IsNil)
	c.Assert(&regionInfo, DeepEquals, api.NewRegionInfo(r2))

	// region key --format=hex <key> command
	args = []string{"-u", pdAddr, "region", "key", "--format=hex", "62"}
	_, output, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	regionInfo = api.RegionInfo{}
	c.Assert(json.Unmarshal(output, &regionInfo), IsNil)
	c.Assert(&regionInfo, DeepEquals, api.NewRegionInfo(r2))

	// region startkey --format=raw <key> command
	args = []string{"-u", pdAddr, "region", "startkey", "--format=raw", "b", "2"}
	_, output, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	regionsInfo = api.RegionsInfo{}
	c.Assert(json.Unmarshal(output, &regionsInfo), IsNil)
	checkRegionsInfo(c, regionsInfo, []*core.RegionInfo{r2, r3})

	// region startkey --format=hex <key> command
	args = []string{"-u", pdAddr, "region", "startkey", "--format=hex", "63", "2"}
	_, output, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	regionsInfo = api.RegionsInfo{}
	c.Assert(json.Unmarshal(output, &regionsInfo), IsNil)
	checkRegionsInfo(c, regionsInfo, []*core.RegionInfo{r3, r4})
}

func (s *cmdTestSuite) TestConfig(c *C) {
	c.Parallel()

	cluster, err := tests.NewTestCluster(1)
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	pdAddr := cluster.GetConfig().GetClientURLs()
	cmd := initCommand()

	store := metapb.Store{
		Id:    1,
		State: metapb.StoreState_Up,
	}
	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)
	svr := leaderServer.GetServer()
	mustPutStore(c, svr, store.Id, store.State, store.Labels)
	defer cluster.Destroy()

	// config show
	args := []string{"-u", pdAddr, "config", "show"}
	_, output, err := executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	scheduleCfg := server.ScheduleConfig{}
	c.Assert(json.Unmarshal(output, &scheduleCfg), IsNil)
	c.Assert(&scheduleCfg, DeepEquals, svr.GetScheduleConfig())

	// config show replication
	args = []string{"-u", pdAddr, "config", "show", "replication"}
	_, output, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	replicationCfg := server.ReplicationConfig{}
	c.Assert(json.Unmarshal(output, &replicationCfg), IsNil)
	c.Assert(&replicationCfg, DeepEquals, svr.GetReplicationConfig())

	// config show cluster-version
	args1 := []string{"-u", pdAddr, "config", "show", "cluster-version"}
	_, output, err = executeCommandC(cmd, args1...)
	c.Assert(err, IsNil)
	clusterVersion := semver.Version{}
	c.Assert(json.Unmarshal(output, &clusterVersion), IsNil)
	c.Assert(clusterVersion, DeepEquals, svr.GetClusterVersion())

	// config set cluster-version <value>
	args2 := []string{"-u", pdAddr, "config", "set", "cluster-version", "2.1.0-rc.5"}
	_, _, err = executeCommandC(cmd, args2...)
	c.Assert(err, IsNil)
	c.Assert(clusterVersion, Not(DeepEquals), svr.GetClusterVersion())
	_, output, err = executeCommandC(cmd, args1...)
	c.Assert(err, IsNil)
	clusterVersion = semver.Version{}
	c.Assert(json.Unmarshal(output, &clusterVersion), IsNil)
	c.Assert(clusterVersion, DeepEquals, svr.GetClusterVersion())

	// config show namespace <name> && config set namespace <type> <key> <value>
	args = []string{"-u", pdAddr, "table_ns", "create", "ts1"}
	_, _, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args = []string{"-u", pdAddr, "table_ns", "set_store", "1", "ts1"}
	_, _, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args1 = []string{"-u", pdAddr, "config", "show", "namespace", "ts1"}
	_, output, err = executeCommandC(cmd, args1...)
	c.Assert(err, IsNil)
	namespaceCfg := server.NamespaceConfig{}
	c.Assert(json.Unmarshal(output, &namespaceCfg), IsNil)
	args2 = []string{"-u", pdAddr, "config", "set", "namespace", "ts1", "region-schedule-limit", "128"}
	_, _, err = executeCommandC(cmd, args2...)
	c.Assert(err, IsNil)
	c.Assert(namespaceCfg.RegionScheduleLimit, Not(Equals), svr.GetNamespaceConfig("ts1").RegionScheduleLimit)
	_, output, err = executeCommandC(cmd, args1...)
	c.Assert(err, IsNil)
	namespaceCfg = server.NamespaceConfig{}
	c.Assert(json.Unmarshal(output, &namespaceCfg), IsNil)
	c.Assert(namespaceCfg.RegionScheduleLimit, Equals, svr.GetNamespaceConfig("ts1").RegionScheduleLimit)

	// config delete namespace <name>
	args3 := []string{"-u", pdAddr, "config", "delete", "namespace", "ts1"}
	_, _, err = executeCommandC(cmd, args3...)
	c.Assert(err, IsNil)
	_, output, err = executeCommandC(cmd, args1...)
	c.Assert(err, IsNil)
	namespaceCfg = server.NamespaceConfig{}
	c.Assert(json.Unmarshal(output, &namespaceCfg), IsNil)
	c.Assert(namespaceCfg.RegionScheduleLimit, Not(Equals), svr.GetNamespaceConfig("ts1").RegionScheduleLimit)

	// config show label-property
	args1 = []string{"-u", pdAddr, "config", "show", "label-property"}
	_, output, err = executeCommandC(cmd, args1...)
	c.Assert(err, IsNil)
	labelPropertyCfg := server.LabelPropertyConfig{}
	c.Assert(json.Unmarshal(output, &labelPropertyCfg), IsNil)
	c.Assert(labelPropertyCfg, DeepEquals, svr.GetLabelProperty())

	// config set label-property <type> <key> <value>
	args2 = []string{"-u", pdAddr, "config", "set", "label-property", "reject-leader", "zone", "cn"}
	_, _, err = executeCommandC(cmd, args2...)
	c.Assert(err, IsNil)
	c.Assert(labelPropertyCfg, Not(DeepEquals), svr.GetLabelProperty())
	_, output, err = executeCommandC(cmd, args1...)
	c.Assert(err, IsNil)
	labelPropertyCfg = server.LabelPropertyConfig{}
	c.Assert(json.Unmarshal(output, &labelPropertyCfg), IsNil)
	c.Assert(labelPropertyCfg, DeepEquals, svr.GetLabelProperty())

	// config delete label-property <type> <key> <value>
	args3 = []string{"-u", pdAddr, "config", "delete", "label-property", "reject-leader", "zone", "cn"}
	_, _, err = executeCommandC(cmd, args3...)
	c.Assert(err, IsNil)
	c.Assert(labelPropertyCfg, Not(DeepEquals), svr.GetLabelProperty())
	_, output, err = executeCommandC(cmd, args1...)
	c.Assert(err, IsNil)
	labelPropertyCfg = server.LabelPropertyConfig{}
	c.Assert(json.Unmarshal(output, &labelPropertyCfg), IsNil)
	c.Assert(labelPropertyCfg, DeepEquals, svr.GetLabelProperty())

	// config set <option> <value>
	args1 = []string{"-u", pdAddr, "config", "set", "leader-schedule-limit", "64"}
	_, _, err = executeCommandC(cmd, args1...)
	c.Assert(err, IsNil)
	args1 = []string{"-u", pdAddr, "config", "set", "hot-region-schedule-limit", "64"}
	_, _, err = executeCommandC(cmd, args1...)
	c.Assert(err, IsNil)
	args1 = []string{"-u", pdAddr, "config", "set", "hot-region-cache-hits-threshold", "5"}
	_, _, err = executeCommandC(cmd, args1...)
	c.Assert(err, IsNil)
	args2 = []string{"-u", pdAddr, "config", "show"}
	_, output, err = executeCommandC(cmd, args2...)
	c.Assert(err, IsNil)
	scheduleCfg = server.ScheduleConfig{}
	c.Assert(json.Unmarshal(output, &scheduleCfg), IsNil)
	c.Assert(scheduleCfg.LeaderScheduleLimit, Equals, svr.GetScheduleConfig().LeaderScheduleLimit)
	c.Assert(scheduleCfg.HotRegionScheduleLimit, Equals, svr.GetScheduleConfig().HotRegionScheduleLimit)
	c.Assert(scheduleCfg.HotRegionCacheHitsThreshold, Equals, svr.GetScheduleConfig().HotRegionCacheHitsThreshold)
	c.Assert(scheduleCfg.HotRegionCacheHitsThreshold, Equals, uint64(5))
	c.Assert(scheduleCfg.HotRegionScheduleLimit, Equals, uint64(64))
	c.Assert(scheduleCfg.LeaderScheduleLimit, Equals, uint64(64))
	args1 = []string{"-u", pdAddr, "config", "set", "disable-raft-learner", "true"}
	_, _, err = executeCommandC(cmd, args1...)
	c.Assert(err, IsNil)
	args2 = []string{"-u", pdAddr, "config", "show"}
	_, output, err = executeCommandC(cmd, args2...)
	c.Assert(err, IsNil)
	scheduleCfg = server.ScheduleConfig{}
	c.Assert(json.Unmarshal(output, &scheduleCfg), IsNil)
	c.Assert(scheduleCfg.DisableLearner, Equals, svr.GetScheduleConfig().DisableLearner)
}

func (s *cmdTestSuite) TestLog(c *C) {
	c.Parallel()

	cluster, err := tests.NewTestCluster(1)
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	pdAddr := cluster.GetConfig().GetClientURLs()
	cmd := initCommand()

	store := metapb.Store{
		Id:    1,
		State: metapb.StoreState_Up,
	}
	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)
	svr := leaderServer.GetServer()
	mustPutStore(c, svr, store.Id, store.State, store.Labels)
	defer cluster.Destroy()

	var testCases = []struct {
		cmd    []string
		expect string
	}{
		// log [fatal|error|warn|info|debug]
		{
			cmd:    []string{"-u", pdAddr, "log", "fatal"},
			expect: "fatal",
		},
		{
			cmd:    []string{"-u", pdAddr, "log", "error"},
			expect: "error",
		},
		{
			cmd:    []string{"-u", pdAddr, "log", "warn"},
			expect: "warn",
		},
		{
			cmd:    []string{"-u", pdAddr, "log", "info"},
			expect: "info",
		},
		{
			cmd:    []string{"-u", pdAddr, "log", "debug"},
			expect: "debug",
		},
	}

	for _, testCase := range testCases {
		_, _, err = executeCommandC(cmd, testCase.cmd...)
		c.Assert(err, IsNil)
		c.Assert(svr.GetConfig().Log.Level, Equals, testCase.expect)
	}
}

func (s *cmdTestSuite) TestTableNS(c *C) {
	c.Parallel()

	cluster, err := tests.NewTestCluster(1)
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	pdAddr := cluster.GetConfig().GetClientURLs()
	cmd := initCommand()

	store := metapb.Store{
		Id:    1,
		State: metapb.StoreState_Up,
	}
	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)
	svr := leaderServer.GetServer()
	mustPutStore(c, svr, store.Id, store.State, store.Labels)
	classifier := svr.GetClassifier()
	defer cluster.Destroy()

	// table_ns create <namespace>
	c.Assert(svr.IsNamespaceExist("ts1"), IsFalse)
	args := []string{"-u", pdAddr, "table_ns", "create", "ts1"}
	_, _, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(svr.IsNamespaceExist("ts1"), IsTrue)

	// table_ns add <name> <table_id>
	args = []string{"-u", pdAddr, "table_ns", "add", "ts1", "1"}
	_, _, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(classifier.IsTableIDExist(1), IsTrue)

	// table_ns remove <name> <table_id>
	args = []string{"-u", pdAddr, "table_ns", "remove", "ts1", "1"}
	_, _, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(classifier.IsTableIDExist(1), IsFalse)

	// table_ns set_meta <namespace>
	args = []string{"-u", pdAddr, "table_ns", "set_meta", "ts1"}
	_, _, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(classifier.IsMetaExist(), IsTrue)

	// table_ns rm_meta <namespace>
	args = []string{"-u", pdAddr, "table_ns", "rm_meta", "ts1"}
	_, _, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(classifier.IsMetaExist(), IsFalse)

	// table_ns set_store <store_id> <namespace>
	args = []string{"-u", pdAddr, "table_ns", "set_store", "1", "ts1"}
	_, _, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(classifier.IsStoreIDExist(1), IsTrue)

	// table_ns rm_store <store_id> <namespace>
	args = []string{"-u", pdAddr, "table_ns", "rm_store", "1", "ts1"}
	_, _, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(classifier.IsStoreIDExist(1), IsFalse)
}

func (s *cmdTestSuite) TestOperator(c *C) {
	c.Parallel()

	var err error
	var t time.Time
	t = t.Add(time.Hour)
	cluster, err := tests.NewTestCluster(3,
		func(conf *server.Config) { conf.Replication.MaxReplicas = 2 },
		func(conf *server.Config) { conf.Schedule.MaxStoreDownTime.Duration = time.Since(t) },
	)
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	pdAddr := cluster.GetConfig().GetClientURLs()
	cmd := initCommand()

	stores := []*metapb.Store{
		{
			Id:    1,
			State: metapb.StoreState_Up,
		},
		{
			Id:    2,
			State: metapb.StoreState_Up,
		},
		{
			Id:    3,
			State: metapb.StoreState_Up,
		},
	}

	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)
	for _, store := range stores {
		mustPutStore(c, leaderServer.GetServer(), store.Id, store.State, store.Labels)
	}

	mustPutRegion(c, cluster, 1, 1, []byte("a"), []byte("b"), core.SetPeers([]*metapb.Peer{
		{Id: 1, StoreId: 1},
		{Id: 2, StoreId: 2},
	}))
	mustPutRegion(c, cluster, 3, 2, []byte("b"), []byte("c"), core.SetPeers([]*metapb.Peer{
		{Id: 3, StoreId: 1},
		{Id: 4, StoreId: 2},
	}))
	defer cluster.Destroy()

	var testCases = []struct {
		cmd    []string
		show   []string
		expect string
		reset  []string
	}{
		{
			// operator add add-peer <region_id> <to_store_id>
			cmd:    []string{"-u", pdAddr, "operator", "add", "add-peer", "1", "3"},
			show:   []string{"-u", pdAddr, "operator", "show"},
			expect: "promote learner peer 1 on store 3",
			reset:  []string{"-u", pdAddr, "operator", "remove", "1"},
		},
		{
			// operator add remove-peer <region_id> <to_store_id>
			cmd:    []string{"-u", pdAddr, "operator", "add", "remove-peer", "1", "2"},
			show:   []string{"-u", pdAddr, "operator", "show"},
			expect: "remove peer on store 2",
			reset:  []string{"-u", pdAddr, "operator", "remove", "1"},
		},
		{
			// operator add transfer-leader <region_id> <to_store_id>
			cmd:    []string{"-u", pdAddr, "operator", "add", "transfer-leader", "1", "2"},
			show:   []string{"-u", pdAddr, "operator", "show", "leader"},
			expect: "transfer leader from store 1 to store 2",
			reset:  []string{"-u", pdAddr, "operator", "remove", "1"},
		},
		{
			// operator add transfer-region <region_id> <to_store_id>...
			cmd:    []string{"-u", pdAddr, "operator", "add", "transfer-region", "1", "2", "3"},
			show:   []string{"-u", pdAddr, "operator", "show", "region"},
			expect: "remove peer on store 1",
			reset:  []string{"-u", pdAddr, "operator", "remove", "1"},
		},
		{
			// operator add transfer-peer <region_id> <from_store_id> <to_store_id>
			cmd:    []string{"-u", pdAddr, "operator", "add", "transfer-peer", "1", "2", "3"},
			show:   []string{"-u", pdAddr, "operator", "show"},
			expect: "remove peer on store 2",
			reset:  []string{"-u", pdAddr, "operator", "remove", "1"},
		},
		{
			// operator add split-region <region_id> [--policy=scan|approximate]
			cmd:    []string{"-u", pdAddr, "operator", "add", "split-region", "3", "--policy=scan"},
			show:   []string{"-u", pdAddr, "operator", "show"},
			expect: "split region with policy SCAN",
			reset:  []string{"-u", pdAddr, "operator", "remove", "3"},
		},
		{
			// operator add split-region <region_id> [--policy=scan|approximate]
			cmd:    []string{"-u", pdAddr, "operator", "add", "split-region", "3", "--policy=approximate"},
			show:   []string{"-u", pdAddr, "operator", "show"},
			expect: "split region with policy APPROXIMATE",
			reset:  []string{"-u", pdAddr, "operator", "remove", "3"},
		},
		{
			// operator add split-region <region_id> [--policy=scan|approximate]
			cmd:    []string{"-u", pdAddr, "operator", "add", "split-region", "3", "--policy=scan"},
			show:   []string{"-u", pdAddr, "operator", "check", "3"},
			expect: "split region with policy SCAN",
			reset:  []string{"-u", pdAddr, "operator", "remove", "3"},
		},
		{
			// operator add split-region <region_id> [--policy=scan|approximate]
			cmd:    []string{"-u", pdAddr, "operator", "add", "split-region", "3", "--policy=approximate"},
			show:   []string{"-u", pdAddr, "operator", "check", "3"},
			expect: "status: RUNNING",
			reset:  []string{"-u", pdAddr, "operator", "remove", "3"},
		},
	}

	for _, testCase := range testCases {
		_, _, e := executeCommandC(cmd, testCase.cmd...)
		c.Assert(e, IsNil)
		_, output, e := executeCommandC(cmd, testCase.show...)
		c.Assert(e, IsNil)
		c.Assert(strings.Contains(string(output), testCase.expect), IsTrue)
		_, _, e = executeCommandC(cmd, testCase.reset...)
		c.Assert(e, IsNil)
	}

	// operator add merge-region <source_region_id> <target_region_id>
	args := []string{"-u", pdAddr, "operator", "add", "merge-region", "1", "3"}
	_, _, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args = []string{"-u", pdAddr, "operator", "show"}
	_, output, err := executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(output), "merge region 1 into region 3"), IsTrue)
	args = []string{"-u", pdAddr, "operator", "remove", "1"}
	_, _, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args = []string{"-u", pdAddr, "operator", "remove", "3"}
	_, _, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)

	// operator add scatter-region <region_id>
	args = []string{"-u", pdAddr, "operator", "add", "scatter-region", "3"}
	_, _, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args = []string{"-u", pdAddr, "operator", "add", "scatter-region", "1"}
	_, _, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args = []string{"-u", pdAddr, "operator", "show", "region"}
	_, output, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(output), "scatter-region"), IsTrue)
}

func (s *cmdTestSuite) TestMember(c *C) {
	c.Parallel()

	cluster, err := tests.NewTestCluster(3)
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	pdAddr := cluster.GetConfig().GetClientURLs()
	c.Assert(err, IsNil)
	cmd := initCommand()
	svr := cluster.GetServer("pd2")
	id := svr.GetServerID()
	name := svr.GetServer().Name()
	client := cluster.GetServer("pd1").GetEtcdClient()
	defer cluster.Destroy()

	// member leader show
	args := []string{"-u", pdAddr, "member", "leader", "show"}
	_, output, err := executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	leader := pdpb.Member{}
	c.Assert(json.Unmarshal(output, &leader), IsNil)
	c.Assert(&leader, DeepEquals, svr.GetLeader())

	// member leader transfer <member_name>
	args = []string{"-u", pdAddr, "member", "leader", "transfer", "pd2"}
	_, _, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert("pd2", Equals, svr.GetLeader().GetName())

	// member leader resign
	args = []string{"-u", pdAddr, "member", "leader", "resign"}
	_, _, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert("pd2", Not(Equals), svr.GetLeader().GetName())

	// member leader_priority <member_name> <priority>
	args = []string{"-u", pdAddr, "member", "leader_priority", name, "100"}
	_, _, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	priority, err := svr.GetServer().GetMemberLeaderPriority(id)
	c.Assert(err, IsNil)
	c.Assert(priority, Equals, 100)

	// member delete name <member_name>
	err = svr.Destroy()
	c.Assert(err, IsNil)
	members, err := etcdutil.ListEtcdMembers(client)
	c.Assert(err, IsNil)
	c.Assert(len(members.Members), Equals, 3)
	args = []string{"-u", pdAddr, "member", "delete", "name", name}
	_, _, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	members, err = etcdutil.ListEtcdMembers(client)
	c.Assert(err, IsNil)
	c.Assert(len(members.Members), Equals, 2)

	// member delete id <member_id>
	args = []string{"-u", pdAddr, "member", "delete", "id", string(id)}
	_, _, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	members, err = etcdutil.ListEtcdMembers(client)
	c.Assert(err, IsNil)
	c.Assert(len(members.Members), Equals, 2)
}

func (s *cmdTestSuite) TestHot(c *C) {
	c.Parallel()

	cluster, err := tests.NewTestCluster(1)
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	pdAddr := cluster.GetConfig().GetClientURLs()
	cmd := initCommand()

	store := metapb.Store{
		Id:      1,
		Address: "tikv1",
		State:   metapb.StoreState_Up,
		Version: "2.0.0",
	}

	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)
	mustPutStore(c, leaderServer.GetServer(), store.Id, store.State, store.Labels)
	defer cluster.Destroy()

	ss, err := leaderServer.GetStore(1)
	c.Assert(err, IsNil)
	now := time.Now().Second()
	interval := &pdpb.TimeInterval{StartTimestamp: uint64(now - 10), EndTimestamp: uint64(now)}
	newStats := proto.Clone(ss.GetStoreStats()).(*pdpb.StoreStats)
	bytesWritten := uint64(8 * 1024 * 1024)
	bytesRead := uint64(16 * 1024 * 1024)
	keysWritten := uint64(2000)
	keysRead := uint64(4000)
	newStats.BytesWritten = bytesWritten
	newStats.BytesRead = bytesRead
	newStats.KeysWritten = keysWritten
	newStats.KeysRead = keysRead
	newStats.Interval = interval
	newStore := ss.Clone(core.SetStoreStats(newStats))
	newStore.GetRollingStoreStats().Observe(newStore.GetStoreStats())

	// TODO: Provide a way to test the result of hot read and hot write commands
	// hot read
	args := []string{"-u", pdAddr, "hot", "read"}
	_, _, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)

	// hot write
	args = []string{"-u", pdAddr, "hot", "write"}
	_, _, err = executeCommandC(cmd, args...)
	c.Assert(err, IsNil)

	// hot store
	args = []string{"-u", pdAddr, "hot", "store"}
	_, output, err := executeCommandC(cmd, args...)
	c.Assert(err, IsNil)
	hotStores := api.HotStoreStats{}
	c.Assert(json.Unmarshal(output, &hotStores), IsNil)
	c.Assert(hotStores.BytesWriteStats[1], Equals, bytesWritten/10)
	c.Assert(hotStores.BytesReadStats[1], Equals, bytesRead/10)
	c.Assert(hotStores.KeysWriteStats[1], Equals, keysWritten/10)
	c.Assert(hotStores.KeysReadStats[1], Equals, keysRead/10)
}

func initCommand() *cobra.Command {
	commandFlags := pdctl.CommandFlags{}
	rootCmd := &cobra.Command{}
	rootCmd.PersistentFlags().StringVarP(&commandFlags.URL, "pd", "u", "", "")
	rootCmd.Flags().StringVar(&commandFlags.CAPath, "cacert", "", "")
	rootCmd.Flags().StringVar(&commandFlags.CertPath, "cert", "", "")
	rootCmd.Flags().StringVar(&commandFlags.KeyPath, "key", "", "")
	rootCmd.AddCommand(
		command.NewConfigCommand(),
		command.NewRegionCommand(),
		command.NewStoreCommand(),
		command.NewStoresCommand(),
		command.NewMemberCommand(),
		command.NewExitCommand(),
		command.NewLabelCommand(),
		command.NewPingCommand(),
		command.NewOperatorCommand(),
		command.NewSchedulerCommand(),
		command.NewTSOCommand(),
		command.NewHotSpotCommand(),
		command.NewClusterCommand(),
		command.NewTableNamespaceCommand(),
		command.NewHealthCommand(),
		command.NewLogCommand(),
	)
	return rootCmd
}

func executeCommandC(root *cobra.Command, args ...string) (c *cobra.Command, output []byte, err error) {
	buf := new(bytes.Buffer)
	root.SetOutput(buf)
	root.SetArgs(args)

	c, err = root.ExecuteC()
	return c, buf.Bytes(), err
}

func checkStoresInfo(c *C, stores []*api.StoreInfo, want []*metapb.Store) {
	c.Assert(len(stores), Equals, len(want))
	mapWant := make(map[uint64]*metapb.Store)
	for _, s := range want {
		if _, ok := mapWant[s.Id]; !ok {
			mapWant[s.Id] = s
		}
	}
	for _, s := range stores {
		c.Assert(s.Store.Store, DeepEquals, mapWant[s.Store.Store.Id])
	}
}

func checkRegionsInfo(c *C, output api.RegionsInfo, expected []*core.RegionInfo) {
	c.Assert(output.Count, Equals, len(expected))
	got := output.Regions
	sort.Slice(got, func(i, j int) bool {
		return got[i].ID < got[j].ID
	})
	sort.Slice(expected, func(i, j int) bool {
		return expected[i].GetID() < expected[j].GetID()
	})
	for i, region := range expected {
		c.Assert(api.NewRegionInfo(region), DeepEquals, got[i])
	}
}

func mustPutStore(c *C, svr *server.Server, id uint64, state metapb.StoreState, labels []*metapb.StoreLabel) {
	_, err := svr.PutStore(context.Background(), &pdpb.PutStoreRequest{
		Header: &pdpb.RequestHeader{ClusterId: svr.ClusterID()},
		Store: &metapb.Store{
			Id:      id,
			Address: fmt.Sprintf("tikv%d", id),
			State:   state,
			Labels:  labels,
			Version: server.MinSupportedVersion(server.Version2_0).String(),
		},
	})
	c.Assert(err, IsNil)
}

func mustPutRegion(c *C, cluster *tests.TestCluster, regionID, storeID uint64, start, end []byte, opts ...core.RegionCreateOption) *core.RegionInfo {
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
	r := core.NewRegionInfo(metaRegion, leader, opts...)
	err := cluster.HandleRegionHeartbeat(r)
	c.Assert(err, IsNil)
	return r
}
