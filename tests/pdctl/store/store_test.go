// Copyright 2019 PingCAP, Inc.
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

package store_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/v4/server"
	"github.com/pingcap/pd/v4/server/api"
	"github.com/pingcap/pd/v4/server/schedule"
	"github.com/pingcap/pd/v4/tests"
	"github.com/pingcap/pd/v4/tests/pdctl"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&storeTestSuite{})

type storeTestSuite struct{}

func (s *storeTestSuite) SetUpSuite(c *C) {
	server.EnableZap = true
}

func (s *storeTestSuite) TestStore(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	pdAddr := cluster.GetConfig().GetClientURL()
	cmd := pdctl.InitCommand()

	stores := []*metapb.Store{
		{
			Id:      1,
			Address: "tikv1",
			State:   metapb.StoreState_Up,
			Version: "2.0.0",
		},
		{
			Id:      3,
			Address: "tikv3",
			State:   metapb.StoreState_Up,
			Version: "2.0.0",
		},
		{
			Id:      2,
			Address: "tikv2",
			State:   metapb.StoreState_Tombstone,
			Version: "2.0.0",
		},
	}

	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)

	for _, store := range stores {
		pdctl.MustPutStore(c, leaderServer.GetServer(), store.Id, store.State, store.Labels)
	}
	defer cluster.Destroy()

	// store command
	args := []string{"-u", pdAddr, "store"}
	_, output, err := pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	storesInfo := new(api.StoresInfo)
	c.Assert(json.Unmarshal(output, &storesInfo), IsNil)
	pdctl.CheckStoresInfo(c, storesInfo.Stores, stores[:2])

	// store <store_id> command
	args = []string{"-u", pdAddr, "store", "1"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	storeInfo := new(api.StoreInfo)
	c.Assert(json.Unmarshal(output, &storeInfo), IsNil)
	pdctl.CheckStoresInfo(c, []*api.StoreInfo{storeInfo}, stores[:1])

	// store label <store_id> <key> <value> [<key> <value>]... [flags] command
	c.Assert(storeInfo.Store.Labels, IsNil)
	args = []string{"-u", pdAddr, "store", "label", "1", "zone", "cn"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args = []string{"-u", pdAddr, "store", "1"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(json.Unmarshal(output, &storeInfo), IsNil)
	label := storeInfo.Store.Labels[0]
	c.Assert(label.Key, Equals, "zone")
	c.Assert(label.Value, Equals, "cn")

	// store label <store_id> <key> <value> <key> <value>... command
	args = []string{"-u", pdAddr, "store", "label", "1", "zone", "us", "language", "English"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args = []string{"-u", pdAddr, "store", "1"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(json.Unmarshal(output, &storeInfo), IsNil)
	label0 := storeInfo.Store.Labels[0]
	c.Assert(label0.Key, Equals, "zone")
	c.Assert(label0.Value, Equals, "us")
	label1 := storeInfo.Store.Labels[1]
	c.Assert(label1.Key, Equals, "language")
	c.Assert(label1.Value, Equals, "English")

	// store label <store_id> <key> <value> <key> <value>... -f command
	args = []string{"-u", pdAddr, "store", "label", "1", "zone", "uk", "-f"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args = []string{"-u", pdAddr, "store", "1"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(json.Unmarshal(output, &storeInfo), IsNil)
	label0 = storeInfo.Store.Labels[0]
	c.Assert(label0.Key, Equals, "zone")
	c.Assert(label0.Value, Equals, "uk")
	c.Assert(len(storeInfo.Store.Labels), Equals, 1)

	// store weight <store_id> <leader_weight> <region_weight> command
	c.Assert(storeInfo.Status.LeaderWeight, Equals, float64(1))
	c.Assert(storeInfo.Status.RegionWeight, Equals, float64(1))
	args = []string{"-u", pdAddr, "store", "weight", "1", "5", "10"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args = []string{"-u", pdAddr, "store", "1"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(json.Unmarshal(output, &storeInfo), IsNil)
	c.Assert(storeInfo.Status.LeaderWeight, Equals, float64(5))
	c.Assert(storeInfo.Status.RegionWeight, Equals, float64(10))

	// store limit <store_id> <rate>
	args = []string{"-u", pdAddr, "store", "limit", "1", "10"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	limits := leaderServer.GetRaftCluster().GetOperatorController().GetAllStoresLimit()
	c.Assert(limits[1].Rate()*60, Equals, float64(10))

	// store limit all <rate>
	args = []string{"-u", pdAddr, "store", "limit", "all", "20"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	limits = leaderServer.GetRaftCluster().GetOperatorController().GetAllStoresLimit()
	c.Assert(limits[1].Rate()*60, Equals, float64(20))
	c.Assert(limits[3].Rate()*60, Equals, float64(20))
	_, ok := limits[2]
	c.Assert(ok, IsFalse)

	// store limit
	args = []string{"-u", pdAddr, "store", "limit"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	limits = leaderServer.GetRaftCluster().GetOperatorController().GetAllStoresLimit()
	c.Assert(limits[1].Rate()*60, Equals, float64(20))
	c.Assert(limits[3].Rate()*60, Equals, float64(20))
	_, ok = limits[2]
	c.Assert(ok, IsFalse)

	// store delete <store_id> command
	c.Assert(storeInfo.Store.State, Equals, metapb.StoreState_Up)
	args = []string{"-u", pdAddr, "store", "delete", "1"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args = []string{"-u", pdAddr, "store", "1"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(json.Unmarshal(output, &storeInfo), IsNil)
	c.Assert(storeInfo.Store.State, Equals, metapb.StoreState_Offline)

	// store delete addr <address>
	args = []string{"-u", pdAddr, "store", "delete", "addr", "tikv3"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(string(output), Equals, "Success!\n")
	c.Assert(err, IsNil)
	args = []string{"-u", pdAddr, "store", "3"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(json.Unmarshal(output, &storeInfo), IsNil)
	c.Assert(storeInfo.Store.State, Equals, metapb.StoreState_Offline)

	// store remove-tombstone
	args = []string{"-u", pdAddr, "store", "remove-tombstone"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args = []string{"-u", pdAddr, "store"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	storesInfo = new(api.StoresInfo)
	c.Assert(json.Unmarshal(output, &storesInfo), IsNil)
	c.Assert(len([]*api.StoreInfo{storeInfo}), Equals, 1)

	// It should be called after stores remove-tombstone.
	echo := pdctl.GetEcho([]string{"-u", pdAddr, "stores", "show", "limit"})
	c.Assert(strings.Contains(echo, "PANIC"), IsFalse)

	// store limit-scene
	args = []string{"-u", pdAddr, "store", "limit-scene"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	scene := &schedule.StoreLimitScene{}
	err = json.Unmarshal(output, scene)
	c.Assert(err, IsNil)
	c.Assert(scene, DeepEquals, schedule.DefaultStoreLimitScene())

	// store limit-scene <scene> <rate>
	args = []string{"-u", pdAddr, "store", "limit-scene", "idle", "200"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args = []string{"-u", pdAddr, "store", "limit-scene"}
	scene = &schedule.StoreLimitScene{}
	_, output, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	err = json.Unmarshal(output, scene)
	c.Assert(err, IsNil)
	c.Assert(scene.Idle, Equals, 200)
}
