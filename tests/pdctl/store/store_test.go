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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package store_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/api"
	"github.com/tikv/pd/server/core/storelimit"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/pdctl"
	cmd "github.com/tikv/pd/tools/pd-ctl/pdctl"
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
	cmd := cmd.GetRootCmd()

	stores := []*api.StoreInfo{
		{
			Store: &api.MetaStore{
				Store: &metapb.Store{
					Id:            1,
					State:         metapb.StoreState_Up,
					LastHeartbeat: time.Now().UnixNano(),
				},
				StateName: metapb.StoreState_Up.String(),
			},
		},
		{
			Store: &api.MetaStore{
				Store: &metapb.Store{
					Id:            3,
					State:         metapb.StoreState_Up,
					LastHeartbeat: time.Now().UnixNano(),
				},
				StateName: metapb.StoreState_Up.String(),
			},
		},
		{
			Store: &api.MetaStore{
				Store: &metapb.Store{
					Id:            2,
					State:         metapb.StoreState_Tombstone,
					NodeState:     metapb.NodeState_Removed,
					LastHeartbeat: time.Now().UnixNano(),
				},
				StateName: metapb.StoreState_Tombstone.String(),
			},
		},
	}

	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)

	for _, store := range stores {
		pdctl.MustPutStore(c, leaderServer.GetServer(), store.Store.Store)
	}
	defer cluster.Destroy()

	// store command
	args := []string{"-u", pdAddr, "store"}
	output, err := pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	storesInfo := new(api.StoresInfo)
	c.Assert(json.Unmarshal(output, &storesInfo), IsNil)
	pdctl.CheckStoresInfo(c, storesInfo.Stores, stores[:2])

	// store --state=<query states> command
	args = []string{"-u", pdAddr, "store", "--state", "Up,Tombstone"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(output), "\"state\":"), Equals, false)
	storesInfo = new(api.StoresInfo)
	c.Assert(json.Unmarshal(output, &storesInfo), IsNil)
	pdctl.CheckStoresInfo(c, storesInfo.Stores, stores)

	// store <store_id> command
	args = []string{"-u", pdAddr, "store", "1"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	storeInfo := new(api.StoreInfo)
	c.Assert(json.Unmarshal(output, &storeInfo), IsNil)
	pdctl.CheckStoresInfo(c, []*api.StoreInfo{storeInfo}, stores[:1])

	// store label <store_id> <key> <value> [<key> <value>]... [flags] command
	c.Assert(storeInfo.Store.Labels, IsNil)
	args = []string{"-u", pdAddr, "store", "label", "1", "zone", "cn"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	args = []string{"-u", pdAddr, "store", "1"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(json.Unmarshal(output, &storeInfo), IsNil)
	label := storeInfo.Store.Labels[0]
	c.Assert(label.Key, Equals, "zone")
	c.Assert(label.Value, Equals, "cn")

	// store label <store_id> <key> <value> <key> <value>... command
	args = []string{"-u", pdAddr, "store", "label", "1", "zone", "us", "language", "English"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	args = []string{"-u", pdAddr, "store", "1"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
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
	_, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	args = []string{"-u", pdAddr, "store", "1"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(json.Unmarshal(output, &storeInfo), IsNil)
	label0 = storeInfo.Store.Labels[0]
	c.Assert(label0.Key, Equals, "zone")
	c.Assert(label0.Value, Equals, "uk")
	c.Assert(storeInfo.Store.Labels, HasLen, 1)

	// store weight <store_id> <leader_weight> <region_weight> command
	c.Assert(storeInfo.Status.LeaderWeight, Equals, float64(1))
	c.Assert(storeInfo.Status.RegionWeight, Equals, float64(1))
	args = []string{"-u", pdAddr, "store", "weight", "1", "5", "10"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	args = []string{"-u", pdAddr, "store", "1"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(json.Unmarshal(output, &storeInfo), IsNil)
	c.Assert(storeInfo.Status.LeaderWeight, Equals, float64(5))
	c.Assert(storeInfo.Status.RegionWeight, Equals, float64(10))

	// store limit <store_id> <rate>
	args = []string{"-u", pdAddr, "store", "limit", "1", "10"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	limit := leaderServer.GetRaftCluster().GetStoreLimitByType(1, storelimit.AddPeer)
	c.Assert(limit, Equals, float64(10))
	limit = leaderServer.GetRaftCluster().GetStoreLimitByType(1, storelimit.RemovePeer)
	c.Assert(limit, Equals, float64(10))

	// store limit <store_id> <rate> <type>
	args = []string{"-u", pdAddr, "store", "limit", "1", "5", "remove-peer"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	limit = leaderServer.GetRaftCluster().GetStoreLimitByType(1, storelimit.RemovePeer)
	c.Assert(limit, Equals, float64(5))
	limit = leaderServer.GetRaftCluster().GetStoreLimitByType(1, storelimit.AddPeer)
	c.Assert(limit, Equals, float64(10))

	// store limit all <rate>
	args = []string{"-u", pdAddr, "store", "limit", "all", "20"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	limit1 := leaderServer.GetRaftCluster().GetStoreLimitByType(1, storelimit.AddPeer)
	limit2 := leaderServer.GetRaftCluster().GetStoreLimitByType(2, storelimit.AddPeer)
	limit3 := leaderServer.GetRaftCluster().GetStoreLimitByType(3, storelimit.AddPeer)
	c.Assert(limit1, Equals, float64(20))
	c.Assert(limit2, Equals, float64(20))
	c.Assert(limit3, Equals, float64(20))
	limit1 = leaderServer.GetRaftCluster().GetStoreLimitByType(1, storelimit.RemovePeer)
	limit2 = leaderServer.GetRaftCluster().GetStoreLimitByType(2, storelimit.RemovePeer)
	limit3 = leaderServer.GetRaftCluster().GetStoreLimitByType(3, storelimit.RemovePeer)
	c.Assert(limit1, Equals, float64(20))
	c.Assert(limit2, Equals, float64(20))
	c.Assert(limit3, Equals, float64(20))

	c.Assert(leaderServer.Stop(), IsNil)
	c.Assert(leaderServer.Run(), IsNil)
	cluster.WaitLeader()
	storesLimit := leaderServer.GetPersistOptions().GetAllStoresLimit()
	c.Assert(storesLimit[1].AddPeer, Equals, float64(20))
	c.Assert(storesLimit[1].RemovePeer, Equals, float64(20))

	// store limit all <rate> <type>
	args = []string{"-u", pdAddr, "store", "limit", "all", "25", "remove-peer"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	limit1 = leaderServer.GetRaftCluster().GetStoreLimitByType(1, storelimit.RemovePeer)
	limit3 = leaderServer.GetRaftCluster().GetStoreLimitByType(3, storelimit.RemovePeer)
	c.Assert(limit1, Equals, float64(25))
	c.Assert(limit3, Equals, float64(25))
	limit2 = leaderServer.GetRaftCluster().GetStoreLimitByType(2, storelimit.RemovePeer)
	c.Assert(limit2, Equals, float64(25))

	// store limit all <key> <value> <rate> <type>
	args = []string{"-u", pdAddr, "store", "limit", "all", "zone", "uk", "20", "remove-peer"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	limit1 = leaderServer.GetRaftCluster().GetStoreLimitByType(1, storelimit.RemovePeer)
	c.Assert(limit1, Equals, float64(20))

	// store limit all 0 is invalid
	args = []string{"-u", pdAddr, "store", "limit", "all", "0"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(output), "rate should be a number that > 0"), IsTrue)

	// store limit <type>
	args = []string{"-u", pdAddr, "store", "limit"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)

	allAddPeerLimit := make(map[string]map[string]interface{})
	json.Unmarshal(output, &allAddPeerLimit)
	c.Assert(allAddPeerLimit["1"]["add-peer"].(float64), Equals, float64(20))
	c.Assert(allAddPeerLimit["3"]["add-peer"].(float64), Equals, float64(20))
	_, ok := allAddPeerLimit["2"]["add-peer"]
	c.Assert(ok, IsFalse)

	args = []string{"-u", pdAddr, "store", "limit", "remove-peer"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)

	allRemovePeerLimit := make(map[string]map[string]interface{})
	json.Unmarshal(output, &allRemovePeerLimit)
	c.Assert(allRemovePeerLimit["1"]["remove-peer"].(float64), Equals, float64(20))
	c.Assert(allRemovePeerLimit["3"]["remove-peer"].(float64), Equals, float64(25))
	_, ok = allRemovePeerLimit["2"]["add-peer"]
	c.Assert(ok, IsFalse)

	// store delete <store_id> command
	storeInfo.Store.State = metapb.StoreState(metapb.StoreState_value[storeInfo.Store.StateName])
	c.Assert(storeInfo.Store.State, Equals, metapb.StoreState_Up)
	args = []string{"-u", pdAddr, "store", "delete", "1"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	args = []string{"-u", pdAddr, "store", "1"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(json.Unmarshal(output, &storeInfo), IsNil)
	storeInfo.Store.State = metapb.StoreState(metapb.StoreState_value[storeInfo.Store.StateName])
	c.Assert(storeInfo.Store.State, Equals, metapb.StoreState_Offline)

	// store check status
	args = []string{"-u", pdAddr, "store", "check", "Offline"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(output), "\"id\": 1,"), IsTrue)
	args = []string{"-u", pdAddr, "store", "check", "Tombstone"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(output), "\"id\": 2,"), IsTrue)
	args = []string{"-u", pdAddr, "store", "check", "Up"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(output), "\"id\": 3,"), IsTrue)
	args = []string{"-u", pdAddr, "store", "check", "Invalid_State"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(output), "Unknown state: Invalid_state"), IsTrue)

	// store delete addr <address>
	args = []string{"-u", pdAddr, "store", "delete", "addr", "tikv3"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(string(output), Equals, "Success!\n")
	c.Assert(err, IsNil)
	args = []string{"-u", pdAddr, "store", "3"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(json.Unmarshal(output, &storeInfo), IsNil)
	storeInfo.Store.State = metapb.StoreState(metapb.StoreState_value[storeInfo.Store.StateName])
	c.Assert(storeInfo.Store.State, Equals, metapb.StoreState_Offline)

	// store remove-tombstone
	args = []string{"-u", pdAddr, "store", "remove-tombstone"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	args = []string{"-u", pdAddr, "store"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	storesInfo = new(api.StoresInfo)
	c.Assert(json.Unmarshal(output, &storesInfo), IsNil)
	c.Assert([]*api.StoreInfo{storeInfo}, HasLen, 1)

	// It should be called after stores remove-tombstone.
	args = []string{"-u", pdAddr, "stores", "show", "limit"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(output), "PANIC"), IsFalse)

	args = []string{"-u", pdAddr, "stores", "show", "limit", "remove-peer"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(output), "PANIC"), IsFalse)

	args = []string{"-u", pdAddr, "stores", "show", "limit", "add-peer"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(output), "PANIC"), IsFalse)
	// store limit-scene
	args = []string{"-u", pdAddr, "store", "limit-scene"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	scene := &storelimit.Scene{}
	err = json.Unmarshal(output, scene)
	c.Assert(err, IsNil)
	c.Assert(scene, DeepEquals, storelimit.DefaultScene(storelimit.AddPeer))

	// store limit-scene <scene> <rate>
	args = []string{"-u", pdAddr, "store", "limit-scene", "idle", "200"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	args = []string{"-u", pdAddr, "store", "limit-scene"}
	scene = &storelimit.Scene{}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	err = json.Unmarshal(output, scene)
	c.Assert(err, IsNil)
	c.Assert(scene.Idle, Equals, 200)

	// store limit-scene <scene> <rate> <type>
	args = []string{"-u", pdAddr, "store", "limit-scene", "idle", "100", "remove-peer"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	args = []string{"-u", pdAddr, "store", "limit-scene", "remove-peer"}
	scene = &storelimit.Scene{}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	err = json.Unmarshal(output, scene)
	c.Assert(err, IsNil)
	c.Assert(scene.Idle, Equals, 100)

	// store limit all 201 is invalid for all
	args = []string{"-u", pdAddr, "store", "limit", "all", "201"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(output), "rate should less than"), IsTrue)

	// store limit all 201 is invalid for label
	args = []string{"-u", pdAddr, "store", "limit", "all", "engine", "key", "201", "add-peer"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(output), "rate should less than"), IsTrue)
}
