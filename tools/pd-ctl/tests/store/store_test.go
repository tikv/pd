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
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/response"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/server/config"
	pdTests "github.com/tikv/pd/tests"
	ctl "github.com/tikv/pd/tools/pd-ctl/pdctl"
	"github.com/tikv/pd/tools/pd-ctl/tests"
	"go.etcd.io/etcd/client/pkg/v3/transport"
)

func TestStoreLimitV2(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := pdTests.NewTestCluster(ctx, 1)
	re.NoError(err)
	err = cluster.RunInitialServers()
	re.NoError(err)
	re.NotEmpty(cluster.WaitLeader())
	pdAddr := cluster.GetConfig().GetClientURL()
	cmd := ctl.GetRootCmd()

	leaderServer := cluster.GetLeaderServer()
	re.NoError(leaderServer.BootstrapCluster())
	defer cluster.Destroy()

	// store command
	args := []string{"-u", pdAddr, "config", "set", "store-limit-version", "v2"}
	_, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)

	args = []string{"-u", pdAddr, "store", "limit"}
	output, err := tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Contains(string(output), "not support get limit")

	args = []string{"-u", pdAddr, "store", "limit", "1", "10"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Contains(string(output), "not support set limit")
}

func TestStore(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := pdTests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer cluster.Destroy()
	err = cluster.RunInitialServers()
	re.NoError(err)
	re.NotEmpty(cluster.WaitLeader())
	pdAddr := cluster.GetConfig().GetClientURL()
	cmd := ctl.GetRootCmd()

	stores := []*response.StoreInfo{
		{
			Store: &response.MetaStore{
				Store: &metapb.Store{
					Id:            1,
					State:         metapb.StoreState_Up,
					NodeState:     metapb.NodeState_Serving,
					LastHeartbeat: time.Now().UnixNano(),
				},
				StateName: metapb.StoreState_Up.String(),
			},
		},
		{
			Store: &response.MetaStore{
				Store: &metapb.Store{
					Id:            3,
					State:         metapb.StoreState_Up,
					NodeState:     metapb.NodeState_Serving,
					LastHeartbeat: time.Now().UnixNano(),
				},
				StateName: metapb.StoreState_Up.String(),
			},
		},
		{
			Store: &response.MetaStore{
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

	leaderServer := cluster.GetLeaderServer()
	re.NoError(leaderServer.BootstrapCluster())

	for _, store := range stores {
		pdTests.MustPutStore(re, cluster, store.Store.Store)
	}
	defer cluster.Destroy()

	// store command
	args := []string{"-u", pdAddr, "store"}
	output, err := tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	storesInfo := new(response.StoresInfo)
	re.NoError(json.Unmarshal(output, &storesInfo))

	tests.CheckStoresInfo(re, storesInfo.Stores, stores[:2])

	// store --state=<query states> command
	args = []string{"-u", pdAddr, "store", "--state", "Up,Tombstone"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.NotContains(string(output), "\"state\":")
	storesInfo = new(response.StoresInfo)
	re.NoError(json.Unmarshal(output, &storesInfo))

	tests.CheckStoresInfo(re, storesInfo.Stores, stores)

	// store <store_id> command
	args = []string{"-u", pdAddr, "store", "1"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	storeInfo := new(response.StoreInfo)
	re.NoError(json.Unmarshal(output, &storeInfo))

	tests.CheckStoresInfo(re, []*response.StoreInfo{storeInfo}, stores[:1])
	re.Nil(storeInfo.Store.Labels)

	// store <store_id> label command
	labelTestCases := []struct {
		args              []string
		newArgs           []string
		expectLabelLength int
		expectKeys        []string
		expectValues      []string
	}{
		{ // add label
			args:              []string{"-u", pdAddr, "store", "label", "1", "zone", "cn"},
			newArgs:           []string{"-u", pdAddr, "store", "label", "1", "zone=cn"},
			expectLabelLength: 1,
			expectKeys:        []string{"zone"},
			expectValues:      []string{"cn"},
		},
		{ // update label
			args:              []string{"-u", pdAddr, "store", "label", "1", "zone", "us", "language", "English"},
			newArgs:           []string{"-u", pdAddr, "store", "label", "1", "zone=us", "language=English"},
			expectLabelLength: 2,
			expectKeys:        []string{"zone", "language"},
			expectValues:      []string{"us", "English"},
		},
		{ // rewrite label
			args:              []string{"-u", pdAddr, "store", "label", "1", "zone", "uk", "-f"},
			newArgs:           []string{"-u", pdAddr, "store", "label", "1", "zone=uk", "--rewrite"},
			expectLabelLength: 1,
			expectKeys:        []string{"zone"},
			expectValues:      []string{"uk"},
		},
		{ // delete label
			args:              []string{"-u", pdAddr, "store", "label", "1", "zone", "--delete"},
			newArgs:           []string{"-u", pdAddr, "store", "label", "1", "zone", "--delete"},
			expectLabelLength: 0,
			expectKeys:        []string{""},
			expectValues:      []string{""},
		},
	}
	for i := 0; i <= 1; i++ {
		for _, testcase := range labelTestCases {
			switch {
			case i == 0: // old way
				args = testcase.args
			case i == 1: // new way
				args = testcase.newArgs
			}
			cmd := ctl.GetRootCmd()
			storeInfo := new(response.StoreInfo)
			_, err = tests.ExecuteCommand(cmd, args...)
			re.NoError(err)
			args = []string{"-u", pdAddr, "store", "1"}
			output, err = tests.ExecuteCommand(cmd, args...)
			re.NoError(err)
			re.NoError(json.Unmarshal(output, &storeInfo))
			labels := storeInfo.Store.Labels
			re.Len(labels, testcase.expectLabelLength)
			for i := range testcase.expectLabelLength {
				re.Equal(testcase.expectKeys[i], labels[i].Key)
				re.Equal(testcase.expectValues[i], labels[i].Value)
			}
		}
	}

	// store weight <store_id> <leader_weight> <region_weight> command
	re.Equal(float64(1), storeInfo.Status.LeaderWeight)
	re.Equal(float64(1), storeInfo.Status.RegionWeight)
	args = []string{"-u", pdAddr, "store", "weight", "1", "5", "10"}
	_, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	args = []string{"-u", pdAddr, "store", "1"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.NoError(json.Unmarshal(output, &storeInfo))

	re.Equal(float64(5), storeInfo.Status.LeaderWeight)
	re.Equal(float64(10), storeInfo.Status.RegionWeight)

	// store limit <store_id> <rate>
	args = []string{"-u", pdAddr, "store", "limit", "1", "10"}
	_, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	limit := leaderServer.GetRaftCluster().GetStoreLimitByType(1, storelimit.AddPeer)
	re.Equal(float64(10), limit)
	limit = leaderServer.GetRaftCluster().GetStoreLimitByType(1, storelimit.RemovePeer)
	re.Equal(float64(10), limit)

	// store limit <store_id> <rate> <type>
	args = []string{"-u", pdAddr, "store", "limit", "1", "5", "remove-peer"}
	_, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	limit = leaderServer.GetRaftCluster().GetStoreLimitByType(1, storelimit.RemovePeer)
	re.Equal(float64(5), limit)
	limit = leaderServer.GetRaftCluster().GetStoreLimitByType(1, storelimit.AddPeer)
	re.Equal(float64(10), limit)

	// store limit all <rate>
	args = []string{"-u", pdAddr, "store", "limit", "all", "20"}
	_, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	limit1 := leaderServer.GetRaftCluster().GetStoreLimitByType(1, storelimit.AddPeer)
	limit2 := leaderServer.GetRaftCluster().GetStoreLimitByType(2, storelimit.AddPeer)
	limit3 := leaderServer.GetRaftCluster().GetStoreLimitByType(3, storelimit.AddPeer)
	re.Equal(float64(20), limit1)
	re.Equal(float64(20), limit2)
	re.Equal(float64(20), limit3)
	limit1 = leaderServer.GetRaftCluster().GetStoreLimitByType(1, storelimit.RemovePeer)
	limit2 = leaderServer.GetRaftCluster().GetStoreLimitByType(2, storelimit.RemovePeer)
	limit3 = leaderServer.GetRaftCluster().GetStoreLimitByType(3, storelimit.RemovePeer)
	re.Equal(float64(20), limit1)
	re.Equal(float64(20), limit2)
	re.Equal(float64(20), limit3)

	re.NoError(leaderServer.Stop())
	re.NoError(leaderServer.Run())

	re.NotEmpty(cluster.WaitLeader())
	storesLimit := leaderServer.GetPersistOptions().GetAllStoresLimit()
	re.Equal(float64(20), storesLimit[1].AddPeer)
	re.Equal(float64(20), storesLimit[1].RemovePeer)

	// store limit all <rate> <type>
	args = []string{"-u", pdAddr, "store", "limit", "all", "25", "remove-peer"}
	_, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	limit1 = leaderServer.GetRaftCluster().GetStoreLimitByType(1, storelimit.RemovePeer)
	limit3 = leaderServer.GetRaftCluster().GetStoreLimitByType(3, storelimit.RemovePeer)
	re.Equal(float64(25), limit1)
	re.Equal(float64(25), limit3)
	limit2 = leaderServer.GetRaftCluster().GetStoreLimitByType(2, storelimit.RemovePeer)
	re.Equal(float64(25), limit2)

	// store limit all <key> <value> <rate> <type>
	args = []string{"-u", pdAddr, "store", "label", "1", "zone=uk"}
	_, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	args = []string{"-u", pdAddr, "store", "limit", "all", "zone", "uk", "20", "remove-peer"}
	_, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	limit1 = leaderServer.GetRaftCluster().GetStoreLimitByType(1, storelimit.RemovePeer)
	re.Equal(float64(20), limit1)

	// store limit all 0 is invalid
	args = []string{"-u", pdAddr, "store", "limit", "all", "0"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Contains(string(output), "rate should be a number that > 0")

	// store limit <type>
	args = []string{"-u", pdAddr, "store", "limit"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)

	allAddPeerLimit := make(map[string]map[string]any)
	json.Unmarshal(output, &allAddPeerLimit)
	re.Equal(float64(20), allAddPeerLimit["1"]["add-peer"].(float64))
	re.Equal(float64(20), allAddPeerLimit["3"]["add-peer"].(float64))
	_, ok := allAddPeerLimit["2"]["add-peer"]
	re.False(ok)

	args = []string{"-u", pdAddr, "store", "limit", "remove-peer"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)

	allRemovePeerLimit := make(map[string]map[string]any)
	json.Unmarshal(output, &allRemovePeerLimit)
	re.Equal(float64(20), allRemovePeerLimit["1"]["remove-peer"].(float64))
	re.Equal(float64(25), allRemovePeerLimit["3"]["remove-peer"].(float64))
	_, ok = allRemovePeerLimit["2"]["add-peer"]
	re.False(ok)

	// put enough stores for replica.
	for id := 1000; id <= 1005; id++ {
		store2 := &metapb.Store{
			Id:            uint64(id),
			State:         metapb.StoreState_Up,
			NodeState:     metapb.NodeState_Serving,
			LastHeartbeat: time.Now().UnixNano(),
		}
		pdTests.MustPutStore(re, cluster, store2)
	}

	// store delete <store_id> command
	storeInfo.Store.State = metapb.StoreState(metapb.StoreState_value[storeInfo.Store.StateName])
	re.Equal(metapb.StoreState_Up, storeInfo.Store.State)
	args = []string{"-u", pdAddr, "store", "remove", "1"} // it means remove-tombstone
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.NotContains(string(output), "Success")
	args = []string{"-u", pdAddr, "store", "delete", "1"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Contains(string(output), "Success")
	args = []string{"-u", pdAddr, "store", "1"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	storeInfo = new(response.StoreInfo)
	re.NoError(json.Unmarshal(output, &storeInfo))

	storeInfo.Store.State = metapb.StoreState(metapb.StoreState_value[storeInfo.Store.StateName])
	re.Equal(metapb.StoreState_Offline, storeInfo.Store.State)

	// store check status
	args = []string{"-u", pdAddr, "store", "check", "Offline"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Contains(string(output), "\"id\": 1,")
	args = []string{"-u", pdAddr, "store", "check", "Tombstone"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Contains(string(output), "\"id\": 2,")
	args = []string{"-u", pdAddr, "store", "check", "Up"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Contains(string(output), "\"id\": 3,")
	args = []string{"-u", pdAddr, "store", "check", "Invalid_State"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Contains(string(output), "unknown StoreState: Invalid_state")

	// Mock a disconnected store.
	storeCheck := &metapb.Store{
		Id:            uint64(2000),
		State:         metapb.StoreState_Up,
		NodeState:     metapb.NodeState_Serving,
		LastHeartbeat: time.Now().UnixNano() - int64(1*time.Minute),
	}
	pdTests.MustPutStore(re, cluster, storeCheck)
	args = []string{"-u", pdAddr, "store", "check", "Disconnected"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Contains(string(output), "\"id\": 2000,")
	// Mock a down store.
	storeCheck.Id = uint64(2001)
	storeCheck.LastHeartbeat = time.Now().UnixNano() - int64(1*time.Hour)
	pdTests.MustPutStore(re, cluster, storeCheck)
	args = []string{"-u", pdAddr, "store", "check", "Down"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Contains(string(output), "\"id\": 2001,")

	// store cancel-delete <store_id> command
	limit = leaderServer.GetRaftCluster().GetStoreLimitByType(1, storelimit.RemovePeer)
	re.Equal(storelimit.Unlimited, limit)
	args = []string{"-u", pdAddr, "store", "cancel-delete", "1"}
	_, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	args = []string{"-u", pdAddr, "store", "1"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	storeInfo = new(response.StoreInfo)
	re.NoError(json.Unmarshal(output, &storeInfo))

	re.Equal(metapb.StoreState_Up, storeInfo.Store.State)
	limit = leaderServer.GetRaftCluster().GetStoreLimitByType(1, storelimit.RemovePeer)
	re.Equal(20.0, limit)

	// store delete addr <address>
	args = []string{"-u", pdAddr, "store", "delete", "addr", "tikv3"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.Equal("Success!\n", string(output))
	re.NoError(err)

	args = []string{"-u", pdAddr, "store", "3"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	storeInfo = new(response.StoreInfo)
	re.NoError(json.Unmarshal(output, &storeInfo))

	storeInfo.Store.State = metapb.StoreState(metapb.StoreState_value[storeInfo.Store.StateName])
	re.Equal(metapb.StoreState_Offline, storeInfo.Store.State)

	// store cancel-delete addr <address>
	limit = leaderServer.GetRaftCluster().GetStoreLimitByType(3, storelimit.RemovePeer)
	re.Equal(storelimit.Unlimited, limit)
	args = []string{"-u", pdAddr, "store", "cancel-delete", "addr", "tikv3"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.Equal("Success!\n", string(output))
	re.NoError(err)
	args = []string{"-u", pdAddr, "store", "3"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	storeInfo = new(response.StoreInfo)
	re.NoError(json.Unmarshal(output, &storeInfo))

	re.Equal(metapb.StoreState_Up, storeInfo.Store.State)
	limit = leaderServer.GetRaftCluster().GetStoreLimitByType(3, storelimit.RemovePeer)
	re.Equal(25.0, limit)

	// store remove-tombstone
	args = []string{"-u", pdAddr, "store", "check", "Tombstone"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	storesInfo = new(response.StoresInfo)
	re.NoError(json.Unmarshal(output, &storesInfo))

	re.Equal(1, storesInfo.Count)
	args = []string{"-u", pdAddr, "store", "remove-tombstone"}
	_, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	args = []string{"-u", pdAddr, "store", "check", "Tombstone"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	storesInfo = new(response.StoresInfo)
	re.NoError(json.Unmarshal(output, &storesInfo))

	re.Equal(0, storesInfo.Count)

	// It should be called after stores remove-tombstone.
	args = []string{"-u", pdAddr, "stores", "show", "limit"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.NotContains(string(output), "PANIC")

	args = []string{"-u", pdAddr, "stores", "show", "limit", "remove-peer"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.NotContains(string(output), "PANIC")

	args = []string{"-u", pdAddr, "stores", "show", "limit", "add-peer"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.NotContains(string(output), "PANIC")
	// store limit-scene
	args = []string{"-u", pdAddr, "store", "limit-scene"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	scene := &storelimit.Scene{}
	err = json.Unmarshal(output, scene)
	re.NoError(err)
	re.Equal(storelimit.DefaultScene(storelimit.AddPeer), scene)

	// store limit-scene <scene> <rate>
	args = []string{"-u", pdAddr, "store", "limit-scene", "idle", "200"}
	_, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	args = []string{"-u", pdAddr, "store", "limit-scene"}
	scene = &storelimit.Scene{}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	err = json.Unmarshal(output, scene)
	re.NoError(err)
	re.Equal(200, scene.Idle)

	// store limit-scene <scene> <rate> <type>
	args = []string{"-u", pdAddr, "store", "limit-scene", "idle", "100", "remove-peer"}
	_, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	args = []string{"-u", pdAddr, "store", "limit-scene", "remove-peer"}
	scene = &storelimit.Scene{}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	err = json.Unmarshal(output, scene)
	re.NoError(err)
	re.Equal(100, scene.Idle)

	// store limit all 201 is invalid for all
	args = []string{"-u", pdAddr, "store", "limit", "all", "201"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Contains(string(output), "rate should less than")

	// store limit all 201 is invalid for label
	args = []string{"-u", pdAddr, "store", "limit", "all", "engine", "key", "201", "add-peer"}
	output, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Contains(string(output), "rate should less than")
}

// https://github.com/tikv/pd/issues/5024
func TestTombstoneStore(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := pdTests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer cluster.Destroy()
	err = cluster.RunInitialServers()
	re.NoError(err)
	re.NotEmpty(cluster.WaitLeader())
	pdAddr := cluster.GetConfig().GetClientURL()
	cmd := ctl.GetRootCmd()

	stores := []*response.StoreInfo{
		{
			Store: &response.MetaStore{
				Store: &metapb.Store{
					Id:            2,
					State:         metapb.StoreState_Tombstone,
					NodeState:     metapb.NodeState_Removed,
					LastHeartbeat: time.Now().UnixNano(),
				},
				StateName: metapb.StoreState_Tombstone.String(),
			},
		},
		{
			Store: &response.MetaStore{
				Store: &metapb.Store{
					Id:            3,
					State:         metapb.StoreState_Tombstone,
					NodeState:     metapb.NodeState_Removed,
					LastHeartbeat: time.Now().UnixNano(),
				},
				StateName: metapb.StoreState_Tombstone.String(),
			},
		},
		{
			Store: &response.MetaStore{
				Store: &metapb.Store{
					Id:            4,
					State:         metapb.StoreState_Tombstone,
					NodeState:     metapb.NodeState_Removed,
					LastHeartbeat: time.Now().UnixNano(),
				},
				StateName: metapb.StoreState_Tombstone.String(),
			},
		},
	}

	leaderServer := cluster.GetLeaderServer()
	re.NoError(leaderServer.BootstrapCluster())

	for _, store := range stores {
		pdTests.MustPutStore(re, cluster, store.Store.Store)
	}
	defer cluster.Destroy()
	pdTests.MustPutRegion(re, cluster, 1, 2, []byte("a"), []byte("b"), core.SetWrittenBytes(3000000000), core.SetReportInterval(0, utils.RegionHeartBeatReportInterval))
	pdTests.MustPutRegion(re, cluster, 2, 3, []byte("b"), []byte("c"), core.SetWrittenBytes(3000000000), core.SetReportInterval(0, utils.RegionHeartBeatReportInterval))
	// store remove-tombstone
	args := []string{"-u", pdAddr, "store", "remove-tombstone"}
	output, err := tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	message := string(output)
	re.Contains(message, "2")
	re.Contains(message, "3")
}

func TestStoreTLS(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	certPath := filepath.Join("..", "cert")
	certScript := filepath.Join("..", "cert_opt.sh")
	// generate certs
	if err := os.Mkdir(certPath, 0755); err != nil {
		t.Fatal(err)
	}
	if err := exec.Command(certScript, "generate", certPath).Run(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := exec.Command(certScript, "cleanup", certPath).Run(); err != nil {
			t.Fatal(err)
		}
		if err := os.RemoveAll(certPath); err != nil {
			t.Fatal(err)
		}
	}()

	tlsInfo := transport.TLSInfo{
		KeyFile:       filepath.Join(certPath, "pd-server-key.pem"),
		CertFile:      filepath.Join(certPath, "pd-server.pem"),
		TrustedCAFile: filepath.Join(certPath, "ca.pem"),
	}
	cluster, err := pdTests.NewTestCluster(ctx, 1, func(conf *config.Config, _ string) {
		conf.Security.TLSConfig = grpcutil.TLSConfig{
			KeyPath:  tlsInfo.KeyFile,
			CertPath: tlsInfo.CertFile,
			CAPath:   tlsInfo.TrustedCAFile,
		}
		conf.AdvertiseClientUrls = strings.ReplaceAll(conf.AdvertiseClientUrls, "http", "https")
		conf.ClientUrls = strings.ReplaceAll(conf.ClientUrls, "http", "https")
		conf.AdvertisePeerUrls = strings.ReplaceAll(conf.AdvertisePeerUrls, "http", "https")
		conf.PeerUrls = strings.ReplaceAll(conf.PeerUrls, "http", "https")
		conf.InitialCluster = strings.ReplaceAll(conf.InitialCluster, "http", "https")
	})
	re.NoError(err)
	defer cluster.Destroy()
	err = cluster.RunInitialServers()
	re.NoError(err)
	re.NotEmpty(cluster.WaitLeader())
	cmd := ctl.GetRootCmd()

	stores := []*response.StoreInfo{
		{
			Store: &response.MetaStore{
				Store: &metapb.Store{
					Id:            1,
					State:         metapb.StoreState_Up,
					NodeState:     metapb.NodeState_Serving,
					LastHeartbeat: time.Now().UnixNano(),
				},
				StateName: metapb.StoreState_Up.String(),
			},
		},
		{
			Store: &response.MetaStore{
				Store: &metapb.Store{
					Id:            2,
					State:         metapb.StoreState_Up,
					NodeState:     metapb.NodeState_Serving,
					LastHeartbeat: time.Now().UnixNano(),
				},
				StateName: metapb.StoreState_Up.String(),
			},
		},
	}

	leaderServer := cluster.GetLeaderServer()
	re.NoError(leaderServer.BootstrapCluster())

	for _, store := range stores {
		pdTests.MustPutStore(re, cluster, store.Store.Store)
	}
	defer cluster.Destroy()

	pdAddr := cluster.GetConfig().GetClientURL()
	pdAddr = strings.ReplaceAll(pdAddr, "http", "https")
	// store command
	args := []string{"-u", pdAddr, "store",
		"--cacert=" + filepath.Join("..", "cert", "ca.pem"),
		"--cert=" + filepath.Join("..", "cert", "client.pem"),
		"--key=" + filepath.Join("..", "cert", "client-key.pem")}
	output, err := tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	storesInfo := new(response.StoresInfo)
	re.NoError(json.Unmarshal(output, &storesInfo))
	tests.CheckStoresInfo(re, storesInfo.Stores, stores)
}
