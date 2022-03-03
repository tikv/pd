// Copyright 2022 TiKV Project Authors.
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

package handlers_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/docker/go-units"
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/apiv2/handlers"
	"github.com/tikv/pd/tests"
)

const storesPrefix = "/pd/api/v2/meta/stores"

var _ = Suite(&testStoresAPISuite{})

type testStoresAPISuite struct {
	cleanup      context.CancelFunc
	cluster      *tests.TestCluster
	leaderServer *tests.TestServer
	grpcServer   *server.GrpcServer
}

func (s *testStoresAPISuite) SetUpSuite(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	server.EnableZap = true
	s.cleanup = cancel
	cluster, err := tests.NewTestCluster(ctx, 1)
	c.Assert(err, IsNil)
	c.Assert(cluster.RunInitialServers(), IsNil)
	c.Assert(cluster.WaitLeader(), Not(HasLen), 0)
	s.leaderServer = cluster.GetServer(cluster.GetLeader())
	c.Assert(s.leaderServer.BootstrapCluster(), IsNil)
	clusterID := s.leaderServer.GetClusterID()
	storesReqs := []*pdpb.PutStoreRequest{
		{
			Header: &pdpb.RequestHeader{ClusterId: clusterID},
			Store: &metapb.Store{
				Id:            1,
				Address:       "mock-1",
				NodeState:     metapb.NodeState_Preparing,
				State:         metapb.StoreState_Up,
				LastHeartbeat: time.Now().UnixNano(),
			},
		},
		{
			Header: &pdpb.RequestHeader{ClusterId: clusterID},
			Store: &metapb.Store{
				Id:            4,
				Address:       "mock-4",
				NodeState:     metapb.NodeState_Preparing,
				State:         metapb.StoreState_Up,
				LastHeartbeat: time.Now().UnixNano(),
			},
		},
		{
			Header: &pdpb.RequestHeader{ClusterId: clusterID},
			Store: &metapb.Store{
				Id:            6,
				Address:       "mock-6",
				NodeState:     metapb.NodeState_Removing,
				State:         metapb.StoreState_Offline,
				LastHeartbeat: time.Now().UnixNano(),
			},
		},
		{
			Header: &pdpb.RequestHeader{ClusterId: clusterID},
			Store: &metapb.Store{
				Id:            7,
				Address:       "mock-7",
				NodeState:     metapb.NodeState_Removed,
				State:         metapb.StoreState_Tombstone,
				LastHeartbeat: time.Now().UnixNano(),
			},
		},
	}

	s.grpcServer = &server.GrpcServer{Server: s.leaderServer.GetServer()}
	for _, store := range storesReqs {
		_, err = s.grpcServer.PutStore(context.Background(), store)
		c.Assert(err, IsNil)
	}
	s.cluster = cluster
}

func (s *testStoresAPISuite) TearDownSuite(c *C) {
	s.cleanup()
	s.cluster.Destroy()
}

func (s *testStoresAPISuite) TestStoresGet(c *C) {
	url := s.leaderServer.GetServer().GetAddr() + storesPrefix
	resp, err := dialClient.Get(url)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	buf, err := io.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	got := &handlers.StoresInfo{}
	c.Assert(json.Unmarshal(buf, &got), IsNil)
	var expects []*handlers.MetaStore
	for _, s := range s.leaderServer.GetStores() {
		expects = append(expects, handlers.NewMetaStore(s, handlers.AliveStatusName))
	}
	checkStoresInfo(c, got.Stores, expects)

	resp, err = dialClient.Get(url + "?node_state=Preparing&&node_state=Serving")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	buf, err = io.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	got = &handlers.StoresInfo{}
	c.Assert(json.Unmarshal(buf, &got), IsNil)
	expects1 := []*handlers.MetaStore{
		handlers.NewMetaStore(s.leaderServer.GetStore(1).GetMeta(), handlers.AliveStatusName),
		handlers.NewMetaStore(s.leaderServer.GetStore(4).GetMeta(), handlers.AliveStatusName),
	}
	checkStoresInfo(c, got.Stores, expects1)

	resp, err = dialClient.Get(url + "?node_state=Removing")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	buf, err = io.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	got = &handlers.StoresInfo{}
	c.Assert(json.Unmarshal(buf, &got), IsNil)
	expects2 := []*handlers.MetaStore{handlers.NewMetaStore(s.leaderServer.GetStore(6).GetMeta(), handlers.AliveStatusName)}
	checkStoresInfo(c, got.Stores, expects2)
}

func (s *testStoresAPISuite) TestStoreGet(c *C) {
	s.grpcServer.StoreHeartbeat(
		context.Background(), &pdpb.StoreHeartbeatRequest{
			Header: &pdpb.RequestHeader{ClusterId: s.leaderServer.GetClusterID()},
			Stats: &pdpb.StoreStats{
				StoreId:   1,
				Capacity:  1798985089024,
				Available: 1709868695552,
				UsedSize:  85150956358,
			},
		},
	)

	resp, err := dialClient.Get(s.leaderServer.GetServer().GetAddr() + storesPrefix + "/1")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	buf, err := io.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	got := &handlers.StoreInfo{}
	c.Assert(json.Unmarshal(buf, &got), IsNil)
	expects := []*handlers.MetaStore{handlers.NewMetaStore(s.leaderServer.GetStore(1).GetMeta(), handlers.AliveStatusName)}
	checkStoresInfo(c, []*handlers.StoreInfo{got}, expects)
	capacity, _ := units.RAMInBytes("1.636TiB")
	available, _ := units.RAMInBytes("1.555TiB")
	c.Assert(int64(got.Status.Capacity), Equals, capacity)
	c.Assert(int64(got.Status.Available), Equals, available)
}

func (s *testStoresAPISuite) TestStoreSetNodeState(c *C) {
	url := s.leaderServer.GetServer().GetAddr() + storesPrefix
	c.Assert(getStoreNodeState(c, url+"/1"), Equals, metapb.NodeState_Preparing.String())
	setStoreNodeState(c, url+"/1", "Removing", http.StatusOK)

	// store not found
	setStoreNodeState(c, url+"/2", "Removing", http.StatusNotFound)

	// Invalid state.
	invalidStates := []string{"Foo", "Removed"}
	for _, state := range invalidStates {
		setStoreNodeState(c, url+"/1", state, http.StatusBadRequest)
	}

	// Set back to Serving.
	setStoreNodeState(c, url+"/1", "Serving", http.StatusOK)
	c.Assert(getStoreNodeState(c, url+"/1"), Equals, metapb.NodeState_Serving.String())
}

func checkStoresInfo(c *C, ss []*handlers.StoreInfo, expects []*handlers.MetaStore) {
	c.Assert(len(ss), Equals, len(expects))
	mapWant := make(map[uint64]*handlers.MetaStore)
	for _, s := range expects {
		if _, ok := mapWant[s.ID]; !ok {
			mapWant[s.ID] = s
		}
	}
	for _, s := range ss {
		obtained := s.Store
		expected := mapWant[obtained.ID]
		// Ignore lastHeartbeat
		obtained.LastHeartbeat, expected.LastHeartbeat = 0, 0
		c.Assert(obtained, DeepEquals, expected)
	}
}

func setStoreNodeState(c *C, url, state string, expectStatusCode int) {
	data := map[string]string{"node_state": state}
	putData, err := json.Marshal(data)
	c.Assert(err, IsNil)
	req, err := http.NewRequest(http.MethodPatch, url, bytes.NewBuffer(putData))
	c.Assert(err, IsNil)
	resp, err := dialClient.Do(req)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, expectStatusCode)
}

func getStoreNodeState(c *C, url string) string {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	c.Assert(err, IsNil)
	resp, err := dialClient.Do(req)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	buf, err := io.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	got := &handlers.StoreInfo{}
	c.Assert(json.Unmarshal(buf, &got), IsNil)
	return got.Store.NodeState.String()
}
