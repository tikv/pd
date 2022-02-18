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
				Id:      1,
				Address: "mock-1",
				State:   metapb.StoreState_Up,
			},
		},
		{
			Header: &pdpb.RequestHeader{ClusterId: clusterID},
			Store: &metapb.Store{
				Id:      4,
				Address: "mock-4",
				State:   metapb.StoreState_Up,
			},
		},
		{
			Header: &pdpb.RequestHeader{ClusterId: clusterID},
			Store: &metapb.Store{
				Id:      6,
				Address: "mock-6",
				State:   metapb.StoreState_Offline,
			},
		},
		{
			Header: &pdpb.RequestHeader{ClusterId: clusterID},
			Store: &metapb.Store{
				Id:      7,
				Address: "mock-7",
				State:   metapb.StoreState_Tombstone,
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

func (s *testStoresAPISuite) TestStoresList(c *C) {
	url := s.leaderServer.GetServer().GetAddr() + storesPrefix
	resp, err := dialClient.Get(url)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	buf, err := io.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	got := &handlers.StoresInfo{}
	c.Assert(json.Unmarshal(buf, &got), IsNil)
	checkStoresInfo(c, got.Stores, s.leaderServer.GetStores())

	resp, err = dialClient.Get(url + "?state=Up")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	buf, err = io.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	got = &handlers.StoresInfo{}
	c.Assert(json.Unmarshal(buf, &got), IsNil)
	checkStoresInfo(c, got.Stores, []*metapb.Store{s.leaderServer.GetStore(1).GetMeta(), s.leaderServer.GetStore(4).GetMeta()})

	resp, err = dialClient.Get(url + "?state=Offline")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	buf, err = io.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	got = &handlers.StoresInfo{}
	c.Assert(json.Unmarshal(buf, &got), IsNil)
	checkStoresInfo(c, got.Stores, []*metapb.Store{s.leaderServer.GetStore(6).GetMeta()})
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
	checkStoresInfo(c, []*handlers.StoreInfo{got}, []*metapb.Store{s.leaderServer.GetStore(1).GetMeta()})
	capacity, _ := units.RAMInBytes("1.636TiB")
	available, _ := units.RAMInBytes("1.555TiB")
	c.Assert(int64(got.Status.Capacity), Equals, capacity)
	c.Assert(int64(got.Status.Available), Equals, available)
}

func (s *testStoresAPISuite) TestStoreSetState(c *C) {
	url := s.leaderServer.GetServer().GetAddr() + storesPrefix
	c.Assert(getStoreState(c, url+"/1"), Equals, "Up")
	setStoreState(c, url+"/1", "Offline", http.StatusOK)

	// store not found
	setStoreState(c, url+"/2", "Offline", http.StatusNotFound)

	// Invalid state.
	invalidStates := []string{"Foo", "Tombstone"}
	for _, state := range invalidStates {
		setStoreState(c, url+"/1", state, http.StatusBadRequest)
	}

	// Set back to Up.
	setStoreState(c, url+"/1", "Up", http.StatusOK)
	c.Assert(getStoreState(c, url+"/1"), Equals, "Up")
}

func checkStoresInfo(c *C, ss []*handlers.StoreInfo, want []*metapb.Store) {
	c.Assert(len(ss), Equals, len(want))
	mapWant := make(map[uint64]*metapb.Store)
	for _, s := range want {
		if _, ok := mapWant[s.Id]; !ok {
			mapWant[s.Id] = s
		}
	}
	for _, s := range ss {
		obtained := s.Store.Store
		expected := mapWant[obtained.Id]
		// Ignore lastHeartbeat
		obtained.LastHeartbeat, expected.LastHeartbeat = 0, 0
		c.Assert(obtained, DeepEquals, expected)
	}
}

func setStoreState(c *C, url, state string, expectStatusCode int) {
	data := map[string]string{"state": state}
	putData, err := json.Marshal(data)
	c.Assert(err, IsNil)
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(putData))
	c.Assert(err, IsNil)
	resp, err := dialClient.Do(req)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, expectStatusCode)
}

func getStoreState(c *C, url string) string {
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
	return got.Store.Store.State.String()
}
