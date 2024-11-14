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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	tu "github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
)

type storeTestSuite struct {
	suite.Suite
	svr       *server.Server
	grpcSvr   *server.GrpcServer
	cleanup   cleanUpFunc
	urlPrefix string
	stores    []*metapb.Store
}

func TestStoreTestSuite(t *testing.T) {
	suite.Run(t, new(storeTestSuite))
}

func (suite *storeTestSuite) requestStatusBody(client *http.Client, method string, url string) int {
	req, err := http.NewRequest(method, url, nil)
	suite.NoError(err)
	resp, err := client.Do(req)
	suite.NoError(err)
	_, err = io.ReadAll(resp.Body)
	suite.NoError(err)
	err = resp.Body.Close()
	suite.NoError(err)
	return resp.StatusCode
}

func (suite *storeTestSuite) SetupSuite() {
	suite.stores = []*metapb.Store{
		{
			// metapb.StoreState_Up == 0
			Id:        1,
			Address:   "tikv1",
			State:     metapb.StoreState_Up,
			NodeState: metapb.NodeState_Serving,
			Version:   "2.0.0",
		},
		{
			Id:        4,
			Address:   "tikv4",
			State:     metapb.StoreState_Up,
			NodeState: metapb.NodeState_Serving,
			Version:   "2.0.0",
		},
		{
			// metapb.StoreState_Offline == 1
			Id:        6,
			Address:   "tikv6",
			State:     metapb.StoreState_Offline,
			NodeState: metapb.NodeState_Removing,
			Version:   "2.0.0",
		},
		{
			// metapb.StoreState_Tombstone == 2
			Id:        7,
			Address:   "tikv7",
			State:     metapb.StoreState_Tombstone,
			NodeState: metapb.NodeState_Removed,
			Version:   "2.0.0",
		},
	}
	// TODO: enable placmentrules
	re := suite.Require()
	suite.svr, suite.cleanup = mustNewServer(re, func(cfg *config.Config) { cfg.Replication.EnablePlacementRules = false })
	server.MustWaitLeader(re, []*server.Server{suite.svr})

	addr := suite.svr.GetAddr()
	suite.grpcSvr = &server.GrpcServer{Server: suite.svr}
	suite.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)

	mustBootstrapCluster(re, suite.svr)

	for _, store := range suite.stores {
		mustPutStore(re, suite.svr, store.Id, store.State, store.NodeState, nil)
	}
}

func (suite *storeTestSuite) TearDownSuite() {
	suite.cleanup()
}

func checkStoresInfo(re *require.Assertions, ss []*StoreInfo, want []*metapb.Store) {
	re.Len(ss, len(want))
	mapWant := make(map[uint64]*metapb.Store)
	for _, s := range want {
		if _, ok := mapWant[s.Id]; !ok {
			mapWant[s.Id] = s
		}
	}
	for _, s := range ss {
		obtained := typeutil.DeepClone(s.Store.Store, core.StoreFactory)
		expected := typeutil.DeepClone(mapWant[obtained.Id], core.StoreFactory)
		// Ignore lastHeartbeat
		obtained.LastHeartbeat, expected.LastHeartbeat = 0, 0
		re.Equal(expected, obtained)
	}
}

func (suite *storeTestSuite) TestStoresList() {
	url := fmt.Sprintf("%s/stores", suite.urlPrefix)
	info := new(StoresInfo)
	re := suite.Require()
	err := tu.ReadGetJSON(re, testDialClient, url, info)
	suite.NoError(err)
	checkStoresInfo(re, info.Stores, suite.stores[:3])

	url = fmt.Sprintf("%s/stores?state=0", suite.urlPrefix)
	info = new(StoresInfo)
	err = tu.ReadGetJSON(re, testDialClient, url, info)
	suite.NoError(err)
	checkStoresInfo(re, info.Stores, suite.stores[:2])

	url = fmt.Sprintf("%s/stores?state=1", suite.urlPrefix)
	info = new(StoresInfo)
	err = tu.ReadGetJSON(re, testDialClient, url, info)
	suite.NoError(err)
	checkStoresInfo(re, info.Stores, suite.stores[2:3])
}

func (suite *storeTestSuite) TestStoreGet() {
	url := fmt.Sprintf("%s/store/1", suite.urlPrefix)
	suite.grpcSvr.StoreHeartbeat(
		context.Background(), &pdpb.StoreHeartbeatRequest{
			Header: &pdpb.RequestHeader{ClusterId: suite.svr.ClusterID()},
			Stats: &pdpb.StoreStats{
				StoreId:   1,
				Capacity:  1798985089024,
				Available: 1709868695552,
				UsedSize:  85150956358,
			},
		},
	)
	info := new(StoreInfo)
	err := tu.ReadGetJSON(suite.Require(), testDialClient, url, info)
	suite.NoError(err)
	capacity, _ := units.RAMInBytes("1.636TiB")
	available, _ := units.RAMInBytes("1.555TiB")
	suite.Equal(capacity, int64(info.Status.Capacity))
	suite.Equal(available, int64(info.Status.Available))
	checkStoresInfo(suite.Require(), []*StoreInfo{info}, suite.stores[:1])
}

func (suite *storeTestSuite) TestStoreLabel() {
	url := fmt.Sprintf("%s/store/1", suite.urlPrefix)
	re := suite.Require()
	var info StoreInfo
	err := tu.ReadGetJSON(re, testDialClient, url, &info)
	suite.NoError(err)
	suite.Empty(info.Store.Labels)

	// Test merge.
	// enable label match check.
	labelCheck := map[string]string{"strictly-match-label": "true"}
	lc, _ := json.Marshal(labelCheck)
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/config", lc, tu.StatusOK(re))
	suite.NoError(err)
	// Test set.
	labels := map[string]string{"zone": "cn", "host": "local"}
	b, err := json.Marshal(labels)
	suite.NoError(err)
	// TODO: supports strictly match check in placement rules
	err = tu.CheckPostJSON(testDialClient, url+"/label", b,
		tu.StatusNotOK(re),
		tu.StringContain(re, "key matching the label was not found"))
	suite.NoError(err)
	locationLabels := map[string]string{"location-labels": "zone,host"}
	ll, _ := json.Marshal(locationLabels)
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/config", ll, tu.StatusOK(re))
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, url+"/label", b, tu.StatusOK(re))
	suite.NoError(err)

	err = tu.ReadGetJSON(re, testDialClient, url, &info)
	suite.NoError(err)
	suite.Len(info.Store.Labels, len(labels))
	for _, l := range info.Store.Labels {
		suite.Equal(l.Value, labels[l.Key])
	}

	// Test merge.
	// disable label match check.
	labelCheck = map[string]string{"strictly-match-label": "false"}
	lc, _ = json.Marshal(labelCheck)
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/config", lc, tu.StatusOK(re))
	suite.NoError(err)

	labels = map[string]string{"zack": "zack1", "Host": "host1"}
	b, err = json.Marshal(labels)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, url+"/label", b, tu.StatusOK(re))
	suite.NoError(err)

	expectLabel := map[string]string{"zone": "cn", "zack": "zack1", "host": "host1"}
	err = tu.ReadGetJSON(re, testDialClient, url, &info)
	suite.NoError(err)
	suite.Len(info.Store.Labels, len(expectLabel))
	for _, l := range info.Store.Labels {
		suite.Equal(expectLabel[l.Key], l.Value)
	}

	// delete label
	b, err = json.Marshal(map[string]string{"host": ""})
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, url+"/label", b, tu.StatusOK(re))
	suite.NoError(err)
	err = tu.ReadGetJSON(re, testDialClient, url, &info)
	suite.NoError(err)
	delete(expectLabel, "host")
	suite.Len(info.Store.Labels, len(expectLabel))
	for _, l := range info.Store.Labels {
		suite.Equal(expectLabel[l.Key], l.Value)
	}

	suite.stores[0].Labels = info.Store.Labels
}

func (suite *storeTestSuite) TestStoreDelete() {
	re := suite.Require()
	// prepare enough online stores to store replica.
	for id := 1111; id <= 1115; id++ {
		mustPutStore(re, suite.svr, uint64(id), metapb.StoreState_Up, metapb.NodeState_Serving, nil)
	}
	testCases := []struct {
		id     int
		status int
	}{
		{
			id:     6,
			status: http.StatusOK,
		},
		{
			id:     7,
			status: http.StatusGone,
		},
	}
	for _, testCase := range testCases {
		url := fmt.Sprintf("%s/store/%d", suite.urlPrefix, testCase.id)
		status := suite.requestStatusBody(testDialClient, http.MethodDelete, url)
		suite.Equal(testCase.status, status)
	}
	// store 6 origin status:offline
	url := fmt.Sprintf("%s/store/6", suite.urlPrefix)
	store := new(StoreInfo)
	err := tu.ReadGetJSON(re, testDialClient, url, store)
	suite.NoError(err)
	suite.False(store.Store.PhysicallyDestroyed)
	suite.Equal(metapb.StoreState_Offline, store.Store.State)

	// up store success because it is offline but not physically destroyed
	status := suite.requestStatusBody(testDialClient, http.MethodPost, fmt.Sprintf("%s/state?state=Up", url))
	suite.Equal(http.StatusOK, status)

	status = suite.requestStatusBody(testDialClient, http.MethodGet, url)
	suite.Equal(http.StatusOK, status)
	store = new(StoreInfo)
	err = tu.ReadGetJSON(re, testDialClient, url, store)
	suite.NoError(err)
	suite.Equal(metapb.StoreState_Up, store.Store.State)
	suite.False(store.Store.PhysicallyDestroyed)

	// offline store with physically destroyed
	status = suite.requestStatusBody(testDialClient, http.MethodDelete, fmt.Sprintf("%s?force=true", url))
	suite.Equal(http.StatusOK, status)
	err = tu.ReadGetJSON(re, testDialClient, url, store)
	suite.NoError(err)
	suite.Equal(metapb.StoreState_Offline, store.Store.State)
	suite.True(store.Store.PhysicallyDestroyed)

	// try to up store again failed because it is physically destroyed
	status = suite.requestStatusBody(testDialClient, http.MethodPost, fmt.Sprintf("%s/state?state=Up", url))
	suite.Equal(http.StatusBadRequest, status)
	// reset store 6
	suite.cleanup()
	suite.SetupSuite()
}

func (suite *storeTestSuite) TestStoreSetState() {
	re := suite.Require()
	// prepare enough online stores to store replica.
	for id := 1111; id <= 1115; id++ {
		mustPutStore(re, suite.svr, uint64(id), metapb.StoreState_Up, metapb.NodeState_Serving, nil)
	}
	url := fmt.Sprintf("%s/store/1", suite.urlPrefix)
	info := StoreInfo{}
	err := tu.ReadGetJSON(re, testDialClient, url, &info)
	suite.NoError(err)
	suite.Equal(metapb.StoreState_Up, info.Store.State)

	// Set to Offline.
	info = StoreInfo{}
	err = tu.CheckPostJSON(testDialClient, url+"/state?state=Offline", nil, tu.StatusOK(re))
	suite.NoError(err)
	err = tu.ReadGetJSON(re, testDialClient, url, &info)
	suite.NoError(err)
	suite.Equal(metapb.StoreState_Offline, info.Store.State)

	// store not found
	info = StoreInfo{}
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/store/10086/state?state=Offline", nil, tu.StatusNotOK(re))
	suite.NoError(err)

	// Invalid state.
	invalidStates := []string{"Foo", "Tombstone"}
	for _, state := range invalidStates {
		info = StoreInfo{}
		err = tu.CheckPostJSON(testDialClient, url+"/state?state="+state, nil, tu.StatusNotOK(re))
		suite.NoError(err)
		err := tu.ReadGetJSON(re, testDialClient, url, &info)
		suite.NoError(err)
		suite.Equal(metapb.StoreState_Offline, info.Store.State)
	}

	// Set back to Up.
	info = StoreInfo{}
	err = tu.CheckPostJSON(testDialClient, url+"/state?state=Up", nil, tu.StatusOK(re))
	suite.NoError(err)
	err = tu.ReadGetJSON(re, testDialClient, url, &info)
	suite.NoError(err)
	suite.Equal(metapb.StoreState_Up, info.Store.State)
	suite.cleanup()
	suite.SetupSuite()
}

func (suite *storeTestSuite) TestUrlStoreFilter() {
	testCases := []struct {
		u    string
		want []*metapb.Store
	}{
		{
			u:    "http://localhost:2379/pd/api/v1/stores",
			want: suite.stores[:3],
		},
		{
			u:    "http://localhost:2379/pd/api/v1/stores?state=2",
			want: suite.stores[3:],
		},
		{
			u:    "http://localhost:2379/pd/api/v1/stores?state=0",
			want: suite.stores[:2],
		},
		{
			u:    "http://localhost:2379/pd/api/v1/stores?state=2&state=1",
			want: suite.stores[2:],
		},
	}

	for _, testCase := range testCases {
		uu, err := url.Parse(testCase.u)
		suite.NoError(err)
		f, err := newStoreStateFilter(uu)
		suite.NoError(err)
		suite.Equal(testCase.want, f.filter(suite.stores))
	}

	u, err := url.Parse("http://localhost:2379/pd/api/v1/stores?state=foo")
	suite.NoError(err)
	_, err = newStoreStateFilter(u)
	suite.Error(err)

	u, err = url.Parse("http://localhost:2379/pd/api/v1/stores?state=999999")
	suite.NoError(err)
	_, err = newStoreStateFilter(u)
	suite.Error(err)
}

func (suite *storeTestSuite) TestDownState() {
	store := core.NewStoreInfo(
		&metapb.Store{
			State: metapb.StoreState_Up,
		},
		core.SetStoreStats(&pdpb.StoreStats{}),
		core.SetLastHeartbeatTS(time.Now()),
	)
	storeInfo := newStoreInfo(suite.svr.GetScheduleConfig(), store)
	suite.Equal(metapb.StoreState_Up.String(), storeInfo.Store.StateName)

	newStore := store.Clone(core.SetLastHeartbeatTS(time.Now().Add(-time.Minute * 2)))
	storeInfo = newStoreInfo(suite.svr.GetScheduleConfig(), newStore)
	suite.Equal(disconnectedName, storeInfo.Store.StateName)

	newStore = store.Clone(core.SetLastHeartbeatTS(time.Now().Add(-time.Hour * 2)))
	storeInfo = newStoreInfo(suite.svr.GetScheduleConfig(), newStore)
	suite.Equal(downStateName, storeInfo.Store.StateName)
}

func (suite *storeTestSuite) TestGetAllLimit() {
	testCases := []struct {
		name           string
		url            string
		expectedStores map[uint64]struct{}
	}{
		{
			name: "includeTombstone",
			url:  fmt.Sprintf("%s/stores/limit?include_tombstone=true", suite.urlPrefix),
			expectedStores: map[uint64]struct{}{
				1: {},
				4: {},
				6: {},
				7: {},
			},
		},
		{
			name: "excludeTombStone",
			url:  fmt.Sprintf("%s/stores/limit?include_tombstone=false", suite.urlPrefix),
			expectedStores: map[uint64]struct{}{
				1: {},
				4: {},
				6: {},
			},
		},
		{
			name: "default",
			url:  fmt.Sprintf("%s/stores/limit", suite.urlPrefix),
			expectedStores: map[uint64]struct{}{
				1: {},
				4: {},
				6: {},
			},
		},
	}

	re := suite.Require()
	for _, testCase := range testCases {
		suite.T().Log(testCase.name)
		info := make(map[uint64]interface{}, 4)
		err := tu.ReadGetJSON(re, testDialClient, testCase.url, &info)
		suite.NoError(err)
		suite.Len(info, len(testCase.expectedStores))
		for id := range testCase.expectedStores {
			_, ok := info[id]
			suite.True(ok)
		}
	}
}

func (suite *storeTestSuite) TestStoreLimitTTL() {
	// add peer
	url := fmt.Sprintf("%s/store/1/limit?ttlSecond=%v", suite.urlPrefix, 5)
	data := map[string]interface{}{
		"type": "add-peer",
		"rate": 999,
	}
	postData, err := json.Marshal(data)
	suite.NoError(err)
	re := suite.Require()
	err = tu.CheckPostJSON(testDialClient, url, postData, tu.StatusOK(re))
	suite.NoError(err)
	// remove peer
	data = map[string]interface{}{
		"type": "remove-peer",
		"rate": 998,
	}
	postData, err = json.Marshal(data)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, url, postData, tu.StatusOK(re))
	suite.NoError(err)
	// all store limit add peer
	url = fmt.Sprintf("%s/stores/limit?ttlSecond=%v", suite.urlPrefix, 3)
	data = map[string]interface{}{
		"type": "add-peer",
		"rate": 997,
	}
	postData, err = json.Marshal(data)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, url, postData, tu.StatusOK(re))
	suite.NoError(err)
	// all store limit remove peer
	data = map[string]interface{}{
		"type": "remove-peer",
		"rate": 996,
	}
	postData, err = json.Marshal(data)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, url, postData, tu.StatusOK(re))
	suite.NoError(err)

	suite.Equal(float64(999), suite.svr.GetPersistOptions().GetStoreLimit(uint64(1)).AddPeer)
	suite.Equal(float64(998), suite.svr.GetPersistOptions().GetStoreLimit(uint64(1)).RemovePeer)
	suite.Equal(float64(997), suite.svr.GetPersistOptions().GetStoreLimit(uint64(2)).AddPeer)
	suite.Equal(float64(996), suite.svr.GetPersistOptions().GetStoreLimit(uint64(2)).RemovePeer)
	time.Sleep(5 * time.Second)
	suite.NotEqual(float64(999), suite.svr.GetPersistOptions().GetStoreLimit(uint64(1)).AddPeer)
	suite.NotEqual(float64(998), suite.svr.GetPersistOptions().GetStoreLimit(uint64(1)).RemovePeer)
	suite.NotEqual(float64(997), suite.svr.GetPersistOptions().GetStoreLimit(uint64(2)).AddPeer)
	suite.NotEqual(float64(996), suite.svr.GetPersistOptions().GetStoreLimit(uint64(2)).RemovePeer)
}
