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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/docker/go-units"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/response"
	"github.com/tikv/pd/pkg/utils/keypath"
	tu "github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/pkg/versioninfo"
	"github.com/tikv/pd/tests"
)

type storeTestSuite struct {
	suite.Suite
	env *tests.SchedulingTestEnvironment
}

func TestStoreTestSuite(t *testing.T) {
	suite.Run(t, new(storeTestSuite))
}

func (suite *storeTestSuite) SetupTest() {
	suite.env = tests.NewSchedulingTestEnvironment(suite.T())
}

func (suite *storeTestSuite) TearDownTest() {
	suite.env.Cleanup()
}

func (suite *storeTestSuite) TestStoresList() {
	suite.env.RunTestInNonMicroserviceEnv(suite.checkStoresList)
}

func (suite *storeTestSuite) checkStoresList(cluster *tests.TestCluster) {
	re := suite.Require()

	stores := initStores()
	for _, store := range stores {
		tests.MustPutStore(re, cluster, store)
	}
	leader := cluster.GetLeaderServer()
	urlPrefix := leader.GetAddr() + "/pd/api/v1"

	// store 1 is used to bootstrapped that its state might be different the store inside initStores.
	leader.GetRaftCluster().ReadyToServe(1)

	url := fmt.Sprintf("%s/stores", urlPrefix)
	info := new(response.StoresInfo)
	err := tu.ReadGetJSON(re, tests.TestDialClient, url, info)
	re.NoError(err)
	checkStoresInfo(re, info.Stores, stores[:3])

	url = fmt.Sprintf("%s/stores/check?state=up", urlPrefix)
	info = new(response.StoresInfo)
	err = tu.ReadGetJSON(re, tests.TestDialClient, url, info)
	re.NoError(err)
	checkStoresInfo(re, info.Stores, stores[:2])

	url = fmt.Sprintf("%s/stores/check?state=offline", urlPrefix)
	info = new(response.StoresInfo)
	err = tu.ReadGetJSON(re, tests.TestDialClient, url, info)
	re.NoError(err)
	checkStoresInfo(re, info.Stores, stores[2:3])

	url = fmt.Sprintf("%s/stores/check?state=tombstone", urlPrefix)
	info = new(response.StoresInfo)
	err = tu.ReadGetJSON(re, tests.TestDialClient, url, info)
	re.NoError(err)
	checkStoresInfo(re, info.Stores, stores[3:])

	url = fmt.Sprintf("%s/stores/check?state=tombstone&state=offline", urlPrefix)
	info = new(response.StoresInfo)
	err = tu.ReadGetJSON(re, tests.TestDialClient, url, info)
	re.NoError(err)
	checkStoresInfo(re, info.Stores, stores[2:])

	// down store
	store := &metapb.Store{
		Id:            100,
		Address:       "mock://tikv-100:100",
		State:         metapb.StoreState_Up,
		Version:       versioninfo.MinSupportedVersion(versioninfo.Version2_0).String(),
		LastHeartbeat: time.Now().UnixNano() - int64(1*time.Hour),
	}
	tests.MustPutStore(re, cluster, store)

	url = fmt.Sprintf("%s/stores/check?state=down", urlPrefix)
	info = new(response.StoresInfo)
	err = tu.ReadGetJSON(re, tests.TestDialClient, url, info)
	re.NoError(err)
	checkStoresInfo(re, info.Stores, []*metapb.Store{store})

	// disconnect store
	store.LastHeartbeat = time.Now().UnixNano() - int64(1*time.Minute)
	tests.MustPutStore(re, cluster, store)

	url = fmt.Sprintf("%s/stores/check?state=disconnected", urlPrefix)
	info = new(response.StoresInfo)
	err = tu.ReadGetJSON(re, tests.TestDialClient, url, info)
	re.NoError(err)
	checkStoresInfo(re, info.Stores, []*metapb.Store{store})
}

func (suite *storeTestSuite) TestStores() {
	suite.env.RunTestInNonMicroserviceEnv(suite.checkGetAllLimit)
	suite.env.RunTestInNonMicroserviceEnv(suite.checkStoreLimitTTL)
	suite.env.RunTestInNonMicroserviceEnv(suite.checkStoreLabel)
}

func (suite *storeTestSuite) checkGetAllLimit(cluster *tests.TestCluster) {
	re := suite.Require()

	for _, store := range initStores() {
		tests.MustPutStore(re, cluster, store)
	}
	leader := cluster.GetLeaderServer()
	urlPrefix := leader.GetAddr() + "/pd/api/v1"
	testCases := []struct {
		name           string
		url            string
		expectedStores map[uint64]struct{}
	}{
		{
			name: "includeTombstone",
			url:  fmt.Sprintf("%s/stores/limit?include_tombstone=true", urlPrefix),
			expectedStores: map[uint64]struct{}{
				1: {},
				4: {},
				6: {},
				7: {},
			},
		},
		{
			name: "excludeTombStone",
			url:  fmt.Sprintf("%s/stores/limit?include_tombstone=false", urlPrefix),
			expectedStores: map[uint64]struct{}{
				1: {},
				4: {},
				6: {},
			},
		},
		{
			name: "default",
			url:  fmt.Sprintf("%s/stores/limit", urlPrefix),
			expectedStores: map[uint64]struct{}{
				1: {},
				4: {},
				6: {},
			},
		},
	}

	for _, testCase := range testCases {
		suite.T().Log(testCase.name)
		info := make(map[uint64]any, 4)
		err := tu.ReadGetJSON(re, tests.TestDialClient, testCase.url, &info)
		re.NoError(err)
		re.Len(info, len(testCase.expectedStores))
		for id := range testCase.expectedStores {
			_, ok := info[id]
			re.True(ok)
		}
	}
}

func (suite *storeTestSuite) checkStoreLimitTTL(cluster *tests.TestCluster) {
	re := suite.Require()

	leader := cluster.GetLeaderServer()
	urlPrefix := leader.GetAddr() + "/pd/api/v1"
	// add peer
	url := fmt.Sprintf("%s/store/1/limit?ttlSecond=%v", urlPrefix, 5)
	data := map[string]any{
		"type": "add-peer",
		"rate": 999,
	}
	postData, err := json.Marshal(data)
	re.NoError(err)
	err = tu.CheckPostJSON(tests.TestDialClient, url, postData, tu.StatusOK(re))
	re.NoError(err)
	// remove peer
	data = map[string]any{
		"type": "remove-peer",
		"rate": 998,
	}
	postData, err = json.Marshal(data)
	re.NoError(err)
	err = tu.CheckPostJSON(tests.TestDialClient, url, postData, tu.StatusOK(re))
	re.NoError(err)
	// all store limit add peer
	url = fmt.Sprintf("%s/stores/limit?ttlSecond=%v", urlPrefix, 3)
	data = map[string]any{
		"type": "add-peer",
		"rate": 997,
	}
	postData, err = json.Marshal(data)
	re.NoError(err)
	err = tu.CheckPostJSON(tests.TestDialClient, url, postData, tu.StatusOK(re))
	re.NoError(err)
	// all store limit remove peer
	data = map[string]any{
		"type": "remove-peer",
		"rate": 996,
	}
	postData, err = json.Marshal(data)
	re.NoError(err)
	err = tu.CheckPostJSON(tests.TestDialClient, url, postData, tu.StatusOK(re))
	re.NoError(err)

	re.Equal(float64(999), leader.GetPersistOptions().GetStoreLimit(uint64(1)).AddPeer)
	re.Equal(float64(998), leader.GetPersistOptions().GetStoreLimit(uint64(1)).RemovePeer)
	re.Equal(float64(997), leader.GetPersistOptions().GetStoreLimit(uint64(2)).AddPeer)
	re.Equal(float64(996), leader.GetPersistOptions().GetStoreLimit(uint64(2)).RemovePeer)
	time.Sleep(5 * time.Second)
	re.NotEqual(float64(999), leader.GetPersistOptions().GetStoreLimit(uint64(1)).AddPeer)
	re.NotEqual(float64(998), leader.GetPersistOptions().GetStoreLimit(uint64(1)).RemovePeer)
	re.NotEqual(float64(997), leader.GetPersistOptions().GetStoreLimit(uint64(2)).AddPeer)
	re.NotEqual(float64(996), leader.GetPersistOptions().GetStoreLimit(uint64(2)).RemovePeer)
}

func (suite *storeTestSuite) checkStoreLabel(cluster *tests.TestCluster) {
	re := suite.Require()

	leader := cluster.GetLeaderServer()
	urlPrefix := leader.GetAddr() + "/pd/api/v1"
	url := fmt.Sprintf("%s/store/1", urlPrefix)
	var info response.StoreInfo
	err := tu.ReadGetJSON(re, tests.TestDialClient, url, &info)
	re.NoError(err)
	re.Empty(info.Store.Labels)

	// Test merge.
	// enable label match check.
	labelCheck := map[string]string{"strictly-match-label": "true"}
	lc, _ := json.Marshal(labelCheck)
	err = tu.CheckPostJSON(tests.TestDialClient, urlPrefix+"/config", lc, tu.StatusOK(re))
	re.NoError(err)
	// Test set.
	labels := map[string]string{"zone": "cn", "host": "local"}
	b, err := json.Marshal(labels)
	re.NoError(err)
	// TODO: supports strictly match check in placement rules
	err = tu.CheckPostJSON(tests.TestDialClient, url+"/label", b,
		tu.StatusNotOK(re),
		tu.StringContain(re, "key matching the label was not found"))
	re.NoError(err)
	locationLabels := map[string]string{"location-labels": "zone,host"}
	ll, _ := json.Marshal(locationLabels)
	err = tu.CheckPostJSON(tests.TestDialClient, urlPrefix+"/config", ll, tu.StatusOK(re))
	re.NoError(err)
	err = tu.CheckPostJSON(tests.TestDialClient, url+"/label", b, tu.StatusOK(re))
	re.NoError(err)

	err = tu.ReadGetJSON(re, tests.TestDialClient, url, &info)
	re.NoError(err)
	re.Len(info.Store.Labels, len(labels))
	for _, l := range info.Store.Labels {
		re.Equal(l.Value, labels[l.Key])
	}

	// Test merge.
	// disable label match check.
	labelCheck = map[string]string{"strictly-match-label": "false"}
	lc, _ = json.Marshal(labelCheck)
	err = tu.CheckPostJSON(tests.TestDialClient, urlPrefix+"/config", lc, tu.StatusOK(re))
	re.NoError(err)

	labels = map[string]string{"zack": "zack1", "Host": "host1"}
	b, err = json.Marshal(labels)
	re.NoError(err)
	err = tu.CheckPostJSON(tests.TestDialClient, url+"/label", b, tu.StatusOK(re))
	re.NoError(err)

	expectLabel := map[string]string{"zone": "cn", "zack": "zack1", "host": "host1"}
	err = tu.ReadGetJSON(re, tests.TestDialClient, url, &info)
	re.NoError(err)
	re.Len(info.Store.Labels, len(expectLabel))
	for _, l := range info.Store.Labels {
		re.Equal(expectLabel[l.Key], l.Value)
	}

	// delete label
	b, err = json.Marshal(map[string]string{"host": ""})
	re.NoError(err)
	err = tu.CheckPostJSON(tests.TestDialClient, url+"/label", b, tu.StatusOK(re))
	re.NoError(err)
	err = tu.ReadGetJSON(re, tests.TestDialClient, url, &info)
	re.NoError(err)
	delete(expectLabel, "host")
	re.Len(info.Store.Labels, len(expectLabel))
	for _, l := range info.Store.Labels {
		re.Equal(expectLabel[l.Key], l.Value)
	}
}

func (suite *storeTestSuite) TestStoreGet() {
	suite.env.RunTest(suite.checkStoreGet)
}

func (suite *storeTestSuite) checkStoreGet(cluster *tests.TestCluster) {
	re := suite.Require()

	stores := initStores()
	for _, store := range stores {
		tests.MustPutStore(re, cluster, store)
	}

	leader := cluster.GetLeaderServer()
	// store 1 is used to bootstrapped that its state might be different the store inside initStores.
	leader.GetRaftCluster().ReadyToServe(1)
	urlPrefix := leader.GetAddr() + "/pd/api/v1"
	url := fmt.Sprintf("%s/store/1", urlPrefix)

	tests.MustHandleStoreHeartbeat(re, cluster, &pdpb.StoreHeartbeatRequest{
		Header: &pdpb.RequestHeader{ClusterId: keypath.ClusterID()},
		Stats: &pdpb.StoreStats{
			StoreId:   1,
			Capacity:  1798985089024,
			Available: 1709868695552,
			UsedSize:  85150956358,
		},
	})
	info := new(response.StoreInfo)
	err := tu.ReadGetJSON(re, tests.TestDialClient, url, info)
	re.NoError(err)
	capacity, _ := units.RAMInBytes("1.636TiB")
	available, _ := units.RAMInBytes("1.555TiB")
	re.Equal(capacity, int64(info.Status.Capacity))
	re.Equal(available, int64(info.Status.Available))
	checkStoresInfo(re, []*response.StoreInfo{info}, stores[:1])
}

func (suite *storeTestSuite) TestStoreDelete() {
	suite.env.RunTest(suite.checkStoreDelete)
}

func (suite *storeTestSuite) checkStoreDelete(cluster *tests.TestCluster) {
	re := suite.Require()

	for _, store := range initStores() {
		tests.MustPutStore(re, cluster, store)
	}
	leader := cluster.GetLeaderServer()
	urlPrefix := leader.GetAddr() + "/pd/api/v1"

	for id := 1111; id <= 1115; id++ {
		tests.MustPutStore(re, cluster, &metapb.Store{
			Id:        uint64(id),
			Address:   fmt.Sprintf("tikv%d", id),
			State:     metapb.StoreState_Up,
			NodeState: metapb.NodeState_Serving,
		})
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
		url := fmt.Sprintf("%s/store/%d", urlPrefix, testCase.id)
		status := requestStatusBody(re, tests.TestDialClient, http.MethodDelete, url)
		re.Equal(testCase.status, status)
	}
	// store 6 origin status:offline
	url := fmt.Sprintf("%s/store/6", urlPrefix)
	store := new(response.StoreInfo)
	err := tu.ReadGetJSON(re, tests.TestDialClient, url, store)
	re.NoError(err)
	re.False(store.Store.PhysicallyDestroyed)
	re.Equal(metapb.StoreState_Offline, store.Store.State)

	// up store success because it is offline but not physically destroyed
	status := requestStatusBody(re, tests.TestDialClient, http.MethodPost, fmt.Sprintf("%s/state?state=Up", url))
	re.Equal(http.StatusOK, status)

	status = requestStatusBody(re, tests.TestDialClient, http.MethodGet, url)
	re.Equal(http.StatusOK, status)
	store = new(response.StoreInfo)
	err = tu.ReadGetJSON(re, tests.TestDialClient, url, store)
	re.NoError(err)
	re.Equal(metapb.StoreState_Up, store.Store.State)
	re.False(store.Store.PhysicallyDestroyed)

	// offline store with physically destroyed
	status = requestStatusBody(re, tests.TestDialClient, http.MethodDelete, fmt.Sprintf("%s?force=true", url))
	re.Equal(http.StatusOK, status)
	err = tu.ReadGetJSON(re, tests.TestDialClient, url, store)
	re.NoError(err)
	re.Equal(metapb.StoreState_Offline, store.Store.State)
	re.True(store.Store.PhysicallyDestroyed)

	// try to up store again failed because it is physically destroyed
	status = requestStatusBody(re, tests.TestDialClient, http.MethodPost, fmt.Sprintf("%s/state?state=Up", url))
	re.Equal(http.StatusBadRequest, status)
}

func (suite *storeTestSuite) TestStoreSetState() {
	suite.env.RunTest(suite.checkStoreSetState)
}

func (suite *storeTestSuite) checkStoreSetState(cluster *tests.TestCluster) {
	re := suite.Require()

	for _, store := range initStores() {
		tests.MustPutStore(re, cluster, store)
	}
	leader := cluster.GetLeaderServer()
	urlPrefix := leader.GetAddr() + "/pd/api/v1"

	// prepare enough online stores to store replica.
	for id := 1111; id <= 1115; id++ {
		tests.MustPutStore(re, cluster, &metapb.Store{
			Id:        uint64(id),
			Address:   fmt.Sprintf("tikv%d", id),
			State:     metapb.StoreState_Up,
			NodeState: metapb.NodeState_Serving,
		})
	}
	url := fmt.Sprintf("%s/store/1", urlPrefix)
	info := response.StoreInfo{}
	err := tu.ReadGetJSON(re, tests.TestDialClient, url, &info)
	re.NoError(err)
	re.Equal(metapb.StoreState_Up, info.Store.State)

	// Set to Offline.
	info = response.StoreInfo{}
	err = tu.CheckPostJSON(tests.TestDialClient, url+"/state?state=Offline", nil, tu.StatusOK(re))
	re.NoError(err)
	err = tu.ReadGetJSON(re, tests.TestDialClient, url, &info)
	re.NoError(err)
	re.Equal(metapb.StoreState_Offline, info.Store.State)

	// store not found
	info = response.StoreInfo{}
	err = tu.CheckPostJSON(tests.TestDialClient, urlPrefix+"/store/10086/state?state=Offline", nil, tu.StatusNotOK(re))
	re.NoError(err)

	// Invalid state.
	invalidStates := []string{"Foo", "Tombstone"}
	for _, state := range invalidStates {
		info = response.StoreInfo{}
		err = tu.CheckPostJSON(tests.TestDialClient, url+"/state?state="+state, nil, tu.StatusNotOK(re))
		re.NoError(err)
		err := tu.ReadGetJSON(re, tests.TestDialClient, url, &info)
		re.NoError(err)
		re.Equal(metapb.StoreState_Offline, info.Store.State)
	}

	// Set back to Up.
	info = response.StoreInfo{}
	err = tu.CheckPostJSON(tests.TestDialClient, url+"/state?state=Up", nil, tu.StatusOK(re))
	re.NoError(err)
	err = tu.ReadGetJSON(re, tests.TestDialClient, url, &info)
	re.NoError(err)
	re.Equal(metapb.StoreState_Up, info.Store.State)
}

func initStores() []*metapb.Store {
	return []*metapb.Store{
		{
			// metapb.StoreState_Up == 0
			Id:        1,
			Address:   "mock://tikv-1:1",
			State:     metapb.StoreState_Up,
			NodeState: metapb.NodeState_Serving,
			Version:   "2.0.0",
		},
		{
			Id:        4,
			Address:   "mock://tikv-4:4",
			State:     metapb.StoreState_Up,
			NodeState: metapb.NodeState_Serving,
			Version:   "2.0.0",
		},
		{
			// metapb.StoreState_Offline == 1
			Id:        6,
			Address:   "mock://tikv-6:6",
			State:     metapb.StoreState_Offline,
			NodeState: metapb.NodeState_Removing,
			Version:   "2.0.0",
		},
		{
			// metapb.StoreState_Tombstone == 2
			Id:        7,
			Address:   "mock://tikv-7:7",
			State:     metapb.StoreState_Tombstone,
			NodeState: metapb.NodeState_Removed,
			Version:   "2.0.0",
		},
	}
}

func requestStatusBody(re *require.Assertions, client *http.Client, method string, url string) int {
	req, err := http.NewRequest(method, url, http.NoBody)
	re.NoError(err)
	resp, err := client.Do(req)
	re.NoError(err)
	_, err = io.ReadAll(resp.Body)
	re.NoError(err)
	err = resp.Body.Close()
	re.NoError(err)
	return resp.StatusCode
}

func checkStoresInfo(re *require.Assertions, ss []*response.StoreInfo, want []*metapb.Store) {
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
