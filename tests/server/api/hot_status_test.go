// Copyright 2017 TiKV Project Authors.
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
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"github.com/syndtr/goleveldb/leveldb"

	"github.com/tikv/pd/pkg/schedule/handler"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/tests"
)

type hotStatusTestSuite struct {
	suite.Suite
	env *tests.SchedulingTestEnvironment
}

func TestHotStatusTestSuite(t *testing.T) {
	suite.Run(t, new(hotStatusTestSuite))
}

func (suite *hotStatusTestSuite) SetupSuite() {
	suite.env = tests.NewSchedulingTestEnvironment(suite.T())
}

func (suite *hotStatusTestSuite) TearDownSuite() {
	suite.env.Cleanup()
}

func (suite *hotStatusTestSuite) TestHotStatus() {
	suite.env.RunTestInNonMicroserviceEnv(suite.checkGetHotStore)
	suite.env.RunTestInNonMicroserviceEnv(suite.checkGetHistoryHotRegionsBasic)
	suite.env.RunTestInNonMicroserviceEnv(suite.checkGetHistoryHotRegionsTimeRange)
	suite.env.RunTestInNonMicroserviceEnv(suite.checkGetHistoryHotRegionsIDAndTypes)
}

func (suite *hotStatusTestSuite) checkGetHotStore(cluster *tests.TestCluster) {
	re := suite.Require()

	leader := cluster.GetLeaderServer()
	urlPrefix := leader.GetAddr() + "/pd/api/v1"
	stat := handler.HotStoreStats{}
	err := testutil.ReadGetJSON(re, tests.TestDialClient, urlPrefix+"/hotspot/stores", &stat)
	re.NoError(err)
}

func (suite *hotStatusTestSuite) checkGetHistoryHotRegionsBasic(cluster *tests.TestCluster) {
	re := suite.Require()

	leader := cluster.GetLeaderServer()
	urlPrefix := leader.GetAddr() + "/pd/api/v1"
	request := server.HistoryHotRegionsRequest{
		StartTime: 0,
		EndTime:   time.Now().AddDate(0, 2, 0).UnixNano() / int64(time.Millisecond),
	}
	data, err := json.Marshal(request)
	re.NoError(err)
	err = testutil.CheckGetJSON(tests.TestDialClient, urlPrefix+"/hotspot/regions/history", data, testutil.StatusOK(re))
	re.NoError(err)
	errRequest := "{\"start_time\":\"err\"}"
	err = testutil.CheckGetJSON(tests.TestDialClient, urlPrefix+"/hotspot/regions/history", []byte(errRequest), testutil.StatusNotOK(re))
	re.NoError(err)
}

func (suite *hotStatusTestSuite) checkGetHistoryHotRegionsTimeRange(cluster *tests.TestCluster) {
	re := suite.Require()

	leader := cluster.GetLeaderServer()
	urlPrefix := leader.GetAddr() + "/pd/api/v1"
	hotRegionStorage := leader.GetServer().GetHistoryHotRegionStorage()
	now := time.Now()
	hotRegions := []*storage.HistoryHotRegion{
		{
			RegionID:   1,
			UpdateTime: now.UnixNano() / int64(time.Millisecond),
		},
		{
			RegionID:   1,
			UpdateTime: now.Add(10*time.Minute).UnixNano() / int64(time.Millisecond),
		},
	}
	request := server.HistoryHotRegionsRequest{
		StartTime: now.UnixNano() / int64(time.Millisecond),
		EndTime:   now.Add(10*time.Second).UnixNano() / int64(time.Millisecond),
	}
	check := func(res []byte, statusCode int, _ http.Header) {
		re.Equal(200, statusCode)
		historyHotRegions := &storage.HistoryHotRegions{}
		err := json.Unmarshal(res, historyHotRegions)
		re.NoError(err)
		for _, region := range historyHotRegions.HistoryHotRegion {
			re.GreaterOrEqual(region.UpdateTime, request.StartTime)
			re.LessOrEqual(region.UpdateTime, request.EndTime)
		}
	}
	err := writeToDB(hotRegionStorage.LevelDBKV, hotRegions)
	re.NoError(err)
	data, err := json.Marshal(request)
	re.NoError(err)
	err = testutil.CheckGetJSON(tests.TestDialClient, urlPrefix+"/hotspot/regions/history", data, check)
	re.NoError(err)
}

func (suite *hotStatusTestSuite) checkGetHistoryHotRegionsIDAndTypes(cluster *tests.TestCluster) {
	re := suite.Require()

	leader := cluster.GetLeaderServer()
	urlPrefix := leader.GetAddr() + "/pd/api/v1"
	hotRegionStorage := leader.GetServer().GetHistoryHotRegionStorage()
	now := time.Now()
	hotRegions := []*storage.HistoryHotRegion{
		{
			RegionID:      1,
			StoreID:       1,
			PeerID:        1,
			IsLeader:      false,
			IsLearner:     false,
			HotRegionType: "read",
			UpdateTime:    now.UnixNano() / int64(time.Millisecond),
		},
		{
			RegionID:      1,
			StoreID:       2,
			PeerID:        1,
			IsLeader:      false,
			IsLearner:     false,
			HotRegionType: "read",
			UpdateTime:    now.Add(10*time.Second).UnixNano() / int64(time.Millisecond),
		},
		{
			RegionID:      1,
			StoreID:       1,
			PeerID:        2,
			IsLeader:      false,
			IsLearner:     false,
			HotRegionType: "read",
			UpdateTime:    now.Add(20*time.Second).UnixNano() / int64(time.Millisecond),
		},
		{
			RegionID:      1,
			StoreID:       1,
			PeerID:        1,
			IsLeader:      false,
			IsLearner:     false,
			HotRegionType: "write",
			UpdateTime:    now.Add(30*time.Second).UnixNano() / int64(time.Millisecond),
		},
		{
			RegionID:      1,
			StoreID:       1,
			PeerID:        1,
			IsLeader:      true,
			IsLearner:     false,
			HotRegionType: "read",
			UpdateTime:    now.Add(40*time.Second).UnixNano() / int64(time.Millisecond),
		},
		{
			RegionID:      1,
			StoreID:       1,
			PeerID:        1,
			IsLeader:      false,
			IsLearner:     true,
			HotRegionType: "read",
			UpdateTime:    now.Add(50*time.Second).UnixNano() / int64(time.Millisecond),
		},
	}
	request := server.HistoryHotRegionsRequest{
		RegionIDs:      []uint64{1},
		StoreIDs:       []uint64{1},
		PeerIDs:        []uint64{1},
		HotRegionTypes: []string{"read"},
		IsLeaders:      []bool{false},
		IsLearners:     []bool{false},
		EndTime:        now.Add(10*time.Minute).UnixNano() / int64(time.Millisecond),
	}
	check := func(res []byte, statusCode int, _ http.Header) {
		re.Equal(200, statusCode)
		historyHotRegions := &storage.HistoryHotRegions{}
		err := json.Unmarshal(res, historyHotRegions)
		re.NoError(err)
		re.Len(historyHotRegions.HistoryHotRegion, 1)
		re.Equal(hotRegions[0], historyHotRegions.HistoryHotRegion[0])
	}
	err := writeToDB(hotRegionStorage.LevelDBKV, hotRegions)
	re.NoError(err)
	data, err := json.Marshal(request)
	re.NoError(err)
	err = testutil.CheckGetJSON(tests.TestDialClient, urlPrefix+"/hotspot/regions/history", data, check)
	re.NoError(err)
}

func writeToDB(kv *kv.LevelDBKV, hotRegions []*storage.HistoryHotRegion) error {
	batch := new(leveldb.Batch)
	for _, region := range hotRegions {
		key := storage.HotRegionStorePath(region.HotRegionType, region.UpdateTime, region.RegionID)
		value, err := json.Marshal(region)
		if err != nil {
			return err
		}
		batch.Put([]byte(key), value)
	}
	return kv.Write(batch, nil)
}
