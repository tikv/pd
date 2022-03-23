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
	"fmt"
	"reflect"
	"time"

	. "github.com/pingcap/check"
	"github.com/syndtr/goleveldb/leveldb"
	tu "github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server"
	_ "github.com/tikv/pd/server/schedulers"
	"github.com/tikv/pd/server/storage"
	"github.com/tikv/pd/server/storage/kv"
)

var _ = Suite(&testHotStatusSuite{})

type testHotStatusSuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func (s *testHotStatusSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = mustNewServer(c)
	mustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1/hotspot", addr, apiPrefix)

	mustBootstrapCluster(c, s.svr)
}

func (s *testHotStatusSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s testHotStatusSuite) TestGetHotStore(c *C) {
	stat := HotStoreStats{}
	err := tu.ReadGetJSON(c, testDialClient, s.urlPrefix+"/stores", &stat)
	c.Assert(err, IsNil)
}

func (s testHotStatusSuite) TestGetHistoryHotRegionsBasic(c *C) {
	request := HistoryHotRegionsRequest{
		StartTime: 0,
		EndTime:   time.Now().AddDate(0, 2, 0).UnixNano() / int64(time.Millisecond),
	}
	data, err := json.Marshal(request)
	c.Assert(err, IsNil)
	err = tu.CheckGetJSON(testDialClient, s.urlPrefix+"/regions/history", data, tu.StatusOK(c))
	c.Assert(err, IsNil)
	errRequest := "{\"start_time\":\"err\"}"
	err = tu.CheckGetJSON(testDialClient, s.urlPrefix+"/regions/history", []byte(errRequest), tu.StatusNotOK(c))
	c.Assert(err, IsNil)
}

func (s testHotStatusSuite) TestGetHistoryHotRegionsTimeRange(c *C) {
	hotRegionStorage := s.svr.GetHistoryHotRegionStorage()
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
	request := HistoryHotRegionsRequest{
		StartTime: now.UnixNano() / int64(time.Millisecond),
		EndTime:   now.Add(10*time.Second).UnixNano() / int64(time.Millisecond),
	}
	check := func(res []byte, statusCode int) {
		c.Assert(statusCode, Equals, 200)
		historyHotRegions := &storage.HistoryHotRegions{}
		json.Unmarshal(res, historyHotRegions)
		for _, region := range historyHotRegions.HistoryHotRegion {
			c.Assert(region.UpdateTime, GreaterEqual, request.StartTime)
			c.Assert(region.UpdateTime, LessEqual, request.EndTime)
		}
	}
	err := writeToDB(hotRegionStorage.LevelDBKV, hotRegions)
	c.Assert(err, IsNil)
	data, err := json.Marshal(request)
	c.Assert(err, IsNil)
	err = tu.CheckGetJSON(testDialClient, s.urlPrefix+"/regions/history", data, check)
	c.Assert(err, IsNil)
}

func (s testHotStatusSuite) TestGetHistoryHotRegionsIDAndTypes(c *C) {
	hotRegionStorage := s.svr.GetHistoryHotRegionStorage()
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
	request := HistoryHotRegionsRequest{
		RegionIDs:      []uint64{1},
		StoreIDs:       []uint64{1},
		PeerIDs:        []uint64{1},
		HotRegionTypes: []string{"read"},
		IsLeaders:      []bool{false},
		IsLearners:     []bool{false},
		EndTime:        now.Add(10*time.Minute).UnixNano() / int64(time.Millisecond),
	}
	check := func(res []byte, statusCode int) {
		c.Assert(statusCode, Equals, 200)
		historyHotRegions := &storage.HistoryHotRegions{}
		json.Unmarshal(res, historyHotRegions)
		c.Assert(historyHotRegions.HistoryHotRegion, HasLen, 1)
		c.Assert(reflect.DeepEqual(historyHotRegions.HistoryHotRegion[0], hotRegions[0]), IsTrue)
	}
	err := writeToDB(hotRegionStorage.LevelDBKV, hotRegions)
	c.Assert(err, IsNil)
	data, err := json.Marshal(request)
	c.Assert(err, IsNil)
	err = tu.CheckGetJSON(testDialClient, s.urlPrefix+"/regions/history", data, check)
	c.Assert(err, IsNil)
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
	kv.Write(batch, nil)
	return nil
}
