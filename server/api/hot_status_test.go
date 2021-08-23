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
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/cluster"
	"github.com/tikv/pd/server/kv"
	_ "github.com/tikv/pd/server/schedulers"
	"github.com/tikv/pd/server/statistics"
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
	err := readJSON(testDialClient, s.urlPrefix+"/stores", &stat)
	c.Assert(err, IsNil)
}

func (s testHotStatusSuite) TestGetHistoryHotRegionsBasic(c *C) {
	request := HistoryHotRegionsRequest{
		StartTime: 0,
		EndTime:   time.Now().AddDate(0, 2, 0).UnixNano() / int64(time.Millisecond),
	}
	data, err := json.Marshal(request)
	c.Assert(err, IsNil)
	err = postJSON(testDialClient, s.urlPrefix+"/regions/history", data)
	c.Assert(err, IsNil)
}

func (s testHotStatusSuite) TestGetHistoryHotRegionsTimeRange(c *C) {
	storage := s.svr.GetHistoryHotRegionStorage()
	now := time.Now()
	hotRegions := []*statistics.HistoryHotRegion{
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
		historyHotRegions := &statistics.HistoryHotRegions{}
		json.Unmarshal(res, historyHotRegions)
		for _, region := range historyHotRegions.HistoryHotRegion {
			c.Assert(region.UpdateTime, GreaterEqual, request.StartTime)
			c.Assert(region.UpdateTime, LessEqual, request.EndTime)
		}
	}
	writeToDB(c, storage.LeveldbKV, hotRegions)
	data, err := json.Marshal(request)
	c.Assert(err, IsNil)
	err = postJSON(testDialClient, s.urlPrefix+"/regions/history", data, check)
	c.Assert(err, IsNil)
}

func (s testHotStatusSuite) TestGetHistoryHotRegionsIDAndTypes(c *C) {
	storage := s.svr.GetHistoryHotRegionStorage()
	now := time.Now()
	hotRegions := []*statistics.HistoryHotRegion{
		{
			RegionID:      1,
			StoreID:       1,
			PeerID:        1,
			HotRegionType: "read",
			UpdateTime:    now.UnixNano() / int64(time.Millisecond),
		},
		{
			RegionID:      1,
			StoreID:       2,
			PeerID:        1,
			HotRegionType: "read",
			UpdateTime:    now.Add(10*time.Second).UnixNano() / int64(time.Millisecond),
		},
		{
			RegionID:      1,
			StoreID:       1,
			PeerID:        2,
			HotRegionType: "read",
			UpdateTime:    now.Add(20*time.Second).UnixNano() / int64(time.Millisecond),
		},
		{
			RegionID:      1,
			StoreID:       1,
			PeerID:        1,
			HotRegionType: "write",
			UpdateTime:    now.Add(30*time.Second).UnixNano() / int64(time.Millisecond),
		},
	}
	request := HistoryHotRegionsRequest{
		RegionIDs:      []uint64{1},
		StoreIDs:       []uint64{1},
		PeerIDs:        []uint64{1},
		HotRegionTypes: []string{"read"},
		EndTime:        now.Add(10*time.Minute).UnixNano() / int64(time.Millisecond),
	}
	check := func(res []byte, statusCode int) {
		c.Assert(statusCode, Equals, 200)
		historyHotRegions := &statistics.HistoryHotRegions{}
		json.Unmarshal(res, historyHotRegions)
		c.Assert(len(historyHotRegions.HistoryHotRegion), Equals, 1)
		c.Assert(reflect.DeepEqual(historyHotRegions.HistoryHotRegion[0], hotRegions[0]), IsTrue)
	}
	writeToDB(c, storage.LeveldbKV, hotRegions)
	data, err := json.Marshal(request)
	c.Assert(err, IsNil)
	err = postJSON(testDialClient, s.urlPrefix+"/regions/history", data, check)
	c.Assert(err, IsNil)
}

func (s testHotStatusSuite) TestGetHistoryHotRegionsBetween(c *C) {
	storage := s.svr.GetHistoryHotRegionStorage()
	now := time.Now()
	hotRegions := []*statistics.HistoryHotRegion{
		{
			RegionID:      1,
			HotDegree:     10,
			FlowBytes:     10.0,
			KeyRate:       10.0,
			QueryRate:     10.0,
			StartKey:      []byte("3"),
			EndKey:        []byte("5"),
			UpdateTime:    now.UnixNano() / int64(time.Millisecond),
			HotRegionType: "read",
		},
		{
			RegionID:      2,
			HotDegree:     20,
			FlowBytes:     10.0,
			KeyRate:       10.0,
			QueryRate:     10.0,
			StartKey:      []byte("3"),
			EndKey:        []byte("5"),
			UpdateTime:    now.UnixNano() / int64(time.Millisecond),
			HotRegionType: "read",
		},
		{
			RegionID:      3,
			HotDegree:     1,
			FlowBytes:     10.0,
			KeyRate:       10.0,
			QueryRate:     10.0,
			StartKey:      []byte("3"),
			EndKey:        []byte("5"),
			UpdateTime:    now.UnixNano() / int64(time.Millisecond),
			HotRegionType: "read",
		},
		{
			RegionID:      4,
			HotDegree:     10,
			FlowBytes:     20.0,
			KeyRate:       10.0,
			QueryRate:     10.0,
			StartKey:      []byte("3"),
			EndKey:        []byte("5"),
			UpdateTime:    now.UnixNano() / int64(time.Millisecond),
			HotRegionType: "read",
		},
		{
			RegionID:      5,
			HotDegree:     10,
			FlowBytes:     1.0,
			KeyRate:       10.0,
			QueryRate:     10.0,
			StartKey:      []byte("3"),
			EndKey:        []byte("5"),
			UpdateTime:    now.UnixNano() / int64(time.Millisecond),
			HotRegionType: "read",
		},
		{
			RegionID:      6,
			HotDegree:     10,
			FlowBytes:     10.0,
			KeyRate:       20.0,
			QueryRate:     10.0,
			StartKey:      []byte("3"),
			EndKey:        []byte("5"),
			UpdateTime:    now.UnixNano() / int64(time.Millisecond),
			HotRegionType: "read",
		},
		{
			RegionID:      7,
			HotDegree:     10,
			FlowBytes:     10.0,
			KeyRate:       1.0,
			QueryRate:     10.0,
			StartKey:      []byte("3"),
			EndKey:        []byte("5"),
			UpdateTime:    now.UnixNano() / int64(time.Millisecond),
			HotRegionType: "read",
		},
		{
			RegionID:      8,
			HotDegree:     10,
			FlowBytes:     10.0,
			KeyRate:       10.0,
			QueryRate:     20.0,
			StartKey:      []byte("3"),
			EndKey:        []byte("5"),
			UpdateTime:    now.UnixNano() / int64(time.Millisecond),
			HotRegionType: "read",
		},
		{
			RegionID:      9,
			HotDegree:     10,
			FlowBytes:     10.0,
			KeyRate:       10.0,
			QueryRate:     1.0,
			StartKey:      []byte("3"),
			EndKey:        []byte("5"),
			UpdateTime:    now.UnixNano() / int64(time.Millisecond),
			HotRegionType: "read",
		},
	}
	request := HistoryHotRegionsRequest{
		HighHotDegree: 11,
		LowHotDegree:  10,
		HighFlowBytes: 11.0,
		LowFlowBytes:  10.0,
		HighKeyRate:   11.0,
		LowKeyRate:    10.0,
		HighQueryRate: 11.0,
		LowQueryRate:  10.0,
		EndTime:       now.UnixNano() / int64(time.Millisecond),
	}
	check := func(res []byte, statusCode int) {
		c.Assert(statusCode, Equals, 200)
		historyHotRegions := &statistics.HistoryHotRegions{}
		json.Unmarshal(res, historyHotRegions)
		c.Assert(len(historyHotRegions.HistoryHotRegion), Equals, 1)
		c.Assert(reflect.DeepEqual(historyHotRegions.HistoryHotRegion[0], hotRegions[0]), IsTrue)
	}
	writeToDB(c, storage.LeveldbKV, hotRegions)
	data, err := json.Marshal(request)
	c.Assert(err, IsNil)
	err = postJSON(testDialClient, s.urlPrefix+"/regions/history", data, check)
	c.Assert(err, IsNil)
}

func writeToDB(c *C, kv *kv.LeveldbKV, hotRegions []*statistics.HistoryHotRegion) {
	batch := new(leveldb.Batch)
	for _, region := range hotRegions {
		key := cluster.HotRegionStorePath(region.HotRegionType, region.UpdateTime, region.RegionID)
		value, err := json.Marshal(region)
		c.Assert(err, IsNil)
		batch.Put([]byte(key), value)
	}
	kv.Write(batch, nil)
}
