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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
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
	err = getJSON(testDialClient, s.urlPrefix+"/regions/history", data)
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
	err = getJSON(testDialClient, s.urlPrefix+"/regions/history", data, check)
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
			IsLeader:      false,
			HotRegionType: "read",
			UpdateTime:    now.UnixNano() / int64(time.Millisecond),
		},
		{
			RegionID:      1,
			StoreID:       2,
			PeerID:        1,
			IsLeader:      false,
			HotRegionType: "read",
			UpdateTime:    now.Add(10*time.Second).UnixNano() / int64(time.Millisecond),
		},
		{
			RegionID:      1,
			StoreID:       1,
			PeerID:        2,
			IsLeader:      false,
			HotRegionType: "read",
			UpdateTime:    now.Add(20*time.Second).UnixNano() / int64(time.Millisecond),
		},
		{
			RegionID:      1,
			StoreID:       1,
			PeerID:        1,
			IsLeader:      false,
			HotRegionType: "write",
			UpdateTime:    now.Add(30*time.Second).UnixNano() / int64(time.Millisecond),
		},
		{
			RegionID:      1,
			StoreID:       1,
			PeerID:        1,
			IsLeader:      true,
			HotRegionType: "read",
			UpdateTime:    now.Add(40*time.Second).UnixNano() / int64(time.Millisecond),
		},
	}
	request := HistoryHotRegionsRequest{
		RegionIDs:      []uint64{1},
		StoreIDs:       []uint64{1},
		PeerIDs:        []uint64{1},
		HotRegionTypes: []string{"read"},
		Roles:          []int64{0},
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
	err = getJSON(testDialClient, s.urlPrefix+"/regions/history", data, check)
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

func getJSON(client *http.Client, url string, data []byte, checkOpts ...func([]byte, int)) error {
	body := bytes.NewBuffer([]byte(data))
	req, err := http.NewRequest("GET", url, body)
	if err != nil {
		return errors.WithStack(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return errors.WithStack(err)
	}
	defer resp.Body.Close()
	res, err := io.ReadAll(resp.Body)

	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return errors.New(string(res))
	}
	for _, opt := range checkOpts {
		opt(res, resp.StatusCode)
	}
	return nil
}
