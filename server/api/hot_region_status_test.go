package api

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"time"

	. "github.com/pingcap/check"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/cluster"
	"github.com/tikv/pd/server/kv"
	_ "github.com/tikv/pd/server/schedulers"
	"github.com/tikv/pd/server/statistics"
)

var _ = Suite(&testHotHistoryStatusSuite{})

type testHotHistoryStatusSuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func (s *testHotHistoryStatusSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = mustNewServer(c)
	mustWaitLeader(c, []*server.Server{s.svr})
	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1/hotspot/history-hot-regions", addr, apiPrefix)
	mustBootstrapCluster(c, s.svr)
}

func (s *testHotHistoryStatusSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s testHotHistoryStatusSuite) TestGetHistoryHotRegionsBasic(c *C) {
	data := EncodeToBytes(statistics.HistoryHotRegionsRequest{}, c)
	err := postJSON(testDialClient, s.urlPrefix, data)
	c.Assert(err, IsNil)
}

func (s testHotHistoryStatusSuite) TestGetHistoryHotRegionsTimeRange(c *C) {
	storage := s.svr.GetHistoryHotRegionStorage()
	now := time.Now()
	hotRegions := []*statistics.HistoryHotRegion{
		{
			RegionID:   1,
			UpdateTime: now.Unix(),
		},
		{
			RegionID:   1,
			UpdateTime: now.Add(10 * time.Minute).Unix(),
		},
	}
	request := statistics.HistoryHotRegionsRequest{
		StartTime: now.Unix(),
		EndTime:   now.Add(10 * time.Second).Unix(),
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
	WriteToDB(c, storage.LeveldbKV, hotRegions)
	data := EncodeToBytes(request, c)
	err := postJSON(testDialClient, s.urlPrefix, data, check)
	c.Assert(err, IsNil)
}

func (s testHotHistoryStatusSuite) TestGetHistoryHotRegionsRegionID(c *C) {
	storage := s.svr.GetHistoryHotRegionStorage()
	now := time.Now()
	hotRegions := []*statistics.HistoryHotRegion{
		{
			RegionID:   1,
			UpdateTime: now.Unix(),
		},
		{
			RegionID:   2,
			UpdateTime: now.Add(10 * time.Minute).Unix(),
		},
	}
	request := statistics.HistoryHotRegionsRequest{
		RegionID:  []uint64{1},
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
	WriteToDB(c, storage.LeveldbKV, hotRegions)
	data := EncodeToBytes(request, c)
	err := postJSON(testDialClient, s.urlPrefix, data, check)
	c.Assert(err, IsNil)
}

func WriteToDB(c *C, kv *kv.LeveldbKV, hotRegions []*statistics.HistoryHotRegion) {
	batch := new(leveldb.Batch)
	for _, region := range hotRegions {
		key, err := cluster.EncodeHistoryHotRegion(region)
		c.Assert(err, IsNil)
		batch.Put(key, EncodeToBytes(region, c))
	}
}

func EncodeToBytes(p interface{}, c *C) []byte {
	buf := bytes.Buffer{}
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(p)
	c.Assert(err, IsNil)
	fmt.Println("uncompressed size (bytes): ", len(buf.Bytes()))
	return buf.Bytes()
}
