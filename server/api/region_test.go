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
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"sort"
	"testing"
	"time"

	"github.com/docker/go-units"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/response"
	"github.com/tikv/pd/pkg/utils/apiutil"
	tu "github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
)

type regionTestSuite struct {
	suite.Suite
	svr       *server.Server
	cleanup   tu.CleanupFunc
	urlPrefix string
}

func TestRegionTestSuite(t *testing.T) {
	suite.Run(t, new(regionTestSuite))
}

func (suite *regionTestSuite) SetupSuite() {
	re := suite.Require()
	suite.svr, suite.cleanup = mustNewServer(re)
	server.MustWaitLeader(re, []*server.Server{suite.svr})

	addr := suite.svr.GetAddr()
	suite.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)

	mustBootstrapCluster(re, suite.svr)
}

func (suite *regionTestSuite) TearDownSuite() {
	suite.cleanup()
}

func (suite *regionTestSuite) TestRegion() {
	r := core.NewTestRegionInfo(2, 1, []byte("a"), []byte("b"),
		core.SetWrittenBytes(100*units.MiB),
		core.SetWrittenKeys(1*units.MiB),
		core.SetReadBytes(200*units.MiB),
		core.SetReadKeys(2*units.MiB))
	buckets := &metapb.Buckets{
		RegionId: 2,
		Keys:     [][]byte{[]byte("a"), []byte("b")},
		Version:  1,
	}
	r.UpdateBuckets(buckets, r.GetBuckets())
	re := suite.Require()
	mustRegionHeartbeat(re, suite.svr, r)
	url := fmt.Sprintf("%s/region/id/%d", suite.urlPrefix, r.GetID())
	r1 := &response.RegionInfo{}
	r1m := make(map[string]any)
	re.NoError(tu.ReadGetJSON(re, testDialClient, url, r1))
	r1.Adjust()
	re.Equal(response.NewAPIRegionInfo(r), r1)
	re.NoError(tu.ReadGetJSON(re, testDialClient, url, &r1m))
	re.Equal(float64(r.GetBytesWritten()), r1m["written_bytes"].(float64))
	re.Equal(float64(r.GetKeysWritten()), r1m["written_keys"].(float64))
	re.Equal(float64(r.GetBytesRead()), r1m["read_bytes"].(float64))
	re.Equal(float64(r.GetKeysRead()), r1m["read_keys"].(float64))
	keys := r1m["buckets"].([]any)
	re.Len(keys, 2)
	re.Equal(core.HexRegionKeyStr([]byte("a")), keys[0].(string))
	re.Equal(core.HexRegionKeyStr([]byte("b")), keys[1].(string))

	url = fmt.Sprintf("%s/region/key/%s", suite.urlPrefix, "a")
	r2 := &response.RegionInfo{}
	re.NoError(tu.ReadGetJSON(re, testDialClient, url, r2))
	r2.Adjust()
	re.Equal(response.NewAPIRegionInfo(r), r2)

	url = fmt.Sprintf("%s/region/key/%s?format=hex", suite.urlPrefix, hex.EncodeToString([]byte("a")))
	r2 = &response.RegionInfo{}
	re.NoError(tu.ReadGetJSON(re, testDialClient, url, r2))
	r2.Adjust()
	re.Equal(response.NewAPIRegionInfo(r), r2)
}

func (suite *regionTestSuite) TestRegionCheck() {
	r := core.NewTestRegionInfo(2, 1, []byte("a"), []byte("b"),
		core.SetApproximateKeys(10),
		core.SetApproximateSize(10))
	downPeer := &metapb.Peer{Id: 13, StoreId: 2}
	r = r.Clone(
		core.WithAddPeer(downPeer),
		core.WithDownPeers([]*pdpb.PeerStats{{Peer: downPeer, DownSeconds: 3600}}),
		core.WithPendingPeers([]*metapb.Peer{downPeer}))
	re := suite.Require()
	mustRegionHeartbeat(re, suite.svr, r)
	url := fmt.Sprintf("%s/region/id/%d", suite.urlPrefix, r.GetID())
	r1 := &response.RegionInfo{}
	re.NoError(tu.ReadGetJSON(re, testDialClient, url, r1))
	r1.Adjust()
	re.Equal(response.NewAPIRegionInfo(r), r1)

	url = fmt.Sprintf("%s/regions/check/%s", suite.urlPrefix, "down-peer")
	r2 := &response.RegionsInfo{}
	tu.Eventually(re, func() bool {
		if err := tu.ReadGetJSON(re, testDialClient, url, r2); err != nil {
			return false
		}
		r2.Adjust()
		return suite.Equal(&response.RegionsInfo{Count: 1, Regions: []response.RegionInfo{*response.NewAPIRegionInfo(r)}}, r2)
	})

	url = fmt.Sprintf("%s/regions/check/%s", suite.urlPrefix, "pending-peer")
	r3 := &response.RegionsInfo{}
	re.NoError(tu.ReadGetJSON(re, testDialClient, url, r3))
	r3.Adjust()
	re.Equal(&response.RegionsInfo{Count: 1, Regions: []response.RegionInfo{*response.NewAPIRegionInfo(r)}}, r3)

	url = fmt.Sprintf("%s/regions/check/%s", suite.urlPrefix, "offline-peer")
	r4 := &response.RegionsInfo{}
	re.NoError(tu.ReadGetJSON(re, testDialClient, url, r4))
	r4.Adjust()
	re.Equal(&response.RegionsInfo{Count: 0, Regions: []response.RegionInfo{}}, r4)

	r = r.Clone(core.SetApproximateSize(1))
	mustRegionHeartbeat(re, suite.svr, r)
	url = fmt.Sprintf("%s/regions/check/%s", suite.urlPrefix, "empty-region")
	r5 := &response.RegionsInfo{}
	tu.Eventually(re, func() bool {
		if err := tu.ReadGetJSON(re, testDialClient, url, r5); err != nil {
			return false
		}
		r5.Adjust()
		return suite.Equal(&response.RegionsInfo{Count: 1, Regions: []response.RegionInfo{*response.NewAPIRegionInfo(r)}}, r5)
	})

	r = r.Clone(core.SetApproximateSize(1))
	mustRegionHeartbeat(re, suite.svr, r)
	url = fmt.Sprintf("%s/regions/check/%s", suite.urlPrefix, "hist-size")
	r6 := make([]*histItem, 1)
	tu.Eventually(re, func() bool {
		if err := tu.ReadGetJSON(re, testDialClient, url, &r6); err != nil {
			return false
		}
		histSizes := []*histItem{{Start: 1, End: 1, Count: 1}}
		return suite.Equal(histSizes, r6)
	})

	r = r.Clone(core.SetApproximateKeys(1000))
	mustRegionHeartbeat(re, suite.svr, r)
	url = fmt.Sprintf("%s/regions/check/%s", suite.urlPrefix, "hist-keys")
	r7 := make([]*histItem, 1)
	tu.Eventually(re, func() bool {
		if err := tu.ReadGetJSON(re, testDialClient, url, &r7); err != nil {
			return false
		}
		histKeys := []*histItem{{Start: 1000, End: 1999, Count: 1}}
		return suite.Equal(histKeys, r7)
	})

	// ref https://github.com/tikv/pd/issues/3558, we should change size to pass `NeedUpdate` for observing.
	r = r.Clone(core.SetApproximateKeys(0))
	mustPutStore(re, suite.svr, 2, metapb.StoreState_Offline, metapb.NodeState_Removing, []*metapb.StoreLabel{})
	mustRegionHeartbeat(re, suite.svr, r)
	url = fmt.Sprintf("%s/regions/check/%s", suite.urlPrefix, "offline-peer")
	r8 := &response.RegionsInfo{}
	tu.Eventually(re, func() bool {
		if err := tu.ReadGetJSON(re, testDialClient, url, r8); err != nil {
			return false
		}
		r4.Adjust()
		return suite.Equal(r.GetID(), r8.Regions[0].ID) && r8.Count == 1
	})
}

func (suite *regionTestSuite) TestRegions() {
	re := suite.Require()
	r := response.NewAPIRegionInfo(core.NewRegionInfo(&metapb.Region{Id: 1}, nil))
	re.Nil(r.Leader.Peer)
	re.Empty(r.Leader.RoleName)

	rs := []*core.RegionInfo{
		core.NewTestRegionInfo(2, 1, []byte("a"), []byte("b"), core.SetApproximateKeys(10), core.SetApproximateSize(10)),
		core.NewTestRegionInfo(3, 1, []byte("b"), []byte("c"), core.SetApproximateKeys(10), core.SetApproximateSize(10)),
		core.NewTestRegionInfo(4, 2, []byte("c"), []byte("d"), core.SetApproximateKeys(10), core.SetApproximateSize(10)),
	}
	regions := make([]response.RegionInfo, 0, len(rs))
	for _, r := range rs {
		regions = append(regions, *response.NewAPIRegionInfo(r))
		mustRegionHeartbeat(re, suite.svr, r)
	}
	url := fmt.Sprintf("%s/regions", suite.urlPrefix)
	regionsInfo := &response.RegionsInfo{}
	err := tu.ReadGetJSON(re, testDialClient, url, regionsInfo)
	re.NoError(err)
	re.Len(regions, regionsInfo.Count)
	sort.Slice(regionsInfo.Regions, func(i, j int) bool {
		return regionsInfo.Regions[i].ID < regionsInfo.Regions[j].ID
	})
	for i, r := range regionsInfo.Regions {
		re.Equal(regions[i].ID, r.ID)
		re.Equal(regions[i].ApproximateSize, r.ApproximateSize)
		re.Equal(regions[i].ApproximateKeys, r.ApproximateKeys)
	}
}

func (suite *regionTestSuite) TestStoreRegions() {
	re := suite.Require()
	r1 := core.NewTestRegionInfo(2, 1, []byte("a"), []byte("b"))
	r2 := core.NewTestRegionInfo(3, 1, []byte("b"), []byte("c"))
	r3 := core.NewTestRegionInfo(4, 2, []byte("c"), []byte("d"))
	mustRegionHeartbeat(re, suite.svr, r1)
	mustRegionHeartbeat(re, suite.svr, r2)
	mustRegionHeartbeat(re, suite.svr, r3)

	regionIDs := []uint64{2, 3}
	url := fmt.Sprintf("%s/regions/store/%d", suite.urlPrefix, 1)
	r4 := &response.RegionsInfo{}
	err := tu.ReadGetJSON(re, testDialClient, url, r4)
	re.NoError(err)
	re.Len(regionIDs, r4.Count)
	sort.Slice(r4.Regions, func(i, j int) bool { return r4.Regions[i].ID < r4.Regions[j].ID })
	for i, r := range r4.Regions {
		re.Equal(regionIDs[i], r.ID)
	}

	regionIDs = []uint64{4}
	url = fmt.Sprintf("%s/regions/store/%d", suite.urlPrefix, 2)
	r5 := &response.RegionsInfo{}
	err = tu.ReadGetJSON(re, testDialClient, url, r5)
	re.NoError(err)
	re.Len(regionIDs, r5.Count)
	for i, r := range r5.Regions {
		re.Equal(regionIDs[i], r.ID)
	}

	regionIDs = []uint64{}
	url = fmt.Sprintf("%s/regions/store/%d", suite.urlPrefix, 3)
	r6 := &response.RegionsInfo{}
	err = tu.ReadGetJSON(re, testDialClient, url, r6)
	re.NoError(err)
	re.Len(regionIDs, r6.Count)
}

func (suite *regionTestSuite) TestTop() {
	// Top flow.
	re := suite.Require()
	r1 := core.NewTestRegionInfo(1, 1, []byte("a"), []byte("b"), core.SetWrittenBytes(1000), core.SetReadBytes(1000), core.SetRegionConfVer(1), core.SetRegionVersion(1))
	mustRegionHeartbeat(re, suite.svr, r1)
	r2 := core.NewTestRegionInfo(2, 1, []byte("b"), []byte("c"), core.SetWrittenBytes(2000), core.SetReadBytes(0), core.SetRegionConfVer(2), core.SetRegionVersion(3))
	mustRegionHeartbeat(re, suite.svr, r2)
	r3 := core.NewTestRegionInfo(3, 1, []byte("c"), []byte("d"), core.SetWrittenBytes(500), core.SetReadBytes(800), core.SetRegionConfVer(3), core.SetRegionVersion(2))
	mustRegionHeartbeat(re, suite.svr, r3)
	checkTopRegions(re, fmt.Sprintf("%s/regions/writeflow", suite.urlPrefix), []uint64{2, 1, 3})
	checkTopRegions(re, fmt.Sprintf("%s/regions/readflow", suite.urlPrefix), []uint64{1, 3, 2})
	checkTopRegions(re, fmt.Sprintf("%s/regions/writeflow?limit=2", suite.urlPrefix), []uint64{2, 1})
	checkTopRegions(re, fmt.Sprintf("%s/regions/confver", suite.urlPrefix), []uint64{3, 2, 1})
	checkTopRegions(re, fmt.Sprintf("%s/regions/confver?limit=2", suite.urlPrefix), []uint64{3, 2})
	checkTopRegions(re, fmt.Sprintf("%s/regions/version", suite.urlPrefix), []uint64{2, 3, 1})
	checkTopRegions(re, fmt.Sprintf("%s/regions/version?limit=2", suite.urlPrefix), []uint64{2, 3})
	// Top size.
	baseOpt := []core.RegionCreateOption{core.SetRegionConfVer(3), core.SetRegionVersion(3)}
	opt := core.SetApproximateSize(1000)
	r1 = core.NewTestRegionInfo(1, 1, []byte("a"), []byte("b"), append(baseOpt, opt)...)
	mustRegionHeartbeat(re, suite.svr, r1)
	opt = core.SetApproximateSize(900)
	r2 = core.NewTestRegionInfo(2, 1, []byte("b"), []byte("c"), append(baseOpt, opt)...)
	mustRegionHeartbeat(re, suite.svr, r2)
	opt = core.SetApproximateSize(800)
	r3 = core.NewTestRegionInfo(3, 1, []byte("c"), []byte("d"), append(baseOpt, opt)...)
	mustRegionHeartbeat(re, suite.svr, r3)
	checkTopRegions(re, fmt.Sprintf("%s/regions/size?limit=2", suite.urlPrefix), []uint64{1, 2})
	checkTopRegions(re, fmt.Sprintf("%s/regions/size", suite.urlPrefix), []uint64{1, 2, 3})
	// Top CPU usage.
	baseOpt = []core.RegionCreateOption{core.SetRegionConfVer(4), core.SetRegionVersion(4)}
	opt = core.SetCPUUsage(100)
	r1 = core.NewTestRegionInfo(1, 1, []byte("a"), []byte("b"), append(baseOpt, opt)...)
	mustRegionHeartbeat(re, suite.svr, r1)
	opt = core.SetCPUUsage(300)
	r2 = core.NewTestRegionInfo(2, 1, []byte("b"), []byte("c"), append(baseOpt, opt)...)
	mustRegionHeartbeat(re, suite.svr, r2)
	opt = core.SetCPUUsage(500)
	r3 = core.NewTestRegionInfo(3, 1, []byte("c"), []byte("d"), append(baseOpt, opt)...)
	mustRegionHeartbeat(re, suite.svr, r3)
	checkTopRegions(re, fmt.Sprintf("%s/regions/cpu?limit=2", suite.urlPrefix), []uint64{3, 2})
	checkTopRegions(re, fmt.Sprintf("%s/regions/cpu", suite.urlPrefix), []uint64{3, 2, 1})
}

func checkTopRegions(re *require.Assertions, url string, regionIDs []uint64) {
	regions := &response.RegionsInfo{}
	err := tu.ReadGetJSON(re, testDialClient, url, regions)
	re.NoError(err)
	re.Len(regionIDs, regions.Count)
	for i, r := range regions.Regions {
		re.Equal(regionIDs[i], r.ID)
	}
}

func (suite *regionTestSuite) TestTopN() {
	re := suite.Require()
	writtenBytes := []uint64{10, 10, 9, 5, 3, 2, 2, 1, 0, 0}
	for n := 0; n <= len(writtenBytes)+1; n++ {
		regions := make([]*core.RegionInfo, 0, len(writtenBytes))
		for _, i := range rand.Perm(len(writtenBytes)) {
			id := uint64(i + 1)
			region := core.NewTestRegionInfo(id, id, nil, nil, core.SetWrittenBytes(writtenBytes[i]))
			regions = append(regions, region)
		}
		topN := TopNRegions(regions, func(a, b *core.RegionInfo) bool { return a.GetBytesWritten() < b.GetBytesWritten() }, n)
		if n > len(writtenBytes) {
			re.Len(topN, len(writtenBytes))
		} else {
			re.Len(topN, n)
		}
		for i := range topN {
			re.Equal(writtenBytes[i], topN[i].GetBytesWritten())
		}
	}
}

func TestRegionsWithKillRequest(t *testing.T) {
	re := require.New(t)
	svr, cleanup := mustNewServer(re)
	defer cleanup()
	server.MustWaitLeader(re, []*server.Server{svr})

	addr := svr.GetAddr()
	url := fmt.Sprintf("%s%s/api/v1/regions", addr, apiPrefix)
	mustBootstrapCluster(re, svr)

	regionCount := 10000
	tu.GenerateTestDataConcurrently(regionCount, func(i int) {
		r := core.NewTestRegionInfo(uint64(i+2), 1,
			[]byte(fmt.Sprintf("%09d", i)),
			[]byte(fmt.Sprintf("%09d", i+1)),
			core.SetApproximateKeys(10), core.SetApproximateSize(10))
		mustRegionHeartbeat(re, svr, r)
	})

	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	re.NoError(err)
	doneCh := make(chan struct{})
	go func() {
		resp, err := testDialClient.Do(req)
		defer func() {
			if resp != nil {
				resp.Body.Close()
			}
		}()
		re.Error(err)
		re.Contains(err.Error(), "context canceled")
		re.Nil(resp)
		doneCh <- struct{}{}
	}()
	time.Sleep(100 * time.Millisecond) // wait for the request to be sent
	cancel()
	<-doneCh
	close(doneCh)
}

type getRegionTestSuite struct {
	suite.Suite
	svr       *server.Server
	cleanup   tu.CleanupFunc
	urlPrefix string
}

func TestGetRegionTestSuite(t *testing.T) {
	suite.Run(t, new(getRegionTestSuite))
}

func (suite *getRegionTestSuite) SetupSuite() {
	re := suite.Require()
	suite.svr, suite.cleanup = mustNewServer(re)
	server.MustWaitLeader(re, []*server.Server{suite.svr})

	addr := suite.svr.GetAddr()
	suite.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)

	mustBootstrapCluster(re, suite.svr)
}

func (suite *getRegionTestSuite) TearDownSuite() {
	suite.cleanup()
}

func (suite *getRegionTestSuite) TestRegionKey() {
	re := suite.Require()
	r := core.NewTestRegionInfo(99, 1, []byte{0xFF, 0xFF, 0xAA}, []byte{0xFF, 0xFF, 0xCC}, core.SetWrittenBytes(500), core.SetReadBytes(800), core.SetRegionConfVer(3), core.SetRegionVersion(2))
	mustRegionHeartbeat(re, suite.svr, r)
	url := fmt.Sprintf("%s/region/key/%s", suite.urlPrefix, url.QueryEscape(string([]byte{0xFF, 0xFF, 0xBB})))
	RegionInfo := &response.RegionInfo{}
	err := tu.ReadGetJSON(re, testDialClient, url, RegionInfo)
	re.NoError(err)
	re.Equal(RegionInfo.ID, r.GetID())
}

func (suite *getRegionTestSuite) TestScanRegionByKeys() {
	re := suite.Require()
	r1 := core.NewTestRegionInfo(2, 1, []byte("a"), []byte("b"))
	r2 := core.NewTestRegionInfo(3, 1, []byte("b"), []byte("c"))
	r3 := core.NewTestRegionInfo(4, 2, []byte("c"), []byte("e"))
	r4 := core.NewTestRegionInfo(5, 2, []byte("x"), []byte("z"))
	r := core.NewTestRegionInfo(99, 1, []byte{0xFF, 0xFF, 0xAA}, []byte{0xFF, 0xFF, 0xCC}, core.SetWrittenBytes(500), core.SetReadBytes(800), core.SetRegionConfVer(3), core.SetRegionVersion(2))
	mustRegionHeartbeat(re, suite.svr, r1)
	mustRegionHeartbeat(re, suite.svr, r2)
	mustRegionHeartbeat(re, suite.svr, r3)
	mustRegionHeartbeat(re, suite.svr, r4)
	mustRegionHeartbeat(re, suite.svr, r)

	url := fmt.Sprintf("%s/regions/key?key=%s", suite.urlPrefix, "b")
	regionIDs := []uint64{3, 4, 5, 99}
	regions := &response.RegionsInfo{}
	err := tu.ReadGetJSON(re, testDialClient, url, regions)
	re.NoError(err)
	re.Len(regionIDs, regions.Count)
	for i, v := range regionIDs {
		re.Equal(regions.Regions[i].ID, v)
	}
	url = fmt.Sprintf("%s/regions/key?key=%s", suite.urlPrefix, "d")
	regionIDs = []uint64{4, 5, 99}
	regions = &response.RegionsInfo{}
	err = tu.ReadGetJSON(re, testDialClient, url, regions)
	re.NoError(err)
	re.Len(regionIDs, regions.Count)
	for i, v := range regionIDs {
		re.Equal(regions.Regions[i].ID, v)
	}
	url = fmt.Sprintf("%s/regions/key?key=%s", suite.urlPrefix, "g")
	regionIDs = []uint64{5, 99}
	regions = &response.RegionsInfo{}
	err = tu.ReadGetJSON(re, testDialClient, url, regions)
	re.NoError(err)
	re.Len(regionIDs, regions.Count)
	for i, v := range regionIDs {
		re.Equal(regions.Regions[i].ID, v)
	}
	url = fmt.Sprintf("%s/regions/key?end_key=%s", suite.urlPrefix, "e")
	regionIDs = []uint64{2, 3, 4}
	regions = &response.RegionsInfo{}
	err = tu.ReadGetJSON(re, testDialClient, url, regions)
	re.NoError(err)
	re.Len(regionIDs, regions.Count)
	for i, v := range regionIDs {
		re.Equal(regions.Regions[i].ID, v)
	}
	url = fmt.Sprintf("%s/regions/key?key=%s&end_key=%s", suite.urlPrefix, "b", "g")
	regionIDs = []uint64{3, 4}
	regions = &response.RegionsInfo{}
	err = tu.ReadGetJSON(re, testDialClient, url, regions)
	re.NoError(err)
	re.Len(regionIDs, regions.Count)
	for i, v := range regionIDs {
		re.Equal(regions.Regions[i].ID, v)
	}
	url = fmt.Sprintf("%s/regions/key?key=%s&end_key=%s", suite.urlPrefix, "b", []byte{0xFF, 0xFF, 0xCC})
	regionIDs = []uint64{3, 4, 5, 99}
	regions = &response.RegionsInfo{}
	err = tu.ReadGetJSON(re, testDialClient, url, regions)
	re.NoError(err)
	re.Len(regionIDs, regions.Count)
	for i, v := range regionIDs {
		re.Equal(regions.Regions[i].ID, v)
	}
	url = fmt.Sprintf("%s/regions/key?key=%s&format=hex", suite.urlPrefix, hex.EncodeToString([]byte("b")))
	regionIDs = []uint64{3, 4, 5, 99}
	regions = &response.RegionsInfo{}
	err = tu.ReadGetJSON(re, testDialClient, url, regions)
	re.NoError(err)
	re.Len(regionIDs, regions.Count)
	for i, v := range regionIDs {
		re.Equal(regions.Regions[i].ID, v)
	}
	url = fmt.Sprintf("%s/regions/key?key=%s&end_key=%s&format=hex",
		suite.urlPrefix, hex.EncodeToString([]byte("b")), hex.EncodeToString([]byte("g")))
	regionIDs = []uint64{3, 4}
	regions = &response.RegionsInfo{}
	err = tu.ReadGetJSON(re, testDialClient, url, regions)
	re.NoError(err)
	re.Len(regionIDs, regions.Count)
	for i, v := range regionIDs {
		re.Equal(regions.Regions[i].ID, v)
	}
	url = fmt.Sprintf("%s/regions/key?key=%s&end_key=%s&format=hex",
		suite.urlPrefix, hex.EncodeToString([]byte("b")), hex.EncodeToString([]byte{0xFF, 0xFF, 0xCC}))
	regionIDs = []uint64{3, 4, 5, 99}
	regions = &response.RegionsInfo{}
	err = tu.ReadGetJSON(re, testDialClient, url, regions)
	re.NoError(err)
	re.Len(regionIDs, regions.Count)
	for i, v := range regionIDs {
		re.Equal(regions.Regions[i].ID, v)
	}
	// test invalid key
	url = fmt.Sprintf("%s/regions/key?key=%s&format=hex", suite.urlPrefix, "invalid")
	err = tu.CheckGetJSON(testDialClient, url, nil,
		tu.Status(re, http.StatusBadRequest),
		tu.StringEqual(re, "\"encoding/hex: invalid byte: U+0069 'i'\"\n"))
	re.NoError(err)
}

// Start a new test suite to prevent from being interfered by other tests.

type getRegionRangeHolesTestSuite struct {
	suite.Suite
	svr       *server.Server
	cleanup   tu.CleanupFunc
	urlPrefix string
}

func TestGetRegionRangeHolesTestSuite(t *testing.T) {
	suite.Run(t, new(getRegionRangeHolesTestSuite))
}

func (suite *getRegionRangeHolesTestSuite) SetupSuite() {
	re := suite.Require()
	suite.svr, suite.cleanup = mustNewServer(re)
	server.MustWaitLeader(re, []*server.Server{suite.svr})
	addr := suite.svr.GetAddr()
	suite.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)
	mustBootstrapCluster(re, suite.svr)
}

func (suite *getRegionRangeHolesTestSuite) TearDownSuite() {
	suite.cleanup()
}

func (suite *getRegionRangeHolesTestSuite) TestRegionRangeHoles() {
	re := suite.Require()
	// Missing r0 with range [0, 0xEA]
	r1 := core.NewTestRegionInfo(2, 1, []byte{0xEA}, []byte{0xEB})
	// Missing r2 with range [0xEB, 0xEC]
	r3 := core.NewTestRegionInfo(3, 1, []byte{0xEC}, []byte{0xED})
	r4 := core.NewTestRegionInfo(4, 2, []byte{0xED}, []byte{0xEE})
	// Missing r5 with range [0xEE, 0xFE]
	r6 := core.NewTestRegionInfo(5, 2, []byte{0xFE}, []byte{0xFF})
	mustRegionHeartbeat(re, suite.svr, r1)
	mustRegionHeartbeat(re, suite.svr, r3)
	mustRegionHeartbeat(re, suite.svr, r4)
	mustRegionHeartbeat(re, suite.svr, r6)

	url := fmt.Sprintf("%s/regions/range-holes", suite.urlPrefix)
	rangeHoles := new([][]string)
	re.NoError(tu.ReadGetJSON(re, testDialClient, url, rangeHoles))
	re.Equal([][]string{
		{"", core.HexRegionKeyStr(r1.GetStartKey())},
		{core.HexRegionKeyStr(r1.GetEndKey()), core.HexRegionKeyStr(r3.GetStartKey())},
		{core.HexRegionKeyStr(r4.GetEndKey()), core.HexRegionKeyStr(r6.GetStartKey())},
		{core.HexRegionKeyStr(r6.GetEndKey()), ""},
	}, *rangeHoles)
}

func TestRegionsInfoMarshal(t *testing.T) {
	re := require.New(t)
	regionWithNilPeer := core.NewRegionInfo(&metapb.Region{Id: 1}, &metapb.Peer{Id: 1})
	core.SetPeers([]*metapb.Peer{{Id: 2}, nil})(regionWithNilPeer)
	cases := [][]*core.RegionInfo{
		{},
		{
			// leader is nil
			core.NewRegionInfo(&metapb.Region{Id: 1}, nil),
			// Peers is empty
			core.NewRegionInfo(&metapb.Region{Id: 1}, &metapb.Peer{Id: 1},
				core.SetPeers([]*metapb.Peer{})),
			// There is nil peer in peers.
			regionWithNilPeer,
		},
		{
			// PendingPeers is empty
			core.NewRegionInfo(&metapb.Region{Id: 1}, &metapb.Peer{Id: 1},
				core.WithPendingPeers([]*metapb.Peer{})),
			// There is nil peer in peers.
			core.NewRegionInfo(&metapb.Region{Id: 1}, &metapb.Peer{Id: 1},
				core.WithPendingPeers([]*metapb.Peer{nil})),
		},
		{
			// DownPeers is empty
			core.NewRegionInfo(&metapb.Region{Id: 1}, &metapb.Peer{Id: 1},
				core.WithDownPeers([]*pdpb.PeerStats{})),
			// There is nil peer in peers.
			core.NewRegionInfo(&metapb.Region{Id: 1}, &metapb.Peer{Id: 1},
				core.WithDownPeers([]*pdpb.PeerStats{{Peer: nil}})),
		},
		{
			// Buckets is nil
			core.NewRegionInfo(&metapb.Region{Id: 1}, &metapb.Peer{Id: 1},
				core.SetBuckets(nil)),
			// Buckets is empty
			core.NewRegionInfo(&metapb.Region{Id: 1}, &metapb.Peer{Id: 1},
				core.SetBuckets(&metapb.Buckets{})),
		},
		{
			core.NewRegionInfo(&metapb.Region{Id: 1, StartKey: []byte{}, EndKey: []byte{},
				RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1}},
				&metapb.Peer{Id: 1}, core.SetCPUUsage(10),
				core.SetApproximateKeys(10), core.SetApproximateSize(10),
				core.SetWrittenBytes(10), core.SetReadBytes(10),
				core.SetReadKeys(10), core.SetWrittenKeys(10)),
		},
	}
	regionsInfo := &response.RegionsInfo{}
	for _, regions := range cases {
		b, err := response.MarshalRegionsInfoJSON(context.Background(), regions)
		re.NoError(err)
		err = json.Unmarshal(b, regionsInfo)
		re.NoError(err)
	}
}

func BenchmarkHexRegionKey(b *testing.B) {
	key := []byte("region_number_infinity")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = core.HexRegionKey(key)
	}
}

func BenchmarkHexRegionKeyStr(b *testing.B) {
	key := []byte("region_number_infinity")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = core.HexRegionKeyStr(key)
	}
}

func BenchmarkGetRegions(b *testing.B) {
	re := require.New(b)
	svr, cleanup := mustNewServer(re)
	defer cleanup()
	server.MustWaitLeader(re, []*server.Server{svr})

	addr := svr.GetAddr()
	url := fmt.Sprintf("%s%s/api/v1/regions", addr, apiPrefix)
	mustBootstrapCluster(re, svr)
	regionCount := 1000000
	for i := range regionCount {
		r := core.NewTestRegionInfo(uint64(i+2), 1,
			[]byte(fmt.Sprintf("%09d", i)),
			[]byte(fmt.Sprintf("%09d", i+1)),
			core.SetApproximateKeys(10), core.SetApproximateSize(10))
		mustRegionHeartbeat(re, svr, r)
	}
	resp, _ := apiutil.GetJSON(testDialClient, url, nil)
	regions := &response.RegionsInfo{}
	err := json.NewDecoder(resp.Body).Decode(regions)
	re.NoError(err)
	re.Equal(regionCount, regions.Count)
	resp.Body.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, _ := apiutil.GetJSON(testDialClient, url, nil)
		resp.Body.Close()
	}
}
