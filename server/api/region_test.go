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
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"sort"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	tu "github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/placement"
)

func TestPeer(t *testing.T) {
	re := require.New(t)
	peers := []*metapb.Peer{
		{Id: 1, StoreId: 10, Role: metapb.PeerRole_Voter},
		{Id: 2, StoreId: 20, Role: metapb.PeerRole_Learner},
		{Id: 3, StoreId: 30, Role: metapb.PeerRole_IncomingVoter},
		{Id: 4, StoreId: 40, Role: metapb.PeerRole_DemotingVoter},
	}
	// float64 is the default numeric type for JSON
	expected := []map[string]interface{}{
		{"id": float64(1), "store_id": float64(10), "role_name": "Voter"},
		{"id": float64(2), "store_id": float64(20), "role": float64(1), "role_name": "Learner", "is_learner": true},
		{"id": float64(3), "store_id": float64(30), "role": float64(2), "role_name": "IncomingVoter"},
		{"id": float64(4), "store_id": float64(40), "role": float64(3), "role_name": "DemotingVoter"},
	}

	data, err := json.Marshal(fromPeerSlice(peers))
	re.NoError(err)
	var ret []map[string]interface{}
	re.NoError(json.Unmarshal(data, &ret))
	re.Equal(expected, ret)
}

func TestPeerStats(t *testing.T) {
	re := require.New(t)
	peers := []*pdpb.PeerStats{
		{Peer: &metapb.Peer{Id: 1, StoreId: 10, Role: metapb.PeerRole_Voter}, DownSeconds: 0},
		{Peer: &metapb.Peer{Id: 2, StoreId: 20, Role: metapb.PeerRole_Learner}, DownSeconds: 1},
		{Peer: &metapb.Peer{Id: 3, StoreId: 30, Role: metapb.PeerRole_IncomingVoter}, DownSeconds: 2},
		{Peer: &metapb.Peer{Id: 4, StoreId: 40, Role: metapb.PeerRole_DemotingVoter}, DownSeconds: 3},
	}
	// float64 is the default numeric type for JSON
	expected := []map[string]interface{}{
		{"peer": map[string]interface{}{"id": float64(1), "store_id": float64(10), "role_name": "Voter"}},
		{"peer": map[string]interface{}{"id": float64(2), "store_id": float64(20), "role": float64(1), "role_name": "Learner", "is_learner": true}, "down_seconds": float64(1)},
		{"peer": map[string]interface{}{"id": float64(3), "store_id": float64(30), "role": float64(2), "role_name": "IncomingVoter"}, "down_seconds": float64(2)},
		{"peer": map[string]interface{}{"id": float64(4), "store_id": float64(40), "role": float64(3), "role_name": "DemotingVoter"}, "down_seconds": float64(3)},
	}

	data, err := json.Marshal(fromPeerStatsSlice(peers))
	re.NoError(err)
	var ret []map[string]interface{}
	re.NoError(json.Unmarshal(data, &ret))
	re.Equal(expected, ret)
}

type regionTestSuite struct {
	suite.Suite
	svr       *server.Server
	cleanup   cleanUpFunc
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

func newTestRegionInfo(regionID, storeID uint64, start, end []byte, opts ...core.RegionCreateOption) *core.RegionInfo {
	leader := &metapb.Peer{
		Id:      regionID,
		StoreId: storeID,
	}
	metaRegion := &metapb.Region{
		Id:          regionID,
		StartKey:    start,
		EndKey:      end,
		Peers:       []*metapb.Peer{leader},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
	}
	newOpts := []core.RegionCreateOption{
		core.SetApproximateKeys(10),
		core.SetApproximateSize(10),
		core.SetWrittenBytes(100 * 1024 * 1024),
		core.SetWrittenKeys(1 * 1024 * 1024),
		core.SetReadBytes(200 * 1024 * 1024),
		core.SetReadKeys(2 * 1024 * 1024),
	}
	newOpts = append(newOpts, opts...)
	region := core.NewRegionInfo(metaRegion, leader, newOpts...)
	return region
}

func (suite *regionTestSuite) TestRegion() {
	r := newTestRegionInfo(2, 1, []byte("a"), []byte("b"))
	buckets := &metapb.Buckets{
		RegionId: 2,
		Keys:     [][]byte{[]byte("a"), []byte("b")},
		Version:  1,
	}
	r.UpdateBuckets(buckets, r.GetBuckets())
	re := suite.Require()
	mustRegionHeartbeat(re, suite.svr, r)
	url := fmt.Sprintf("%s/region/id/%d", suite.urlPrefix, r.GetID())
	r1 := &RegionInfo{}
	r1m := make(map[string]interface{})
	suite.NoError(tu.ReadGetJSON(re, testDialClient, url, r1))
	r1.Adjust()
	suite.Equal(NewAPIRegionInfo(r), r1)
	suite.NoError(tu.ReadGetJSON(re, testDialClient, url, &r1m))
	suite.Equal(float64(r.GetBytesWritten()), r1m["written_bytes"].(float64))
	suite.Equal(float64(r.GetKeysWritten()), r1m["written_keys"].(float64))
	suite.Equal(float64(r.GetBytesRead()), r1m["read_bytes"].(float64))
	suite.Equal(float64(r.GetKeysRead()), r1m["read_keys"].(float64))
	keys := r1m["buckets"].([]interface{})
	suite.Len(keys, 2)
	suite.Equal(core.HexRegionKeyStr([]byte("a")), keys[0].(string))
	suite.Equal(core.HexRegionKeyStr([]byte("b")), keys[1].(string))

	url = fmt.Sprintf("%s/region/key/%s", suite.urlPrefix, "a")
	r2 := &RegionInfo{}
	suite.NoError(tu.ReadGetJSON(re, testDialClient, url, r2))
	r2.Adjust()
	suite.Equal(NewAPIRegionInfo(r), r2)
}

func (suite *regionTestSuite) TestRegionCheck() {
	r := newTestRegionInfo(2, 1, []byte("a"), []byte("b"))
	downPeer := &metapb.Peer{Id: 13, StoreId: 2}
	r = r.Clone(core.WithAddPeer(downPeer), core.WithDownPeers([]*pdpb.PeerStats{{Peer: downPeer, DownSeconds: 3600}}), core.WithPendingPeers([]*metapb.Peer{downPeer}))
	re := suite.Require()
	mustRegionHeartbeat(re, suite.svr, r)
	url := fmt.Sprintf("%s/region/id/%d", suite.urlPrefix, r.GetID())
	r1 := &RegionInfo{}
	suite.NoError(tu.ReadGetJSON(re, testDialClient, url, r1))
	r1.Adjust()
	suite.Equal(NewAPIRegionInfo(r), r1)

	url = fmt.Sprintf("%s/regions/check/%s", suite.urlPrefix, "down-peer")
	r2 := &RegionsInfo{}
	suite.NoError(tu.ReadGetJSON(re, testDialClient, url, r2))
	r2.Adjust()
	suite.Equal(&RegionsInfo{Count: 1, Regions: []RegionInfo{*NewAPIRegionInfo(r)}}, r2)

	url = fmt.Sprintf("%s/regions/check/%s", suite.urlPrefix, "pending-peer")
	r3 := &RegionsInfo{}
	suite.NoError(tu.ReadGetJSON(re, testDialClient, url, r3))
	r3.Adjust()
	suite.Equal(&RegionsInfo{Count: 1, Regions: []RegionInfo{*NewAPIRegionInfo(r)}}, r3)

	url = fmt.Sprintf("%s/regions/check/%s", suite.urlPrefix, "offline-peer")
	r4 := &RegionsInfo{}
	suite.NoError(tu.ReadGetJSON(re, testDialClient, url, r4))
	r4.Adjust()
	suite.Equal(&RegionsInfo{Count: 0, Regions: []RegionInfo{}}, r4)

	r = r.Clone(core.SetApproximateSize(1))
	mustRegionHeartbeat(re, suite.svr, r)
	url = fmt.Sprintf("%s/regions/check/%s", suite.urlPrefix, "empty-region")
	r5 := &RegionsInfo{}
	suite.NoError(tu.ReadGetJSON(re, testDialClient, url, r5))
	r5.Adjust()
	suite.Equal(&RegionsInfo{Count: 1, Regions: []RegionInfo{*NewAPIRegionInfo(r)}}, r5)

	r = r.Clone(core.SetApproximateSize(1))
	mustRegionHeartbeat(re, suite.svr, r)
	url = fmt.Sprintf("%s/regions/check/%s", suite.urlPrefix, "hist-size")
	r6 := make([]*histItem, 1)
	suite.NoError(tu.ReadGetJSON(re, testDialClient, url, &r6))
	histSizes := []*histItem{{Start: 1, End: 1, Count: 1}}
	suite.Equal(histSizes, r6)

	r = r.Clone(core.SetApproximateKeys(1000))
	mustRegionHeartbeat(re, suite.svr, r)
	url = fmt.Sprintf("%s/regions/check/%s", suite.urlPrefix, "hist-keys")
	r7 := make([]*histItem, 1)
	suite.NoError(tu.ReadGetJSON(re, testDialClient, url, &r7))
	histKeys := []*histItem{{Start: 1000, End: 1999, Count: 1}}
	suite.Equal(histKeys, r7)
}

func (suite *regionTestSuite) TestRegions() {
	rs := []*core.RegionInfo{
		newTestRegionInfo(2, 1, []byte("a"), []byte("b")),
		newTestRegionInfo(3, 1, []byte("b"), []byte("c")),
		newTestRegionInfo(4, 2, []byte("c"), []byte("d")),
	}
	regions := make([]RegionInfo, 0, len(rs))
	re := suite.Require()
	for _, r := range rs {
		regions = append(regions, *NewAPIRegionInfo(r))
		mustRegionHeartbeat(re, suite.svr, r)
	}
	url := fmt.Sprintf("%s/regions", suite.urlPrefix)
	RegionsInfo := &RegionsInfo{}
	err := tu.ReadGetJSON(re, testDialClient, url, RegionsInfo)
	suite.NoError(err)
	suite.Len(regions, RegionsInfo.Count)
	sort.Slice(RegionsInfo.Regions, func(i, j int) bool {
		return RegionsInfo.Regions[i].ID < RegionsInfo.Regions[j].ID
	})
	for i, r := range RegionsInfo.Regions {
		suite.Equal(regions[i].ID, r.ID)
		suite.Equal(regions[i].ApproximateSize, r.ApproximateSize)
		suite.Equal(regions[i].ApproximateKeys, r.ApproximateKeys)
	}
}

func (suite *regionTestSuite) TestStoreRegions() {
	re := suite.Require()
	r1 := newTestRegionInfo(2, 1, []byte("a"), []byte("b"))
	r2 := newTestRegionInfo(3, 1, []byte("b"), []byte("c"))
	r3 := newTestRegionInfo(4, 2, []byte("c"), []byte("d"))
	mustRegionHeartbeat(re, suite.svr, r1)
	mustRegionHeartbeat(re, suite.svr, r2)
	mustRegionHeartbeat(re, suite.svr, r3)

	regionIDs := []uint64{2, 3}
	url := fmt.Sprintf("%s/regions/store/%d", suite.urlPrefix, 1)
	r4 := &RegionsInfo{}
	err := tu.ReadGetJSON(re, testDialClient, url, r4)
	suite.NoError(err)
	suite.Len(regionIDs, r4.Count)
	sort.Slice(r4.Regions, func(i, j int) bool { return r4.Regions[i].ID < r4.Regions[j].ID })
	for i, r := range r4.Regions {
		suite.Equal(regionIDs[i], r.ID)
	}

	regionIDs = []uint64{4}
	url = fmt.Sprintf("%s/regions/store/%d", suite.urlPrefix, 2)
	r5 := &RegionsInfo{}
	err = tu.ReadGetJSON(re, testDialClient, url, r5)
	suite.NoError(err)
	suite.Len(regionIDs, r5.Count)
	for i, r := range r5.Regions {
		suite.Equal(regionIDs[i], r.ID)
	}

	regionIDs = []uint64{}
	url = fmt.Sprintf("%s/regions/store/%d", suite.urlPrefix, 3)
	r6 := &RegionsInfo{}
	err = tu.ReadGetJSON(re, testDialClient, url, r6)
	suite.NoError(err)
	suite.Len(regionIDs, r6.Count)
}

func (suite *regionTestSuite) TestTopFlow() {
	re := suite.Require()
	r1 := newTestRegionInfo(1, 1, []byte("a"), []byte("b"), core.SetWrittenBytes(1000), core.SetReadBytes(1000), core.SetRegionConfVer(1), core.SetRegionVersion(1))
	mustRegionHeartbeat(re, suite.svr, r1)
	r2 := newTestRegionInfo(2, 1, []byte("b"), []byte("c"), core.SetWrittenBytes(2000), core.SetReadBytes(0), core.SetRegionConfVer(2), core.SetRegionVersion(3))
	mustRegionHeartbeat(re, suite.svr, r2)
	r3 := newTestRegionInfo(3, 1, []byte("c"), []byte("d"), core.SetWrittenBytes(500), core.SetReadBytes(800), core.SetRegionConfVer(3), core.SetRegionVersion(2))
	mustRegionHeartbeat(re, suite.svr, r3)
	suite.checkTopRegions(fmt.Sprintf("%s/regions/writeflow", suite.urlPrefix), []uint64{2, 1, 3})
	suite.checkTopRegions(fmt.Sprintf("%s/regions/readflow", suite.urlPrefix), []uint64{1, 3, 2})
	suite.checkTopRegions(fmt.Sprintf("%s/regions/writeflow?limit=2", suite.urlPrefix), []uint64{2, 1})
	suite.checkTopRegions(fmt.Sprintf("%s/regions/confver", suite.urlPrefix), []uint64{3, 2, 1})
	suite.checkTopRegions(fmt.Sprintf("%s/regions/confver?limit=2", suite.urlPrefix), []uint64{3, 2})
	suite.checkTopRegions(fmt.Sprintf("%s/regions/version", suite.urlPrefix), []uint64{2, 3, 1})
	suite.checkTopRegions(fmt.Sprintf("%s/regions/version?limit=2", suite.urlPrefix), []uint64{2, 3})
}

func (suite *regionTestSuite) TestTopSize() {
	re := suite.Require()
	baseOpt := []core.RegionCreateOption{core.SetRegionConfVer(3), core.SetRegionVersion(3)}
	opt := core.SetApproximateSize(1000)
	r1 := newTestRegionInfo(7, 1, []byte("a"), []byte("b"), append(baseOpt, opt)...)
	mustRegionHeartbeat(re, suite.svr, r1)
	opt = core.SetApproximateSize(900)
	r2 := newTestRegionInfo(8, 1, []byte("b"), []byte("c"), append(baseOpt, opt)...)
	mustRegionHeartbeat(re, suite.svr, r2)
	opt = core.SetApproximateSize(800)
	r3 := newTestRegionInfo(9, 1, []byte("c"), []byte("d"), append(baseOpt, opt)...)
	mustRegionHeartbeat(re, suite.svr, r3)
	// query with limit
	suite.checkTopRegions(fmt.Sprintf("%s/regions/size?limit=%d", suite.urlPrefix, 2), []uint64{7, 8})
}

func (suite *regionTestSuite) TestAccelerateRegionsScheduleInRange() {
	re := suite.Require()
	r1 := newTestRegionInfo(557, 13, []byte("a1"), []byte("a2"))
	r2 := newTestRegionInfo(558, 14, []byte("a2"), []byte("a3"))
	r3 := newTestRegionInfo(559, 15, []byte("a3"), []byte("a4"))
	mustRegionHeartbeat(re, suite.svr, r1)
	mustRegionHeartbeat(re, suite.svr, r2)
	mustRegionHeartbeat(re, suite.svr, r3)
	body := fmt.Sprintf(`{"start_key":"%s", "end_key": "%s"}`, hex.EncodeToString([]byte("a1")), hex.EncodeToString([]byte("a3")))

	err := tu.CheckPostJSON(testDialClient, fmt.Sprintf("%s/regions/accelerate-schedule", suite.urlPrefix), []byte(body), tu.StatusOK(re))
	suite.NoError(err)
	idList := suite.svr.GetRaftCluster().GetSuspectRegions()
	suite.Len(idList, 2)
}

func (suite *regionTestSuite) TestScatterRegions() {
	re := suite.Require()
	r1 := newTestRegionInfo(601, 13, []byte("b1"), []byte("b2"))
	r1.GetMeta().Peers = append(r1.GetMeta().Peers, &metapb.Peer{Id: 5, StoreId: 14}, &metapb.Peer{Id: 6, StoreId: 15})
	r2 := newTestRegionInfo(602, 13, []byte("b2"), []byte("b3"))
	r2.GetMeta().Peers = append(r2.GetMeta().Peers, &metapb.Peer{Id: 7, StoreId: 14}, &metapb.Peer{Id: 8, StoreId: 15})
	r3 := newTestRegionInfo(603, 13, []byte("b4"), []byte("b4"))
	r3.GetMeta().Peers = append(r3.GetMeta().Peers, &metapb.Peer{Id: 9, StoreId: 14}, &metapb.Peer{Id: 10, StoreId: 15})
	mustRegionHeartbeat(re, suite.svr, r1)
	mustRegionHeartbeat(re, suite.svr, r2)
	mustRegionHeartbeat(re, suite.svr, r3)
	mustPutStore(re, suite.svr, 13, metapb.StoreState_Up, metapb.NodeState_Serving, []*metapb.StoreLabel{})
	mustPutStore(re, suite.svr, 14, metapb.StoreState_Up, metapb.NodeState_Serving, []*metapb.StoreLabel{})
	mustPutStore(re, suite.svr, 15, metapb.StoreState_Up, metapb.NodeState_Serving, []*metapb.StoreLabel{})
	mustPutStore(re, suite.svr, 16, metapb.StoreState_Up, metapb.NodeState_Serving, []*metapb.StoreLabel{})
	body := fmt.Sprintf(`{"start_key":"%s", "end_key": "%s"}`, hex.EncodeToString([]byte("b1")), hex.EncodeToString([]byte("b3")))

	err := tu.CheckPostJSON(testDialClient, fmt.Sprintf("%s/regions/scatter", suite.urlPrefix), []byte(body), tu.StatusOK(re))
	suite.NoError(err)
	op1 := suite.svr.GetRaftCluster().GetOperatorController().GetOperator(601)
	op2 := suite.svr.GetRaftCluster().GetOperatorController().GetOperator(602)
	op3 := suite.svr.GetRaftCluster().GetOperatorController().GetOperator(603)
	// At least one operator used to scatter region
	suite.True(op1 != nil || op2 != nil || op3 != nil)

	body = `{"regions_id": [601, 602, 603]}`
	err = tu.CheckPostJSON(testDialClient, fmt.Sprintf("%s/regions/scatter", suite.urlPrefix), []byte(body), tu.StatusOK(re))
	suite.NoError(err)
}

func (suite *regionTestSuite) TestSplitRegions() {
	re := suite.Require()
	r1 := newTestRegionInfo(601, 13, []byte("aaa"), []byte("ggg"))
	r1.GetMeta().Peers = append(r1.GetMeta().Peers, &metapb.Peer{Id: 5, StoreId: 13}, &metapb.Peer{Id: 6, StoreId: 13})
	mustRegionHeartbeat(re, suite.svr, r1)
	mustPutStore(re, suite.svr, 13, metapb.StoreState_Up, metapb.NodeState_Serving, []*metapb.StoreLabel{})
	newRegionID := uint64(11)
	body := fmt.Sprintf(`{"retry_limit":%v, "split_keys": ["%s","%s","%s"]}`, 3,
		hex.EncodeToString([]byte("bbb")),
		hex.EncodeToString([]byte("ccc")),
		hex.EncodeToString([]byte("ddd")))
	checkOpt := func(res []byte, code int) {
		s := &struct {
			ProcessedPercentage int      `json:"processed-percentage"`
			NewRegionsID        []uint64 `json:"regions-id"`
		}{}
		err := json.Unmarshal(res, s)
		suite.NoError(err)
		suite.Equal(100, s.ProcessedPercentage)
		suite.Equal([]uint64{newRegionID}, s.NewRegionsID)
	}
	suite.NoError(failpoint.Enable("github.com/tikv/pd/server/api/splitResponses", fmt.Sprintf("return(%v)", newRegionID)))
	err := tu.CheckPostJSON(testDialClient, fmt.Sprintf("%s/regions/split", suite.urlPrefix), []byte(body), checkOpt)
	suite.NoError(failpoint.Disable("github.com/tikv/pd/server/api/splitResponses"))
	suite.NoError(err)
}

func (suite *regionTestSuite) checkTopRegions(url string, regionIDs []uint64) {
	regions := &RegionsInfo{}
	err := tu.ReadGetJSON(suite.Require(), testDialClient, url, regions)
	suite.NoError(err)
	suite.Len(regionIDs, regions.Count)
	for i, r := range regions.Regions {
		suite.Equal(regionIDs[i], r.ID)
	}
}

func (suite *regionTestSuite) TestTopN() {
	writtenBytes := []uint64{10, 10, 9, 5, 3, 2, 2, 1, 0, 0}
	for n := 0; n <= len(writtenBytes)+1; n++ {
		regions := make([]*core.RegionInfo, 0, len(writtenBytes))
		for _, i := range rand.Perm(len(writtenBytes)) {
			id := uint64(i + 1)
			region := newTestRegionInfo(id, id, nil, nil, core.SetWrittenBytes(writtenBytes[i]))
			regions = append(regions, region)
		}
		topN := TopNRegions(regions, func(a, b *core.RegionInfo) bool { return a.GetBytesWritten() < b.GetBytesWritten() }, n)
		if n > len(writtenBytes) {
			suite.Len(topN, len(writtenBytes))
		} else {
			suite.Len(topN, n)
		}
		for i := range topN {
			suite.Equal(writtenBytes[i], topN[i].GetBytesWritten())
		}
	}
}

type getRegionTestSuite struct {
	suite.Suite
	svr       *server.Server
	cleanup   cleanUpFunc
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
	r := newTestRegionInfo(99, 1, []byte{0xFF, 0xFF, 0xAA}, []byte{0xFF, 0xFF, 0xCC}, core.SetWrittenBytes(500), core.SetReadBytes(800), core.SetRegionConfVer(3), core.SetRegionVersion(2))
	mustRegionHeartbeat(re, suite.svr, r)
	url := fmt.Sprintf("%s/region/key/%s", suite.urlPrefix, url.QueryEscape(string([]byte{0xFF, 0xFF, 0xBB})))
	RegionInfo := &RegionInfo{}
	err := tu.ReadGetJSON(re, testDialClient, url, RegionInfo)
	suite.NoError(err)
	suite.Equal(RegionInfo.ID, r.GetID())
}

func (suite *getRegionTestSuite) TestScanRegionByKeys() {
	re := suite.Require()
	r1 := newTestRegionInfo(2, 1, []byte("a"), []byte("b"))
	r2 := newTestRegionInfo(3, 1, []byte("b"), []byte("c"))
	r3 := newTestRegionInfo(4, 2, []byte("c"), []byte("e"))
	r4 := newTestRegionInfo(5, 2, []byte("x"), []byte("z"))
	r := newTestRegionInfo(99, 1, []byte{0xFF, 0xFF, 0xAA}, []byte{0xFF, 0xFF, 0xCC}, core.SetWrittenBytes(500), core.SetReadBytes(800), core.SetRegionConfVer(3), core.SetRegionVersion(2))
	mustRegionHeartbeat(re, suite.svr, r1)
	mustRegionHeartbeat(re, suite.svr, r2)
	mustRegionHeartbeat(re, suite.svr, r3)
	mustRegionHeartbeat(re, suite.svr, r4)
	mustRegionHeartbeat(re, suite.svr, r)

	url := fmt.Sprintf("%s/regions/key?key=%s", suite.urlPrefix, "b")
	regionIDs := []uint64{3, 4, 5, 99}
	regions := &RegionsInfo{}
	err := tu.ReadGetJSON(re, testDialClient, url, regions)
	suite.NoError(err)
	suite.Len(regionIDs, regions.Count)
	for i, v := range regionIDs {
		suite.Equal(regions.Regions[i].ID, v)
	}
	url = fmt.Sprintf("%s/regions/key?key=%s", suite.urlPrefix, "d")
	regionIDs = []uint64{4, 5, 99}
	regions = &RegionsInfo{}
	err = tu.ReadGetJSON(re, testDialClient, url, regions)
	suite.NoError(err)
	suite.Len(regionIDs, regions.Count)
	for i, v := range regionIDs {
		suite.Equal(regions.Regions[i].ID, v)
	}
	url = fmt.Sprintf("%s/regions/key?key=%s", suite.urlPrefix, "g")
	regionIDs = []uint64{5, 99}
	regions = &RegionsInfo{}
	err = tu.ReadGetJSON(re, testDialClient, url, regions)
	suite.NoError(err)
	suite.Len(regionIDs, regions.Count)
	for i, v := range regionIDs {
		suite.Equal(regions.Regions[i].ID, v)
	}
	url = fmt.Sprintf("%s/regions/key?end_key=%s", suite.urlPrefix, "e")
	regionIDs = []uint64{2, 3, 4}
	regions = &RegionsInfo{}
	err = tu.ReadGetJSON(re, testDialClient, url, regions)
	suite.NoError(err)
	suite.Len(regionIDs, regions.Count)
	for i, v := range regionIDs {
		suite.Equal(regions.Regions[i].ID, v)
	}
	url = fmt.Sprintf("%s/regions/key?key=%s&end_key=%s", suite.urlPrefix, "b", "g")
	regionIDs = []uint64{3, 4}
	regions = &RegionsInfo{}
	err = tu.ReadGetJSON(re, testDialClient, url, regions)
	suite.NoError(err)
	suite.Len(regionIDs, regions.Count)
	for i, v := range regionIDs {
		suite.Equal(regions.Regions[i].ID, v)
	}
	url = fmt.Sprintf("%s/regions/key?key=%s&end_key=%s", suite.urlPrefix, "b", []byte{0xFF, 0xFF, 0xCC})
	regionIDs = []uint64{3, 4, 5, 99}
	regions = &RegionsInfo{}
	err = tu.ReadGetJSON(re, testDialClient, url, regions)
	suite.NoError(err)
	suite.Len(regionIDs, regions.Count)
	for i, v := range regionIDs {
		suite.Equal(regions.Regions[i].ID, v)
	}
}

// Start a new test suite to prevent from being interfered by other tests.

type getRegionRangeHolesTestSuite struct {
	suite.Suite
	svr       *server.Server
	cleanup   cleanUpFunc
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
	r1 := newTestRegionInfo(2, 1, []byte{0xEA}, []byte{0xEB})
	// Missing r2 with range [0xEB, 0xEC]
	r3 := newTestRegionInfo(3, 1, []byte{0xEC}, []byte{0xED})
	r4 := newTestRegionInfo(4, 2, []byte{0xED}, []byte{0xEE})
	// Missing r5 with range [0xEE, 0xFE]
	r6 := newTestRegionInfo(5, 2, []byte{0xFE}, []byte{0xFF})
	mustRegionHeartbeat(re, suite.svr, r1)
	mustRegionHeartbeat(re, suite.svr, r3)
	mustRegionHeartbeat(re, suite.svr, r4)
	mustRegionHeartbeat(re, suite.svr, r6)

	url := fmt.Sprintf("%s/regions/range-holes", suite.urlPrefix)
	rangeHoles := new([][]string)
	suite.NoError(tu.ReadGetJSON(re, testDialClient, url, rangeHoles))
	suite.Equal([][]string{
		{"", core.HexRegionKeyStr(r1.GetStartKey())},
		{core.HexRegionKeyStr(r1.GetEndKey()), core.HexRegionKeyStr(r3.GetStartKey())},
		{core.HexRegionKeyStr(r4.GetEndKey()), core.HexRegionKeyStr(r6.GetStartKey())},
		{core.HexRegionKeyStr(r6.GetEndKey()), ""},
	}, *rangeHoles)
}

type regionsReplicatedTestSuite struct {
	suite.Suite
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func TestRegionsReplicatedTestSuite(t *testing.T) {
	suite.Run(t, new(regionsReplicatedTestSuite))
}

func (suite *regionsReplicatedTestSuite) SetupSuite() {
	re := suite.Require()
	suite.svr, suite.cleanup = mustNewServer(re)
	server.MustWaitLeader(re, []*server.Server{suite.svr})

	addr := suite.svr.GetAddr()
	suite.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)

	mustBootstrapCluster(re, suite.svr)
}

func (suite *regionsReplicatedTestSuite) TearDownSuite() {
	suite.cleanup()
}

func (suite *regionsReplicatedTestSuite) TestCheckRegionsReplicated() {
	re := suite.Require()
	// enable placement rule
	suite.NoError(tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/config", []byte(`{"enable-placement-rules":"true"}`), tu.StatusOK(re)))
	defer func() {
		suite.NoError(tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/config", []byte(`{"enable-placement-rules":"false"}`), tu.StatusOK(re)))
	}()

	// add test region
	r1 := newTestRegionInfo(2, 1, []byte("a"), []byte("b"))
	mustRegionHeartbeat(re, suite.svr, r1)

	// set the bundle
	bundle := []placement.GroupBundle{
		{
			ID:    "5",
			Index: 5,
			Rules: []*placement.Rule{
				{
					ID: "foo", Index: 1, Role: "voter", Count: 1,
				},
			},
		},
	}

	status := ""

	// invalid url
	url := fmt.Sprintf(`%s/regions/replicated?startKey=%s&endKey=%s`, suite.urlPrefix, "_", "t")
	err := tu.CheckGetJSON(testDialClient, url, nil, tu.Status(re, http.StatusBadRequest))
	suite.NoError(err)

	url = fmt.Sprintf(`%s/regions/replicated?startKey=%s&endKey=%s`, suite.urlPrefix, hex.EncodeToString(r1.GetStartKey()), "_")
	err = tu.CheckGetJSON(testDialClient, url, nil, tu.Status(re, http.StatusBadRequest))
	suite.NoError(err)

	// correct test
	url = fmt.Sprintf(`%s/regions/replicated?startKey=%s&endKey=%s`, suite.urlPrefix, hex.EncodeToString(r1.GetStartKey()), hex.EncodeToString(r1.GetEndKey()))

	// test one rule
	data, err := json.Marshal(bundle)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/config/placement-rule", data, tu.StatusOK(re))
	suite.NoError(err)

	err = tu.ReadGetJSON(re, testDialClient, url, &status)
	suite.NoError(err)
	suite.Equal("REPLICATED", status)

	suite.NoError(failpoint.Enable("github.com/tikv/pd/server/api/mockPending", "return(true)"))
	err = tu.ReadGetJSON(re, testDialClient, url, &status)
	suite.NoError(err)
	suite.Equal("PENDING", status)
	suite.NoError(failpoint.Disable("github.com/tikv/pd/server/api/mockPending"))
	// test multiple rules
	r1 = newTestRegionInfo(2, 1, []byte("a"), []byte("b"))
	r1.GetMeta().Peers = append(r1.GetMeta().Peers, &metapb.Peer{Id: 5, StoreId: 1})
	mustRegionHeartbeat(re, suite.svr, r1)

	bundle[0].Rules = append(bundle[0].Rules, &placement.Rule{
		ID: "bar", Index: 1, Role: "voter", Count: 1,
	})
	data, err = json.Marshal(bundle)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/config/placement-rule", data, tu.StatusOK(re))
	suite.NoError(err)

	err = tu.ReadGetJSON(re, testDialClient, url, &status)
	suite.NoError(err)
	suite.Equal("REPLICATED", status)

	// test multiple bundles
	bundle = append(bundle, placement.GroupBundle{
		ID:    "6",
		Index: 6,
		Rules: []*placement.Rule{
			{
				ID: "foo", Index: 1, Role: "voter", Count: 2,
			},
		},
	})
	data, err = json.Marshal(bundle)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/config/placement-rule", data, tu.StatusOK(re))
	suite.NoError(err)

	err = tu.ReadGetJSON(re, testDialClient, url, &status)
	suite.NoError(err)
	suite.Equal("INPROGRESS", status)

	r1 = newTestRegionInfo(2, 1, []byte("a"), []byte("b"))
	r1.GetMeta().Peers = append(r1.GetMeta().Peers, &metapb.Peer{Id: 5, StoreId: 1}, &metapb.Peer{Id: 6, StoreId: 1}, &metapb.Peer{Id: 7, StoreId: 1})
	mustRegionHeartbeat(re, suite.svr, r1)

	err = tu.ReadGetJSON(re, testDialClient, url, &status)
	suite.NoError(err)
	suite.Equal("REPLICATED", status)
}

// Create n regions (0..n) of n stores (0..n).
// Each region contains np peers, the first peer is the leader.
// (copied from server/cluster_test.go)
func newTestRegions() []*core.RegionInfo {
	n := uint64(10000)
	np := uint64(3)

	regions := make([]*core.RegionInfo, 0, n)
	for i := uint64(0); i < n; i++ {
		peers := make([]*metapb.Peer, 0, np)
		for j := uint64(0); j < np; j++ {
			peer := &metapb.Peer{
				Id: i*np + j,
			}
			peer.StoreId = (i + j) % n
			peers = append(peers, peer)
		}
		region := &metapb.Region{
			Id:          i,
			Peers:       peers,
			StartKey:    []byte(fmt.Sprintf("%d", i)),
			EndKey:      []byte(fmt.Sprintf("%d", i+1)),
			RegionEpoch: &metapb.RegionEpoch{ConfVer: 2, Version: 2},
		}
		regions = append(regions, core.NewRegionInfo(region, peers[0]))
	}
	return regions
}

func BenchmarkRenderJSON(b *testing.B) {
	regionInfos := newTestRegions()
	rd := createStreamingRender()
	regions := convertToAPIRegions(regionInfos)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buffer bytes.Buffer
		rd.JSON(&buffer, 200, regions)
	}
}

func BenchmarkConvertToAPIRegions(b *testing.B) {
	regionInfos := newTestRegions()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		regions := convertToAPIRegions(regionInfos)
		_ = regions.Count
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
