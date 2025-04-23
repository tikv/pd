// Copyright 2023 TiKV Project Authors.
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
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/docker/go-units"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/response"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/utils/apiutil"
	tu "github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/api"
	"github.com/tikv/pd/tests"
)

type regionTestSuite struct {
	suite.Suite
	env *tests.SchedulingTestEnvironment
}

func TestRegionTestSuite(t *testing.T) {
	suite.Run(t, new(regionTestSuite))
}

func (suite *regionTestSuite) SetupTest() {
	// use a new environment to avoid affecting other tests
	suite.env = tests.NewSchedulingTestEnvironment(suite.T())
}

func (suite *regionTestSuite) TearDownTest() {
	suite.env.Cleanup()
}

func (suite *regionTestSuite) TestSplitRegions() {
	suite.env.RunTest(suite.checkSplitRegions)
}

func (suite *regionTestSuite) checkSplitRegions(cluster *tests.TestCluster) {
	leader := cluster.GetLeaderServer()
	urlPrefix := leader.GetAddr() + "/pd/api/v1"
	re := suite.Require()
	s1 := &metapb.Store{
		Id:        13,
		State:     metapb.StoreState_Up,
		NodeState: metapb.NodeState_Serving,
	}
	tests.MustPutStore(re, cluster, s1)
	r1 := core.NewTestRegionInfo(601, 13, []byte("aaa"), []byte("ggg"))
	r1.GetMeta().Peers = append(r1.GetMeta().Peers, &metapb.Peer{Id: 5, StoreId: 14}, &metapb.Peer{Id: 6, StoreId: 15})
	tests.MustPutRegionInfo(re, cluster, r1)
	checkRegionCount(re, cluster, 1)

	newRegionID := uint64(11)
	body := fmt.Sprintf(`{"retry_limit":%v, "split_keys": ["%s","%s","%s"]}`, 3,
		hex.EncodeToString([]byte("bbb")),
		hex.EncodeToString([]byte("ccc")),
		hex.EncodeToString([]byte("ddd")))
	checkOpt := func(res []byte, _ int, _ http.Header) {
		s := &struct {
			ProcessedPercentage int      `json:"processed-percentage"`
			NewRegionsID        []uint64 `json:"regions-id"`
		}{}
		err := json.Unmarshal(res, s)
		re.NoError(err)
		re.Equal(100, s.ProcessedPercentage)
		re.Equal([]uint64{newRegionID}, s.NewRegionsID)
	}
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/handler/splitResponses", fmt.Sprintf("return(%v)", newRegionID)))
	err := tu.CheckPostJSON(tests.TestDialClient, fmt.Sprintf("%s/regions/split", urlPrefix), []byte(body), checkOpt)
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/handler/splitResponses"))
	re.NoError(err)
}

func (suite *regionTestSuite) TestAccelerateRegionsScheduleInRange() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/checker/skipCheckSuspectRanges", "return(true)"))
	suite.env.RunTest(suite.checkAccelerateRegionsScheduleInRange)
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/checker/skipCheckSuspectRanges"))
}

func (suite *regionTestSuite) checkAccelerateRegionsScheduleInRange(cluster *tests.TestCluster) {
	leader := cluster.GetLeaderServer()
	urlPrefix := leader.GetAddr() + "/pd/api/v1"
	re := suite.Require()
	for i := 1; i <= 3; i++ {
		s1 := &metapb.Store{
			Id:        uint64(i),
			State:     metapb.StoreState_Up,
			NodeState: metapb.NodeState_Serving,
		}
		tests.MustPutStore(re, cluster, s1)
	}
	regionCount := uint64(3)
	for i := uint64(1); i <= regionCount; i++ {
		r1 := core.NewTestRegionInfo(550+i, 1, []byte("a"+strconv.FormatUint(i, 10)), []byte("a"+strconv.FormatUint(i+1, 10)))
		r1.GetMeta().Peers = append(r1.GetMeta().Peers, &metapb.Peer{Id: 100 + i, StoreId: (i + 1) % regionCount}, &metapb.Peer{Id: 200 + i, StoreId: (i + 2) % regionCount})
		tests.MustPutRegionInfo(re, cluster, r1)
	}
	checkRegionCount(re, cluster, regionCount)

	body := fmt.Sprintf(`{"start_key":"%s", "end_key": "%s"}`, hex.EncodeToString([]byte("a1")), hex.EncodeToString([]byte("a3")))
	err := tu.CheckPostJSON(tests.TestDialClient, fmt.Sprintf("%s/regions/accelerate-schedule", urlPrefix), []byte(body),
		tu.StatusOK(re))
	re.NoError(err)
	idList := leader.GetRaftCluster().GetPendingProcessedRegions()
	if sche := cluster.GetSchedulingPrimaryServer(); sche != nil {
		idList = sche.GetCluster().GetCoordinator().GetCheckerController().GetPendingProcessedRegions()
	}
	re.Len(idList, 2, len(idList))
}

func (suite *regionTestSuite) TestAccelerateRegionsScheduleInRanges() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/checker/skipCheckSuspectRanges", "return(true)"))
	suite.env.RunTest(suite.checkAccelerateRegionsScheduleInRanges)
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/checker/skipCheckSuspectRanges"))
}

func (suite *regionTestSuite) checkAccelerateRegionsScheduleInRanges(cluster *tests.TestCluster) {
	leader := cluster.GetLeaderServer()
	urlPrefix := leader.GetAddr() + "/pd/api/v1"
	re := suite.Require()
	for i := 1; i <= 6; i++ {
		s1 := &metapb.Store{
			Id:        uint64(i),
			State:     metapb.StoreState_Up,
			NodeState: metapb.NodeState_Serving,
		}
		tests.MustPutStore(re, cluster, s1)
	}
	regionCount := uint64(6)
	for i := uint64(1); i <= regionCount; i++ {
		r1 := core.NewTestRegionInfo(550+i, 1, []byte("a"+strconv.FormatUint(i, 10)), []byte("a"+strconv.FormatUint(i+1, 10)))
		r1.GetMeta().Peers = append(r1.GetMeta().Peers, &metapb.Peer{Id: 100 + i, StoreId: (i + 1) % regionCount}, &metapb.Peer{Id: 200 + i, StoreId: (i + 2) % regionCount})
		tests.MustPutRegionInfo(re, cluster, r1)
	}
	checkRegionCount(re, cluster, regionCount)

	body := fmt.Sprintf(`[{"start_key":"%s", "end_key": "%s"}, {"start_key":"%s", "end_key": "%s"}]`,
		hex.EncodeToString([]byte("a1")), hex.EncodeToString([]byte("a3")), hex.EncodeToString([]byte("a4")), hex.EncodeToString([]byte("a6")))
	err := tu.CheckPostJSON(tests.TestDialClient, fmt.Sprintf("%s/regions/accelerate-schedule/batch", urlPrefix), []byte(body),
		tu.StatusOK(re))
	re.NoError(err)
	idList := leader.GetRaftCluster().GetPendingProcessedRegions()
	if sche := cluster.GetSchedulingPrimaryServer(); sche != nil {
		idList = sche.GetCluster().GetCoordinator().GetCheckerController().GetPendingProcessedRegions()
	}
	re.Len(idList, 4)
}

func (suite *regionTestSuite) TestScatterRegions() {
	suite.env.RunTest(suite.checkScatterRegions)
}

func (suite *regionTestSuite) checkScatterRegions(cluster *tests.TestCluster) {
	leader := cluster.GetLeaderServer()
	urlPrefix := leader.GetAddr() + "/pd/api/v1"
	re := suite.Require()
	for i := 13; i <= 16; i++ {
		s1 := &metapb.Store{
			Id:        uint64(i),
			State:     metapb.StoreState_Up,
			NodeState: metapb.NodeState_Serving,
		}
		tests.MustPutStore(re, cluster, s1)
	}
	r1 := core.NewTestRegionInfo(701, 13, []byte("b1"), []byte("b2"))
	r1.GetMeta().Peers = append(r1.GetMeta().Peers, &metapb.Peer{Id: 5, StoreId: 14}, &metapb.Peer{Id: 6, StoreId: 15})
	r2 := core.NewTestRegionInfo(702, 13, []byte("b2"), []byte("b3"))
	r2.GetMeta().Peers = append(r2.GetMeta().Peers, &metapb.Peer{Id: 7, StoreId: 14}, &metapb.Peer{Id: 8, StoreId: 15})
	r3 := core.NewTestRegionInfo(703, 13, []byte("b4"), []byte("b4"))
	r3.GetMeta().Peers = append(r3.GetMeta().Peers, &metapb.Peer{Id: 9, StoreId: 14}, &metapb.Peer{Id: 10, StoreId: 15})
	tests.MustPutRegionInfo(re, cluster, r1)
	tests.MustPutRegionInfo(re, cluster, r2)
	tests.MustPutRegionInfo(re, cluster, r3)
	checkRegionCount(re, cluster, 3)

	body := fmt.Sprintf(`{"start_key":"%s", "end_key": "%s"}`, hex.EncodeToString([]byte("b1")), hex.EncodeToString([]byte("b3")))
	err := tu.CheckPostJSON(tests.TestDialClient, fmt.Sprintf("%s/regions/scatter", urlPrefix), []byte(body), tu.StatusOK(re))
	re.NoError(err)
	oc := leader.GetRaftCluster().GetOperatorController()
	if sche := cluster.GetSchedulingPrimaryServer(); sche != nil {
		oc = sche.GetCoordinator().GetOperatorController()
	}

	op1 := oc.GetOperator(701)
	op2 := oc.GetOperator(702)
	op3 := oc.GetOperator(703)
	// At least one operator used to scatter region
	re.True(op1 != nil || op2 != nil || op3 != nil)

	body = `{"regions_id": [701, 702, 703]}`
	err = tu.CheckPostJSON(tests.TestDialClient, fmt.Sprintf("%s/regions/scatter", urlPrefix), []byte(body), tu.StatusOK(re))
	re.NoError(err)
}

func (suite *regionTestSuite) TestCheckRegionsReplicated() {
	suite.env.RunTest(suite.checkRegionsReplicated)
}

func (suite *regionTestSuite) checkRegionsReplicated(cluster *tests.TestCluster) {
	re := suite.Require()
	pauseAllCheckers(re, cluster)
	leader := cluster.GetLeaderServer()
	urlPrefix := leader.GetAddr() + "/pd/api/v1"

	// add test region
	s1 := &metapb.Store{
		Id:        1,
		State:     metapb.StoreState_Up,
		NodeState: metapb.NodeState_Serving,
	}
	tests.MustPutStore(re, cluster, s1)
	r1 := core.NewTestRegionInfo(2, 1, []byte("a"), []byte("b"))
	tests.MustPutRegionInfo(re, cluster, r1)
	checkRegionCount(re, cluster, 1)

	// set the bundle
	bundle := []placement.GroupBundle{
		{
			ID:    "5",
			Index: 5,
			Rules: []*placement.Rule{
				{
					ID: "foo", Index: 1, Role: placement.Voter, Count: 1,
				},
			},
		},
	}

	status := ""

	// invalid url
	url := fmt.Sprintf(`%s/regions/replicated?startKey=%s&endKey=%s`, urlPrefix, "_", "t")
	err := tu.CheckGetJSON(tests.TestDialClient, url, nil, tu.Status(re, http.StatusBadRequest))
	re.NoError(err)

	url = fmt.Sprintf(`%s/regions/replicated?startKey=%s&endKey=%s`, urlPrefix, hex.EncodeToString(r1.GetStartKey()), "_")
	err = tu.CheckGetJSON(tests.TestDialClient, url, nil, tu.Status(re, http.StatusBadRequest))
	re.NoError(err)

	// correct test
	url = fmt.Sprintf(`%s/regions/replicated?startKey=%s&endKey=%s`, urlPrefix, hex.EncodeToString(r1.GetStartKey()), hex.EncodeToString(r1.GetEndKey()))
	err = tu.CheckGetJSON(tests.TestDialClient, url, nil, tu.StatusOK(re))
	re.NoError(err)

	// test one rule
	data, err := json.Marshal(bundle)
	re.NoError(err)
	err = tu.CheckPostJSON(tests.TestDialClient, urlPrefix+"/config/placement-rule", data, tu.StatusOK(re))
	re.NoError(err)

	tu.Eventually(re, func() bool {
		respBundle := make([]placement.GroupBundle, 0)
		err = tu.CheckGetJSON(tests.TestDialClient, urlPrefix+"/config/placement-rule", nil,
			tu.StatusOK(re), tu.ExtractJSON(re, &respBundle))
		re.NoError(err)
		return len(respBundle) == 1 && respBundle[0].ID == "5"
	})

	tu.Eventually(re, func() bool {
		err = tu.ReadGetJSON(re, tests.TestDialClient, url, &status)
		re.NoError(err)
		return status == "REPLICATED"
	})

	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/handler/mockPending", "return(true)"))
	err = tu.ReadGetJSON(re, tests.TestDialClient, url, &status)
	re.NoError(err)
	re.Equal("PENDING", status)
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/handler/mockPending"))
	// test multiple rules
	r1 = core.NewTestRegionInfo(2, 1, []byte("a"), []byte("b"))
	r1.GetMeta().Peers = append(r1.GetMeta().Peers, &metapb.Peer{Id: 5, StoreId: 1})
	tests.MustPutRegionInfo(re, cluster, r1)

	bundle[0].Rules = append(bundle[0].Rules, &placement.Rule{
		ID: "bar", Index: 1, Role: placement.Voter, Count: 1,
	})
	data, err = json.Marshal(bundle)
	re.NoError(err)
	err = tu.CheckPostJSON(tests.TestDialClient, urlPrefix+"/config/placement-rule", data, tu.StatusOK(re))
	re.NoError(err)

	tu.Eventually(re, func() bool {
		respBundle := make([]placement.GroupBundle, 0)
		err = tu.CheckGetJSON(tests.TestDialClient, urlPrefix+"/config/placement-rule", nil,
			tu.StatusOK(re), tu.ExtractJSON(re, &respBundle))
		re.NoError(err)
		return len(respBundle) == 1 && len(respBundle[0].Rules) == 2
	})

	tu.Eventually(re, func() bool {
		err = tu.ReadGetJSON(re, tests.TestDialClient, url, &status)
		re.NoError(err)
		return status == "REPLICATED"
	})

	// test multiple bundles
	bundle = append(bundle, placement.GroupBundle{
		ID:    "6",
		Index: 6,
		Rules: []*placement.Rule{
			{
				ID: "foo", Index: 1, Role: placement.Voter, Count: 2,
			},
		},
	})
	data, err = json.Marshal(bundle)
	re.NoError(err)
	err = tu.CheckPostJSON(tests.TestDialClient, urlPrefix+"/config/placement-rule", data, tu.StatusOK(re))
	re.NoError(err)

	tu.Eventually(re, func() bool {
		respBundle := make([]placement.GroupBundle, 0)
		err = tu.CheckGetJSON(tests.TestDialClient, urlPrefix+"/config/placement-rule", nil,
			tu.StatusOK(re), tu.ExtractJSON(re, &respBundle))
		re.NoError(err)
		if len(respBundle) != 2 {
			return false
		}
		s1 := respBundle[0].ID == "5" && respBundle[1].ID == "6"
		s2 := respBundle[0].ID == "6" && respBundle[1].ID == "5"
		return s1 || s2
	})

	tu.Eventually(re, func() bool {
		err = tu.ReadGetJSON(re, tests.TestDialClient, url, &status)
		re.NoError(err)
		return status == "INPROGRESS"
	})

	r1 = core.NewTestRegionInfo(2, 1, []byte("a"), []byte("b"))
	r1.GetMeta().Peers = append(r1.GetMeta().Peers, &metapb.Peer{Id: 5, StoreId: 1}, &metapb.Peer{Id: 6, StoreId: 1}, &metapb.Peer{Id: 7, StoreId: 1})
	tests.MustPutRegionInfo(re, cluster, r1)

	tu.Eventually(re, func() bool {
		err = tu.ReadGetJSON(re, tests.TestDialClient, url, &status)
		re.NoError(err)
		return status == "REPLICATED"
	})
}

func checkRegionCount(re *require.Assertions, cluster *tests.TestCluster, count uint64) {
	leader := cluster.GetLeaderServer()
	tu.Eventually(re, func() bool {
		return leader.GetRaftCluster().GetRegionCount([]byte{}, []byte{}) == int(count)
	})
	if sche := cluster.GetSchedulingPrimaryServer(); sche != nil {
		tu.Eventually(re, func() bool {
			return sche.GetCluster().GetRegionCount([]byte{}, []byte{}) == int(count)
		})
	}
}

func pauseAllCheckers(re *require.Assertions, cluster *tests.TestCluster) {
	checkerNames := []string{"learner", "replica", "rule", "split", "merge", "joint-state"}
	addr := cluster.GetLeaderServer().GetAddr()
	for _, checkerName := range checkerNames {
		resp := make(map[string]any)
		url := fmt.Sprintf("%s/pd/api/v1/checker/%s", addr, checkerName)
		err := tu.CheckPostJSON(tests.TestDialClient, url, []byte(`{"delay":1000}`), tu.StatusOK(re))
		re.NoError(err)
		err = tu.ReadGetJSON(re, tests.TestDialClient, url, &resp)
		re.NoError(err)
		re.True(resp["paused"].(bool))
	}
}

func (suite *regionTestSuite) TestRegion() {
	suite.env.RunTest(suite.checkRegion)
}

func (suite *regionTestSuite) checkRegion(cluster *tests.TestCluster) {
	re := suite.Require()
	leader := cluster.GetLeaderServer()
	urlPrefix := leader.GetAddr() + "/pd/api/v1"
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
	tests.MustPutRegionInfo(re, cluster, r)
	url := fmt.Sprintf("%s/region/id/%d", urlPrefix, 0)
	re.NoError(tu.CheckGetJSON(testDialClient, url, nil, tu.Status(re, http.StatusBadRequest)))
	url = fmt.Sprintf("%s/region/id/%d", urlPrefix, 2333)
	re.NoError(tu.CheckGetJSON(testDialClient, url, nil, tu.Status(re, http.StatusNotFound)))
	url = fmt.Sprintf("%s/region/id/%d", urlPrefix, r.GetID())
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

	url = fmt.Sprintf("%s/region/key/%s", urlPrefix, "c")
	re.NoError(tu.CheckGetJSON(testDialClient, url, nil, tu.Status(re, http.StatusNotFound)))
	url = fmt.Sprintf("%s/region/key/%s", urlPrefix, "a")
	r2 := &response.RegionInfo{}
	re.NoError(tu.ReadGetJSON(re, testDialClient, url, r2))
	r2.Adjust()
	re.Equal(response.NewAPIRegionInfo(r), r2)

	url = fmt.Sprintf("%s/region/key/%s?format=hex", urlPrefix, hex.EncodeToString([]byte("a")))
	r2 = &response.RegionInfo{}
	re.NoError(tu.ReadGetJSON(re, testDialClient, url, r2))
	r2.Adjust()
	re.Equal(response.NewAPIRegionInfo(r), r2)
}

func (suite *regionTestSuite) TestRegionCheck() {
	suite.env.RunTest(suite.checkRegionCheck)
}

func (suite *regionTestSuite) checkRegionCheck(cluster *tests.TestCluster) {
	re := suite.Require()
	leader := cluster.GetLeaderServer()
	urlPrefix := leader.GetAddr() + "/pd/api/v1"
	r := core.NewTestRegionInfo(2, 1, []byte("a"), []byte("b"),
		core.SetApproximateKeys(10),
		core.SetApproximateSize(10))
	downPeer := &metapb.Peer{Id: 13, StoreId: 2}
	r = r.Clone(
		core.WithAddPeer(downPeer),
		core.WithDownPeers([]*pdpb.PeerStats{{Peer: downPeer, DownSeconds: 3600}}),
		core.WithPendingPeers([]*metapb.Peer{downPeer}))
	tests.MustPutRegionInfo(re, cluster, r)
	url := fmt.Sprintf("%s/region/id/%d", urlPrefix, r.GetID())
	r1 := &response.RegionInfo{}
	re.NoError(tu.ReadGetJSON(re, testDialClient, url, r1))
	r1.Adjust()
	re.Equal(response.NewAPIRegionInfo(r), r1)

	url = fmt.Sprintf("%s/regions/check/%s", urlPrefix, "down-peer")
	r2 := &response.RegionsInfo{}
	tu.Eventually(re, func() bool {
		if err := tu.ReadGetJSON(re, testDialClient, url, r2); err != nil {
			return false
		}
		r2.Adjust()
		return suite.Equal(&response.RegionsInfo{Count: 1, Regions: []response.RegionInfo{*response.NewAPIRegionInfo(r)}}, r2)
	})

	url = fmt.Sprintf("%s/regions/check/%s", urlPrefix, "pending-peer")
	r3 := &response.RegionsInfo{}
	re.NoError(tu.ReadGetJSON(re, testDialClient, url, r3))
	r3.Adjust()
	re.Equal(&response.RegionsInfo{Count: 1, Regions: []response.RegionInfo{*response.NewAPIRegionInfo(r)}}, r3)

	url = fmt.Sprintf("%s/regions/check/%s", urlPrefix, "offline-peer")
	r4 := &response.RegionsInfo{}
	re.NoError(tu.ReadGetJSON(re, testDialClient, url, r4))
	r4.Adjust()
	re.Equal(&response.RegionsInfo{Count: 0, Regions: []response.RegionInfo{}}, r4)

	r = r.Clone(core.SetApproximateSize(1))
	tests.MustPutRegionInfo(re, cluster, r)
	url = fmt.Sprintf("%s/regions/check/%s", urlPrefix, "empty-region")
	r5 := &response.RegionsInfo{}
	tu.Eventually(re, func() bool {
		if err := tu.ReadGetJSON(re, testDialClient, url, r5); err != nil {
			return false
		}
		r5.Adjust()
		return suite.Equal(&response.RegionsInfo{Count: 1, Regions: []response.RegionInfo{*response.NewAPIRegionInfo(r)}}, r5)
	})

	r = r.Clone(core.SetApproximateSize(1))
	tests.MustPutRegionInfo(re, cluster, r)
	url = fmt.Sprintf("%s/regions/check/%s", urlPrefix, "hist-size")
	r6 := make([]*api.HistItem, 1)
	tu.Eventually(re, func() bool {
		if err := tu.ReadGetJSON(re, testDialClient, url, &r6); err != nil {
			return false
		}
		histSizes := []*api.HistItem{{Start: 1, End: 1, Count: 1}}
		return suite.Equal(histSizes, r6)
	})

	r = r.Clone(core.SetApproximateKeys(1000))
	tests.MustPutRegionInfo(re, cluster, r)
	url = fmt.Sprintf("%s/regions/check/%s", urlPrefix, "hist-keys")
	r7 := make([]*api.HistItem, 1)
	tu.Eventually(re, func() bool {
		if err := tu.ReadGetJSON(re, testDialClient, url, &r7); err != nil {
			return false
		}
		histKeys := []*api.HistItem{{Start: 1000, End: 1999, Count: 1}}
		return suite.Equal(histKeys, r7)
	})

	// ref https://github.com/tikv/pd/issues/3558, we should change size to pass `NeedUpdate` for observing.
	r = r.Clone(core.SetApproximateKeys(0))
	s := &metapb.Store{
		Id:        2,
		State:     metapb.StoreState_Offline,
		NodeState: metapb.NodeState_Removing,
	}
	tests.MustPutStore(re, cluster, s)
	tests.MustPutRegionInfo(re, cluster, r)
	url = fmt.Sprintf("%s/regions/check/%s", urlPrefix, "offline-peer")
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
	suite.env.RunTest(suite.checkRegions)
}

func (suite *regionTestSuite) checkRegions(cluster *tests.TestCluster) {
	re := suite.Require()
	leader := cluster.GetLeaderServer()
	urlPrefix := leader.GetAddr() + "/pd/api/v1"
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
		tests.MustPutRegionInfo(re, cluster, r)
	}
	url := fmt.Sprintf("%s/regions", urlPrefix)
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
	suite.env.RunTest(suite.checkStoreRegions)
}

func (suite *regionTestSuite) checkStoreRegions(cluster *tests.TestCluster) {
	re := suite.Require()
	leader := cluster.GetLeaderServer()
	urlPrefix := leader.GetAddr() + "/pd/api/v1"
	r1 := core.NewTestRegionInfo(2, 1, []byte("a"), []byte("b"))
	r2 := core.NewTestRegionInfo(3, 1, []byte("b"), []byte("c"))
	r3 := core.NewTestRegionInfo(4, 2, []byte("c"), []byte("d"))
	tests.MustPutRegionInfo(re, cluster, r1)
	tests.MustPutRegionInfo(re, cluster, r2)
	tests.MustPutRegionInfo(re, cluster, r3)

	regionIDs := []uint64{2, 3}
	url := fmt.Sprintf("%s/regions/store/%d", urlPrefix, 1)
	r4 := &response.RegionsInfo{}
	err := tu.ReadGetJSON(re, testDialClient, url, r4)
	re.NoError(err)
	re.Len(regionIDs, r4.Count)
	sort.Slice(r4.Regions, func(i, j int) bool { return r4.Regions[i].ID < r4.Regions[j].ID })
	for i, r := range r4.Regions {
		re.Equal(regionIDs[i], r.ID)
	}

	regionIDs = []uint64{4}
	url = fmt.Sprintf("%s/regions/store/%d", urlPrefix, 2)
	r5 := &response.RegionsInfo{}
	err = tu.ReadGetJSON(re, testDialClient, url, r5)
	re.NoError(err)
	re.Len(regionIDs, r5.Count)
	for i, r := range r5.Regions {
		re.Equal(regionIDs[i], r.ID)
	}

	regionIDs = []uint64{}
	url = fmt.Sprintf("%s/regions/store/%d", urlPrefix, 3)
	r6 := &response.RegionsInfo{}
	err = tu.ReadGetJSON(re, testDialClient, url, r6)
	re.NoError(err)
	re.Len(regionIDs, r6.Count)
}

func (suite *regionTestSuite) TestTop() {
	suite.env.RunTest(suite.checkTop)
}

func (suite *regionTestSuite) checkTop(cluster *tests.TestCluster) {
	re := suite.Require()
	leader := cluster.GetLeaderServer()
	urlPrefix := leader.GetAddr() + "/pd/api/v1"
	r1 := core.NewTestRegionInfo(1, 1, []byte("a"), []byte("b"), core.SetWrittenBytes(1000), core.SetReadBytes(1000), core.SetRegionConfVer(1), core.SetRegionVersion(1))
	tests.MustPutRegionInfo(re, cluster, r1)
	r2 := core.NewTestRegionInfo(2, 1, []byte("b"), []byte("c"), core.SetWrittenBytes(2000), core.SetReadBytes(0), core.SetRegionConfVer(2), core.SetRegionVersion(3))
	tests.MustPutRegionInfo(re, cluster, r2)
	r3 := core.NewTestRegionInfo(3, 1, []byte("c"), []byte("d"), core.SetWrittenBytes(500), core.SetReadBytes(800), core.SetRegionConfVer(3), core.SetRegionVersion(2))
	tests.MustPutRegionInfo(re, cluster, r3)
	checkTopRegions(re, fmt.Sprintf("%s/regions/writeflow", urlPrefix), []uint64{2, 1, 3})
	checkTopRegions(re, fmt.Sprintf("%s/regions/readflow", urlPrefix), []uint64{1, 3, 2})
	checkTopRegions(re, fmt.Sprintf("%s/regions/writeflow?limit=2", urlPrefix), []uint64{2, 1})
	checkTopRegions(re, fmt.Sprintf("%s/regions/confver", urlPrefix), []uint64{3, 2, 1})
	checkTopRegions(re, fmt.Sprintf("%s/regions/confver?limit=2", urlPrefix), []uint64{3, 2})
	checkTopRegions(re, fmt.Sprintf("%s/regions/version", urlPrefix), []uint64{2, 3, 1})
	checkTopRegions(re, fmt.Sprintf("%s/regions/version?limit=2", urlPrefix), []uint64{2, 3})
	// Top size.
	baseOpt := []core.RegionCreateOption{core.SetRegionConfVer(3), core.SetRegionVersion(3)}
	opt := core.SetApproximateSize(1000)
	r1 = core.NewTestRegionInfo(1, 1, []byte("a"), []byte("b"), append(baseOpt, opt)...)
	tests.MustPutRegionInfo(re, cluster, r1)
	opt = core.SetApproximateSize(900)
	r2 = core.NewTestRegionInfo(2, 1, []byte("b"), []byte("c"), append(baseOpt, opt)...)
	tests.MustPutRegionInfo(re, cluster, r2)
	opt = core.SetApproximateSize(800)
	r3 = core.NewTestRegionInfo(3, 1, []byte("c"), []byte("d"), append(baseOpt, opt)...)
	tests.MustPutRegionInfo(re, cluster, r3)
	checkTopRegions(re, fmt.Sprintf("%s/regions/size?limit=2", urlPrefix), []uint64{1, 2})
	checkTopRegions(re, fmt.Sprintf("%s/regions/size", urlPrefix), []uint64{1, 2, 3})
	// Top CPU usage.
	baseOpt = []core.RegionCreateOption{core.SetRegionConfVer(4), core.SetRegionVersion(4)}
	opt = core.SetCPUUsage(100)
	r1 = core.NewTestRegionInfo(1, 1, []byte("a"), []byte("b"), append(baseOpt, opt)...)
	tests.MustPutRegionInfo(re, cluster, r1)
	opt = core.SetCPUUsage(300)
	r2 = core.NewTestRegionInfo(2, 1, []byte("b"), []byte("c"), append(baseOpt, opt)...)
	tests.MustPutRegionInfo(re, cluster, r2)
	opt = core.SetCPUUsage(500)
	r3 = core.NewTestRegionInfo(3, 1, []byte("c"), []byte("d"), append(baseOpt, opt)...)
	tests.MustPutRegionInfo(re, cluster, r3)
	checkTopRegions(re, fmt.Sprintf("%s/regions/cpu?limit=2", urlPrefix), []uint64{3, 2})
	checkTopRegions(re, fmt.Sprintf("%s/regions/cpu", urlPrefix), []uint64{3, 2, 1})
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

func (suite *regionTestSuite) TestRegionsWithKillRequest() {
	suite.env.RunTest(suite.checkRegionsWithKillRequest)
}

func (suite *regionTestSuite) checkRegionsWithKillRequest(cluster *tests.TestCluster) {
	re := suite.Require()
	leader := cluster.GetLeaderServer()
	urlPrefix := leader.GetAddr() + "/pd/api/v1"
	url := fmt.Sprintf("%s/regions", urlPrefix)

	regionCount := 100
	tu.GenerateTestDataConcurrently(regionCount, func(i int) {
		r := core.NewTestRegionInfo(uint64(i+2), 1,
			[]byte(fmt.Sprintf("%09d", i)),
			[]byte(fmt.Sprintf("%09d", i+1)),
			core.SetApproximateKeys(10), core.SetApproximateSize(10))
		tests.MustPutRegionInfo(re, cluster, r)
	})

	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	re.NoError(err)
	doneCh := make(chan struct{}, 1)
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/api/slowRequest", "return(true)"))
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
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/api/slowRequest"))
}

func (suite *regionTestSuite) TestRegionKey() {
	suite.env.RunTest(suite.checkRegionKey)
}

func (suite *regionTestSuite) checkRegionKey(cluster *tests.TestCluster) {
	re := suite.Require()
	leader := cluster.GetLeaderServer()
	urlPrefix := leader.GetAddr() + "/pd/api/v1"
	r := core.NewTestRegionInfo(99, 1, []byte{0xFF, 0xFF, 0xAA}, []byte{0xFF, 0xFF, 0xCC}, core.SetWrittenBytes(500), core.SetReadBytes(800), core.SetRegionConfVer(3), core.SetRegionVersion(2))
	tests.MustPutRegionInfo(re, cluster, r)
	url := fmt.Sprintf("%s/region/key/%s", urlPrefix, url.QueryEscape(string([]byte{0xFF, 0xFF, 0xBB})))
	RegionInfo := &response.RegionInfo{}
	err := tu.ReadGetJSON(re, testDialClient, url, RegionInfo)
	re.NoError(err)
	re.Equal(RegionInfo.ID, r.GetID())
}

func (suite *regionTestSuite) TestScanRegionByKeys() {
	suite.env.RunTest(suite.checkScanRegionByKeys)
}

func (suite *regionTestSuite) checkScanRegionByKeys(cluster *tests.TestCluster) {
	re := suite.Require()
	leader := cluster.GetLeaderServer()
	urlPrefix := leader.GetAddr() + "/pd/api/v1"
	r1 := core.NewTestRegionInfo(2, 1, []byte("a"), []byte("b"))
	r2 := core.NewTestRegionInfo(3, 1, []byte("b"), []byte("c"))
	r3 := core.NewTestRegionInfo(4, 2, []byte("c"), []byte("e"))
	r4 := core.NewTestRegionInfo(5, 2, []byte("x"), []byte("z"))
	r := core.NewTestRegionInfo(99, 1, []byte{0xFF, 0xFF, 0xAA}, []byte{0xFF, 0xFF, 0xCC}, core.SetWrittenBytes(500), core.SetReadBytes(800), core.SetRegionConfVer(3), core.SetRegionVersion(2))
	tests.MustPutRegionInfo(re, cluster, r1)
	tests.MustPutRegionInfo(re, cluster, r2)
	tests.MustPutRegionInfo(re, cluster, r3)
	tests.MustPutRegionInfo(re, cluster, r4)
	tests.MustPutRegionInfo(re, cluster, r)

	url := fmt.Sprintf("%s/regions/key?key=%s", urlPrefix, "b")
	regionIDs := []uint64{3, 4, 5, 99}
	regions := &response.RegionsInfo{}
	err := tu.ReadGetJSON(re, testDialClient, url, regions)
	re.NoError(err)
	re.Len(regionIDs, regions.Count)
	for i, v := range regionIDs {
		re.Equal(regions.Regions[i].ID, v)
	}
	url = fmt.Sprintf("%s/regions/key?key=%s", urlPrefix, "d")
	regionIDs = []uint64{4, 5, 99}
	regions = &response.RegionsInfo{}
	err = tu.ReadGetJSON(re, testDialClient, url, regions)
	re.NoError(err)
	re.Len(regionIDs, regions.Count)
	for i, v := range regionIDs {
		re.Equal(regions.Regions[i].ID, v)
	}
	url = fmt.Sprintf("%s/regions/key?key=%s", urlPrefix, "g")
	regionIDs = []uint64{5, 99}
	regions = &response.RegionsInfo{}
	err = tu.ReadGetJSON(re, testDialClient, url, regions)
	re.NoError(err)
	re.Len(regionIDs, regions.Count)
	for i, v := range regionIDs {
		re.Equal(regions.Regions[i].ID, v)
	}
	url = fmt.Sprintf("%s/regions/key?end_key=%s", urlPrefix, "e")
	regionIDs = []uint64{2, 3, 4}
	regions = &response.RegionsInfo{}
	err = tu.ReadGetJSON(re, testDialClient, url, regions)
	re.NoError(err)
	re.Len(regionIDs, regions.Count)
	for i, v := range regionIDs {
		re.Equal(regions.Regions[i].ID, v)
	}
	url = fmt.Sprintf("%s/regions/key?key=%s&end_key=%s", urlPrefix, "b", "g")
	regionIDs = []uint64{3, 4}
	regions = &response.RegionsInfo{}
	err = tu.ReadGetJSON(re, testDialClient, url, regions)
	re.NoError(err)
	re.Len(regionIDs, regions.Count)
	for i, v := range regionIDs {
		re.Equal(regions.Regions[i].ID, v)
	}
	url = fmt.Sprintf("%s/regions/key?key=%s&end_key=%s", urlPrefix, "b", []byte{0xFF, 0xFF, 0xCC})
	regionIDs = []uint64{3, 4, 5, 99}
	regions = &response.RegionsInfo{}
	err = tu.ReadGetJSON(re, testDialClient, url, regions)
	re.NoError(err)
	re.Len(regionIDs, regions.Count)
	for i, v := range regionIDs {
		re.Equal(regions.Regions[i].ID, v)
	}
	url = fmt.Sprintf("%s/regions/key?key=%s&format=hex", urlPrefix, hex.EncodeToString([]byte("b")))
	regionIDs = []uint64{3, 4, 5, 99}
	regions = &response.RegionsInfo{}
	err = tu.ReadGetJSON(re, testDialClient, url, regions)
	re.NoError(err)
	re.Len(regionIDs, regions.Count)
	for i, v := range regionIDs {
		re.Equal(regions.Regions[i].ID, v)
	}
	url = fmt.Sprintf("%s/regions/key?key=%s&end_key=%s&format=hex",
		urlPrefix, hex.EncodeToString([]byte("b")), hex.EncodeToString([]byte("g")))
	regionIDs = []uint64{3, 4}
	regions = &response.RegionsInfo{}
	err = tu.ReadGetJSON(re, testDialClient, url, regions)
	re.NoError(err)
	re.Len(regionIDs, regions.Count)
	for i, v := range regionIDs {
		re.Equal(regions.Regions[i].ID, v)
	}
	url = fmt.Sprintf("%s/regions/key?key=%s&end_key=%s&format=hex",
		urlPrefix, hex.EncodeToString([]byte("b")), hex.EncodeToString([]byte{0xFF, 0xFF, 0xCC}))
	regionIDs = []uint64{3, 4, 5, 99}
	regions = &response.RegionsInfo{}
	err = tu.ReadGetJSON(re, testDialClient, url, regions)
	re.NoError(err)
	re.Len(regionIDs, regions.Count)
	for i, v := range regionIDs {
		re.Equal(regions.Regions[i].ID, v)
	}
	// test invalid key
	url = fmt.Sprintf("%s/regions/key?key=%s&format=hex", urlPrefix, "invalid")
	err = tu.CheckGetJSON(testDialClient, url, nil,
		tu.Status(re, http.StatusBadRequest),
		tu.StringEqual(re, "\"encoding/hex: invalid byte: U+0069 'i'\"\n"))
	re.NoError(err)
}

func (suite *regionTestSuite) TestRegionRangeHoles() {
	suite.env.RunTest(suite.checkRegionRangeHoles)
}

func (suite *regionTestSuite) checkRegionRangeHoles(cluster *tests.TestCluster) {
	re := suite.Require()
	leader := cluster.GetLeaderServer()
	urlPrefix := leader.GetAddr() + "/pd/api/v1"
	// Missing r0 with range [0, 0xEA]
	r1 := core.NewTestRegionInfo(2, 1, []byte{0xEA}, []byte{0xEB})
	// Missing r2 with range [0xEB, 0xEC]
	r3 := core.NewTestRegionInfo(3, 1, []byte{0xEC}, []byte{0xED})
	r4 := core.NewTestRegionInfo(4, 2, []byte{0xED}, []byte{0xEE})
	// Missing r5 with range [0xEE, 0xFE]
	r6 := core.NewTestRegionInfo(5, 2, []byte{0xFE}, []byte{0xFF})
	tests.MustPutRegionInfo(re, cluster, r1)
	tests.MustPutRegionInfo(re, cluster, r3)
	tests.MustPutRegionInfo(re, cluster, r4)
	tests.MustPutRegionInfo(re, cluster, r6)

	url := fmt.Sprintf("%s/regions/range-holes", urlPrefix)
	rangeHoles := new([][]string)
	re.NoError(tu.ReadGetJSON(re, testDialClient, url, rangeHoles))
	re.Equal([][]string{
		{"", core.HexRegionKeyStr(r1.GetStartKey())},
		{core.HexRegionKeyStr(r1.GetEndKey()), core.HexRegionKeyStr(r3.GetStartKey())},
		{core.HexRegionKeyStr(r4.GetEndKey()), core.HexRegionKeyStr(r6.GetStartKey())},
		{core.HexRegionKeyStr(r6.GetEndKey()), ""},
	}, *rangeHoles)
}

func BenchmarkGetRegions(b *testing.B) {
	re := require.New(b)
	svr, cleanup := mustNewServer(re)
	defer cleanup()
	tests.MustWaitLeader(re, []*server.Server{svr})

	addr := svr.GetAddr()
	url := fmt.Sprintf("%s%s/api/v1/regions", addr, api.APIPrefix)
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
	for range b.N {
		resp, _ := apiutil.GetJSON(testDialClient, url, nil)
		resp.Body.Close()
	}
}
