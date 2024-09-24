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
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/placement"
	tu "github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/tests"
)

type regionTestSuite struct {
	suite.Suite
	env *tests.SchedulingTestEnvironment
}

func TestRegionTestSuite(t *testing.T) {
	suite.Run(t, new(regionTestSuite))
}

func (suite *regionTestSuite) SetupSuite() {
	suite.env = tests.NewSchedulingTestEnvironment(suite.T())
}

func (suite *regionTestSuite) TearDownSuite() {
	suite.env.Cleanup()
}

func (suite *regionTestSuite) TearDownTest() {
	cleanFunc := func(cluster *tests.TestCluster) {
		// clean region cache
		leader := cluster.GetLeaderServer()
		re := suite.Require()
		pdAddr := cluster.GetConfig().GetClientURL()
		for _, region := range leader.GetRegions() {
			url := fmt.Sprintf("%s/pd/api/v1/admin/cache/region/%d", pdAddr, region.GetID())
			err := tu.CheckDelete(tests.TestDialClient, url, tu.StatusOK(re))
			re.NoError(err)
		}
		re.Empty(leader.GetRegions())
		// clean rules
		def := placement.GroupBundle{
			ID: "pd",
			Rules: []*placement.Rule{
				{GroupID: "pd", ID: "default", Role: "voter", Count: 3},
			},
		}
		data, err := json.Marshal([]placement.GroupBundle{def})
		re.NoError(err)
		urlPrefix := cluster.GetLeaderServer().GetAddr()
		err = tu.CheckPostJSON(tests.TestDialClient, urlPrefix+"/pd/api/v1/config/placement-rule", data, tu.StatusOK(re))
		re.NoError(err)
		// clean stores
		for _, store := range leader.GetStores() {
			re.NoError(cluster.GetLeaderServer().GetRaftCluster().RemoveStore(store.GetId(), true))
			re.NoError(cluster.GetLeaderServer().GetRaftCluster().BuryStore(store.GetId(), true))
		}
		re.NoError(cluster.GetLeaderServer().GetRaftCluster().RemoveTombStoneRecords())
		re.Empty(leader.GetStores())
		tu.Eventually(re, func() bool {
			if sche := cluster.GetSchedulingPrimaryServer(); sche != nil {
				for _, s := range sche.GetBasicCluster().GetStores() {
					if s.GetState() != metapb.StoreState_Tombstone {
						return false
					}
				}
			}
			return true
		})
	}
	suite.env.RunTestBasedOnMode(cleanFunc)
}

func (suite *regionTestSuite) TestSplitRegions() {
	// use a new environment to avoid affecting other tests
	env := tests.NewSchedulingTestEnvironment(suite.T())
	env.RunTestBasedOnMode(suite.checkSplitRegions)
	env.Cleanup()
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
	suite.env.RunTestBasedOnMode(suite.checkAccelerateRegionsScheduleInRange)
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
	suite.env.RunTestBasedOnMode(suite.checkAccelerateRegionsScheduleInRanges)
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
	// use a new environment to avoid affecting other tests
	env := tests.NewSchedulingTestEnvironment(suite.T())
	env.RunTestBasedOnMode(suite.checkScatterRegions)
	env.Cleanup()
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
	suite.env.RunTestBasedOnMode(suite.checkRegionsReplicated)
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
