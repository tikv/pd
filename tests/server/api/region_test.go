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
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/placement"
	tu "github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

type regionTestSuite struct {
	suite.Suite
}

func TestRegionTestSuite(t *testing.T) {
	suite.Run(t, new(regionTestSuite))
}

func (suite *regionTestSuite) TestSplitRegions() {
	env := tests.NewSchedulingTestEnvironment(suite.T())
	env.RunTestInTwoModes(suite.checkSplitRegions)
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
	suite.checkRegionCount(cluster, 1)

	newRegionID := uint64(11)
	body := fmt.Sprintf(`{"retry_limit":%v, "split_keys": ["%s","%s","%s"]}`, 3,
		hex.EncodeToString([]byte("bbb")),
		hex.EncodeToString([]byte("ccc")),
		hex.EncodeToString([]byte("ddd")))
	checkOpt := func(res []byte, code int, _ http.Header) {
		s := &struct {
			ProcessedPercentage int      `json:"processed-percentage"`
			NewRegionsID        []uint64 `json:"regions-id"`
		}{}
		err := json.Unmarshal(res, s)
		suite.NoError(err)
		suite.Equal(100, s.ProcessedPercentage)
		suite.Equal([]uint64{newRegionID}, s.NewRegionsID)
	}
	suite.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/handler/splitResponses", fmt.Sprintf("return(%v)", newRegionID)))
	err := tu.CheckPostJSON(testDialClient, fmt.Sprintf("%s/regions/split", urlPrefix), []byte(body), checkOpt)
	suite.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/handler/splitResponses"))
	suite.NoError(err)
}

func (suite *regionTestSuite) TestAccelerateRegionsScheduleInRange() {
	env := tests.NewSchedulingTestEnvironment(suite.T(), func(conf *config.Config, serverName string) {
		// FIXME: enable placement rules
		conf.Replication.EnablePlacementRules = false
		conf.Replication.MaxReplicas = 1
	})
	env.RunTestInTwoModes(suite.checkAccelerateRegionsScheduleInRange)
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
	r1 := core.NewTestRegionInfo(557, 1, []byte("a1"), []byte("a2"))
	r2 := core.NewTestRegionInfo(558, 2, []byte("a2"), []byte("a3"))
	r3 := core.NewTestRegionInfo(559, 3, []byte("a3"), []byte("a4"))
	tests.MustPutRegionInfo(re, cluster, r1)
	tests.MustPutRegionInfo(re, cluster, r2)
	tests.MustPutRegionInfo(re, cluster, r3)
	suite.checkRegionCount(cluster, 3)

	body := fmt.Sprintf(`{"start_key":"%s", "end_key": "%s"}`, hex.EncodeToString([]byte("a1")), hex.EncodeToString([]byte("a3")))
	err := tu.CheckPostJSON(testDialClient, fmt.Sprintf("%s/regions/accelerate-schedule", urlPrefix), []byte(body),
		tu.StatusOK(re))
	suite.NoError(err)
	idList := leader.GetRaftCluster().GetSuspectRegions()
	if sche := cluster.GetSchedulingPrimaryServer(); sche != nil {
		idList = sche.GetCluster().GetCoordinator().GetCheckerController().GetSuspectRegions()
	}
	re.Len(idList, 2, len(idList))
}

func (suite *regionTestSuite) TestAccelerateRegionsScheduleInRanges() {
	env := tests.NewSchedulingTestEnvironment(suite.T(), func(conf *config.Config, serverName string) {
		// FIXME: enable placement rules
		conf.Replication.EnablePlacementRules = false
		conf.Replication.MaxReplicas = 1
	})
	env.RunTestInTwoModes(suite.checkAccelerateRegionsScheduleInRanges)
}

func (suite *regionTestSuite) checkAccelerateRegionsScheduleInRanges(cluster *tests.TestCluster) {
	leader := cluster.GetLeaderServer()
	urlPrefix := leader.GetAddr() + "/pd/api/v1"
	re := suite.Require()
	for i := 1; i <= 5; i++ {
		s1 := &metapb.Store{
			Id:        uint64(i),
			State:     metapb.StoreState_Up,
			NodeState: metapb.NodeState_Serving,
		}
		tests.MustPutStore(re, cluster, s1)
	}
	r1 := core.NewTestRegionInfo(557, 1, []byte("a1"), []byte("a2"))
	r2 := core.NewTestRegionInfo(558, 2, []byte("a2"), []byte("a3"))
	r3 := core.NewTestRegionInfo(559, 3, []byte("a3"), []byte("a4"))
	r4 := core.NewTestRegionInfo(560, 4, []byte("a4"), []byte("a5"))
	r5 := core.NewTestRegionInfo(561, 5, []byte("a5"), []byte("a6"))
	tests.MustPutRegionInfo(re, cluster, r1)
	tests.MustPutRegionInfo(re, cluster, r2)
	tests.MustPutRegionInfo(re, cluster, r3)
	tests.MustPutRegionInfo(re, cluster, r4)
	tests.MustPutRegionInfo(re, cluster, r5)
	suite.checkRegionCount(cluster, 5)

	body := fmt.Sprintf(`[{"start_key":"%s", "end_key": "%s"}, {"start_key":"%s", "end_key": "%s"}]`,
		hex.EncodeToString([]byte("a1")), hex.EncodeToString([]byte("a3")), hex.EncodeToString([]byte("a4")), hex.EncodeToString([]byte("a6")))
	err := tu.CheckPostJSON(testDialClient, fmt.Sprintf("%s/regions/accelerate-schedule/batch", urlPrefix), []byte(body),
		tu.StatusOK(re))
	suite.NoError(err)
	idList := leader.GetRaftCluster().GetSuspectRegions()
	if sche := cluster.GetSchedulingPrimaryServer(); sche != nil {
		idList = sche.GetCluster().GetCoordinator().GetCheckerController().GetSuspectRegions()
	}
	re.Len(idList, 4)
}

func (suite *regionTestSuite) TestScatterRegions() {
	env := tests.NewSchedulingTestEnvironment(suite.T())
	env.RunTestInTwoModes(suite.checkScatterRegions)
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
	r1 := core.NewTestRegionInfo(601, 13, []byte("b1"), []byte("b2"))
	r1.GetMeta().Peers = append(r1.GetMeta().Peers, &metapb.Peer{Id: 5, StoreId: 14}, &metapb.Peer{Id: 6, StoreId: 15})
	r2 := core.NewTestRegionInfo(602, 13, []byte("b2"), []byte("b3"))
	r2.GetMeta().Peers = append(r2.GetMeta().Peers, &metapb.Peer{Id: 7, StoreId: 14}, &metapb.Peer{Id: 8, StoreId: 15})
	r3 := core.NewTestRegionInfo(603, 13, []byte("b4"), []byte("b4"))
	r3.GetMeta().Peers = append(r3.GetMeta().Peers, &metapb.Peer{Id: 9, StoreId: 14}, &metapb.Peer{Id: 10, StoreId: 15})
	tests.MustPutRegionInfo(re, cluster, r1)
	tests.MustPutRegionInfo(re, cluster, r2)
	tests.MustPutRegionInfo(re, cluster, r3)
	suite.checkRegionCount(cluster, 3)

	body := fmt.Sprintf(`{"start_key":"%s", "end_key": "%s"}`, hex.EncodeToString([]byte("b1")), hex.EncodeToString([]byte("b3")))
	err := tu.CheckPostJSON(testDialClient, fmt.Sprintf("%s/regions/scatter", urlPrefix), []byte(body), tu.StatusOK(re))
	suite.NoError(err)
	oc := leader.GetRaftCluster().GetOperatorController()
	if sche := cluster.GetSchedulingPrimaryServer(); sche != nil {
		oc = sche.GetCoordinator().GetOperatorController()
	}

	op1 := oc.GetOperator(601)
	op2 := oc.GetOperator(602)
	op3 := oc.GetOperator(603)
	// At least one operator used to scatter region
	suite.True(op1 != nil || op2 != nil || op3 != nil)

	body = `{"regions_id": [601, 602, 603]}`
	err = tu.CheckPostJSON(testDialClient, fmt.Sprintf("%s/regions/scatter", urlPrefix), []byte(body), tu.StatusOK(re))
	suite.NoError(err)
}

func (suite *regionTestSuite) TestCheckRegionsReplicated() {
	env := tests.NewSchedulingTestEnvironment(suite.T(),
		func(conf *config.Config, serverName string) {
			conf.Replication.EnablePlacementRules = true
		})
	env.RunTestInPDMode(suite.checkRegionsReplicated)
}

func (suite *regionTestSuite) checkRegionsReplicated(cluster *tests.TestCluster) {
	leader := cluster.GetLeaderServer()
	urlPrefix := leader.GetAddr() + "/pd/api/v1"
	re := suite.Require()

	// add test region
	s1 := &metapb.Store{
		Id:        1,
		State:     metapb.StoreState_Up,
		NodeState: metapb.NodeState_Serving,
	}
	tests.MustPutStore(re, cluster, s1)
	r1 := core.NewTestRegionInfo(2, 1, []byte("a"), []byte("b"))
	tests.MustPutRegionInfo(re, cluster, r1)
	suite.checkRegionCount(cluster, 1)

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
	err := tu.CheckGetJSON(testDialClient, url, nil, tu.Status(re, http.StatusBadRequest))
	suite.NoError(err)

	url = fmt.Sprintf(`%s/regions/replicated?startKey=%s&endKey=%s`, urlPrefix, hex.EncodeToString(r1.GetStartKey()), "_")
	err = tu.CheckGetJSON(testDialClient, url, nil, tu.Status(re, http.StatusBadRequest))
	suite.NoError(err)

	// correct test
	url = fmt.Sprintf(`%s/regions/replicated?startKey=%s&endKey=%s`, urlPrefix, hex.EncodeToString(r1.GetStartKey()), hex.EncodeToString(r1.GetEndKey()))
	err = tu.CheckGetJSON(testDialClient, url, nil, tu.StatusOK(re))
	suite.NoError(err)

	// test one rule
	data, err := json.Marshal(bundle)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix+"/config/placement-rule", data, tu.StatusOK(re))
	suite.NoError(err)

	tu.Eventually(re, func() bool {
		err = tu.ReadGetJSON(re, testDialClient, url, &status)
		suite.NoError(err)
		return status == "REPLICATED"
	})

	suite.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/handler/mockPending", "return(true)"))
	err = tu.ReadGetJSON(re, testDialClient, url, &status)
	suite.NoError(err)
	suite.Equal("PENDING", status)
	suite.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/handler/mockPending"))
	// test multiple rules
	r1 = core.NewTestRegionInfo(2, 1, []byte("a"), []byte("b"))
	r1.GetMeta().Peers = append(r1.GetMeta().Peers, &metapb.Peer{Id: 5, StoreId: 1})
	tests.MustPutRegionInfo(re, cluster, r1)

	bundle[0].Rules = append(bundle[0].Rules, &placement.Rule{
		ID: "bar", Index: 1, Role: placement.Voter, Count: 1,
	})
	data, err = json.Marshal(bundle)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix+"/config/placement-rule", data, tu.StatusOK(re))
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
				ID: "foo", Index: 1, Role: placement.Voter, Count: 2,
			},
		},
	})
	data, err = json.Marshal(bundle)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix+"/config/placement-rule", data, tu.StatusOK(re))
	suite.NoError(err)

	err = tu.ReadGetJSON(re, testDialClient, url, &status)
	suite.NoError(err)
	suite.Equal("INPROGRESS", status)

	r1 = core.NewTestRegionInfo(2, 1, []byte("a"), []byte("b"))
	r1.GetMeta().Peers = append(r1.GetMeta().Peers, &metapb.Peer{Id: 5, StoreId: 1}, &metapb.Peer{Id: 6, StoreId: 1}, &metapb.Peer{Id: 7, StoreId: 1})
	tests.MustPutRegionInfo(re, cluster, r1)

	err = tu.ReadGetJSON(re, testDialClient, url, &status)
	suite.NoError(err)
	suite.Equal("REPLICATED", status)
}

func (suite *regionTestSuite) checkRegionCount(cluster *tests.TestCluster, count int) {
	leader := cluster.GetLeaderServer()
	tu.Eventually(suite.Require(), func() bool {
		return leader.GetRaftCluster().GetRegionCount([]byte{}, []byte{}).Count == count
	})
	if sche := cluster.GetSchedulingPrimaryServer(); sche != nil {
		tu.Eventually(suite.Require(), func() bool {
			return sche.GetCluster().GetRegionCount([]byte{}, []byte{}) == count
		})
	}
}
