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
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	tu "github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

type operatorTestSuite struct {
	suite.Suite
	env *tests.SchedulingTestEnvironment
}

func TestOperatorTestSuite(t *testing.T) {
	suite.Run(t, new(operatorTestSuite))
}

func (suite *operatorTestSuite) SetupSuite() {
	suite.env = tests.NewSchedulingTestEnvironment(suite.T(),
		func(conf *config.Config, _ string) {
			conf.Replication.MaxReplicas = 1
		})
}

func (suite *operatorTestSuite) TearDownSuite() {
	suite.env.Cleanup()
}

func (suite *operatorTestSuite) TestAddRemovePeer() {
	suite.env.RunTestBasedOnMode(suite.checkAddRemovePeer)
}

func (suite *operatorTestSuite) checkAddRemovePeer(cluster *tests.TestCluster) {
	re := suite.Require()
	pauseAllCheckers(re, cluster)
	stores := []*metapb.Store{
		{
			Id:            1,
			State:         metapb.StoreState_Up,
			NodeState:     metapb.NodeState_Serving,
			LastHeartbeat: time.Now().UnixNano(),
		},
		{
			Id:            2,
			State:         metapb.StoreState_Up,
			NodeState:     metapb.NodeState_Serving,
			LastHeartbeat: time.Now().UnixNano(),
		},
		{
			Id:            3,
			State:         metapb.StoreState_Up,
			NodeState:     metapb.NodeState_Serving,
			LastHeartbeat: time.Now().UnixNano(),
		},
	}

	for _, store := range stores {
		tests.MustPutStore(re, cluster, store)
	}
	peer1 := &metapb.Peer{Id: 1, StoreId: 1}
	peer2 := &metapb.Peer{Id: 2, StoreId: 2}
	region := &metapb.Region{
		Id:    1,
		Peers: []*metapb.Peer{peer1, peer2},
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
		StartKey: []byte("a"),
		EndKey:   []byte("b"),
	}
	regionInfo := core.NewRegionInfo(region, peer1)
	tests.MustPutRegionInfo(re, cluster, regionInfo)

	urlPrefix := fmt.Sprintf("%s/pd/api/v1", cluster.GetLeaderServer().GetAddr())
	regionURL := fmt.Sprintf("%s/operators/%d", urlPrefix, region.GetId())
	err := tu.CheckGetJSON(tests.TestDialClient, regionURL, nil,
		tu.StatusNotOK(re), tu.StringContain(re, "operator not found"))
	re.NoError(err)
	recordURL := fmt.Sprintf("%s/operators/records?from=%s", urlPrefix, strconv.FormatInt(time.Now().Unix(), 10))
	err = tu.CheckGetJSON(tests.TestDialClient, recordURL, nil,
		tu.StatusNotOK(re), tu.StringContain(re, "operator not found"))
	re.NoError(err)

	err = tu.CheckPostJSON(tests.TestDialClient, fmt.Sprintf("%s/operators", urlPrefix), []byte(`{"name":"add-peer", "region_id": 1, "store_id": 3}`), tu.StatusOK(re))
	re.NoError(err)
	err = tu.CheckGetJSON(tests.TestDialClient, regionURL, nil,
		tu.StatusOK(re), tu.StringContain(re, "add learner peer 1 on store 3"), tu.StringContain(re, "RUNNING"))
	re.NoError(err)

	err = tu.CheckDelete(tests.TestDialClient, regionURL, tu.StatusOK(re))
	re.NoError(err)
	err = tu.CheckGetJSON(tests.TestDialClient, recordURL, nil,
		tu.StatusOK(re), tu.StringContain(re, "admin-add-peer {add peer: store [3]}"))
	re.NoError(err)

	err = tu.CheckPostJSON(tests.TestDialClient, fmt.Sprintf("%s/operators", urlPrefix), []byte(`{"name":"remove-peer", "region_id": 1, "store_id": 2}`), tu.StatusOK(re))
	re.NoError(err)
	err = tu.CheckGetJSON(tests.TestDialClient, regionURL, nil,
		tu.StatusOK(re), tu.StringContain(re, "remove peer on store 2"), tu.StringContain(re, "RUNNING"))
	re.NoError(err)

	err = tu.CheckDelete(tests.TestDialClient, regionURL, tu.StatusOK(re))
	re.NoError(err)
	err = tu.CheckGetJSON(tests.TestDialClient, recordURL, nil,
		tu.StatusOK(re), tu.StringContain(re, "admin-remove-peer {rm peer: store [2]}"))
	re.NoError(err)

	tests.MustPutStore(re, cluster, &metapb.Store{
		Id:            4,
		State:         metapb.StoreState_Up,
		NodeState:     metapb.NodeState_Serving,
		LastHeartbeat: time.Now().UnixNano(),
	})
	err = tu.CheckPostJSON(tests.TestDialClient, fmt.Sprintf("%s/operators", urlPrefix), []byte(`{"name":"add-learner", "region_id": 1, "store_id": 4}`), tu.StatusOK(re))
	re.NoError(err)
	err = tu.CheckGetJSON(tests.TestDialClient, regionURL, nil,
		tu.StatusOK(re), tu.StringContain(re, "add learner peer 2 on store 4"))
	re.NoError(err)

	// Fail to add peer to tombstone store.
	err = cluster.GetLeaderServer().GetRaftCluster().RemoveStore(3, true)
	re.NoError(err)
	err = tu.CheckPostJSON(tests.TestDialClient, fmt.Sprintf("%s/operators", urlPrefix), []byte(`{"name":"add-peer", "region_id": 1, "store_id": 3}`), tu.StatusNotOK(re))
	re.NoError(err)
	err = tu.CheckPostJSON(tests.TestDialClient, fmt.Sprintf("%s/operators", urlPrefix), []byte(`{"name":"transfer-peer", "region_id": 1, "from_store_id": 1, "to_store_id": 3}`), tu.StatusNotOK(re))
	re.NoError(err)
	err = tu.CheckPostJSON(tests.TestDialClient, fmt.Sprintf("%s/operators", urlPrefix), []byte(`{"name":"transfer-region", "region_id": 1, "to_store_ids": [1, 2, 3]}`), tu.StatusNotOK(re))
	re.NoError(err)

	// Fail to get operator if from is latest.
	time.Sleep(time.Second)
	url := fmt.Sprintf("%s/operators/records?from=%s", urlPrefix, strconv.FormatInt(time.Now().Unix(), 10))
	err = tu.CheckGetJSON(tests.TestDialClient, url, nil,
		tu.StatusNotOK(re), tu.StringContain(re, "operator not found"))
	re.NoError(err)
}

func (suite *operatorTestSuite) TestMergeRegionOperator() {
	suite.env.RunTestBasedOnMode(suite.checkMergeRegionOperator)
}

func (suite *operatorTestSuite) checkMergeRegionOperator(cluster *tests.TestCluster) {
	re := suite.Require()
	stores := []*metapb.Store{
		{
			Id:            1,
			State:         metapb.StoreState_Up,
			NodeState:     metapb.NodeState_Serving,
			LastHeartbeat: time.Now().UnixNano(),
		},
		{
			Id:            2,
			State:         metapb.StoreState_Up,
			NodeState:     metapb.NodeState_Serving,
			LastHeartbeat: time.Now().UnixNano(),
		},
		{
			Id:            3,
			State:         metapb.StoreState_Up,
			NodeState:     metapb.NodeState_Serving,
			LastHeartbeat: time.Now().UnixNano(),
		},
	}

	for _, store := range stores {
		tests.MustPutStore(re, cluster, store)
	}

	pauseAllCheckers(re, cluster)
	r1 := core.NewTestRegionInfo(10, 1, []byte(""), []byte("b"), core.SetWrittenBytes(1000), core.SetReadBytes(1000), core.SetRegionConfVer(1), core.SetRegionVersion(1))
	tests.MustPutRegionInfo(re, cluster, r1)
	r2 := core.NewTestRegionInfo(20, 1, []byte("b"), []byte("c"), core.SetWrittenBytes(2000), core.SetReadBytes(0), core.SetRegionConfVer(2), core.SetRegionVersion(3))
	tests.MustPutRegionInfo(re, cluster, r2)
	r3 := core.NewTestRegionInfo(30, 1, []byte("c"), []byte(""), core.SetWrittenBytes(500), core.SetReadBytes(800), core.SetRegionConfVer(3), core.SetRegionVersion(2))
	tests.MustPutRegionInfo(re, cluster, r3)

	urlPrefix := fmt.Sprintf("%s/pd/api/v1", cluster.GetLeaderServer().GetAddr())
	err := tu.CheckPostJSON(tests.TestDialClient, fmt.Sprintf("%s/operators", urlPrefix), []byte(`{"name":"merge-region", "source_region_id": 10, "target_region_id": 20}`), tu.StatusOK(re))
	re.NoError(err)

	tu.CheckDelete(tests.TestDialClient, fmt.Sprintf("%s/operators/%d", urlPrefix, 10), tu.StatusOK(re))
	err = tu.CheckPostJSON(tests.TestDialClient, fmt.Sprintf("%s/operators", urlPrefix), []byte(`{"name":"merge-region", "source_region_id": 20, "target_region_id": 10}`), tu.StatusOK(re))
	re.NoError(err)
	tu.CheckDelete(tests.TestDialClient, fmt.Sprintf("%s/operators/%d", urlPrefix, 10), tu.StatusOK(re))
	err = tu.CheckPostJSON(tests.TestDialClient, fmt.Sprintf("%s/operators", urlPrefix), []byte(`{"name":"merge-region", "source_region_id": 10, "target_region_id": 30}`),
		tu.StatusNotOK(re), tu.StringContain(re, "not adjacent"))
	re.NoError(err)
	err = tu.CheckPostJSON(tests.TestDialClient, fmt.Sprintf("%s/operators", urlPrefix), []byte(`{"name":"merge-region", "source_region_id": 30, "target_region_id": 10}`),
		tu.StatusNotOK(re), tu.StringContain(re, "not adjacent"))
	re.NoError(err)
}

func (suite *operatorTestSuite) TestTransferRegionWithPlacementRule() {
	// use a new environment to avoid affecting other tests
	env := tests.NewSchedulingTestEnvironment(suite.T(),
		func(conf *config.Config, _ string) {
			conf.Replication.MaxReplicas = 3
		})
	env.RunTestBasedOnMode(suite.checkTransferRegionWithPlacementRule)
	env.Cleanup()
}

func (suite *operatorTestSuite) checkTransferRegionWithPlacementRule(cluster *tests.TestCluster) {
	re := suite.Require()
	pauseAllCheckers(re, cluster)
	stores := []*metapb.Store{
		{
			Id:            1,
			State:         metapb.StoreState_Up,
			NodeState:     metapb.NodeState_Serving,
			LastHeartbeat: time.Now().UnixNano(),
			Labels:        []*metapb.StoreLabel{{Key: "key", Value: "1"}},
		},
		{
			Id:            2,
			State:         metapb.StoreState_Up,
			NodeState:     metapb.NodeState_Serving,
			LastHeartbeat: time.Now().UnixNano(),
			Labels:        []*metapb.StoreLabel{{Key: "key", Value: "2"}},
		},
		{
			Id:            3,
			State:         metapb.StoreState_Up,
			NodeState:     metapb.NodeState_Serving,
			LastHeartbeat: time.Now().UnixNano(),
			Labels:        []*metapb.StoreLabel{{Key: "key", Value: "3"}},
		},
	}

	for _, store := range stores {
		tests.MustPutStore(re, cluster, store)
	}

	peer1 := &metapb.Peer{Id: 1, StoreId: 1}
	peer2 := &metapb.Peer{Id: 2, StoreId: 2}

	region := &metapb.Region{
		Id:    1,
		Peers: []*metapb.Peer{peer1, peer2},
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
		StartKey: []byte("a"),
		EndKey:   []byte("b"),
	}
	tests.MustPutRegionInfo(re, cluster, core.NewRegionInfo(region, peer1))

	urlPrefix := fmt.Sprintf("%s/pd/api/v1", cluster.GetLeaderServer().GetAddr())
	regionURL := fmt.Sprintf("%s/operators/%d", urlPrefix, region.GetId())
	err := tu.CheckGetJSON(tests.TestDialClient, regionURL, nil,
		tu.StatusNotOK(re), tu.StringContain(re, "operator not found"))
	re.NoError(err)
	convertStepsToStr := func(steps []string) string {
		stepStrs := make([]string, len(steps))
		for i := range steps {
			stepStrs[i] = fmt.Sprintf("%d:{%s}", i, steps[i])
		}
		return strings.Join(stepStrs, ", ")
	}
	testCases := []struct {
		name                string
		placementRuleEnable bool
		rules               []*placement.Rule
		input               []byte
		expectedError       error
		expectSteps         string
	}{
		{
			name:                "placement rule disable without peer role",
			placementRuleEnable: false,
			input:               []byte(`{"name":"transfer-region", "region_id": 1, "to_store_ids": [2, 3]}`),
			expectedError:       nil,
			expectSteps: convertStepsToStr([]string{
				operator.AddLearner{ToStore: 3, PeerID: 1}.String(),
				operator.PromoteLearner{ToStore: 3, PeerID: 1}.String(),
				operator.TransferLeader{FromStore: 1, ToStore: 2}.String(),
				operator.RemovePeer{FromStore: 1, PeerID: 1}.String(),
			}),
		},
		{
			name:                "placement rule disable with peer role",
			placementRuleEnable: false,
			input:               []byte(`{"name":"transfer-region", "region_id": 1, "to_store_ids": [2, 3], "peer_roles":["follower", "leader"]}`),
			expectedError:       nil,
			expectSteps: convertStepsToStr([]string{
				operator.AddLearner{ToStore: 3, PeerID: 2}.String(),
				operator.PromoteLearner{ToStore: 3, PeerID: 2}.String(),
				operator.TransferLeader{FromStore: 1, ToStore: 2}.String(),
				operator.RemovePeer{FromStore: 1, PeerID: 2}.String(),
				operator.TransferLeader{FromStore: 2, ToStore: 3}.String(),
			}),
		},
		{
			name:                "default placement rule without peer role",
			placementRuleEnable: true,
			input:               []byte(`{"name":"transfer-region", "region_id": 1, "to_store_ids": [2, 3]}`),
			expectedError:       errors.New("transfer region without peer role is not supported when placement rules enabled"),
			expectSteps:         "",
		},
		{
			name:                "default placement rule with peer role",
			placementRuleEnable: true,
			input:               []byte(`{"name":"transfer-region", "region_id": 1, "to_store_ids": [2, 3], "peer_roles":["follower", "leader"]}`),
			expectSteps: convertStepsToStr([]string{
				operator.AddLearner{ToStore: 3, PeerID: 3}.String(),
				operator.PromoteLearner{ToStore: 3, PeerID: 3}.String(),
				operator.TransferLeader{FromStore: 1, ToStore: 2}.String(),
				operator.RemovePeer{FromStore: 1, PeerID: 1}.String(),
				operator.TransferLeader{FromStore: 2, ToStore: 3}.String(),
			}),
		},
		{
			name:                "default placement rule with invalid input",
			placementRuleEnable: true,
			input:               []byte(`{"name":"transfer-region", "region_id": 1, "to_store_ids": [2, 3], "peer_roles":["leader"]}`),
			expectedError:       errors.New("transfer region without peer role is not supported when placement rules enabled"),
			expectSteps:         "",
		},
		{
			name:                "customized placement rule with invalid peer role",
			placementRuleEnable: true,
			rules: []*placement.Rule{
				{
					GroupID:  "pd1",
					ID:       "test1",
					Index:    1,
					Override: true,
					Role:     placement.Leader,
					Count:    1,
					LabelConstraints: []placement.LabelConstraint{
						{
							Key:    "key",
							Op:     placement.In,
							Values: []string{"3"},
						},
					},
				},
			},
			input:         []byte(`{"name":"transfer-region", "region_id": 1, "to_store_ids": [2, 3], "peer_roles":["leader", "follower"]}`),
			expectedError: errors.New("cannot create operator"),
			expectSteps:   "",
		},
		{
			name:                "customized placement rule with valid peer role1",
			placementRuleEnable: true,
			rules: []*placement.Rule{
				{
					GroupID:  "pd1",
					ID:       "test1",
					Index:    1,
					Override: true,
					Role:     placement.Leader,
					Count:    1,
					LabelConstraints: []placement.LabelConstraint{
						{
							Key:    "key",
							Op:     placement.In,
							Values: []string{"3"},
						},
					},
				},
			},
			input:         []byte(`{"name":"transfer-region", "region_id": 1, "to_store_ids": [2, 3], "peer_roles":["follower", "leader"]}`),
			expectedError: nil,
			expectSteps: convertStepsToStr([]string{
				operator.AddLearner{ToStore: 3, PeerID: 5}.String(),
				operator.PromoteLearner{ToStore: 3, PeerID: 5}.String(),
				operator.TransferLeader{FromStore: 1, ToStore: 3}.String(),
				operator.RemovePeer{FromStore: 1, PeerID: 1}.String(),
			}),
		},
		{
			name:                "customized placement rule with valid peer role2",
			placementRuleEnable: true,
			rules: []*placement.Rule{
				{
					GroupID: "pd1",
					ID:      "test1",
					Role:    placement.Voter,
					Count:   1,
					LabelConstraints: []placement.LabelConstraint{
						{
							Key:    "key",
							Op:     placement.In,
							Values: []string{"1", "2"},
						},
					},
				},
				{
					GroupID: "pd1",
					ID:      "test2",
					Role:    placement.Follower,
					Count:   1,
					LabelConstraints: []placement.LabelConstraint{
						{
							Key:    "key",
							Op:     placement.In,
							Values: []string{"3"},
						},
					},
				},
			},
			input:         []byte(`{"name":"transfer-region", "region_id": 1, "to_store_ids": [2, 3], "peer_roles":["leader", "follower"]}`),
			expectedError: nil,
			expectSteps: convertStepsToStr([]string{
				operator.AddLearner{ToStore: 3, PeerID: 6}.String(),
				operator.PromoteLearner{ToStore: 3, PeerID: 6}.String(),
				operator.TransferLeader{FromStore: 1, ToStore: 2}.String(),
				operator.RemovePeer{FromStore: 1, PeerID: 1}.String(),
			}),
		},
	}
	svr := cluster.GetLeaderServer()
	url := fmt.Sprintf("%s/pd/api/v1/config", svr.GetAddr())
	for _, testCase := range testCases {
		suite.T().Log(testCase.name)
		data := make(map[string]any)
		if testCase.placementRuleEnable {
			data["enable-placement-rules"] = "true"
		} else {
			data["enable-placement-rules"] = "false"
		}
		reqData, e := json.Marshal(data)
		re.NoError(e)
		err := tu.CheckPostJSON(tests.TestDialClient, url, reqData, tu.StatusOK(re))
		re.NoError(err)
		if sche := cluster.GetSchedulingPrimaryServer(); sche != nil {
			// wait for the scheduling server to update the config
			tu.Eventually(re, func() bool {
				return sche.GetCluster().GetCheckerConfig().IsPlacementRulesEnabled() == testCase.placementRuleEnable
			})
		}
		manager := svr.GetRaftCluster().GetRuleManager()
		if sche := cluster.GetSchedulingPrimaryServer(); sche != nil {
			manager = sche.GetCluster().GetRuleManager()
		}

		if testCase.placementRuleEnable {
			err := manager.Initialize(
				svr.GetRaftCluster().GetOpts().GetMaxReplicas(),
				svr.GetRaftCluster().GetOpts().GetLocationLabels(),
				svr.GetRaftCluster().GetOpts().GetIsolationLevel(),
			)
			re.NoError(err)
		}
		if len(testCase.rules) > 0 {
			// add customized rule first and then remove default rule
			err := manager.SetRules(testCase.rules)
			re.NoError(err)
			err = manager.DeleteRule(placement.DefaultGroupID, placement.DefaultRuleID)
			re.NoError(err)
		}
		if testCase.expectedError == nil {
			err = tu.CheckPostJSON(tests.TestDialClient, fmt.Sprintf("%s/operators", urlPrefix), testCase.input, tu.StatusOK(re))
		} else {
			err = tu.CheckPostJSON(tests.TestDialClient, fmt.Sprintf("%s/operators", urlPrefix), testCase.input,
				tu.StatusNotOK(re), tu.StringContain(re, testCase.expectedError.Error()))
		}
		re.NoError(err)
		if len(testCase.expectSteps) > 0 {
			err = tu.CheckGetJSON(tests.TestDialClient, regionURL, nil,
				tu.StatusOK(re), tu.StringContain(re, testCase.expectSteps))
			re.NoError(err)
			err = tu.CheckDelete(tests.TestDialClient, regionURL, tu.StatusOK(re))
		} else {
			err = tu.CheckDelete(tests.TestDialClient, regionURL, tu.StatusNotOK(re))
		}
		re.NoError(err)
	}
}

func (suite *operatorTestSuite) TestGetOperatorsAsObject() {
	// use a new environment to avoid being affected by other tests
	env := tests.NewSchedulingTestEnvironment(suite.T(),
		func(conf *config.Config, _ string) {
			conf.Replication.MaxReplicas = 1
		})
	env.RunTestBasedOnMode(suite.checkGetOperatorsAsObject)
	env.Cleanup()
}

func (suite *operatorTestSuite) checkGetOperatorsAsObject(cluster *tests.TestCluster) {
	re := suite.Require()
	pauseAllCheckers(re, cluster)
	stores := []*metapb.Store{
		{
			Id:            1,
			State:         metapb.StoreState_Up,
			NodeState:     metapb.NodeState_Serving,
			LastHeartbeat: time.Now().UnixNano(),
		},
		{
			Id:            2,
			State:         metapb.StoreState_Up,
			NodeState:     metapb.NodeState_Serving,
			LastHeartbeat: time.Now().UnixNano(),
		},
		{
			Id:            3,
			State:         metapb.StoreState_Up,
			NodeState:     metapb.NodeState_Serving,
			LastHeartbeat: time.Now().UnixNano(),
		},
	}

	for _, store := range stores {
		tests.MustPutStore(re, cluster, store)
	}

	urlPrefix := fmt.Sprintf("%s/pd/api/v1", cluster.GetLeaderServer().GetAddr())
	objURL := fmt.Sprintf("%s/operators?object=1", urlPrefix)
	resp := make([]operator.OpObject, 0)

	// No operator.
	err := tu.ReadGetJSON(re, tests.TestDialClient, objURL, &resp)
	re.NoError(err)
	re.Empty(resp)

	// Merge operator.
	r1 := core.NewTestRegionInfo(10, 1, []byte(""), []byte("b"), core.SetWrittenBytes(1000), core.SetReadBytes(1000), core.SetRegionConfVer(1), core.SetRegionVersion(1))
	tests.MustPutRegionInfo(re, cluster, r1)
	r2 := core.NewTestRegionInfo(20, 1, []byte("b"), []byte("c"), core.SetWrittenBytes(2000), core.SetReadBytes(0), core.SetRegionConfVer(2), core.SetRegionVersion(3))
	tests.MustPutRegionInfo(re, cluster, r2)
	r3 := core.NewTestRegionInfo(30, 1, []byte("c"), []byte("d"), core.SetWrittenBytes(500), core.SetReadBytes(800), core.SetRegionConfVer(3), core.SetRegionVersion(2))
	tests.MustPutRegionInfo(re, cluster, r3)

	err = tu.CheckPostJSON(tests.TestDialClient, fmt.Sprintf("%s/operators", urlPrefix), []byte(`{"name":"merge-region", "source_region_id": 10, "target_region_id": 20}`), tu.StatusOK(re))
	re.NoError(err)
	err = tu.ReadGetJSON(re, tests.TestDialClient, objURL, &resp)
	re.NoError(err)
	re.Len(resp, 2)
	less := func(i, j int) bool {
		return resp[i].RegionID < resp[j].RegionID
	}
	sort.Slice(resp, less)
	re.Equal(uint64(10), resp[0].RegionID)
	re.Equal("admin-merge-region", resp[0].Desc)
	re.Equal("merge: region 10 to 20", resp[0].Brief)
	re.Equal("10m0s", resp[0].Timeout)
	re.Equal(&metapb.RegionEpoch{
		ConfVer: 1,
		Version: 1,
	}, resp[0].RegionEpoch)
	re.Equal(operator.OpAdmin|operator.OpMerge, resp[0].Kind)
	re.Truef(resp[0].Status == operator.CREATED || resp[0].Status == operator.STARTED, "unexpected status %s", resp[0].Status)
	re.Equal(uint64(20), resp[1].RegionID)
	re.Equal("admin-merge-region", resp[1].Desc)

	// Add peer operator.
	peer1 := &metapb.Peer{Id: 100, StoreId: 1}
	peer2 := &metapb.Peer{Id: 200, StoreId: 2}
	region := &metapb.Region{
		Id:    40,
		Peers: []*metapb.Peer{peer1, peer2},
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
		StartKey: []byte("d"),
		EndKey:   []byte(""),
	}
	regionInfo := core.NewRegionInfo(region, peer1)
	tests.MustPutRegionInfo(re, cluster, regionInfo)
	err = tu.CheckPostJSON(tests.TestDialClient, fmt.Sprintf("%s/operators", urlPrefix), []byte(`{"name":"add-peer", "region_id": 40, "store_id": 3}`), tu.StatusOK(re))
	re.NoError(err)
	err = tu.ReadGetJSON(re, tests.TestDialClient, objURL, &resp)
	re.NoError(err)
	re.Len(resp, 3)
	sort.Slice(resp, less)
	re.Equal(uint64(40), resp[2].RegionID)
	re.Equal("admin-add-peer", resp[2].Desc)
}

func (suite *operatorTestSuite) TestRemoveOperators() {
	suite.env.RunTestBasedOnMode(suite.checkRemoveOperators)
}

func (suite *operatorTestSuite) checkRemoveOperators(cluster *tests.TestCluster) {
	re := suite.Require()
	stores := []*metapb.Store{
		{
			Id:            1,
			State:         metapb.StoreState_Up,
			NodeState:     metapb.NodeState_Serving,
			LastHeartbeat: time.Now().UnixNano(),
		},
		{
			Id:            2,
			State:         metapb.StoreState_Up,
			NodeState:     metapb.NodeState_Serving,
			LastHeartbeat: time.Now().UnixNano(),
		},
		{
			Id:            4,
			State:         metapb.StoreState_Up,
			NodeState:     metapb.NodeState_Serving,
			LastHeartbeat: time.Now().UnixNano(),
		},
	}

	for _, store := range stores {
		tests.MustPutStore(re, cluster, store)
	}

	pauseAllCheckers(re, cluster)
	r1 := core.NewTestRegionInfo(10, 1, []byte(""), []byte("b"), core.SetWrittenBytes(1000), core.SetReadBytes(1000), core.SetRegionConfVer(1), core.SetRegionVersion(1))
	tests.MustPutRegionInfo(re, cluster, r1)
	r2 := core.NewTestRegionInfo(20, 1, []byte("b"), []byte("c"), core.SetWrittenBytes(2000), core.SetReadBytes(0), core.SetRegionConfVer(2), core.SetRegionVersion(3))
	tests.MustPutRegionInfo(re, cluster, r2)
	r3 := core.NewTestRegionInfo(30, 1, []byte("c"), []byte(""), core.SetWrittenBytes(500), core.SetReadBytes(800), core.SetRegionConfVer(3), core.SetRegionVersion(2))
	tests.MustPutRegionInfo(re, cluster, r3)

	urlPrefix := fmt.Sprintf("%s/pd/api/v1", cluster.GetLeaderServer().GetAddr())
	err := tu.CheckPostJSON(tests.TestDialClient, fmt.Sprintf("%s/operators", urlPrefix), []byte(`{"name":"merge-region", "source_region_id": 10, "target_region_id": 20}`), tu.StatusOK(re))
	re.NoError(err)
	err = tu.CheckPostJSON(tests.TestDialClient, fmt.Sprintf("%s/operators", urlPrefix), []byte(`{"name":"add-peer", "region_id": 30, "store_id": 4}`), tu.StatusOK(re))
	re.NoError(err)
	url := fmt.Sprintf("%s/operators", urlPrefix)
	err = tu.CheckGetJSON(tests.TestDialClient, url, nil, tu.StatusOK(re), tu.StringContain(re, "merge: region 10 to 20"), tu.StringContain(re, "add peer: store [4]"))
	re.NoError(err)
	err = tu.CheckDelete(tests.TestDialClient, url, tu.StatusOK(re))
	re.NoError(err)
	err = tu.CheckGetJSON(tests.TestDialClient, url, nil, tu.StatusOK(re), tu.StringNotContain(re, "merge: region 10 to 20"), tu.StringNotContain(re, "add peer: store [4]"))
	re.NoError(err)
}
