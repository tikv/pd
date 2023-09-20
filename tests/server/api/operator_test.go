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
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/core"
	pdoperator "github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	tu "github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

var (
	// testDialClient used to dial http request. only used for test.
	testDialClient = &http.Client{
		Transport: &http.Transport{
			DisableKeepAlives: true,
		},
	}
)

type operatorTestSuite struct {
	suite.Suite
}

func TestOperatorTestSuite(t *testing.T) {
	suite.Run(t, new(operatorTestSuite))
}

func (suite *operatorTestSuite) TestOperator() {
	opts := []tests.ConfigOption{
		func(conf *config.Config, serverName string) {
			conf.Replication.MaxReplicas = 1
		},
	}
	env := tests.NewSchedulingTestEnvironment(suite.Require(), opts...)
	env.RunTestInTwoModes(suite.checkAddRemovePeer)

	env = tests.NewSchedulingTestEnvironment(suite.Require(), opts...)
	env.RunTestInTwoModes(suite.checkMergeRegionOperator)

	opts = []tests.ConfigOption{
		func(conf *config.Config, serverName string) {
			conf.Replication.MaxReplicas = 3
		},
	}
	env = tests.NewSchedulingTestEnvironment(suite.Require(), opts...)
	env.RunTestInTwoModes(suite.checkTransferRegionWithPlacementRule)
}

func (suite *operatorTestSuite) checkAddRemovePeer(cluster *tests.TestCluster) {
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
	peer1 := &metapb.Peer{Id: 1, StoreId: 1}
	peer2 := &metapb.Peer{Id: 2, StoreId: 2}
	region := &metapb.Region{
		Id:    1,
		Peers: []*metapb.Peer{peer1, peer2},
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
	}
	regionInfo := core.NewRegionInfo(region, peer1)
	tests.MustPutRegionInfo(re, cluster, regionInfo)

	urlPrefix := fmt.Sprintf("%s/pd/api/v1", cluster.GetLeaderServer().GetAddr())
	regionURL := fmt.Sprintf("%s/operators/%d", urlPrefix, region.GetId())
	err := tu.CheckGetJSON(testDialClient, regionURL, nil,
		tu.StatusNotOK(re), tu.StringContain(re, "operator not found"))
	suite.NoError(err)
	recordURL := fmt.Sprintf("%s/operators/records?from=%s", urlPrefix, strconv.FormatInt(time.Now().Unix(), 10))
	err = tu.CheckGetJSON(testDialClient, recordURL, nil,
		tu.StatusNotOK(re), tu.StringContain(re, "operator not found"))
	suite.NoError(err)

	err = tu.CheckPostJSON(testDialClient, fmt.Sprintf("%s/operators", urlPrefix), []byte(`{"name":"add-peer", "region_id": 1, "store_id": 3}`), tu.StatusOK(re))
	suite.NoError(err)
	err = tu.CheckGetJSON(testDialClient, regionURL, nil,
		tu.StatusOK(re), tu.StringContain(re, "add learner peer 1 on store 3"), tu.StringContain(re, "RUNNING"))
	suite.NoError(err)

	err = tu.CheckDelete(testDialClient, regionURL, tu.StatusOK(re))
	suite.NoError(err)
	err = tu.CheckGetJSON(testDialClient, recordURL, nil,
		tu.StatusOK(re), tu.StringContain(re, "admin-add-peer {add peer: store [3]}"))
	suite.NoError(err)

	err = tu.CheckPostJSON(testDialClient, fmt.Sprintf("%s/operators", urlPrefix), []byte(`{"name":"remove-peer", "region_id": 1, "store_id": 2}`), tu.StatusOK(re))
	suite.NoError(err)
	err = tu.CheckGetJSON(testDialClient, regionURL, nil,
		tu.StatusOK(re), tu.StringContain(re, "remove peer on store 2"), tu.StringContain(re, "RUNNING"))
	suite.NoError(err)

	err = tu.CheckDelete(testDialClient, regionURL, tu.StatusOK(re))
	suite.NoError(err)
	err = tu.CheckGetJSON(testDialClient, recordURL, nil,
		tu.StatusOK(re), tu.StringContain(re, "admin-remove-peer {rm peer: store [2]}"))
	suite.NoError(err)

	tests.MustPutStore(re, cluster, &metapb.Store{
		Id:            4,
		State:         metapb.StoreState_Up,
		NodeState:     metapb.NodeState_Serving,
		LastHeartbeat: time.Now().UnixNano(),
	})
	err = tu.CheckPostJSON(testDialClient, fmt.Sprintf("%s/operators", urlPrefix), []byte(`{"name":"add-learner", "region_id": 1, "store_id": 4}`), tu.StatusOK(re))
	suite.NoError(err)
	err = tu.CheckGetJSON(testDialClient, regionURL, nil,
		tu.StatusOK(re), tu.StringContain(re, "add learner peer 2 on store 4"))
	suite.NoError(err)

	// Fail to add peer to tombstone store.
	err = cluster.GetLeaderServer().GetRaftCluster().RemoveStore(3, true)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, fmt.Sprintf("%s/operators", urlPrefix), []byte(`{"name":"add-peer", "region_id": 1, "store_id": 3}`), tu.StatusNotOK(re))
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, fmt.Sprintf("%s/operators", urlPrefix), []byte(`{"name":"transfer-peer", "region_id": 1, "from_store_id": 1, "to_store_id": 3}`), tu.StatusNotOK(re))
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, fmt.Sprintf("%s/operators", urlPrefix), []byte(`{"name":"transfer-region", "region_id": 1, "to_store_ids": [1, 2, 3]}`), tu.StatusNotOK(re))
	suite.NoError(err)

	// Fail to get operator if from is latest.
	time.Sleep(time.Second)
	url := fmt.Sprintf("%s/operators/records?from=%s", urlPrefix, strconv.FormatInt(time.Now().Unix(), 10))
	err = tu.CheckGetJSON(testDialClient, url, nil,
		tu.StatusNotOK(re), tu.StringContain(re, "operator not found"))
	suite.NoError(err)
}

func (suite *operatorTestSuite) checkMergeRegionOperator(cluster *tests.TestCluster) {
	re := suite.Require()
	r1 := core.NewTestRegionInfo(10, 1, []byte(""), []byte("b"), core.SetWrittenBytes(1000), core.SetReadBytes(1000), core.SetRegionConfVer(1), core.SetRegionVersion(1))
	tests.MustPutRegionInfo(re, cluster, r1)
	r2 := core.NewTestRegionInfo(20, 1, []byte("b"), []byte("c"), core.SetWrittenBytes(2000), core.SetReadBytes(0), core.SetRegionConfVer(2), core.SetRegionVersion(3))
	tests.MustPutRegionInfo(re, cluster, r2)
	r3 := core.NewTestRegionInfo(30, 1, []byte("c"), []byte(""), core.SetWrittenBytes(500), core.SetReadBytes(800), core.SetRegionConfVer(3), core.SetRegionVersion(2))
	tests.MustPutRegionInfo(re, cluster, r3)

	urlPrefix := fmt.Sprintf("%s/pd/api/v1", cluster.GetLeaderServer().GetAddr())
	err := tu.CheckPostJSON(testDialClient, fmt.Sprintf("%s/operators", urlPrefix), []byte(`{"name":"merge-region", "source_region_id": 10, "target_region_id": 20}`), tu.StatusOK(re))
	suite.NoError(err)

	tu.CheckDelete(testDialClient, fmt.Sprintf("%s/operators/%d", urlPrefix, 10), tu.StatusOK(re))
	err = tu.CheckPostJSON(testDialClient, fmt.Sprintf("%s/operators", urlPrefix), []byte(`{"name":"merge-region", "source_region_id": 20, "target_region_id": 10}`), tu.StatusOK(re))
	suite.NoError(err)
	tu.CheckDelete(testDialClient, fmt.Sprintf("%s/operators/%d", urlPrefix, 10), tu.StatusOK(re))
	err = tu.CheckPostJSON(testDialClient, fmt.Sprintf("%s/operators", urlPrefix), []byte(`{"name":"merge-region", "source_region_id": 10, "target_region_id": 30}`),
		tu.StatusNotOK(re), tu.StringContain(re, "not adjacent"))
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, fmt.Sprintf("%s/operators", urlPrefix), []byte(`{"name":"merge-region", "source_region_id": 30, "target_region_id": 10}`),
		tu.StatusNotOK(re), tu.StringContain(re, "not adjacent"))
	suite.NoError(err)
}

func (suite *operatorTestSuite) checkTransferRegionWithPlacementRule(cluster *tests.TestCluster) {
	re := suite.Require()
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
	}
	tests.MustPutRegionInfo(re, cluster, core.NewRegionInfo(region, peer1))

	urlPrefix := fmt.Sprintf("%s/pd/api/v1", cluster.GetLeaderServer().GetAddr())
	regionURL := fmt.Sprintf("%s/operators/%d", urlPrefix, region.GetId())
	err := tu.CheckGetJSON(testDialClient, regionURL, nil,
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
				pdoperator.AddLearner{ToStore: 3, PeerID: 1}.String(),
				pdoperator.PromoteLearner{ToStore: 3, PeerID: 1}.String(),
				pdoperator.TransferLeader{FromStore: 1, ToStore: 2}.String(),
				pdoperator.RemovePeer{FromStore: 1, PeerID: 1}.String(),
			}),
		},
		{
			name:                "placement rule disable with peer role",
			placementRuleEnable: false,
			input:               []byte(`{"name":"transfer-region", "region_id": 1, "to_store_ids": [2, 3], "peer_roles":["follower", "leader"]}`),
			expectedError:       nil,
			expectSteps: convertStepsToStr([]string{
				pdoperator.AddLearner{ToStore: 3, PeerID: 2}.String(),
				pdoperator.PromoteLearner{ToStore: 3, PeerID: 2}.String(),
				pdoperator.TransferLeader{FromStore: 1, ToStore: 2}.String(),
				pdoperator.RemovePeer{FromStore: 1, PeerID: 2}.String(),
				pdoperator.TransferLeader{FromStore: 2, ToStore: 3}.String(),
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
				pdoperator.AddLearner{ToStore: 3, PeerID: 3}.String(),
				pdoperator.PromoteLearner{ToStore: 3, PeerID: 3}.String(),
				pdoperator.TransferLeader{FromStore: 1, ToStore: 2}.String(),
				pdoperator.RemovePeer{FromStore: 1, PeerID: 1}.String(),
				pdoperator.TransferLeader{FromStore: 2, ToStore: 3}.String(),
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
				pdoperator.AddLearner{ToStore: 3, PeerID: 5}.String(),
				pdoperator.PromoteLearner{ToStore: 3, PeerID: 5}.String(),
				pdoperator.TransferLeader{FromStore: 1, ToStore: 3}.String(),
				pdoperator.RemovePeer{FromStore: 1, PeerID: 1}.String(),
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
				pdoperator.AddLearner{ToStore: 3, PeerID: 6}.String(),
				pdoperator.PromoteLearner{ToStore: 3, PeerID: 6}.String(),
				pdoperator.TransferLeader{FromStore: 1, ToStore: 2}.String(),
				pdoperator.RemovePeer{FromStore: 1, PeerID: 1}.String(),
			}),
		},
	}
	svr := cluster.GetLeaderServer()
	for _, testCase := range testCases {
		suite.T().Log(testCase.name)
		// TODO: remove this after we can sync this config to all servers.
		if sche := cluster.GetSchedulingPrimaryServer(); sche != nil {
			sche.GetOpts().SetPlacementRuleEnabled(testCase.placementRuleEnable)
		} else {
			svr.GetRaftCluster().GetOpts().SetPlacementRuleEnabled(testCase.placementRuleEnable)
		}
		if testCase.placementRuleEnable {
			err := svr.GetRaftCluster().GetRuleManager().Initialize(
				svr.GetRaftCluster().GetOpts().GetMaxReplicas(),
				svr.GetRaftCluster().GetOpts().GetLocationLabels())
			suite.NoError(err)
		}
		if len(testCase.rules) > 0 {
			// add customized rule first and then remove default rule
			if sche := cluster.GetSchedulingPrimaryServer(); sche != nil {
				err := sche.GetCluster().GetRuleManager().SetRules(testCase.rules)
				suite.NoError(err)
				err = sche.GetCluster().GetRuleManager().DeleteRule("pd", "default")
				suite.NoError(err)
			} else {
				err := svr.GetRaftCluster().GetRuleManager().SetRules(testCase.rules)
				suite.NoError(err)
				err = svr.GetRaftCluster().GetRuleManager().DeleteRule("pd", "default")
				suite.NoError(err)
			}
		}
		var err error
		if testCase.expectedError == nil {
			err = tu.CheckPostJSON(testDialClient, fmt.Sprintf("%s/operators", urlPrefix), testCase.input, tu.StatusOK(re))
		} else {
			err = tu.CheckPostJSON(testDialClient, fmt.Sprintf("%s/operators", urlPrefix), testCase.input,
				tu.StatusNotOK(re), tu.StringContain(re, testCase.expectedError.Error()))
		}
		suite.NoError(err)
		if len(testCase.expectSteps) > 0 {
			err = tu.CheckGetJSON(testDialClient, regionURL, nil,
				tu.StatusOK(re), tu.StringContain(re, testCase.expectSteps))
			suite.NoError(err)
			err = tu.CheckDelete(testDialClient, regionURL, tu.StatusOK(re))
		} else {
			err = tu.CheckDelete(testDialClient, regionURL, tu.StatusNotOK(re))
		}
		suite.NoError(err)
	}
}
