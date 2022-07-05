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
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/apiutil"
	"github.com/tikv/pd/pkg/mock/mockhbstream"
	tu "github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	pdoperator "github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/placement"
	"github.com/tikv/pd/server/versioninfo"
)

type operatorTestSuite struct {
	suite.Suite
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func TestOperatorTestSuite(t *testing.T) {
	suite.Run(t, new(operatorTestSuite))
}

func (suite *operatorTestSuite) SetupSuite() {
	re := suite.Require()
	suite.NoError(failpoint.Enable("github.com/tikv/pd/server/schedule/unexpectedOperator", "return(true)"))
	suite.svr, suite.cleanup = mustNewServer(re, func(cfg *config.Config) { cfg.Replication.MaxReplicas = 1 })
	server.MustWaitLeader(re, []*server.Server{suite.svr})

	addr := suite.svr.GetAddr()
	suite.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)

	mustBootstrapCluster(re, suite.svr)
}

func (suite *operatorTestSuite) TearDownSuite() {
	suite.cleanup()
}

func (suite *operatorTestSuite) TestAddRemovePeer() {
	re := suite.Require()
	mustPutStore(re, suite.svr, 1, metapb.StoreState_Up, metapb.NodeState_Serving, nil)
	mustPutStore(re, suite.svr, 2, metapb.StoreState_Up, metapb.NodeState_Serving, nil)

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
	mustRegionHeartbeat(re, suite.svr, regionInfo)

	regionURL := fmt.Sprintf("%s/operators/%d", suite.urlPrefix, region.GetId())
	operator := mustReadURL(re, regionURL)
	suite.Contains(operator, "operator not found")
	recordURL := fmt.Sprintf("%s/operators/records?from=%s", suite.urlPrefix, strconv.FormatInt(time.Now().Unix(), 10))
	records := mustReadURL(re, recordURL)
	suite.Contains(records, "operator not found")

	mustPutStore(re, suite.svr, 3, metapb.StoreState_Up, metapb.NodeState_Serving, nil)
	err := tu.CheckPostJSON(testDialClient, fmt.Sprintf("%s/operators", suite.urlPrefix), []byte(`{"name":"add-peer", "region_id": 1, "store_id": 3}`), tu.StatusOK(re))
	suite.NoError(err)
	operator = mustReadURL(re, regionURL)
	suite.Contains(operator, "add learner peer 1 on store 3")
	suite.Contains(operator, "RUNNING")

	_, err = apiutil.DoDelete(testDialClient, regionURL)
	suite.NoError(err)
	records = mustReadURL(re, recordURL)
	suite.Contains(records, "admin-add-peer {add peer: store [3]}")

	err = tu.CheckPostJSON(testDialClient, fmt.Sprintf("%s/operators", suite.urlPrefix), []byte(`{"name":"remove-peer", "region_id": 1, "store_id": 2}`), tu.StatusOK(re))
	suite.NoError(err)
	operator = mustReadURL(re, regionURL)
	suite.Contains(operator, "RUNNING")
	suite.Contains(operator, "remove peer on store 2")

	_, err = apiutil.DoDelete(testDialClient, regionURL)
	suite.NoError(err)
	records = mustReadURL(re, recordURL)
	suite.Contains(records, "admin-remove-peer {rm peer: store [2]}")

	mustPutStore(re, suite.svr, 4, metapb.StoreState_Up, metapb.NodeState_Serving, nil)
	err = tu.CheckPostJSON(testDialClient, fmt.Sprintf("%s/operators", suite.urlPrefix), []byte(`{"name":"add-learner", "region_id": 1, "store_id": 4}`), tu.StatusOK(re))
	suite.NoError(err)
	operator = mustReadURL(re, regionURL)
	suite.Contains(operator, "add learner peer 2 on store 4")

	// Fail to add peer to tombstone store.
	err = suite.svr.GetRaftCluster().RemoveStore(3, true)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, fmt.Sprintf("%s/operators", suite.urlPrefix), []byte(`{"name":"add-peer", "region_id": 1, "store_id": 3}`), tu.StatusNotOK(re))
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, fmt.Sprintf("%s/operators", suite.urlPrefix), []byte(`{"name":"transfer-peer", "region_id": 1, "from_store_id": 1, "to_store_id": 3}`), tu.StatusNotOK(re))
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, fmt.Sprintf("%s/operators", suite.urlPrefix), []byte(`{"name":"transfer-region", "region_id": 1, "to_store_ids": [1, 2, 3]}`), tu.StatusNotOK(re))
	suite.NoError(err)

	// Fail to get operator if from is latest.
	time.Sleep(time.Second)
	records = mustReadURL(re, fmt.Sprintf("%s/operators/records?from=%s", suite.urlPrefix, strconv.FormatInt(time.Now().Unix(), 10)))
	suite.Contains(records, "operator not found")
}

func (suite *operatorTestSuite) TestMergeRegionOperator() {
	re := suite.Require()
	r1 := newTestRegionInfo(10, 1, []byte(""), []byte("b"), core.SetWrittenBytes(1000), core.SetReadBytes(1000), core.SetRegionConfVer(1), core.SetRegionVersion(1))
	mustRegionHeartbeat(re, suite.svr, r1)
	r2 := newTestRegionInfo(20, 1, []byte("b"), []byte("c"), core.SetWrittenBytes(2000), core.SetReadBytes(0), core.SetRegionConfVer(2), core.SetRegionVersion(3))
	mustRegionHeartbeat(re, suite.svr, r2)
	r3 := newTestRegionInfo(30, 1, []byte("c"), []byte(""), core.SetWrittenBytes(500), core.SetReadBytes(800), core.SetRegionConfVer(3), core.SetRegionVersion(2))
	mustRegionHeartbeat(re, suite.svr, r3)

	err := tu.CheckPostJSON(testDialClient, fmt.Sprintf("%s/operators", suite.urlPrefix), []byte(`{"name":"merge-region", "source_region_id": 10, "target_region_id": 20}`), tu.StatusOK(re))
	suite.NoError(err)

	suite.svr.GetHandler().RemoveOperator(10)
	suite.svr.GetHandler().RemoveOperator(20)
	err = tu.CheckPostJSON(testDialClient, fmt.Sprintf("%s/operators", suite.urlPrefix), []byte(`{"name":"merge-region", "source_region_id": 20, "target_region_id": 10}`), tu.StatusOK(re))
	suite.NoError(err)
	suite.svr.GetHandler().RemoveOperator(10)
	suite.svr.GetHandler().RemoveOperator(20)
	err = tu.CheckPostJSON(testDialClient, fmt.Sprintf("%s/operators", suite.urlPrefix), []byte(`{"name":"merge-region", "source_region_id": 10, "target_region_id": 30}`),
		tu.StatusNotOK(re), tu.StringContain(re, "not adjacent"))
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, fmt.Sprintf("%s/operators", suite.urlPrefix), []byte(`{"name":"merge-region", "source_region_id": 30, "target_region_id": 10}`),
		tu.StatusNotOK(re), tu.StringContain(re, "not adjacent"))
	suite.NoError(err)
}

type transferRegionOperatorTestSuite struct {
	suite.Suite
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func TestTransferRegionOperatorTestSuite(t *testing.T) {
	suite.Run(t, new(transferRegionOperatorTestSuite))
}

func (suite *transferRegionOperatorTestSuite) SetupSuite() {
	re := suite.Require()
	suite.NoError(failpoint.Enable("github.com/tikv/pd/server/schedule/unexpectedOperator", "return(true)"))
	suite.svr, suite.cleanup = mustNewServer(re, func(cfg *config.Config) { cfg.Replication.MaxReplicas = 3 })
	server.MustWaitLeader(re, []*server.Server{suite.svr})

	addr := suite.svr.GetAddr()
	suite.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)

	mustBootstrapCluster(re, suite.svr)
}

func (suite *transferRegionOperatorTestSuite) TearDownSuite() {
	suite.cleanup()
}

func (suite *transferRegionOperatorTestSuite) TestTransferRegionWithPlacementRule() {
	re := suite.Require()
	mustPutStore(re, suite.svr, 1, metapb.StoreState_Up, metapb.NodeState_Serving, []*metapb.StoreLabel{{Key: "key", Value: "1"}})
	mustPutStore(re, suite.svr, 2, metapb.StoreState_Up, metapb.NodeState_Serving, []*metapb.StoreLabel{{Key: "key", Value: "2"}})
	mustPutStore(re, suite.svr, 3, metapb.StoreState_Up, metapb.NodeState_Serving, []*metapb.StoreLabel{{Key: "key", Value: "3"}})

	hbStream := mockhbstream.NewHeartbeatStream()
	suite.svr.GetHBStreams().BindStream(1, hbStream)
	suite.svr.GetHBStreams().BindStream(2, hbStream)
	suite.svr.GetHBStreams().BindStream(3, hbStream)

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
	mustRegionHeartbeat(re, suite.svr, core.NewRegionInfo(region, peer1))

	regionURL := fmt.Sprintf("%s/operators/%d", suite.urlPrefix, region.GetId())
	operator := mustReadURL(re, regionURL)
	suite.Contains(operator, "operator not found")

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
			expectSteps: strings.Join([]string{
				pdoperator.AddLearner{ToStore: 3, PeerID: 1}.String(),
				pdoperator.PromoteLearner{ToStore: 3, PeerID: 1}.String(),
				pdoperator.TransferLeader{FromStore: 1, ToStore: 2}.String(),
				pdoperator.RemovePeer{FromStore: 1, PeerID: 1}.String(),
			}, ", "),
		},
		{
			name:                "placement rule disable with peer role",
			placementRuleEnable: false,
			input:               []byte(`{"name":"transfer-region", "region_id": 1, "to_store_ids": [2, 3], "peer_roles":["follower", "leader"]}`),
			expectedError:       nil,
			expectSteps: strings.Join([]string{
				pdoperator.AddLearner{ToStore: 3, PeerID: 2}.String(),
				pdoperator.PromoteLearner{ToStore: 3, PeerID: 2}.String(),
				pdoperator.TransferLeader{FromStore: 1, ToStore: 2}.String(),
				pdoperator.RemovePeer{FromStore: 1, PeerID: 2}.String(),
				pdoperator.TransferLeader{FromStore: 2, ToStore: 3}.String(),
			}, ", "),
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
			expectSteps: strings.Join([]string{
				pdoperator.AddLearner{ToStore: 3, PeerID: 3}.String(),
				pdoperator.PromoteLearner{ToStore: 3, PeerID: 3}.String(),
				pdoperator.TransferLeader{FromStore: 1, ToStore: 2}.String(),
				pdoperator.RemovePeer{FromStore: 1, PeerID: 1}.String(),
				pdoperator.TransferLeader{FromStore: 2, ToStore: 3}.String(),
			}, ", "),
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
			expectSteps: strings.Join([]string{
				pdoperator.AddLearner{ToStore: 3, PeerID: 5}.String(),
				pdoperator.PromoteLearner{ToStore: 3, PeerID: 5}.String(),
				pdoperator.TransferLeader{FromStore: 1, ToStore: 3}.String(),
				pdoperator.RemovePeer{FromStore: 1, PeerID: 1}.String(),
			}, ", "),
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
			expectSteps: strings.Join([]string{
				pdoperator.AddLearner{ToStore: 3, PeerID: 6}.String(),
				pdoperator.PromoteLearner{ToStore: 3, PeerID: 6}.String(),
				pdoperator.TransferLeader{FromStore: 1, ToStore: 2}.String(),
				pdoperator.RemovePeer{FromStore: 1, PeerID: 1}.String(),
			}, ", "),
		},
	}
	for _, testCase := range testCases {
		suite.T().Log(testCase.name)
		suite.svr.GetRaftCluster().GetOpts().SetPlacementRuleEnabled(testCase.placementRuleEnable)
		if testCase.placementRuleEnable {
			err := suite.svr.GetRaftCluster().GetRuleManager().Initialize(
				suite.svr.GetRaftCluster().GetOpts().GetMaxReplicas(),
				suite.svr.GetRaftCluster().GetOpts().GetLocationLabels())
			suite.NoError(err)
		}
		if len(testCase.rules) > 0 {
			// add customized rule first and then remove default rule
			err := suite.svr.GetRaftCluster().GetRuleManager().SetRules(testCase.rules)
			suite.NoError(err)
			err = suite.svr.GetRaftCluster().GetRuleManager().DeleteRule("pd", "default")
			suite.NoError(err)
		}
		var err error
		if testCase.expectedError == nil {
			err = tu.CheckPostJSON(testDialClient, fmt.Sprintf("%s/operators", suite.urlPrefix), testCase.input, tu.StatusOK(re))
		} else {
			err = tu.CheckPostJSON(testDialClient, fmt.Sprintf("%s/operators", suite.urlPrefix), testCase.input,
				tu.StatusNotOK(re), tu.StringContain(re, testCase.expectedError.Error()))
		}
		suite.NoError(err)
		if len(testCase.expectSteps) > 0 {
			operator = mustReadURL(re, regionURL)
			suite.Contains(operator, testCase.expectSteps)
		}
		_, err = apiutil.DoDelete(testDialClient, regionURL)
		suite.NoError(err)
	}
}

func mustPutStore(re *require.Assertions, svr *server.Server, id uint64, state metapb.StoreState, nodeState metapb.NodeState, labels []*metapb.StoreLabel) {
	s := &server.GrpcServer{Server: svr}
	_, err := s.PutStore(context.Background(), &pdpb.PutStoreRequest{
		Header: &pdpb.RequestHeader{ClusterId: svr.ClusterID()},
		Store: &metapb.Store{
			Id:        id,
			Address:   fmt.Sprintf("tikv%d", id),
			State:     state,
			NodeState: nodeState,
			Labels:    labels,
			Version:   versioninfo.MinSupportedVersion(versioninfo.Version2_0).String(),
		},
	})
	re.NoError(err)
	if state == metapb.StoreState_Up {
		_, err = s.StoreHeartbeat(context.Background(), &pdpb.StoreHeartbeatRequest{
			Header: &pdpb.RequestHeader{ClusterId: svr.ClusterID()},
			Stats:  &pdpb.StoreStats{StoreId: id},
		})
		re.NoError(err)
	}
}

func mustRegionHeartbeat(re *require.Assertions, svr *server.Server, region *core.RegionInfo) {
	cluster := svr.GetRaftCluster()
	err := cluster.HandleRegionHeartbeat(region)
	re.NoError(err)
}

func mustReadURL(re *require.Assertions, url string) string {
	res, err := testDialClient.Get(url)
	re.NoError(err)
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	re.NoError(err)
	return string(data)
}
