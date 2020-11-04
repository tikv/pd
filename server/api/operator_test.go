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
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/pkg/mock/mockhbstream"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	pdoperator "github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/placement"
	"github.com/tikv/pd/server/versioninfo"
)

var _ = Suite(&testOperatorSuite{})

var _ = Suite(&testTransferRegionOperatorSuite{})

type testOperatorSuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func (s *testOperatorSuite) SetUpSuite(c *C) {
	c.Assert(failpoint.Enable("github.com/tikv/pd/server/schedule/unexpectedOperator", "return(true)"), IsNil)
	s.svr, s.cleanup = mustNewServer(c, func(cfg *config.Config) { cfg.Replication.MaxReplicas = 1 })
	mustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)

	mustBootstrapCluster(c, s.svr)
}

func (s *testOperatorSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testOperatorSuite) TestAddRemovePeer(c *C) {
	mustPutStore(c, s.svr, 1, metapb.StoreState_Up, nil)
	mustPutStore(c, s.svr, 2, metapb.StoreState_Up, nil)

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
	mustRegionHeartbeat(c, s.svr, regionInfo)

	regionURL := fmt.Sprintf("%s/operators/%d", s.urlPrefix, region.GetId())
	operator := mustReadURL(c, regionURL)
	c.Assert(strings.Contains(operator, "operator not found"), IsTrue)

	mustPutStore(c, s.svr, 3, metapb.StoreState_Up, nil)
	err := postJSON(testDialClient, fmt.Sprintf("%s/operators", s.urlPrefix), []byte(`{"name":"add-peer", "region_id": 1, "store_id": 3}`))
	c.Assert(err, IsNil)
	operator = mustReadURL(c, regionURL)
	c.Assert(strings.Contains(operator, "add learner peer 1 on store 3"), IsTrue)
	c.Assert(strings.Contains(operator, "RUNNING"), IsTrue)

	_, err = doDelete(testDialClient, regionURL)
	c.Assert(err, IsNil)

	err = postJSON(testDialClient, fmt.Sprintf("%s/operators", s.urlPrefix), []byte(`{"name":"remove-peer", "region_id": 1, "store_id": 2}`))
	c.Assert(err, IsNil)
	operator = mustReadURL(c, regionURL)
	c.Assert(strings.Contains(operator, "RUNNING"), IsTrue)
	c.Assert(strings.Contains(operator, "remove peer on store 2"), IsTrue)

	_, err = doDelete(testDialClient, regionURL)
	c.Assert(err, IsNil)

	mustPutStore(c, s.svr, 4, metapb.StoreState_Up, nil)
	err = postJSON(testDialClient, fmt.Sprintf("%s/operators", s.urlPrefix), []byte(`{"name":"add-learner", "region_id": 1, "store_id": 4}`))
	c.Assert(err, IsNil)
	operator = mustReadURL(c, regionURL)
	c.Assert(strings.Contains(operator, "add learner peer 2 on store 4"), IsTrue)

	// Fail to add peer to tombstone store.
	err = s.svr.GetRaftCluster().BuryStore(3, true)
	c.Assert(err, IsNil)
	err = postJSON(testDialClient, fmt.Sprintf("%s/operators", s.urlPrefix), []byte(`{"name":"add-peer", "region_id": 1, "store_id": 3}`))
	c.Assert(err, NotNil)
	err = postJSON(testDialClient, fmt.Sprintf("%s/operators", s.urlPrefix), []byte(`{"name":"transfer-peer", "region_id": 1, "from_store_id": 1, "to_store_id": 3}`))
	c.Assert(err, NotNil)
	err = postJSON(testDialClient, fmt.Sprintf("%s/operators", s.urlPrefix), []byte(`{"name":"transfer-region", "region_id": 1, "to_store_ids": [1, 2, 3]}`))
	c.Assert(err, NotNil)
}

func (s *testOperatorSuite) TestMergeRegionOperator(c *C) {
	r1 := newTestRegionInfo(10, 1, []byte(""), []byte("b"), core.SetWrittenBytes(1000), core.SetReadBytes(1000), core.SetRegionConfVer(1), core.SetRegionVersion(1))
	mustRegionHeartbeat(c, s.svr, r1)
	r2 := newTestRegionInfo(20, 1, []byte("b"), []byte("c"), core.SetWrittenBytes(2000), core.SetReadBytes(0), core.SetRegionConfVer(2), core.SetRegionVersion(3))
	mustRegionHeartbeat(c, s.svr, r2)
	r3 := newTestRegionInfo(30, 1, []byte("c"), []byte(""), core.SetWrittenBytes(500), core.SetReadBytes(800), core.SetRegionConfVer(3), core.SetRegionVersion(2))
	mustRegionHeartbeat(c, s.svr, r3)

	err := postJSON(testDialClient, fmt.Sprintf("%s/operators", s.urlPrefix), []byte(`{"name":"merge-region", "source_region_id": 10, "target_region_id": 20}`))
	c.Assert(err, IsNil)

	s.svr.GetHandler().RemoveOperator(10)
	s.svr.GetHandler().RemoveOperator(20)
	err = postJSON(testDialClient, fmt.Sprintf("%s/operators", s.urlPrefix), []byte(`{"name":"merge-region", "source_region_id": 20, "target_region_id": 10}`))
	c.Assert(err, IsNil)
	s.svr.GetHandler().RemoveOperator(10)
	s.svr.GetHandler().RemoveOperator(20)
	err = postJSON(testDialClient, fmt.Sprintf("%s/operators", s.urlPrefix), []byte(`{"name":"merge-region", "source_region_id": 10, "target_region_id": 30}`))
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "not adjacent"), IsTrue)
	err = postJSON(testDialClient, fmt.Sprintf("%s/operators", s.urlPrefix), []byte(`{"name":"merge-region", "source_region_id": 30, "target_region_id": 10}`))

	c.Assert(strings.Contains(err.Error(), "not adjacent"), IsTrue)
	c.Assert(err, NotNil)
}

type testTransferRegionOperatorSuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func (s *testTransferRegionOperatorSuite) SetUpSuite(c *C) {
	c.Assert(failpoint.Enable("github.com/tikv/pd/server/schedule/unexpectedOperator", "return(true)"), IsNil)
	s.svr, s.cleanup = mustNewServer(c, func(cfg *config.Config) { cfg.Replication.MaxReplicas = 3 })
	mustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)

	mustBootstrapCluster(c, s.svr)
}

func (s *testTransferRegionOperatorSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testTransferRegionOperatorSuite) TestTransferRegionWithPlacementRule(c *C) {
	mustPutStore(c, s.svr, 1, metapb.StoreState_Up, []*metapb.StoreLabel{{Key: "key", Value: "1"}})
	mustPutStore(c, s.svr, 2, metapb.StoreState_Up, []*metapb.StoreLabel{{Key: "key", Value: "2"}})
	mustPutStore(c, s.svr, 3, metapb.StoreState_Up, []*metapb.StoreLabel{{Key: "key", Value: "3"}})

	hbStream := mockhbstream.NewHeartbeatStream()
	s.svr.GetHBStreams().BindStream(1, hbStream)
	s.svr.GetHBStreams().BindStream(2, hbStream)
	s.svr.GetHBStreams().BindStream(3, hbStream)

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
	mustRegionHeartbeat(c, s.svr, core.NewRegionInfo(region, peer1))

	regionURL := fmt.Sprintf("%s/operators/%d", s.urlPrefix, region.GetId())
	operator := mustReadURL(c, regionURL)
	c.Assert(strings.Contains(operator, "operator not found"), IsTrue)

	tt := []struct {
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

	for _, tc := range tt {
		c.Log(tc.name)
		s.svr.GetRaftCluster().GetOpts().SetPlacementRuleEnabled(tc.placementRuleEnable)
		if tc.placementRuleEnable {
			err := s.svr.GetRaftCluster().GetRuleManager().Initialize(
				s.svr.GetRaftCluster().GetOpts().GetMaxReplicas(),
				s.svr.GetRaftCluster().GetOpts().GetLocationLabels())
			c.Assert(err, IsNil)
		}
		if len(tc.rules) > 0 {
			// add customized rule first and then remove default rule
			err := s.svr.GetRaftCluster().GetRuleManager().SetRules(tc.rules)
			c.Assert(err, IsNil)
			err = s.svr.GetRaftCluster().GetRuleManager().DeleteRule("pd", "default")
			c.Assert(err, IsNil)
		}
		err := postJSON(testDialClient, fmt.Sprintf("%s/operators", s.urlPrefix), tc.input)
		if tc.expectedError == nil {
			c.Assert(err, IsNil)
		} else {
			c.Assert(err, NotNil)
			c.Assert(strings.Contains(err.Error(), tc.expectedError.Error()), IsTrue)
		}
		if len(tc.expectSteps) > 0 {
			operator = mustReadURL(c, regionURL)
			c.Assert(strings.Contains(operator, tc.expectSteps), IsTrue)
		}
		_, err = doDelete(testDialClient, regionURL)
		c.Assert(err, IsNil)
	}
}

func mustPutStore(c *C, svr *server.Server, id uint64, state metapb.StoreState, labels []*metapb.StoreLabel) {
	_, err := svr.PutStore(context.Background(), &pdpb.PutStoreRequest{
		Header: &pdpb.RequestHeader{ClusterId: svr.ClusterID()},
		Store: &metapb.Store{
			Id:      id,
			Address: fmt.Sprintf("tikv%d", id),
			State:   state,
			Labels:  labels,
			Version: versioninfo.MinSupportedVersion(versioninfo.Version2_0).String(),
		},
	})
	c.Assert(err, IsNil)
	_, err = svr.StoreHeartbeat(context.Background(), &pdpb.StoreHeartbeatRequest{
		Header: &pdpb.RequestHeader{ClusterId: svr.ClusterID()},
		Stats:  &pdpb.StoreStats{StoreId: id},
	})
	c.Assert(err, IsNil)
}

func mustRegionHeartbeat(c *C, svr *server.Server, region *core.RegionInfo) {
	cluster := svr.GetRaftCluster()
	err := cluster.HandleRegionHeartbeat(region)
	c.Assert(err, IsNil)
}

func mustReadURL(c *C, url string) string {
	res, err := testDialClient.Get(url)
	c.Assert(err, IsNil)
	defer res.Body.Close()
	data, err := ioutil.ReadAll(res.Body)
	c.Assert(err, IsNil)
	return string(data)
}
