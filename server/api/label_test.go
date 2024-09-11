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
	"fmt"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/response"
	"github.com/tikv/pd/pkg/utils/keypath"
	tu "github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
)

type labelsStoreTestSuite struct {
	suite.Suite
	svr       *server.Server
	cleanup   tu.CleanupFunc
	urlPrefix string
	stores    []*metapb.Store
}

func TestLabelsStoreTestSuite(t *testing.T) {
	suite.Run(t, new(labelsStoreTestSuite))
}

func (suite *labelsStoreTestSuite) SetupSuite() {
	suite.stores = []*metapb.Store{
		{
			Id:        1,
			Address:   "tikv1",
			State:     metapb.StoreState_Up,
			NodeState: metapb.NodeState_Serving,
			Labels: []*metapb.StoreLabel{
				{
					Key:   "zone",
					Value: "us-west-1",
				},
				{
					Key:   "disk",
					Value: "ssd",
				},
			},
			Version: "2.0.0",
		},
		{
			Id:        4,
			Address:   "tikv4",
			State:     metapb.StoreState_Up,
			NodeState: metapb.NodeState_Serving,
			Labels: []*metapb.StoreLabel{
				{
					Key:   "zone",
					Value: "us-west-2",
				},
				{
					Key:   "disk",
					Value: "hdd",
				},
			},
			Version: "2.0.0",
		},
		{
			Id:        6,
			Address:   "tikv6",
			State:     metapb.StoreState_Up,
			NodeState: metapb.NodeState_Serving,
			Labels: []*metapb.StoreLabel{
				{
					Key:   "zone",
					Value: "beijing",
				},
				{
					Key:   "disk",
					Value: "ssd",
				},
			},
			Version: "2.0.0",
		},
		{
			Id:        7,
			Address:   "tikv7",
			State:     metapb.StoreState_Up,
			NodeState: metapb.NodeState_Serving,
			Labels: []*metapb.StoreLabel{
				{
					Key:   "zone",
					Value: "hongkong",
				},
				{
					Key:   "disk",
					Value: "ssd",
				},
				{
					Key:   "other",
					Value: "test",
				},
			},
			Version: "2.0.0",
		},
	}

	re := suite.Require()
	suite.svr, suite.cleanup = mustNewServer(re, func(cfg *config.Config) {
		cfg.Replication.StrictlyMatchLabel = false
	})
	server.MustWaitLeader(re, []*server.Server{suite.svr})

	addr := suite.svr.GetAddr()
	suite.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)

	mustBootstrapCluster(re, suite.svr)
	for _, store := range suite.stores {
		mustPutStore(re, suite.svr, store.Id, store.State, store.NodeState, store.Labels)
	}
}

func (suite *labelsStoreTestSuite) TearDownSuite() {
	suite.cleanup()
}

func (suite *labelsStoreTestSuite) TestLabelsGet() {
	re := suite.Require()
	url := fmt.Sprintf("%s/labels", suite.urlPrefix)
	labels := make([]*metapb.StoreLabel, 0, len(suite.stores))
	re.NoError(tu.ReadGetJSON(re, testDialClient, url, &labels))
}

func (suite *labelsStoreTestSuite) TestStoresLabelFilter() {
	re := suite.Require()
	var testCases = []struct {
		name, value string
		want        []*metapb.Store
	}{
		{
			name: "Zone",
			want: suite.stores,
		},
		{
			name: "other",
			want: suite.stores[3:],
		},
		{
			name:  "zone",
			value: "Us-west-1",
			want:  suite.stores[:1],
		},
		{
			name:  "Zone",
			value: "west",
			want:  suite.stores[:2],
		},
		{
			name:  "Zo",
			value: "Beijing",
			want:  suite.stores[2:3],
		},
		{
			name:  "ZONE",
			value: "SSD",
			want:  []*metapb.Store{},
		},
	}
	for _, testCase := range testCases {
		url := fmt.Sprintf("%s/labels/stores?name=%s&value=%s", suite.urlPrefix, testCase.name, testCase.value)
		info := new(response.StoresInfo)
		err := tu.ReadGetJSON(re, testDialClient, url, info)
		re.NoError(err)
		checkStoresInfo(re, info.Stores, testCase.want)
	}
	_, err := newStoresLabelFilter("test", ".[test")
	re.Error(err)
}

type strictlyLabelsStoreTestSuite struct {
	suite.Suite
	svr       *server.Server
	grpcSvr   *server.GrpcServer
	cleanup   tu.CleanupFunc
	urlPrefix string
}

func TestStrictlyLabelsStoreTestSuite(t *testing.T) {
	suite.Run(t, new(strictlyLabelsStoreTestSuite))
}

func (suite *strictlyLabelsStoreTestSuite) SetupSuite() {
	re := suite.Require()
	suite.svr, suite.cleanup = mustNewServer(re, func(cfg *config.Config) {
		cfg.Replication.LocationLabels = []string{"zone", "disk"}
		cfg.Replication.StrictlyMatchLabel = true
		cfg.Replication.EnablePlacementRules = false
	})
	server.MustWaitLeader(re, []*server.Server{suite.svr})

	suite.grpcSvr = &server.GrpcServer{Server: suite.svr}
	addr := suite.svr.GetAddr()
	suite.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)

	mustBootstrapCluster(re, suite.svr)
}

func (suite *strictlyLabelsStoreTestSuite) TestStoreMatch() {
	re := suite.Require()
	testCases := []struct {
		store       *metapb.Store
		valid       bool
		expectError string
	}{
		{
			store: &metapb.Store{
				Id:      1,
				Address: "tikv1",
				State:   metapb.StoreState_Up,
				Labels: []*metapb.StoreLabel{
					{
						Key:   "zone",
						Value: "us-west-1",
					},
					{
						Key:   "disk",
						Value: "ssd",
					},
				},
				Version: "3.0.0",
			},
			valid: true,
		},
		{
			store: &metapb.Store{
				Id:      2,
				Address: "tikv2",
				State:   metapb.StoreState_Up,
				Labels:  []*metapb.StoreLabel{},
				Version: "3.0.0",
			},
			valid:       false,
			expectError: "label configuration is incorrect",
		},
		{
			store: &metapb.Store{
				Id:      2,
				Address: "tikv2",
				State:   metapb.StoreState_Up,
				Labels: []*metapb.StoreLabel{
					{
						Key:   "zone",
						Value: "cn-beijing-1",
					},
					{
						Key:   "disk",
						Value: "ssd",
					},
					{
						Key:   "other",
						Value: "unknown",
					},
				},
				Version: "3.0.0",
			},
			valid:       false,
			expectError: "key matching the label was not found",
		},
		{
			store: &metapb.Store{
				Id:      3,
				Address: "tiflash1",
				State:   metapb.StoreState_Up,
				Labels: []*metapb.StoreLabel{
					{
						Key:   "zone",
						Value: "us-west-1",
					},
					{
						Key:   "disk",
						Value: "ssd",
					},
					{
						Key:   core.EngineKey,
						Value: core.EngineTiFlash,
					},
				},
				Version: "3.0.0",
			},
			valid:       true,
			expectError: "placement rules is disabled",
		},
	}

	for _, testCase := range testCases {
		resp, err := suite.grpcSvr.PutStore(context.Background(), &pdpb.PutStoreRequest{
			Header: &pdpb.RequestHeader{ClusterId: keypath.ClusterID()},
			Store: &metapb.Store{
				Id:      testCase.store.Id,
				Address: testCase.store.Address,
				State:   testCase.store.State,
				Labels:  testCase.store.Labels,
				Version: testCase.store.Version,
			},
		})
		if testCase.store.Address == "tiflash1" {
			re.Contains(resp.GetHeader().GetError().String(), testCase.expectError)
			continue
		}
		if testCase.valid {
			re.NoError(err)
			re.Nil(resp.GetHeader().GetError())
		} else {
			re.Contains(resp.GetHeader().GetError().String(), testCase.expectError)
		}
	}

	// enable placement rules. Report no error any more.
	re.NoError(tu.CheckPostJSON(
		testDialClient,
		fmt.Sprintf("%s/config", suite.urlPrefix),
		[]byte(`{"enable-placement-rules":"true"}`),
		tu.StatusOK(re)))
	for _, testCase := range testCases {
		resp, err := suite.grpcSvr.PutStore(context.Background(), &pdpb.PutStoreRequest{
			Header: &pdpb.RequestHeader{ClusterId: keypath.ClusterID()},
			Store: &metapb.Store{
				Id:      testCase.store.Id,
				Address: testCase.store.Address,
				State:   testCase.store.State,
				Labels:  testCase.store.Labels,
				Version: testCase.store.Version,
			},
		})
		if testCase.valid {
			re.NoError(err)
			re.Nil(resp.GetHeader().GetError())
		} else {
			re.Contains(resp.GetHeader().GetError().String(), testCase.expectError)
		}
	}
}

func (suite *strictlyLabelsStoreTestSuite) TearDownSuite() {
	suite.cleanup()
}
