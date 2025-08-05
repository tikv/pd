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

	"github.com/stretchr/testify/suite"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/response"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/api"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

type labelsStoreTestSuite struct {
	suite.Suite
	env    *tests.SchedulingTestEnvironment
	stores []*metapb.Store
}

func TestLabelsStoreTestSuite(t *testing.T) {
	suite.Run(t, new(labelsStoreTestSuite))
}

func (suite *labelsStoreTestSuite) SetupSuite() {
	suite.env = tests.NewSchedulingTestEnvironment(suite.T())
}

func (suite *labelsStoreTestSuite) TearDownSuite() {
	suite.env.Cleanup()
}

func (suite *labelsStoreTestSuite) TestLabelsGet() {
	suite.env.RunTest(suite.checkLabelsGet)
	suite.env.RunTest(suite.checkStoresLabelFilter)
}

func (suite *labelsStoreTestSuite) checkLabelsGet(cluster *tests.TestCluster) {
	re := suite.Require()

	suite.stores = []*metapb.Store{
		{
			Id:        1,
			Address:   "mock://tikv-1:1",
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
			Address:   "mock://tikv-4:4",
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
			Address:   "mock://tikv-6:6",
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
			Address:   "mock://tikv-7:7",
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

	for _, store := range suite.stores {
		tests.MustPutStore(re, cluster, store)
	}

	leader := cluster.GetLeaderServer()
	urlPrefix := leader.GetAddr() + "/pd/api/v1"

	url := fmt.Sprintf("%s/labels", urlPrefix)
	labels := make([]*metapb.StoreLabel, 0, len(suite.stores))
	re.NoError(testutil.ReadGetJSON(re, tests.TestDialClient, url, &labels))
}

func (suite *labelsStoreTestSuite) checkStoresLabelFilter(cluster *tests.TestCluster) {
	re := suite.Require()

	leader := cluster.GetLeaderServer()
	urlPrefix := leader.GetAddr() + "/pd/api/v1"

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
		url := fmt.Sprintf("%s/labels/stores?name=%s&value=%s", urlPrefix, testCase.name, testCase.value)
		info := new(response.StoresInfo)
		err := testutil.ReadGetJSON(re, tests.TestDialClient, url, info)
		re.NoError(err)
		checkStoresInfo(re, info.Stores, testCase.want)
	}
	_, err := api.NewStoresLabelFilter("test", ".[test")
	re.Error(err)
}

type strictlyLabelsStoreTestSuite struct {
	suite.Suite
	env *tests.SchedulingTestEnvironment
}

func TestStrictlyLabelsStoreTestSuite(t *testing.T) {
	suite.Run(t, new(strictlyLabelsStoreTestSuite))
}

func (suite *strictlyLabelsStoreTestSuite) SetupSuite() {
	suite.env = tests.NewSchedulingTestEnvironment(suite.T(),
		func(conf *config.Config, _ string) {
			conf.Replication.LocationLabels = []string{"zone", "disk"}
			conf.Replication.StrictlyMatchLabel = true
			conf.Replication.EnablePlacementRules = false
		})
}

func (suite *strictlyLabelsStoreTestSuite) TearDownSuite() {
	suite.env.Cleanup()
}

func (suite *strictlyLabelsStoreTestSuite) TestStoreMatch() {
	suite.env.RunTest(suite.checkStoreMatch)
}

func (suite *strictlyLabelsStoreTestSuite) checkStoreMatch(cluster *tests.TestCluster) {
	re := suite.Require()

	leader := cluster.GetLeaderServer()
	urlPrefix := leader.GetAddr() + "/pd/api/v1"
	grpcServer := server.GrpcServer{Server: leader.GetServer()}

	testCases := []struct {
		store       *metapb.Store
		valid       bool
		expectError string
	}{
		{
			store: &metapb.Store{
				Id:      1,
				Address: "mock://tikv-1:1",
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
				Address: "mock://tikv-2:2",
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
				Address: "mock://tikv-2:2",
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
				Address: "mock://tiflash-3:3",
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
		resp, err := grpcServer.PutStore(context.Background(), &pdpb.PutStoreRequest{
			Header: &pdpb.RequestHeader{ClusterId: keypath.ClusterID()},
			Store: &metapb.Store{
				Id:      testCase.store.Id,
				Address: testCase.store.Address,
				State:   testCase.store.State,
				Labels:  testCase.store.Labels,
				Version: testCase.store.Version,
			},
		})
		if testCase.store.Address == "mock://tiflash-3:3" {
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
	re.NoError(testutil.CheckPostJSON(
		tests.TestDialClient,
		fmt.Sprintf("%s/config", urlPrefix),
		[]byte(`{"enable-placement-rules":"true"}`),
		testutil.StatusOK(re)))
	for _, testCase := range testCases {
		resp, err := grpcServer.PutStore(context.Background(), &pdpb.PutStoreRequest{
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
