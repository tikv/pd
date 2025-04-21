// Copyright 2025 TiKV Project Authors.
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
	"net/http"
	"sort"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/tests"
)

type serviceTestSuite struct {
	suite.Suite
	svr     *server.Server
	cleanup testutil.CleanupFunc
}

func TestServiceTestSuite(t *testing.T) {
	suite.Run(t, new(serviceTestSuite))
}

func (suite *serviceTestSuite) SetupSuite() {
	re := suite.Require()
	suite.svr, suite.cleanup = mustNewServer(re)
	tests.MustWaitLeader(re, []*server.Server{suite.svr})

	mustBootstrapCluster(re, suite.svr)
	mustPutStore(re, suite.svr, 1, metapb.StoreState_Up, metapb.NodeState_Serving, nil)
}

func (suite *serviceTestSuite) TearDownSuite() {
	suite.cleanup()
}

func (suite *serviceTestSuite) TestServiceLabels() {
	re := suite.Require()
	accessPaths := suite.svr.GetServiceLabels("Profile")
	re.Len(accessPaths, 1)
	re.Equal("/pd/api/v1/debug/pprof/profile", accessPaths[0].Path)
	re.Empty(accessPaths[0].Method)
	serviceLabel := suite.svr.GetAPIAccessServiceLabel(
		apiutil.NewAccessPath("/pd/api/v1/debug/pprof/profile", ""))
	re.Equal("Profile", serviceLabel)
	serviceLabel = suite.svr.GetAPIAccessServiceLabel(
		apiutil.NewAccessPath("/pd/api/v1/debug/pprof/profile", http.MethodGet))
	re.Equal("Profile", serviceLabel)

	accessPaths = suite.svr.GetServiceLabels("GetSchedulerConfig")
	re.Len(accessPaths, 1)
	re.Equal("/pd/api/v1/scheduler-config", accessPaths[0].Path)
	re.Equal("GET", accessPaths[0].Method)
	accessPaths = suite.svr.GetServiceLabels("HandleSchedulerConfig")
	re.Len(accessPaths, 4)
	re.Equal("/pd/api/v1/scheduler-config", accessPaths[0].Path)

	accessPaths = suite.svr.GetServiceLabels("ResignLeader")
	re.Len(accessPaths, 1)
	re.Equal("/pd/api/v1/leader/resign", accessPaths[0].Path)
	re.Equal(http.MethodPost, accessPaths[0].Method)
	serviceLabel = suite.svr.GetAPIAccessServiceLabel(
		apiutil.NewAccessPath("/pd/api/v1/leader/resign", http.MethodPost))
	re.Equal("ResignLeader", serviceLabel)
	serviceLabel = suite.svr.GetAPIAccessServiceLabel(
		apiutil.NewAccessPath("/pd/api/v1/leader/resign", http.MethodGet))
	re.Empty(serviceLabel)
	serviceLabel = suite.svr.GetAPIAccessServiceLabel(
		apiutil.NewAccessPath("/pd/api/v1/leader/resign", ""))
	re.Empty(serviceLabel)

	accessPaths = suite.svr.GetServiceLabels("queryMetric")
	re.Len(accessPaths, 4)
	sort.Slice(accessPaths, func(i, j int) bool {
		if accessPaths[i].Path == accessPaths[j].Path {
			return accessPaths[i].Method < accessPaths[j].Method
		}
		return accessPaths[i].Path < accessPaths[j].Path
	})
	re.Equal("/pd/api/v1/metric/query", accessPaths[0].Path)
	re.Equal(http.MethodGet, accessPaths[0].Method)
	re.Equal("/pd/api/v1/metric/query", accessPaths[1].Path)
	re.Equal(http.MethodPost, accessPaths[1].Method)
	re.Equal("/pd/api/v1/metric/query_range", accessPaths[2].Path)
	re.Equal(http.MethodGet, accessPaths[2].Method)
	re.Equal("/pd/api/v1/metric/query_range", accessPaths[3].Path)
	re.Equal(http.MethodPost, accessPaths[3].Method)
	serviceLabel = suite.svr.GetAPIAccessServiceLabel(
		apiutil.NewAccessPath("/pd/api/v1/metric/query", http.MethodPost))
	re.Equal("queryMetric", serviceLabel)
	serviceLabel = suite.svr.GetAPIAccessServiceLabel(
		apiutil.NewAccessPath("/pd/api/v1/metric/query", http.MethodGet))
	re.Equal("queryMetric", serviceLabel)
}
