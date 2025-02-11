// Copyright 2016 TiKV Project Authors.
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
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/assertutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/versioninfo"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
)

var (
	// testDialClient used to dial http request. only used for test.
	testDialClient = &http.Client{
		Transport: &http.Transport{
			DisableKeepAlives: true,
		},
	}

	store = &metapb.Store{
		Id:        1,
		Address:   "localhost",
		NodeState: metapb.NodeState_Serving,
	}
	peers = []*metapb.Peer{
		{
			Id:      2,
			StoreId: store.GetId(),
		},
	}
	region = &metapb.Region{
		Id: 8,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
		Peers: peers,
	}
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func mustNewServer(re *require.Assertions, opts ...func(cfg *config.Config)) (*server.Server, testutil.CleanupFunc) {
	_, svrs, cleanup := mustNewCluster(re, 1, opts...)
	return svrs[0], cleanup
}

var zapLogOnce sync.Once

func mustNewCluster(re *require.Assertions, num int, opts ...func(cfg *config.Config)) ([]*config.Config, []*server.Server, testutil.CleanupFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	svrs := make([]*server.Server, 0, num)
	cfgs := server.NewTestMultiConfig(assertutil.CheckerWithNilAssert(re), num)

	ch := make(chan *server.Server, num)
	for _, cfg := range cfgs {
		go func(cfg *config.Config) {
			err := logutil.SetupLogger(&cfg.Log, &cfg.Logger, &cfg.LogProps, cfg.Security.RedactInfoLog)
			re.NoError(err)
			zapLogOnce.Do(func() {
				log.ReplaceGlobals(cfg.Logger, cfg.LogProps)
			})
			for _, opt := range opts {
				opt(cfg)
			}
			s, err := server.CreateServer(ctx, cfg, nil, NewHandler)
			re.NoError(err)
			err = s.Run()
			re.NoError(err)
			ch <- s
		}(cfg)
	}

	for range num {
		svr := <-ch
		svrs = append(svrs, svr)
	}
	close(ch)
	// wait etcd and http servers
	server.MustWaitLeader(re, svrs)

	// clean up
	clean := func() {
		cancel()
		for _, s := range svrs {
			s.Close()
		}
		for _, cfg := range cfgs {
			testutil.CleanServer(cfg.DataDir)
		}
	}

	return cfgs, svrs, clean
}

func mustBootstrapCluster(re *require.Assertions, s *server.Server) {
	grpcPDClient := testutil.MustNewGrpcClient(re, s.GetAddr())
	req := &pdpb.BootstrapRequest{
		Header: testutil.NewRequestHeader(keypath.ClusterID()),
		Store:  store,
		Region: region,
	}
	resp, err := grpcPDClient.Bootstrap(context.Background(), req)
	re.NoError(err)
	re.Equal(pdpb.ErrorType_OK, resp.GetHeader().GetError().GetType())
}

func mustPutRegion(re *require.Assertions, svr *server.Server, regionID, storeID uint64, start, end []byte, opts ...core.RegionCreateOption) *core.RegionInfo {
	leader := &metapb.Peer{
		Id:      regionID,
		StoreId: storeID,
	}
	metaRegion := &metapb.Region{
		Id:          regionID,
		StartKey:    start,
		EndKey:      end,
		Peers:       []*metapb.Peer{leader},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
	}
	r := core.NewRegionInfo(metaRegion, leader, opts...)
	err := svr.GetRaftCluster().HandleRegionHeartbeat(r)
	re.NoError(err)
	return r
}

func mustPutStore(re *require.Assertions, svr *server.Server, id uint64, state metapb.StoreState, nodeState metapb.NodeState, labels []*metapb.StoreLabel) {
	s := &server.GrpcServer{Server: svr}
	_, err := s.PutStore(context.Background(), &pdpb.PutStoreRequest{
		Header: &pdpb.RequestHeader{ClusterId: keypath.ClusterID()},
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
			Header: &pdpb.RequestHeader{ClusterId: keypath.ClusterID()},
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
	server.MustWaitLeader(re, []*server.Server{suite.svr})

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
	re.Equal("", accessPaths[0].Method)
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
	re.Equal("", serviceLabel)
	serviceLabel = suite.svr.GetAPIAccessServiceLabel(
		apiutil.NewAccessPath("/pd/api/v1/leader/resign", ""))
	re.Equal("", serviceLabel)

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

func (suite *adminTestSuite) TestCleanPath() {
	re := suite.Require()
	// transfer path to /config
	url := fmt.Sprintf("%s/admin/persist-file/../../config", suite.urlPrefix)
	cfg := &config.Config{}
	err := testutil.ReadGetJSON(re, testDialClient, url, cfg)
	re.NoError(err)

	// handled by router
	response := httptest.NewRecorder()
	r, _, _ := NewHandler(context.Background(), suite.svr)
	request, err := http.NewRequest(http.MethodGet, url, http.NoBody)
	re.NoError(err)
	r.ServeHTTP(response, request)
	// handled by `cleanPath` which is in `mux.ServeHTTP`
	result := response.Result()
	defer result.Body.Close()
	re.NotNil(result.Header["Location"])
	re.Contains(result.Header["Location"][0], "/pd/api/v1/config")
}
