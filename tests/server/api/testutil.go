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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"
	"sync"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/utils/assertutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/versioninfo"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/api"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

const (
	schedulersPrefix      = "/pd/api/v1/schedulers"
	schedulerConfigPrefix = "/pd/api/v1/scheduler-config"
)

// MustAddScheduler adds a scheduler with HTTP API.
func MustAddScheduler(
	re *require.Assertions, serverAddr string,
	schedulerName string, args map[string]any,
) {
	request := map[string]any{
		"name": schedulerName,
	}
	for arg, val := range args {
		request[arg] = val
	}
	data, err := json.Marshal(request)
	re.NoError(err)
	httpReq, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s%s", serverAddr, schedulersPrefix), bytes.NewBuffer(data))
	re.NoError(err)
	// Send request.
	resp, err := tests.TestDialClient.Do(httpReq)
	re.NoError(err)
	defer resp.Body.Close()
	data, err = io.ReadAll(resp.Body)
	re.NoError(err)
	re.Equal(http.StatusOK, resp.StatusCode, string(data))
}

// MustDeleteScheduler deletes a scheduler with HTTP API.
func MustDeleteScheduler(re *require.Assertions, serverAddr, schedulerName string) {
	httpReq, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s%s/%s", serverAddr, schedulersPrefix, schedulerName), http.NoBody)
	re.NoError(err)
	resp, err := tests.TestDialClient.Do(httpReq)
	re.NoError(err)
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	re.NoError(err)
	re.Equal(http.StatusOK, resp.StatusCode, string(data))
}

// MustCallSchedulerConfigAPI calls a scheduler config with HTTP API with the given args.
func MustCallSchedulerConfigAPI(
	re *require.Assertions,
	method, serverAddr, schedulerName string, args []string,
	input map[string]any,
) {
	data, err := json.Marshal(input)
	re.NoError(err)
	args = append([]string{schedulerConfigPrefix, schedulerName}, args...)
	httpReq, err := http.NewRequest(method, fmt.Sprintf("%s%s", serverAddr, path.Join(args...)), bytes.NewBuffer(data))
	re.NoError(err)
	resp, err := tests.TestDialClient.Do(httpReq)
	re.NoError(err)
	defer resp.Body.Close()
	data, err = io.ReadAll(resp.Body)
	re.NoError(err)
	re.Equal(http.StatusOK, resp.StatusCode, string(data))
}

var (
	// testDialClient used to dial http request. only used for test.
	testDialClient = &http.Client{
		Transport: &http.Transport{
			DisableKeepAlives: true,
		},
	}

	store = &metapb.Store{
		Id:        1,
		Address:   "mock://tikv-1:1",
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

func mustNewServer(re *require.Assertions, opts ...func(cfg *config.Config)) (*server.Server, testutil.CleanupFunc) {
	_, svrs, cleanup := mustNewCluster(re, 1, opts...)
	return svrs[0], cleanup
}

var zapLogOnce sync.Once

func mustNewCluster(re *require.Assertions, num int, opts ...func(cfg *config.Config)) ([]*config.Config, []*server.Server, testutil.CleanupFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	svrs := make([]*server.Server, 0, num)
	cfgs := tests.NewTestMultiConfig(assertutil.CheckerWithNilAssert(re), num)

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
			s, err := server.CreateServer(ctx, cfg, nil, api.NewHandler)
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
	tests.MustWaitLeader(re, svrs)

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
			Address:   fmt.Sprintf("mock://tikv-%d:%d", id, id),
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
