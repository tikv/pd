// Copyright 2022 TiKV Project Authors.
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

package config

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/ratelimit"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

// dialClient used to dial http request.
var dialClient = &http.Client{
	Transport: &http.Transport{
		DisableKeepAlives: true,
	},
}

func TestRateLimitConfigReload(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 3)
	re.NoError(err)
	defer cluster.Destroy()
	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())
	leader := cluster.GetServer(cluster.GetLeader())
	re.NotNil(leader)
	re.Empty(leader.GetServer().GetServiceMiddlewareConfig().RateLimitConfig.LimiterConfig)
	limitCfg := make(map[string]ratelimit.DimensionConfig)
	limitCfg["GetRegions"] = ratelimit.DimensionConfig{QPS: 1}

	input := map[string]interface{}{
		"enable-rate-limit": "true",
		"limiter-config":    limitCfg,
	}
	data, err := json.Marshal(input)
	re.NoError(err)
	req, _ := http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	resp, err := dialClient.Do(req)
	re.NoError(err)
	resp.Body.Close()
	re.True(leader.GetServer().GetServiceMiddlewarePersistOptions().IsRateLimitEnabled())
	re.Len(leader.GetServer().GetServiceMiddlewarePersistOptions().GetRateLimitConfig().LimiterConfig, 1)

	oldLeaderName := leader.GetServer().Name()
	leader.GetServer().GetMember().ResignEtcdLeader(leader.GetServer().Context(), oldLeaderName, "")
	var servers []*server.Server
	for _, s := range cluster.GetServers() {
		servers = append(servers, s.GetServer())
	}
	server.MustWaitLeader(re, servers)
	leader = cluster.GetServer(cluster.GetLeader())
	re.NotNil(leader)
	re.True(leader.GetServer().GetServiceMiddlewarePersistOptions().IsRateLimitEnabled())
	re.Len(leader.GetServer().GetServiceMiddlewarePersistOptions().GetRateLimitConfig().LimiterConfig, 1)
}

func TestSwitchMode(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer cluster.Destroy()
	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())
	leader := cluster.GetServer(cluster.GetLeader())
	re.NotNil(leader)
	re.NoError(leader.BootstrapCluster())
	// To put store.
	svr := &server.GrpcServer{Server: leader.GetServer()}
	putStore(svr, leader, 1)
	// default mode
	addr := leader.GetAddr() + "/pd/api/v1/config"
	cfg := getConfig(re, addr)
	re.Equal(config.Normal, cfg.ScheduleMode.Mode)
	re.Equal(uint64(2048), cfg.Schedule.RegionScheduleLimit)

	changeModeConfig(re, addr, "normal", true)
	cfg = getConfig(re, addr)
	re.True(cfg.ScheduleMode.EnableAutoTune)
	changeModeConfig(re, addr, "normal", false)
	cfg = getConfig(re, addr)
	re.False(cfg.ScheduleMode.EnableAutoTune)

	// change mode from normal to suspend is ok
	changeModeConfig(re, addr, "suspend")
	cfg = getConfig(re, addr)
	re.Equal(config.Suspend, cfg.ScheduleMode.Mode)
	re.Equal(uint64(0), cfg.Schedule.RegionScheduleLimit)
	re.Equal(uint64(0), cfg.Schedule.LeaderScheduleLimit)
	re.Equal(uint64(0), cfg.Schedule.ReplicaScheduleLimit)
	re.Equal(uint64(0), cfg.Schedule.HotRegionScheduleLimit)
	re.Equal(uint64(0), cfg.Schedule.MergeScheduleLimit)

	// change mode from suspend to scaling is ok
	changeModeConfig(re, addr, "scaling")
	cfg = getConfig(re, addr)
	re.Equal(config.Scaling, cfg.ScheduleMode.Mode)
	re.Equal(uint64(2048), cfg.Schedule.RegionScheduleLimit)
	// change mode from scaling to normal is ok
	changeModeConfig(re, addr, "normal")
	// change mode from normal to scaling is ok
	changeModeConfig(re, addr, "scaling")
	cfg = getConfig(re, addr)
	re.Equal(config.Scaling, cfg.ScheduleMode.Mode)
	re.Equal(200.0, cfg.Schedule.StoreLimit[1].AddPeer)
	re.Equal(200.0, cfg.Schedule.StoreLimit[1].RemovePeer)

	// change mode from scaling to normal is ok
	changeModeConfig(re, addr, "normal")
	cfg = getConfig(re, addr)
	re.Equal(config.Normal, cfg.ScheduleMode.Mode)
	re.Equal(uint64(2048), cfg.Schedule.RegionScheduleLimit)
	re.Equal(15.0, cfg.Schedule.StoreLimit[1].AddPeer)
	re.Equal(15.0, cfg.Schedule.StoreLimit[1].RemovePeer)

	// add a new store
	putStore(svr, leader, 2)
	// the store limit should use the value in scaling mode.
	changeModeConfig(re, addr, "scaling")
	cfg = getConfig(re, addr)
	re.Equal(config.Scaling, cfg.ScheduleMode.Mode)
	re.Equal(uint64(2048), cfg.Schedule.RegionScheduleLimit)
	re.Equal(200.0, cfg.Schedule.StoreLimit[2].AddPeer)
	re.Equal(200.0, cfg.Schedule.StoreLimit[2].RemovePeer)
}

func changeModeConfig(re *require.Assertions, addr, mode string, enableAutoTune ...bool) {
	input := map[string]interface{}{"mode": mode}
	if len(enableAutoTune) != 0 {
		input["enable-auto-tune"] = enableAutoTune[0]
	}
	data, _ := json.Marshal(input)
	req, _ := http.NewRequest(http.MethodPost, addr, bytes.NewBuffer(data))
	resp, err := dialClient.Do(req)
	re.NoError(err)
	defer resp.Body.Close()
	re.Equal(http.StatusOK, resp.StatusCode)
}

func getConfig(re *require.Assertions, addr string) config.Config {
	var cfg config.Config
	reqGet, _ := http.NewRequest(http.MethodGet, addr, nil)
	resp, err := dialClient.Do(reqGet)
	re.NoError(err)
	defer resp.Body.Close()
	bodyBytes, err := io.ReadAll(resp.Body)
	re.NoError(err)
	err = json.Unmarshal(bodyBytes, &cfg)
	re.NoError(err)
	return cfg
}

func putStore(svr *server.GrpcServer, leader *tests.TestServer, storeID uint64) {
	putStoreRequest := &pdpb.PutStoreRequest{
		Header: &pdpb.RequestHeader{ClusterId: leader.GetClusterID()},
		Store: &metapb.Store{
			Id:      storeID,
			Address: fmt.Sprintf("mock-%d", storeID),
			Version: "6.1.0",
		},
	}
	svr.PutStore(context.Background(), putStoreRequest)
}
