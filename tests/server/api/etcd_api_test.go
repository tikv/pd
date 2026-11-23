// Copyright 2019 TiKV Project Authors.
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
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

func TestGRPCGateway(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("enabled=%t", enabled), func(t *testing.T) {
			re := require.New(t)
			cluster, err := tests.NewTestCluster(t.Context(), 1, func(cfg *config.Config, _ string) {
				cfg.EnableGRPCGateway = enabled
			})
			re.NoError(err)
			defer cluster.Destroy()
			re.NoError(cluster.RunInitialServers())
			re.NotEmpty(cluster.WaitLeader())
			checkGRPCGateway(t, cluster.GetLeaderServer(), enabled)
		})
	}
}

func TestGRPCGatewayRestart(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("previously-enabled=%t", enabled), func(t *testing.T) {
			re := require.New(t)
			cluster, err := tests.NewTestCluster(t.Context(), 1, func(cfg *config.Config, _ string) {
				cfg.EnableGRPCGateway = enabled
			})
			re.NoError(err)
			defer cluster.Destroy()
			re.NoError(cluster.RunInitialServers())
			re.NotEmpty(cluster.WaitLeader())
			original := cluster.GetLeaderServer()
			checkGRPCGateway(t, original, enabled)
			cfg := original.GetConfig().Clone()
			re.NoError(original.Stop())
			re.NoError(cfg.Adjust(nil, false))
			re.False(cfg.EnableGRPCGateway)
			restarted, err := tests.NewTestServer(t.Context(), cfg, nil)
			re.NoError(err)
			re.NoError(restarted.Run())
			defer func() { re.NoError(restarted.Stop()) }()
			testutil.Eventually(re, func() bool { return restarted.GetServer().GetMember().IsServing() })
			checkGRPCGateway(t, restarted, false)
		})
	}
}

func checkGRPCGateway(t *testing.T, svr *tests.TestServer, enabled bool) {
	t.Helper()
	re := require.New(t)
	ctx := t.Context()
	client := svr.GetEtcdClient()
	_, err := client.Put(ctx, "normal-grpc-path", "value")
	re.NoError(err)
	resp, err := client.Get(ctx, "normal-grpc-path")
	re.NoError(err)
	re.Len(resp.Kvs, 1)
	re.Equal("value", string(resp.Kvs[0].Value))
	re.NoError(testutil.CheckGetJSON(tests.TestDialClient, svr.GetAddr()+"/pd/api/v1/health", nil, testutil.StatusOK(re)))

	addr := svr.GetAddr() + "/v3/kv/put"
	putKey := map[string]string{"key": "Zm9v", "value": "YmFy"}
	v, err := json.Marshal(putKey)
	re.NoError(err)
	status := http.StatusOK
	if !enabled {
		status = http.StatusNotFound
	}
	err = testutil.CheckPostJSON(tests.TestDialClient, addr, v, testutil.Status(re, status))
	re.NoError(err)
	addr = svr.GetAddr() + "/v3/kv/range"
	getKey := map[string]string{"key": "Zm9v"}
	v, err = json.Marshal(getKey)
	re.NoError(err)
	checks := []func([]byte, int, http.Header){testutil.Status(re, status)}
	if enabled {
		checks = append(checks, testutil.StringContain(re, "Zm9v"))
	}
	err = testutil.CheckPostJSON(tests.TestDialClient, addr, v, checks...)
	re.NoError(err)
}
