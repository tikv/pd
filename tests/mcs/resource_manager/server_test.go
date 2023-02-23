// Copyright 2023 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package resourcemanager_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/stretchr/testify/require"
	rm "github.com/tikv/pd/pkg/mcs/resource_manager/server"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/tests"
	"google.golang.org/grpc"
)

func TestResourceManagerServer(t *testing.T) {
	re := require.New(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	defer cluster.Destroy()
	re.NoError(err)

	err = cluster.RunInitialServers()
	re.NoError(err)

	leaderName := cluster.WaitLeader()
	leader := cluster.GetServer(leaderName)

	cfg := rm.NewConfig()
	cfg.BackendEndpoints = leader.GetAddr()
	cfg.ListenAddr = "127.0.0.1:8086"

	svr := rm.NewServer(ctx, cfg)
	go svr.Run()
	testutil.Eventually(re, func() bool {
		return svr.IsServing()
	}, testutil.WithWaitFor(5*time.Second), testutil.WithTickInterval(50*time.Millisecond))
	defer svr.Close()

	// Test registered GRPC Service
	cc, err := grpc.DialContext(ctx, cfg.ListenAddr, grpc.WithInsecure())
	re.NoError(err)
	defer cc.Close()
	c := rmpb.NewResourceManagerClient(cc)
	_, err = c.GetResourceGroup(context.Background(), &rmpb.GetResourceGroupRequest{
		ResourceGroupName: "pingcap",
	})
	re.ErrorContains(err, "resource group not found")

	// Test registered REST HTTP Handler
	url := "http://" + cfg.ListenAddr + "/resource-manager/api/v1/config"
	{
		resp, err := http.Get(url + "/groups")
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)
		respString, err := io.ReadAll(resp.Body)
		re.NoError(err)
		re.Equal("[]", string(respString))
	}
	{
		group := &rmpb.ResourceGroup{
			Name: "pingcap",
			Mode: 1,
		}
		createJSON, err := json.Marshal(group)
		re.NoError(err)
		resp, err := http.Post(url+"/group", "application/json", strings.NewReader(string(createJSON)))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)
	}
	{
		resp, err := http.Get(url + "/group/pingcap")
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)
		respString, err := io.ReadAll(resp.Body)
		re.NoError(err)
		re.Equal("{\"name\":\"pingcap\",\"mode\":1,\"r_u_settings\":{\"ru\":{\"state\":{\"initialized\":false}}}}", string(respString))
	}
}
