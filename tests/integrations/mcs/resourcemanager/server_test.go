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

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/client/grpcutil"
	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"github.com/tikv/pd/pkg/versioninfo"
	"github.com/tikv/pd/tests"
)

func TestResourceManagerServer(t *testing.T) {
	re := require.New(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestAPICluster(ctx, 1)
	defer cluster.Destroy()
	re.NoError(err)

	err = cluster.RunInitialServers()
	re.NoError(err)

	leaderName := cluster.WaitLeader()
	re.NotEmpty(leaderName)
	leader := cluster.GetServer(leaderName)

	s, cleanup := tests.StartSingleResourceManagerTestServer(ctx, re, leader.GetAddr(), tempurl.Alloc())
	addr := s.GetAddr()
	defer cleanup()
	tests.WaitForPrimaryServing(re, map[string]bs.Server{addr: s})

	// Test registered GRPC Service
	cc, err := grpcutil.GetClientConn(ctx, addr, nil)
	re.NoError(err)
	defer cc.Close()

	c := rmpb.NewResourceManagerClient(cc)
	_, err = c.GetResourceGroup(context.Background(), &rmpb.GetResourceGroupRequest{
		ResourceGroupName: "pingcap",
	})
	re.ErrorContains(err, "resource group not found")

	// Test registered REST HTTP Handler
	url := addr + "/resource-manager/api/v1/config"
	{
		resp, err := tests.TestDialClient.Get(url + "/groups")
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)
		respString, err := io.ReadAll(resp.Body)
		re.NoError(err)
		re.JSONEq(`[{"name":"default","mode":1,"r_u_settings":{"r_u":{"settings":{"fill_rate":2147483647,"burst_limit":-1},"state":{"initialized":false}}},"priority":8}]`, string(respString))
	}
	{
		group := &rmpb.ResourceGroup{
			Name: "pingcap",
			Mode: 1,
		}
		createJSON, err := json.Marshal(group)
		re.NoError(err)
		resp, err := tests.TestDialClient.Post(url+"/group", "application/json", strings.NewReader(string(createJSON)))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)
	}
	{
		resp, err := tests.TestDialClient.Get(url + "/group/pingcap")
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)
		respString, err := io.ReadAll(resp.Body)
		re.NoError(err)
		re.JSONEq("{\"name\":\"pingcap\",\"mode\":1,\"r_u_settings\":{\"r_u\":{\"state\":{\"initialized\":false}}},\"priority\":0}", string(respString))
	}

	// Test metrics handler
	{
		resp, err := tests.TestDialClient.Get(addr + "/metrics")
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)
		respBytes, err := io.ReadAll(resp.Body)
		re.NoError(err)
		re.Contains(string(respBytes), "pd_server_info")
	}

	// Test status handler
	{
		resp, err := tests.TestDialClient.Get(addr + "/status")
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)
		respBytes, err := io.ReadAll(resp.Body)
		re.NoError(err)
		var s versioninfo.Status
		re.NoError(json.Unmarshal(respBytes, &s))
		re.Equal(versioninfo.PDBuildTS, s.BuildTS)
		re.Equal(versioninfo.PDGitHash, s.GitHash)
		re.Equal(versioninfo.PDReleaseVersion, s.Version)
	}
}
