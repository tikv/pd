// Copyright 2022 TiKV Project Authors.
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
	"github.com/tikv/pd/pkg/mcs/resource_manager/server"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/tests"
	"go.uber.org/goleak"
	"google.golang.org/grpc"

	// Register Service
	_ "github.com/tikv/pd/pkg/mcs/registry"
	_ "github.com/tikv/pd/pkg/mcs/resource_manager/server/install"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func TestBasicReourceGroupCURD(t *testing.T) {
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

	// Test registered GRPC Service
	cc, err := grpc.DialContext(ctx, strings.TrimPrefix(leader.GetAddr(), "http://"), grpc.WithInsecure())
	re.NoError(err)
	defer cc.Close()

	grpcclient := rmpb.NewResourceManagerClient(cc)

	testCasesSet1 := []struct {
		name           string
		mode           rmpb.GroupMode
		addSuccess     bool
		modifySuccess  bool
		expectMarshal  string
		modifySettings func(*rmpb.ResourceGroup)
	}{
		{"test1", rmpb.GroupMode_RUMode, true, true,
			`{"name":"test1","mode":0,"r_u_settings":{"rru":{"token_bucket":{"settings":{"fill_rate":10000}},"initialized":false},"wru":{"initialized":false}}}`,
			func(gs *rmpb.ResourceGroup) {
				gs.RUSettings = &rmpb.GroupRequestUnitSettings{
					RRU: &rmpb.TokenBucket{
						Settings: &rmpb.TokenLimitSettings{
							FillRate: 10000,
						},
					},
				}
			},
		},

		{"test2", rmpb.GroupMode_RUMode, true, true,
			`{"name":"test2","mode":0,"r_u_settings":{"rru":{"token_bucket":{"settings":{"fill_rate":20000}},"initialized":false},"wru":{"initialized":false}}}`,
			func(gs *rmpb.ResourceGroup) {
				gs.RUSettings = &rmpb.GroupRequestUnitSettings{
					RRU: &rmpb.TokenBucket{
						Settings: &rmpb.TokenLimitSettings{
							FillRate: 20000,
						},
					},
				}
			},
		},
		{"test2", rmpb.GroupMode_RUMode, false, true,
			`{"name":"test2","mode":0,"r_u_settings":{"rru":{"token_bucket":{"settings":{"fill_rate":30000}},"initialized":false},"wru":{"initialized":false}}}`,
			func(gs *rmpb.ResourceGroup) {
				gs.RUSettings = &rmpb.GroupRequestUnitSettings{
					RRU: &rmpb.TokenBucket{
						Settings: &rmpb.TokenLimitSettings{
							FillRate: 30000,
						},
					},
				}
			},
		},
		{"test3", rmpb.GroupMode_NativeMode, true, false,
			`{"name":"test3","mode":1}`,
			func(gs *rmpb.ResourceGroup) {
				gs.RUSettings = &rmpb.GroupRequestUnitSettings{
					RRU: &rmpb.TokenBucket{
						Settings: &rmpb.TokenLimitSettings{
							FillRate: 10000,
						},
					},
				}
			},
		},
		{"test3", rmpb.GroupMode_NativeMode, false, true,
			`{"name":"test3","mode":1,"resource_settings":{"cpu":{"token_bucket":{"settings":{"fill_rate":1000000}},"initialized":false},"io_read_bandwidth":{"initialized":false},"io_write_bandwidth":{"initialized":false}}}`,
			func(gs *rmpb.ResourceGroup) {
				gs.ResourceSettings = &rmpb.GroupResourceSettings{
					Cpu: &rmpb.TokenBucket{
						Settings: &rmpb.TokenLimitSettings{
							FillRate: 1000000,
						},
					},
				}
			},
		},
	}

	checkErr := func(err error, success bool) {
		if success {
			re.NoError(err)
		} else {
			re.Error(err)
		}
	}

	finalNum := 0
	// Test Resource Group CURD via gRPC
	for i, tcase := range testCasesSet1 {
		group := &rmpb.ResourceGroup{
			Name: tcase.name,
			Mode: tcase.mode,
		}
		// Create Resource Group
		resp, err := grpcclient.AddResourceGroup(ctx, &rmpb.PutResourceGroupRequest{Group: group})
		checkErr(err, tcase.addSuccess)
		if tcase.addSuccess {
			finalNum++
			re.Contains(resp.Body, "Success!")
		}

		// Modify Resource Group
		tcase.modifySettings(group)
		mresp, err := grpcclient.ModifyResourceGroup(ctx, &rmpb.PutResourceGroupRequest{Group: group})
		checkErr(err, tcase.modifySuccess)
		if tcase.modifySuccess {
			re.Contains(mresp.Body, "Success!")
		}

		// Get Resource Group
		gresp, err := grpcclient.GetResourceGroup(ctx, &rmpb.GetResourceGroupRequest{ResourceGroupName: tcase.name})
		re.NoError(err)
		re.Equal(tcase.name, gresp.Group.Name)
		if tcase.modifySuccess {
			re.Equal(group, gresp.Group)
		}

		// Last one, Check list and delete all resource groups
		if i == len(testCasesSet1)-1 {
			// List Resource Group
			lresp, err := grpcclient.ListResourceGroups(ctx, &rmpb.ListResourceGroupsRequest{})
			re.NoError(err)
			re.Equal(finalNum, len(lresp.Groups))

			for _, g := range lresp.Groups {
				// Delete Resource Group
				dresp, err := grpcclient.DeleteResourceGroup(ctx, &rmpb.DeleteResourceGroupRequest{ResourceGroupName: g.Name})
				re.NoError(err)
				re.Contains(dresp.Body, "Success!")
			}
		}
	}

	// Test Resource Group CURD via HTTP
	finalNum = 0
	for i, tcase := range testCasesSet1 {
		// Create Resource Group
		group := &rmpb.ResourceGroup{
			Name: tcase.name,
			Mode: tcase.mode,
		}
		createJSON, err := json.Marshal(group)
		re.NoError(err)
		resp, err := http.Post(leader.GetAddr()+"/resource-manager/api/v1/config/group", "application/json", strings.NewReader(string(createJSON)))
		re.NoError(err)
		defer resp.Body.Close()
		if tcase.addSuccess {
			re.Equal(http.StatusOK, resp.StatusCode)
			finalNum++
		} else {
			re.Equal(http.StatusInternalServerError, resp.StatusCode)
		}

		// Modify Resource Group
		tcase.modifySettings(group)
		modifyJSON, err := json.Marshal(group)
		re.NoError(err)
		req, err := http.NewRequest(http.MethodPut, leader.GetAddr()+"/resource-manager/api/v1/config/group", strings.NewReader(string(modifyJSON)))
		re.NoError(err)
		req.Header.Set("Content-Type", "application/json")
		resp, err = http.DefaultClient.Do(req)
		re.NoError(err)
		defer resp.Body.Close()
		if tcase.modifySuccess {
			re.Equal(http.StatusOK, resp.StatusCode)
		} else {
			re.Equal(http.StatusInternalServerError, resp.StatusCode)
		}

		// Get Resource Group
		resp, err = http.Get(leader.GetAddr() + "/resource-manager/api/v1/config/group/" + tcase.name)
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)
		respString, err := io.ReadAll(resp.Body)
		re.NoError(err)
		re.Contains(string(respString), tcase.name)
		if tcase.modifySuccess {
			re.Equal(string(respString), tcase.expectMarshal)
		}

		// Last one, Check list and delete all resource groups
		if i == len(testCasesSet1)-1 {
			resp, err := http.Get(leader.GetAddr() + "/resource-manager/api/v1/config/groups")
			re.NoError(err)
			defer resp.Body.Close()
			re.Equal(http.StatusOK, resp.StatusCode)
			respString, err := io.ReadAll(resp.Body)
			re.NoError(err)
			groups := make([]*server.ResourceGroup, 0)
			json.Unmarshal(respString, &groups)
			re.Equal(finalNum, len(groups))

			// Delete all resource groups
			for _, g := range groups {
				req, err := http.NewRequest(http.MethodDelete, leader.GetAddr()+"/resource-manager/api/v1/config/group/"+g.Name, nil)
				re.NoError(err)
				resp, err := http.DefaultClient.Do(req)
				re.NoError(err)
				defer resp.Body.Close()
				re.Equal(http.StatusOK, resp.StatusCode)
				respString, err := io.ReadAll(resp.Body)
				re.NoError(err)
				re.Contains(string(respString), "Success!")
			}

			// verify again
			resp1, err := http.Get(leader.GetAddr() + "/resource-manager/api/v1/config/groups")
			re.NoError(err)
			defer resp1.Body.Close()
			re.Equal(http.StatusOK, resp1.StatusCode)
			respString1, err := io.ReadAll(resp1.Body)
			re.NoError(err)
			groups1 := make([]server.ResourceGroup, 0)
			json.Unmarshal(respString1, &groups1)
			re.Equal(0, len(groups1))
		}
	}
}
