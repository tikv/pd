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
	"time"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/stretchr/testify/suite"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/pkg/mcs/resource_manager/server"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/tests"
	"go.uber.org/goleak"

	// Register Service
	_ "github.com/tikv/pd/pkg/mcs/registry"
	_ "github.com/tikv/pd/pkg/mcs/resource_manager/server/install"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

type resourceManagerClientTestSuite struct {
	suite.Suite
	ctx     context.Context
	clean   context.CancelFunc
	cluster *tests.TestCluster
	client  pd.Client
}

func TestResourceManagerClientTestSuite(t *testing.T) {
	suite.Run(t, new(resourceManagerClientTestSuite))
}

func (suite *resourceManagerClientTestSuite) SetupSuite() {
	var err error
	re := suite.Require()

	suite.ctx, suite.clean = context.WithCancel(context.Background())

	suite.cluster, err = tests.NewTestCluster(suite.ctx, 1)
	re.NoError(err)

	err = suite.cluster.RunInitialServers()
	re.NoError(err)

	leaderName := suite.cluster.WaitLeader()
	leader := suite.cluster.GetServer(leaderName)
	suite.client, err = pd.NewClientWithContext(suite.ctx, []string{leader.GetAddr()}, pd.SecurityOption{})
	re.NoError(err)
}

func (suite *resourceManagerClientTestSuite) TearDownSuite() {
	suite.client.Close()
	suite.clean()
	suite.cluster.Destroy()
}

func (suite *resourceManagerClientTestSuite) TestAcquireTokenBucket() {
	re := suite.Require()
	cli := suite.client

	groups := []*rmpb.ResourceGroup{
		{
			Name: "test1",
			Mode: rmpb.GroupMode_RUMode,
			RUSettings: &rmpb.GroupRequestUnitSettings{
				RRU: &rmpb.TokenBucket{
					Settings: &rmpb.TokenLimitSettings{
						FillRate: 10000,
					},
					Tokens: 100000,
				},
			},
		},
		{
			Name: "test2",
			Mode: rmpb.GroupMode_RUMode,
			RUSettings: &rmpb.GroupRequestUnitSettings{
				RRU: &rmpb.TokenBucket{
					Settings: &rmpb.TokenLimitSettings{
						FillRate: 40000,
					},
					Tokens: 100000,
				},
			},
		},
	}
	for _, group := range groups {
		resp, err := cli.AddResourceGroup(suite.ctx, group)
		re.NoError(err)
		re.Contains(resp, "Success!")
	}
	reqs := &rmpb.TokenBucketsRequest{
		Requests:              make([]*rmpb.TokenBucketRequest, 0),
		TargetRequestPeriodMs: uint64(time.Second * 10 / time.Millisecond),
	}

	groups = append(groups, &rmpb.ResourceGroup{Name: "test3"})
	for _, group := range groups {
		requests := make([]*rmpb.RequestUnitItem, 0)
		requests = append(requests, &rmpb.RequestUnitItem{
			Type:  rmpb.RequestUnitType_RRU,
			Value: 10000,
		})
		req := &rmpb.TokenBucketRequest{
			ResourceGroupName: group.Name,
			Request: &rmpb.TokenBucketRequest_RuItems{
				RuItems: &rmpb.TokenBucketRequest_RequestRU{
					RequestRU: requests,
				},
			},
		}
		reqs.Requests = append(reqs.Requests, req)
	}
	aresp, err := cli.AcquireTokenBuckets(suite.ctx, reqs)
	re.NoError(err)
	for _, resp := range aresp {
		re.Len(resp.GrantedRUTokens, 1)
		re.Equal(resp.GrantedRUTokens[0].GrantedTokens.Tokens, float64(10000.))
	}
	gresp, err := cli.GetResourceGroup(suite.ctx, groups[0].GetName())
	re.NoError(err)
	re.Less(gresp.RUSettings.RRU.Tokens, groups[0].RUSettings.RRU.Tokens)

	checkFunc := func(g1 *rmpb.ResourceGroup, g2 *rmpb.ResourceGroup) {
		re.Equal(g1.GetName(), g2.GetName())
		re.Equal(g1.GetMode(), g2.GetMode())
		re.Equal(g1.GetRUSettings().RRU.Settings.FillRate, g2.GetRUSettings().RRU.Settings.FillRate)
		// now we don't persistent tokens in running state, so tokens is original.
		re.Equal(g1.GetRUSettings().RRU.Tokens, g2.GetRUSettings().RRU.Tokens)
		re.NoError(err)
	}

	// to test persistent
	leaderName := suite.cluster.WaitLeader()
	leader := suite.cluster.GetServer(leaderName)
	leader.Stop()
	suite.cluster.RunServers([]*tests.TestServer{leader})
	suite.cluster.WaitLeader()
	gresp, err = cli.GetResourceGroup(suite.ctx, groups[0].GetName())
	re.NoError(err)
	checkFunc(gresp, groups[0])

	for _, g := range groups {
		// Delete Resource Group
		dresp, err := cli.DeleteResourceGroup(suite.ctx, g.Name)
		re.NoError(err)
		re.Contains(dresp, "Success!")
	}
}

func (suite *resourceManagerClientTestSuite) TestBasicReourceGroupCURD() {
	re := suite.Require()
	cli := suite.client

	leaderName := suite.cluster.WaitLeader()
	leader := suite.cluster.GetServer(leaderName)

	testCasesSet1 := []struct {
		name           string
		mode           rmpb.GroupMode
		addSuccess     bool
		modifySuccess  bool
		expectMarshal  string
		modifySettings func(*rmpb.ResourceGroup)
	}{
		{"test1", rmpb.GroupMode_RUMode, true, true,
			`{"name":"test1","mode":1,"r_u_settings":{"rru":{"token_bucket":{"settings":{"fill_rate":10000}},"initialized":false},"wru":{"initialized":false}}}`,
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
			`{"name":"test2","mode":1,"r_u_settings":{"rru":{"token_bucket":{"settings":{"fill_rate":20000}},"initialized":false},"wru":{"initialized":false}}}`,
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
			`{"name":"test2","mode":1,"r_u_settings":{"rru":{"token_bucket":{"settings":{"fill_rate":30000}},"initialized":false},"wru":{"initialized":false}}}`,
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
		{"test3", rmpb.GroupMode_RawMode, true, false,
			`{"name":"test3","mode":2}`,
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
		{"test3", rmpb.GroupMode_RawMode, false, true,
			`{"name":"test3","mode":2,"raw_resource_settings":{"cpu":{"token_bucket":{"settings":{"fill_rate":1000000}},"initialized":false},"io_read_bandwidth":{"initialized":false},"io_write_bandwidth":{"initialized":false}}}`,
			func(gs *rmpb.ResourceGroup) {
				gs.RawResourceSettings = &rmpb.GroupRawResourceSettings{
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
		resp, err := cli.AddResourceGroup(suite.ctx, group)
		checkErr(err, tcase.addSuccess)
		if tcase.addSuccess {
			finalNum++
			re.Contains(resp, "Success!")
		}

		// Modify Resource Group
		tcase.modifySettings(group)
		mresp, err := cli.ModifyResourceGroup(suite.ctx, group)
		checkErr(err, tcase.modifySuccess)
		if tcase.modifySuccess {
			re.Contains(mresp, "Success!")
		}

		// Get Resource Group
		gresp, err := cli.GetResourceGroup(suite.ctx, tcase.name)
		re.NoError(err)
		re.Equal(tcase.name, gresp.Name)
		if tcase.modifySuccess {
			re.Equal(group, gresp)
		}

		// Last one, Check list and delete all resource groups
		if i == len(testCasesSet1)-1 {
			// List Resource Group
			lresp, err := cli.ListResourceGroups(suite.ctx)
			re.NoError(err)
			re.Equal(finalNum, len(lresp))

			for _, g := range lresp {
				// Delete Resource Group
				dresp, err := cli.DeleteResourceGroup(suite.ctx, g.Name)
				re.NoError(err)
				re.Contains(dresp, "Success!")
				_, err = cli.GetResourceGroup(suite.ctx, g.Name)
				re.EqualError(err, "rpc error: code = Unknown desc = resource group not found")
			}

			// to test the deletion of persistence
			leader.Stop()
			suite.cluster.RunServers([]*tests.TestServer{leader})
			suite.cluster.WaitLeader()

			// List Resource Group
			lresp, err = cli.ListResourceGroups(suite.ctx)
			re.NoError(err)
			re.Equal(0, len(lresp))
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
