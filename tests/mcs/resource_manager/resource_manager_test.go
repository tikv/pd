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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/stretchr/testify/suite"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/resource_group/controller"
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
	ctx        context.Context
	clean      context.CancelFunc
	cluster    *tests.TestCluster
	client     pd.Client
	initGroups []*rmpb.ResourceGroup
}

func TestResourceManagerClientTestSuite(t *testing.T) {
	suite.Run(t, new(resourceManagerClientTestSuite))
}

func (suite *resourceManagerClientTestSuite) SetupSuite() {
	var err error
	re := suite.Require()

	suite.ctx, suite.clean = context.WithCancel(context.Background())

	suite.cluster, err = tests.NewTestCluster(suite.ctx, 2)
	re.NoError(err)

	err = suite.cluster.RunInitialServers()
	re.NoError(err)

	suite.client, err = pd.NewClientWithContext(suite.ctx, suite.cluster.GetConfig().GetClientURLs(), pd.SecurityOption{})
	re.NoError(err)
	leader := suite.cluster.GetServer(suite.cluster.WaitLeader())
	suite.waitLeader(suite.client, leader.GetAddr())

	suite.initGroups = []*rmpb.ResourceGroup{
		{
			Name: "test1",
			Mode: rmpb.GroupMode_RUMode,
			RUSettings: &rmpb.GroupRequestUnitSettings{
				RU: &rmpb.TokenBucket{
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
				RU: &rmpb.TokenBucket{
					Settings: &rmpb.TokenLimitSettings{
						FillRate:   20000,
						BurstLimit: -1,
					},
					Tokens: 100000,
				},
			},
		},
	}
}

func (suite *resourceManagerClientTestSuite) waitLeader(cli pd.Client, leaderAddr string) {
	innerCli, ok := cli.(interface {
		GetLeaderAddr() string
		ScheduleCheckLeader()
	})
	suite.True(ok)
	suite.NotNil(innerCli)
	testutil.Eventually(suite.Require(), func() bool {
		innerCli.ScheduleCheckLeader()
		return innerCli.GetLeaderAddr() == leaderAddr
	})
}

func (suite *resourceManagerClientTestSuite) TearDownSuite() {
	suite.client.Close()
	suite.cluster.Destroy()
	suite.clean()
}

func (suite *resourceManagerClientTestSuite) cleanupResourceGroups() {
	cli := suite.client
	groups, err := cli.ListResourceGroups(suite.ctx)
	suite.NoError(err)
	for _, group := range groups {
		deleteResp, err := cli.DeleteResourceGroup(suite.ctx, group.GetName())
		suite.NoError(err)
		suite.Contains(deleteResp, "Success!")
	}
}

func (suite *resourceManagerClientTestSuite) resignAndWaitLeader() {
	suite.NoError(suite.cluster.ResignLeader())
	newLeader := suite.cluster.GetServer(suite.cluster.WaitLeader())
	suite.NotNil(newLeader)
	suite.waitLeader(suite.client, newLeader.GetAddr())
}

func (suite *resourceManagerClientTestSuite) TestWatchResourceGroup() {
	re := suite.Require()
	cli := suite.client
	group := &rmpb.ResourceGroup{
		Name: "test",
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate: 10000,
				},
				Tokens: 100000,
			},
		},
	}
	// Mock get revision by listing
	for i := 0; i < 3; i++ {
		group.Name += strconv.Itoa(i)
		resp, err := cli.AddResourceGroup(suite.ctx, group)
		group.Name = "test"
		re.NoError(err)
		re.Contains(resp, "Success!")
	}
	lresp, err := cli.ListResourceGroups(suite.ctx)
	re.NoError(err)
	re.Equal(len(lresp), 3)
	// Start watcher
	watchChan, err := suite.client.WatchResourceGroup(suite.ctx, int64(0))
	suite.NoError(err)
	// Mock add resource groups
	for i := 3; i < 9; i++ {
		group.Name = "test" + strconv.Itoa(i)
		resp, err := cli.AddResourceGroup(suite.ctx, group)
		re.NoError(err)
		re.Contains(resp, "Success!")
	}
	// Mock modify resource groups
	modifySettings := func(gs *rmpb.ResourceGroup) {
		gs.RUSettings = &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate: 20000,
				},
			},
		}
	}
	for i := 0; i < 9; i++ {
		group.Name = "test" + strconv.Itoa(i)
		modifySettings(group)
		resp, err := cli.ModifyResourceGroup(suite.ctx, group)
		re.NoError(err)
		re.Contains(resp, "Success!")
	}
	// Mock delete resource groups
	suite.cleanupResourceGroups()
	// Check watch result
	i := 0
	for {
		select {
		case <-time.After(time.Second):
			return
		case res := <-watchChan:
			if i < 6 {
				for _, r := range res {
					suite.Equal(uint64(10000), r.RUSettings.RU.Settings.FillRate)
					i++
				}
			} else { // after modify
				for _, r := range res {
					suite.Equal(uint64(20000), r.RUSettings.RU.Settings.FillRate)
					i++
				}
			}
		}
	}
}

const buffDuration = time.Millisecond * 300

type testRequestInfo struct {
	isWrite    bool
	writeBytes uint64
}

func (ti *testRequestInfo) IsWrite() bool {
	return ti.isWrite
}

func (ti *testRequestInfo) WriteBytes() uint64 {
	return ti.writeBytes
}

type testResponseInfo struct {
	cpuMs     uint64
	readBytes uint64
}

func (tri *testResponseInfo) ReadBytes() uint64 {
	return tri.readBytes
}

func (tri *testResponseInfo) KVCPUMs() uint64 {
	return tri.cpuMs
}

type tokenConsumptionPerSecond struct {
	rruTokensAtATime float64
	wruTokensAtATime float64
	times            int
	waitDuration     time.Duration
}

func (t tokenConsumptionPerSecond) makeReadRequest() *testRequestInfo {
	return &testRequestInfo{
		isWrite:    false,
		writeBytes: 0,
	}
}

func (t tokenConsumptionPerSecond) makeWriteRequest() *testRequestInfo {
	return &testRequestInfo{
		isWrite:    true,
		writeBytes: uint64(t.wruTokensAtATime - 1),
	}
}

func (t tokenConsumptionPerSecond) makeReadResponse() *testResponseInfo {
	return &testResponseInfo{
		readBytes: uint64((t.rruTokensAtATime - 1) / 2),
		cpuMs:     uint64(t.rruTokensAtATime / 2),
	}
}

func (t tokenConsumptionPerSecond) makeWriteResponse() *testResponseInfo {
	return &testResponseInfo{
		readBytes: 0,
		cpuMs:     0,
	}
}

func (suite *resourceManagerClientTestSuite) TestResourceGroupController() {
	re := suite.Require()
	cli := suite.client

	for _, group := range suite.initGroups {
		resp, err := cli.AddResourceGroup(suite.ctx, group)
		re.NoError(err)
		re.Contains(resp, "Success!")
	}

	cfg := &controller.RequestUnitConfig{
		ReadBaseCost:     1,
		ReadCostPerByte:  1,
		WriteBaseCost:    1,
		WriteCostPerByte: 1,
		CPUMsCost:        1,
	}

	controller, _ := controller.NewResourceGroupController(1, cli, cfg)
	controller.Start(suite.ctx)

	testCases := []struct {
		resourceGroupName string
		tcs               []tokenConsumptionPerSecond
		len               int
	}{
		{
			resourceGroupName: suite.initGroups[0].Name,
			len:               8,
			tcs: []tokenConsumptionPerSecond{
				{rruTokensAtATime: 50, wruTokensAtATime: 20, times: 100, waitDuration: 0},
				{rruTokensAtATime: 50, wruTokensAtATime: 100, times: 100, waitDuration: 0},
				{rruTokensAtATime: 50, wruTokensAtATime: 100, times: 100, waitDuration: 0},
				{rruTokensAtATime: 20, wruTokensAtATime: 40, times: 250, waitDuration: 0},
				{rruTokensAtATime: 25, wruTokensAtATime: 50, times: 200, waitDuration: 0},
				{rruTokensAtATime: 30, wruTokensAtATime: 60, times: 165, waitDuration: 0},
				{rruTokensAtATime: 40, wruTokensAtATime: 80, times: 125, waitDuration: 0},
				{rruTokensAtATime: 50, wruTokensAtATime: 100, times: 100, waitDuration: 0},
			},
		},
	}
	tricker := time.NewTicker(time.Second)
	defer tricker.Stop()
	i := 0
	for {
		v := false
		<-tricker.C
		for _, cas := range testCases {
			if i >= cas.len {
				continue
			}
			v = true
			sum := time.Duration(0)
			for j := 0; j < cas.tcs[i].times; j++ {
				rreq := cas.tcs[i].makeReadRequest()
				wreq := cas.tcs[i].makeWriteRequest()
				rres := cas.tcs[i].makeReadResponse()
				wres := cas.tcs[i].makeWriteResponse()
				startTime := time.Now()
				controller.OnRequestWait(suite.ctx, cas.resourceGroupName, rreq)
				controller.OnRequestWait(suite.ctx, cas.resourceGroupName, wreq)
				sum += time.Since(startTime)
				controller.OnResponse(suite.ctx, cas.resourceGroupName, rreq, rres)
				controller.OnResponse(suite.ctx, cas.resourceGroupName, wreq, wres)
				time.Sleep(1000 * time.Microsecond)
			}
			re.LessOrEqual(sum, buffDuration+cas.tcs[i].waitDuration)
		}
		i++
		if !v {
			break
		}
	}
	suite.cleanupResourceGroups()
	controller.Stop()
}

func (suite *resourceManagerClientTestSuite) TestAcquireTokenBucket() {
	re := suite.Require()
	cli := suite.client

	groups := make([]*rmpb.ResourceGroup, 0)
	groups = append(groups, suite.initGroups...)
	for _, group := range groups {
		resp, err := cli.AddResourceGroup(suite.ctx, group)
		re.NoError(err)
		re.Contains(resp, "Success!")
	}
	reqs := &rmpb.TokenBucketsRequest{
		Requests:              make([]*rmpb.TokenBucketRequest, 0),
		TargetRequestPeriodMs: uint64(time.Second * 10 / time.Millisecond),
	}
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/mcs/resource_manager/server/fastPersist", `return(true)`))
	suite.resignAndWaitLeader()
	groups = append(groups, &rmpb.ResourceGroup{Name: "test3"})
	for i := 0; i < 3; i++ {
		for _, group := range groups {
			requests := make([]*rmpb.RequestUnitItem, 0)
			requests = append(requests, &rmpb.RequestUnitItem{
				Type:  rmpb.RequestUnitType_RU,
				Value: 30000,
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
			re.Equal(resp.GrantedRUTokens[0].GrantedTokens.Tokens, float64(30000.))
			if resp.ResourceGroupName == "test2" {
				re.Equal(int64(-1), resp.GrantedRUTokens[0].GrantedTokens.GetSettings().GetBurstLimit())
			}
		}
		gresp, err := cli.GetResourceGroup(suite.ctx, groups[0].GetName())
		re.NoError(err)
		re.Less(gresp.RUSettings.RU.Tokens, groups[0].RUSettings.RU.Tokens)

		checkFunc := func(g1 *rmpb.ResourceGroup, g2 *rmpb.ResourceGroup) {
			re.Equal(g1.GetName(), g2.GetName())
			re.Equal(g1.GetMode(), g2.GetMode())
			re.Equal(g1.GetRUSettings().RU.Settings.FillRate, g2.GetRUSettings().RU.Settings.FillRate)
			// now we don't persistent tokens in running state, so tokens is original.
			re.Less(g1.GetRUSettings().RU.Tokens, g2.GetRUSettings().RU.Tokens)
			re.NoError(err)
		}
		time.Sleep(250 * time.Millisecond)
		// to test persistent
		suite.resignAndWaitLeader()
		gresp, err = cli.GetResourceGroup(suite.ctx, groups[0].GetName())
		re.NoError(err)
		checkFunc(gresp, groups[0])
	}
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/mcs/resource_manager/server/fastPersist"))
	suite.cleanupResourceGroups()
}

func (suite *resourceManagerClientTestSuite) TestBasicResourceGroupCURD() {
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
			`{"name":"test1","mode":1,"r_u_settings":{"ru":{"settings":{"fill_rate":10000},"state":{"initialized":false}}}}`,
			func(gs *rmpb.ResourceGroup) {
				gs.RUSettings = &rmpb.GroupRequestUnitSettings{
					RU: &rmpb.TokenBucket{
						Settings: &rmpb.TokenLimitSettings{
							FillRate: 10000,
						},
					},
				}
			},
		},

		{"test2", rmpb.GroupMode_RUMode, true, true,
			`{"name":"test2","mode":1,"r_u_settings":{"ru":{"settings":{"fill_rate":20000},"state":{"initialized":false}}}}`,
			func(gs *rmpb.ResourceGroup) {
				gs.RUSettings = &rmpb.GroupRequestUnitSettings{
					RU: &rmpb.TokenBucket{
						Settings: &rmpb.TokenLimitSettings{
							FillRate: 20000,
						},
					},
				}
			},
		},
		{"test2", rmpb.GroupMode_RUMode, false, true,
			`{"name":"test2","mode":1,"r_u_settings":{"ru":{"settings":{"fill_rate":30000,"burst_limit":-1},"state":{"initialized":false}}}}`,
			func(gs *rmpb.ResourceGroup) {
				gs.RUSettings = &rmpb.GroupRequestUnitSettings{
					RU: &rmpb.TokenBucket{
						Settings: &rmpb.TokenLimitSettings{
							FillRate:   30000,
							BurstLimit: -1,
						},
					},
				}
			},
		},
		{"test3", rmpb.GroupMode_RawMode, true, false,
			`{"name":"test3","mode":2}`,
			func(gs *rmpb.ResourceGroup) {
				gs.RUSettings = &rmpb.GroupRequestUnitSettings{
					RU: &rmpb.TokenBucket{
						Settings: &rmpb.TokenLimitSettings{
							FillRate: 10000,
						},
					},
				}
			},
		},
		{"test3", rmpb.GroupMode_RawMode, false, true,
			`{"name":"test3","mode":2,"raw_resource_settings":{"cpu":{"settings":{"fill_rate":1000000},"state":{"initialized":false}},"io_read_bandwidth":{"state":{"initialized":false}},"io_write_bandwidth":{"state":{"initialized":false}}}}`,
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
			// List Resource Groups
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
			suite.resignAndWaitLeader()
			leader = suite.cluster.GetServer(suite.cluster.GetLeader())
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

func (suite *resourceManagerClientTestSuite) TestResourceManagerClientFailover() {
	re := suite.Require()
	cli := suite.client

	group := &rmpb.ResourceGroup{
		Name: "test3",
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate: 10000,
				},
				Tokens: 100000,
			},
		},
	}
	addResp, err := cli.AddResourceGroup(suite.ctx, group)
	re.NoError(err)
	re.Contains(addResp, "Success!")
	getResp, err := cli.GetResourceGroup(suite.ctx, group.GetName())
	re.NoError(err)
	re.NotNil(getResp)
	re.Equal(*group, *getResp)

	// Change the leader after each time we modify the resource group.
	for i := 0; i < 4; i++ {
		group.RUSettings.RU.Settings.FillRate += uint64(i)
		modifyResp, err := cli.ModifyResourceGroup(suite.ctx, group)
		re.NoError(err)
		re.Contains(modifyResp, "Success!")
		suite.resignAndWaitLeader()
		getResp, err = cli.GetResourceGroup(suite.ctx, group.GetName())
		re.NoError(err)
		re.NotNil(getResp)
		re.Equal(group.RUSettings.RU.Settings.FillRate, getResp.RUSettings.RU.Settings.FillRate)
	}

	// Cleanup the resource group.
	suite.cleanupResourceGroups()
}
