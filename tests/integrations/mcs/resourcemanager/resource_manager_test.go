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

package resourcemanager_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
	"go.uber.org/zap"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/log"

	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/constants"
	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/caller"
	"github.com/tikv/pd/client/resource_group/controller"
	sd "github.com/tikv/pd/client/servicediscovery"
	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/mcs/resourcemanager/server"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	srvconfig "github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/integrations/mcs/utils"
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

	mode resourceManagerDeployMode

	fastUpdateServiceModeEnabled bool

	rmCleanup  func()
	tsoCleanup func()
}

func (suite *resourceManagerClientTestSuite) setupPDClient(re *require.Assertions) pd.Client {
	cli, err := pd.NewClientWithContext(suite.ctx,
		caller.TestComponent,
		suite.cluster.GetConfig().GetClientURLs(),
		pd.SecurityOption{},
		opt.WithTSOServerProxyOption(true), // Avoid tso affects test
	)
	re.NoError(err)
	return cli
}

func (suite *resourceManagerClientTestSuite) setupKeyspaceClient(re *require.Assertions, keyspaceID uint32) pd.Client {
	cli := utils.SetupClientWithKeyspaceID(
		suite.ctx,
		re,
		keyspaceID,
		suite.cluster.GetConfig().GetClientURLs(),
		opt.WithTSOServerProxyOption(true), // Avoid tso affects test
	)
	// In standalone RM mode, ensure the newly created client has already discovered
	// and connected to the RM microservice; otherwise RM RPCs may temporarily fall
	// back to PD and split state across backends.
	if suite.mode == resourceManagerStandaloneWithClientDiscovery {
		waitResourceManagerServiceURL(re, cli, true)
	}
	return cli
}

type resourceManagerDeployMode int

const (
	resourceManagerProvidedByPD resourceManagerDeployMode = iota
	resourceManagerStandaloneWithClientDiscovery
)

func TestResourceManagerClientTestSuite(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode resourceManagerDeployMode
	}{
		{name: "pd-resource-manager", mode: resourceManagerProvidedByPD},
		{name: "standalone-resource-manager-with-client-discovery", mode: resourceManagerStandaloneWithClientDiscovery},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			suite.Run(t, &resourceManagerClientTestSuite{mode: tc.mode})
		})
	}
}

func (suite *resourceManagerClientTestSuite) startMicroservicesForDiscovery(re *require.Assertions) {
	leader := suite.cluster.GetServer(suite.cluster.WaitLeader())
	re.NotNil(leader)
	backendEndpoints := leader.GetAddr()

	// Start Resource Manager microservice and wait it is serving.
	rmServer, cleanup := tests.StartSingleResourceManagerTestServer(suite.ctx, re, backendEndpoints, tempurl.Alloc())
	_ = tests.WaitForPrimaryServing(re, map[string]bs.Server{rmServer.GetAddr(): rmServer})
	suite.rmCleanup = cleanup

	// Start TSO microservice and wait it is serving.
	tsoServer, tsoCleanup := tests.StartSingleTSOTestServer(suite.ctx, re, backendEndpoints, tempurl.Alloc())
	_ = tests.WaitForPrimaryServing(re, map[string]bs.Server{tsoServer.GetAddr(): tsoServer})
	suite.tsoCleanup = tsoCleanup
}

func (suite *resourceManagerClientTestSuite) stopMicroservicesForDiscovery() {
	if suite.rmCleanup != nil {
		suite.rmCleanup()
		suite.rmCleanup = nil
	}
	if suite.tsoCleanup != nil {
		suite.tsoCleanup()
		suite.tsoCleanup = nil
	}
}

func (suite *resourceManagerClientTestSuite) SetupSuite() {
	var err error
	re := suite.Require()

	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/mcs/resourcemanager/server/enableDegradedModeAndTraceLog", `return(true)`))
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/member/skipCampaignLeaderCheck", "return(true)"))
	if suite.mode == resourceManagerStandaloneWithClientDiscovery {
		re.NoError(failpoint.Enable("github.com/tikv/pd/client/servicediscovery/fastUpdateServiceMode", `return(true)`))
		suite.fastUpdateServiceModeEnabled = true
	}

	suite.ctx, suite.clean = context.WithCancel(context.Background())

	if suite.mode == resourceManagerStandaloneWithClientDiscovery {
		suite.cluster, err = tests.NewTestClusterWithKeyspaceGroup(suite.ctx, 2, func(conf *srvconfig.Config, _ string) {
			conf.Microservice.EnableResourceManagerFallback = false
		})
	} else {
		suite.cluster, err = tests.NewTestCluster(suite.ctx, 2)
	}
	re.NoError(err)

	err = suite.cluster.RunInitialServers()
	re.NoError(err)

	leader := suite.cluster.GetServer(suite.cluster.WaitLeader())
	re.NotNil(leader)

	if suite.mode == resourceManagerStandaloneWithClientDiscovery {
		// Bootstrap the cluster so that the microservices (now or later) can work normally.
		re.NoError(leader.BootstrapCluster())
	}

	if suite.mode == resourceManagerStandaloneWithClientDiscovery {
		suite.startMicroservicesForDiscovery(re)
		suite.client = suite.setupPDClient(re)
	} else {
		suite.client = suite.setupPDClient(re)
	}
	waitLeaderServingClient(re, suite.client, leader.GetAddr())
	if suite.mode == resourceManagerStandaloneWithClientDiscovery {
		// Ensure RM service discovery has picked up the standalone endpoint before running tests.
		waitResourceManagerServiceURL(re, suite.client, true)
	}

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
		{
			Name: "test3",
			Mode: rmpb.GroupMode_RUMode,
			RUSettings: &rmpb.GroupRequestUnitSettings{
				RU: &rmpb.TokenBucket{
					Settings: &rmpb.TokenLimitSettings{
						FillRate:   100,
						BurstLimit: 5000000,
					},
					Tokens: 5000000,
				},
			},
		},
		{
			Name: "test4",
			Mode: rmpb.GroupMode_RUMode,
			RUSettings: &rmpb.GroupRequestUnitSettings{
				RU: &rmpb.TokenBucket{
					Settings: &rmpb.TokenLimitSettings{
						FillRate:   1000,
						BurstLimit: 5000000,
					},
					Tokens: 5000000,
				},
			},
		},
	}
}

func waitLeaderServingClient(re *require.Assertions, cli pd.Client, leaderAddr string) {
	innerCli, ok := cli.(interface{ GetServiceDiscovery() sd.ServiceDiscovery })
	re.True(ok)
	re.NotNil(innerCli)
	testutil.Eventually(re, func() bool {
		innerCli.GetServiceDiscovery().ScheduleCheckMemberChanged()
		return innerCli.GetServiceDiscovery().GetServingURL() == leaderAddr
	})
}

func waitResourceManagerServiceURL(re *require.Assertions, cli pd.Client, wantNonEmpty bool) {
	innerCli, ok := cli.(interface{ GetResourceManagerServiceURL() string })
	re.True(ok)
	testutil.Eventually(re, func() bool {
		url := innerCli.GetResourceManagerServiceURL()
		if wantNonEmpty {
			return url != ""
		}
		return url == ""
	})
}

func TestSwitchModeDuringWorkload(t *testing.T) {
	for _, tc := range []struct {
		name      string
		startMode resourceManagerDeployMode
	}{
		{name: "pd-to-standalone", startMode: resourceManagerProvidedByPD},
		{name: "standalone-to-pd", startMode: resourceManagerStandaloneWithClientDiscovery},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/mcs/resourcemanager/server/enableDegradedModeAndTraceLog", `return(true)`))
			t.Cleanup(func() {
				re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/mcs/resourcemanager/server/enableDegradedModeAndTraceLog"))
			})
			re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/member/skipCampaignLeaderCheck", "return(true)"))
			t.Cleanup(func() {
				re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/member/skipCampaignLeaderCheck"))
			})
			// This test switches in/out of standalone mode, so always enable faster SD updates.
			re.NoError(failpoint.Enable("github.com/tikv/pd/client/servicediscovery/fastUpdateServiceMode", `return(true)`))
			t.Cleanup(func() {
				re.NoError(failpoint.Disable("github.com/tikv/pd/client/servicediscovery/fastUpdateServiceMode"))
			})

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			cluster, err := tests.NewTestClusterWithKeyspaceGroup(ctx, 2)
			re.NoError(err)
			defer cluster.Destroy()

			re.NoError(cluster.RunInitialServers())
			leader := cluster.GetServer(cluster.WaitLeader())
			re.NotNil(leader)
			re.NoError(leader.BootstrapCluster())

			startMicroservices := func() (rmCleanup func(), tsoCleanup func()) {
				backendEndpoints := leader.GetAddr()
				rmServer, cleanup := tests.StartSingleResourceManagerTestServer(ctx, re, backendEndpoints, tempurl.Alloc())
				_ = tests.WaitForPrimaryServing(re, map[string]bs.Server{rmServer.GetAddr(): rmServer})
				rmCleanup = cleanup

				tsoServer, cleanupTSO := tests.StartSingleTSOTestServer(ctx, re, backendEndpoints, tempurl.Alloc())
				_ = tests.WaitForPrimaryServing(re, map[string]bs.Server{tsoServer.GetAddr(): tsoServer})
				tsoCleanup = cleanupTSO
				return rmCleanup, tsoCleanup
			}

			var rmCleanup func()
			var tsoCleanup func()
			stopMicroservices := func() {
				if rmCleanup != nil {
					rmCleanup()
					rmCleanup = nil
				}
				if tsoCleanup != nil {
					tsoCleanup()
					tsoCleanup = nil
				}
			}
			defer stopMicroservices()

			if tc.startMode == resourceManagerStandaloneWithClientDiscovery {
				rmCleanup, tsoCleanup = startMicroservices()
			}

			cli, err := pd.NewClientWithContext(ctx,
				caller.TestComponent,
				cluster.GetConfig().GetClientURLs(),
				pd.SecurityOption{},
				opt.WithTSOServerProxyOption(true),
			)
			re.NoError(err)
			defer cli.Close()

			waitLeaderServingClient(re, cli, leader.GetAddr())
			if tc.startMode == resourceManagerStandaloneWithClientDiscovery {
				waitResourceManagerServiceURL(re, cli, true)
			} else {
				waitResourceManagerServiceURL(re, cli, false)
			}

			group := &rmpb.ResourceGroup{
				Name: "switch_workload",
				Mode: rmpb.GroupMode_RUMode,
				RUSettings: &rmpb.GroupRequestUnitSettings{
					RU: &rmpb.TokenBucket{
						Settings: &rmpb.TokenLimitSettings{FillRate: 10000},
						Tokens:   100000,
					},
				},
			}
			resp, err := cli.AddResourceGroup(ctx, group)
			re.NoError(err)
			re.Contains(resp, "Success!")

			cfg := &controller.RequestUnitConfig{
				ReadBaseCost:     1,
				ReadCostPerByte:  1,
				WriteBaseCost:    1,
				WriteCostPerByte: 1,
				CPUMsCost:        1,
			}
			rgController, err := controller.NewResourceGroupController(
				ctx,
				1,
				cli,
				cfg,
				constants.NullKeyspaceID,
				controller.EnableSingleGroupByKeyspace(),
			)
			re.NoError(err)
			rgController.Start(ctx)
			defer func() {
				re.NoError(rgController.Stop())
			}()

			workCtx, workCancel := context.WithCancel(ctx)
			defer workCancel()

			var switched atomic.Bool
			var okBefore, okAfter int64
			var errBefore, errAfter int64

			go func() {
				ticker := time.NewTicker(10 * time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-workCtx.Done():
						return
					case <-ticker.C:
					}
					req := controller.NewTestRequestInfo(false, 0, 0, controller.AccessUnknown)
					res := controller.NewTestResponseInfo(0, 0, true)
					_, _, _, _, err := rgController.OnRequestWait(workCtx, group.Name, req)
					if err != nil {
						if switched.Load() {
							atomic.AddInt64(&errAfter, 1)
						} else {
							atomic.AddInt64(&errBefore, 1)
						}
						continue
					}
					if switched.Load() {
						atomic.AddInt64(&okAfter, 1)
					} else {
						atomic.AddInt64(&okBefore, 1)
					}
					_, _ = rgController.OnResponse(group.Name, req, res)
				}
			}()

			testutil.Eventually(re, func() bool {
				return atomic.LoadInt64(&okBefore) >= 10
			}, testutil.WithTickInterval(20*time.Millisecond))

			log.Info("switching mode during workload",
				zap.Int("fromMode", int(tc.startMode)),
				zap.Int64("okBefore", atomic.LoadInt64(&okBefore)),
				zap.Int64("errBefore", atomic.LoadInt64(&errBefore)),
			)

			if tc.startMode == resourceManagerProvidedByPD {
				rmCleanup, tsoCleanup = startMicroservices()
				waitResourceManagerServiceURL(re, cli, true)
				testutil.Eventually(re, func() bool {
					ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
					defer cancel()
					_, err := cli.ListResourceGroups(ctx)
					return err == nil
				})
			} else {
				stopMicroservices()
				testutil.Eventually(re, func() bool {
					ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
					defer cancel()
					_, err := cli.ListResourceGroups(ctx)
					return err == nil
				})
				waitResourceManagerServiceURL(re, cli, false)
			}
			leader = cluster.GetServer(cluster.WaitLeader())
			re.NotNil(leader)
			waitLeaderServingClient(re, cli, leader.GetAddr())

			switched.Store(true)
			testutil.Eventually(re, func() bool {
				return atomic.LoadInt64(&okAfter) >= 10
			}, testutil.WithTickInterval(20*time.Millisecond))

			log.Info("switch during workload finished",
				zap.Int("startMode", int(tc.startMode)),
				zap.Int64("okBefore", atomic.LoadInt64(&okBefore)),
				zap.Int64("errBefore", atomic.LoadInt64(&errBefore)),
				zap.Int64("okAfter", atomic.LoadInt64(&okAfter)),
				zap.Int64("errAfter", atomic.LoadInt64(&errAfter)),
			)
		})
	}
}

func (suite *resourceManagerClientTestSuite) TearDownSuite() {
	re := suite.Require()
	suite.stopMicroservicesForDiscovery()
	suite.client.Close()
	suite.cluster.Destroy()
	suite.clean()
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/mcs/resourcemanager/server/enableDegradedModeAndTraceLog"))
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/member/skipCampaignLeaderCheck"))
	if suite.fastUpdateServiceModeEnabled {
		re.NoError(failpoint.Disable("github.com/tikv/pd/client/servicediscovery/fastUpdateServiceMode"))
	}
}

func (suite *resourceManagerClientTestSuite) TearDownTest() {
	re := suite.Require()
	suite.cleanupResourceGroups(re)
}

func (suite *resourceManagerClientTestSuite) cleanupResourceGroups(re *require.Assertions) {
	cli := suite.client
	groups, err := cli.ListResourceGroups(suite.ctx)
	re.NoError(err)
	for _, group := range groups {
		deleteResp, err := cli.DeleteResourceGroup(suite.ctx, group.GetName())
		if group.Name == server.DefaultResourceGroupName {
			re.Contains(err.Error(), "cannot delete reserved group")
			continue
		}
		re.NoError(err)
		re.Contains(deleteResp, "Success!")
	}
}

func (suite *resourceManagerClientTestSuite) resignAndWaitLeader(re *require.Assertions) {
	re.NoError(suite.cluster.ResignLeader())
	newLeader := suite.cluster.GetServer(suite.cluster.WaitLeader())
	re.NotNil(newLeader)
	waitLeaderServingClient(re, suite.client, newLeader.GetAddr())
}

func (suite *resourceManagerClientTestSuite) TestWatchResourceGroup() {
	re := suite.Require()
	cli := suite.client
	groupNamePrefix := "watch_test"
	group := &rmpb.ResourceGroup{
		Name: groupNamePrefix,
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate: 10000,
				},
			},
		},
	}
	controller, err := controller.NewResourceGroupController(suite.ctx, 1, cli, nil, constants.NullKeyspaceID)
	re.NoError(err)
	controller.Start(suite.ctx)
	defer func() {
		err := controller.Stop()
		re.NoError(err)
	}()

	// Mock add resource groups
	var meta *rmpb.ResourceGroup
	groupsNum := 10
	for i := range groupsNum {
		group.Name = groupNamePrefix + strconv.Itoa(i)
		resp, err := cli.AddResourceGroup(suite.ctx, group)
		re.NoError(err)
		re.Contains(resp, "Success!")

		// Make sure the resource group active
		meta, err = controller.GetResourceGroup(group.Name)
		re.NotNil(meta)
		re.NoError(err)
		meta = controller.GetActiveResourceGroup(group.Name)
		re.NotNil(meta)
	}
	// Mock modify resource groups
	modifySettings := func(gs *rmpb.ResourceGroup, fillRate uint64) {
		gs.RUSettings = &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate: fillRate,
				},
			},
		}
	}
	for i := range groupsNum {
		group.Name = groupNamePrefix + strconv.Itoa(i)
		modifySettings(group, 20000)
		resp, err := cli.ModifyResourceGroup(suite.ctx, group)
		re.NoError(err)
		re.Contains(resp, "Success!")
	}
	for i := range groupsNum {
		testutil.Eventually(re, func() bool {
			name := groupNamePrefix + strconv.Itoa(i)
			meta = controller.GetActiveResourceGroup(name)
			if meta != nil {
				return meta.RUSettings.RU.Settings.FillRate == uint64(20000)
			}
			return false
		}, testutil.WithTickInterval(50*time.Millisecond))
	}

	// Mock reset watch stream
	re.NoError(failpoint.Enable("github.com/tikv/pd/client/resource_group/controller/watchStreamError", "return(true)"))
	group.Name = groupNamePrefix + strconv.Itoa(groupsNum)
	resp, err := cli.AddResourceGroup(suite.ctx, group)
	re.NoError(err)
	re.Contains(resp, "Success!")
	// Make sure the resource group active
	meta, err = controller.GetResourceGroup(group.Name)
	re.NotNil(meta)
	re.NoError(err)
	modifySettings(group, 30000)
	resp, err = cli.ModifyResourceGroup(suite.ctx, group)
	re.NoError(err)
	re.Contains(resp, "Success!")
	testutil.Eventually(re, func() bool {
		meta = controller.GetActiveResourceGroup(group.Name)
		return meta.RUSettings.RU.Settings.FillRate == uint64(30000)
	}, testutil.WithTickInterval(100*time.Millisecond))
	re.NoError(failpoint.Disable("github.com/tikv/pd/client/resource_group/controller/watchStreamError"))

	// Mock delete resource groups
	suite.cleanupResourceGroups(re)
	for i := range groupsNum {
		testutil.Eventually(re, func() bool {
			name := groupNamePrefix + strconv.Itoa(i)
			meta = controller.GetActiveResourceGroup(name)
			// The deleted resource group may not be immediately removed from the controller.
			return meta == nil || meta.Name == server.DefaultResourceGroupName
		}, testutil.WithTickInterval(50*time.Millisecond))
	}
}

func (suite *resourceManagerClientTestSuite) TestWatchWithSingleGroupByKeyspace() {
	re := suite.Require()
	cli := suite.client

	// We need to disable watch stream for `isSingleGroupByKeyspace`.
	re.NoError(failpoint.Enable("github.com/tikv/pd/client/resource_group/controller/disableWatch", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/client/resource_group/controller/disableWatch"))
	}()
	// Distinguish the controller with and without enabling `isSingleGroupByKeyspace`.
	controllerKeySpace, err := controller.NewResourceGroupController(suite.ctx, 1, cli, nil, constants.NullKeyspaceID, controller.EnableSingleGroupByKeyspace())
	re.NoError(err)
	controller, err := controller.NewResourceGroupController(suite.ctx, 2, cli, nil, constants.NullKeyspaceID)
	re.NoError(err)
	controller.Start(suite.ctx)
	controllerKeySpace.Start(suite.ctx)
	defer func() {
		err := controller.Stop()
		re.NoError(err)
		err = controllerKeySpace.Stop()
		re.NoError(err)
	}()

	// Mock add resource group.
	group := &rmpb.ResourceGroup{
		Name: "keyspace_test",
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
	resp, err := cli.AddResourceGroup(suite.ctx, group)
	re.NoError(err)
	re.Contains(resp, "Success!")

	tcs := tokenConsumptionPerSecond{rruTokensAtATime: 100}
	_, _, _, _, err = controller.OnRequestWait(suite.ctx, group.Name, tcs.makeReadRequest())
	re.NoError(err)
	meta := controller.GetActiveResourceGroup(group.Name)
	re.NotNil(meta)
	re.Equal(meta.RUSettings.RU, group.RUSettings.RU)

	_, _, _, _, err = controllerKeySpace.OnRequestWait(suite.ctx, group.Name, tcs.makeReadRequest())
	re.NoError(err)
	metaKeySpace := controllerKeySpace.GetActiveResourceGroup(group.Name)
	re.Equal(metaKeySpace.RUSettings.RU, group.RUSettings.RU)

	// Mock modify resource groups
	modifySettings := func(gs *rmpb.ResourceGroup, fillRate uint64) {
		gs.RUSettings = &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate: fillRate,
				},
			},
		}
	}
	modifySettings(group, 20000)
	resp, err = cli.ModifyResourceGroup(suite.ctx, group)
	re.NoError(err)
	re.Contains(resp, "Success!")

	testutil.Eventually(re, func() bool {
		meta = controller.GetActiveResourceGroup(group.Name)
		return meta.RUSettings.RU.Settings.FillRate == uint64(20000)
	}, testutil.WithTickInterval(100*time.Millisecond))
	metaKeySpace = controllerKeySpace.GetActiveResourceGroup(group.Name)
	re.Equal(uint64(10000), metaKeySpace.RUSettings.RU.Settings.FillRate)
}

const buffDuration = time.Millisecond * 300

type tokenConsumptionPerSecond struct {
	rruTokensAtATime float64
	wruTokensAtATime float64
	times            int
	waitDuration     time.Duration
}

func (tokenConsumptionPerSecond) makeReadRequest() *controller.TestRequestInfo {
	return controller.NewTestRequestInfo(false, 0, 0, controller.AccessUnknown)
}

func (t tokenConsumptionPerSecond) makeWriteRequest() *controller.TestRequestInfo {
	return controller.NewTestRequestInfo(true, uint64(t.wruTokensAtATime-1), 0, controller.AccessUnknown)
}

func (t tokenConsumptionPerSecond) makeReadResponse() *controller.TestResponseInfo {
	return controller.NewTestResponseInfo(
		uint64((t.rruTokensAtATime-1)/2),
		time.Duration(t.rruTokensAtATime/2)*time.Millisecond,
		false,
	)
}

func (tokenConsumptionPerSecond) makeWriteResponse() *controller.TestResponseInfo {
	return controller.NewTestResponseInfo(
		0,
		time.Duration(0),
		true,
	)
}

func (suite *resourceManagerClientTestSuite) TestResourceGroupController() {
	re := suite.Require()
	cli := suite.client

	rg := &rmpb.ResourceGroup{
		Name: "controller_test",
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
	resp, err := cli.AddResourceGroup(suite.ctx, rg)
	re.NoError(err)
	re.Contains(resp, "Success!")

	cfg := &controller.RequestUnitConfig{
		ReadBaseCost:     1,
		ReadCostPerByte:  1,
		WriteBaseCost:    1,
		WriteCostPerByte: 1,
		CPUMsCost:        1,
	}

	rgsController, err := controller.NewResourceGroupController(suite.ctx, 1, cli, cfg, constants.NullKeyspaceID)
	re.NoError(err)
	rgsController.Start(suite.ctx)
	defer func() {
		err = rgsController.Stop()
		re.NoError(err)
	}()

	testCases := []struct {
		resourceGroupName string
		tcs               []tokenConsumptionPerSecond
	}{
		{
			resourceGroupName: rg.Name,
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
			if i >= len(cas.tcs) {
				continue
			}
			v = true
			sum := time.Duration(0)
			for range cas.tcs[i].times {
				rreq := cas.tcs[i].makeReadRequest()
				wreq := cas.tcs[i].makeWriteRequest()
				rres := cas.tcs[i].makeReadResponse()
				wres := cas.tcs[i].makeWriteResponse()
				startTime := time.Now()
				_, _, _, _, err := rgsController.OnRequestWait(suite.ctx, cas.resourceGroupName, rreq)
				re.NoError(err)
				_, _, _, _, err = rgsController.OnRequestWait(suite.ctx, cas.resourceGroupName, wreq)
				re.NoError(err)
				sum += time.Since(startTime)
				_, err = rgsController.OnResponse(cas.resourceGroupName, rreq, rres)
				re.NoError(err)
				_, err = rgsController.OnResponse(cas.resourceGroupName, wreq, wres)
				re.NoError(err)
				time.Sleep(time.Millisecond)
			}
			log.Info("finished test case", zap.Int("index", i), zap.Duration("sum", sum), zap.Duration("waitDuration", cas.tcs[i].waitDuration))
			re.LessOrEqual(sum, buffDuration+cas.tcs[i].waitDuration)
		}
		i++
		if !v {
			break
		}
	}
	re.NoError(failpoint.Enable("github.com/tikv/pd/client/resource_group/controller/triggerUpdate", "return(true)"))
	tcs := tokenConsumptionPerSecond{rruTokensAtATime: 1, wruTokensAtATime: 900000000, times: 1, waitDuration: 0}
	wreq := tcs.makeWriteRequest()
	_, _, _, _, err = rgsController.OnRequestWait(suite.ctx, rg.Name, wreq)
	re.Error(err)
	re.NoError(failpoint.Disable("github.com/tikv/pd/client/resource_group/controller/triggerUpdate"))

	group, err := rgsController.GetResourceGroup(rg.Name)
	re.NoError(err)
	re.Equal(rg, group)
	// Delete the resource group and make sure it is tombstone.
	resp, err = cli.DeleteResourceGroup(suite.ctx, rg.Name)
	re.NoError(err)
	re.Contains(resp, "Success!")
	// Make sure the resource group is watched by the controller and marked as tombstone.
	expectedErr := controller.NewResourceGroupNotExistErr(rg.Name)
	testutil.Eventually(re, func() bool {
		gc, err := rgsController.GetResourceGroup(rg.Name)
		return err.Error() == expectedErr.Error() && gc == nil
	}, testutil.WithTickInterval(50*time.Millisecond))
	// Add the resource group again.
	resp, err = cli.AddResourceGroup(suite.ctx, rg)
	re.NoError(err)
	re.Contains(resp, "Success!")
	// Make sure the resource group can be get by the controller again.
	testutil.Eventually(re, func() bool {
		gc, err := rgsController.GetResourceGroup(rg.Name)
		if err != nil {
			re.EqualError(err, expectedErr.Error())
		}
		return gc.GetName() == rg.Name
	}, testutil.WithTickInterval(50*time.Millisecond))
}

// TestSwitchBurst is used to test https://github.com/tikv/pd/issues/6209
func (suite *resourceManagerClientTestSuite) TestSwitchBurst() {
	suite.T().Skip("skip this test because it is not stable")
	re := suite.Require()
	cli := suite.client
	re.NoError(failpoint.Enable("github.com/tikv/pd/client/resource_group/controller/acceleratedReportingPeriod", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/client/resource_group/controller/acceleratedReportingPeriod"))
	}()

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
	controller, err := controller.NewResourceGroupController(suite.ctx, 1, cli, cfg, constants.NullKeyspaceID, controller.EnableSingleGroupByKeyspace())
	re.NoError(err)
	controller.Start(suite.ctx)
	resourceGroupName := suite.initGroups[1].Name
	tcs := tokenConsumptionPerSecond{rruTokensAtATime: 1, wruTokensAtATime: 2, times: 100, waitDuration: 0}
	for range tcs.times {
		rreq := tcs.makeReadRequest()
		wreq := tcs.makeWriteRequest()
		rres := tcs.makeReadResponse()
		wres := tcs.makeWriteResponse()
		_, _, _, _, err := controller.OnRequestWait(suite.ctx, resourceGroupName, rreq)
		re.NoError(err)
		_, _, _, _, err = controller.OnRequestWait(suite.ctx, resourceGroupName, wreq)
		re.NoError(err)
		_, err = controller.OnResponse(resourceGroupName, rreq, rres)
		re.NoError(err)
		_, err = controller.OnResponse(resourceGroupName, wreq, wres)
		re.NoError(err)
	}
	time.Sleep(2 * time.Second)
	_, err = cli.ModifyResourceGroup(suite.ctx, &rmpb.ResourceGroup{
		Name: "test2",
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate:   20000,
					BurstLimit: 20000,
				},
			},
		},
	})
	re.NoError(err)
	time.Sleep(100 * time.Millisecond)
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
			for range cas.tcs[i].times {
				rreq := cas.tcs[i].makeReadRequest()
				wreq := cas.tcs[i].makeWriteRequest()
				rres := cas.tcs[i].makeReadResponse()
				wres := cas.tcs[i].makeWriteResponse()
				startTime := time.Now()
				_, _, _, _, err := controller.OnRequestWait(suite.ctx, resourceGroupName, rreq)
				re.NoError(err)
				_, _, _, _, err = controller.OnRequestWait(suite.ctx, resourceGroupName, wreq)
				re.NoError(err)
				sum += time.Since(startTime)
				_, err = controller.OnResponse(resourceGroupName, rreq, rres)
				re.NoError(err)
				_, err = controller.OnResponse(resourceGroupName, wreq, wres)
				re.NoError(err)
				time.Sleep(1000 * time.Microsecond)
			}
			re.LessOrEqual(sum, buffDuration+cas.tcs[i].waitDuration)
		}
		i++
		if !v {
			break
		}
	}

	resourceGroupName2 := suite.initGroups[2].Name
	tcs = tokenConsumptionPerSecond{rruTokensAtATime: 1, wruTokensAtATime: 100000, times: 1, waitDuration: 0}
	wreq := tcs.makeWriteRequest()
	_, _, _, _, err = controller.OnRequestWait(suite.ctx, resourceGroupName2, wreq)
	re.NoError(err)

	re.NoError(failpoint.Enable("github.com/tikv/pd/client/resource_group/controller/acceleratedSpeedTrend", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/client/resource_group/controller/acceleratedSpeedTrend"))
	}()
	resourceGroupName3 := suite.initGroups[3].Name
	tcs = tokenConsumptionPerSecond{rruTokensAtATime: 1, wruTokensAtATime: 1000, times: 1, waitDuration: 0}
	wreq = tcs.makeWriteRequest()
	_, _, _, _, err = controller.OnRequestWait(suite.ctx, resourceGroupName3, wreq)
	re.NoError(err)
	time.Sleep(110 * time.Millisecond)
	tcs = tokenConsumptionPerSecond{rruTokensAtATime: 1, wruTokensAtATime: 10, times: 1010, waitDuration: 0}
	duration := time.Duration(0)
	for range tcs.times {
		wreq = tcs.makeWriteRequest()
		startTime := time.Now()
		_, _, _, _, err = controller.OnRequestWait(suite.ctx, resourceGroupName3, wreq)
		duration += time.Since(startTime)
		re.NoError(err)
	}
	re.Less(duration, 100*time.Millisecond)
	err = controller.Stop()
	re.NoError(err)
}

func (suite *resourceManagerClientTestSuite) TestResourcePenalty() {
	re := suite.Require()
	cli := suite.client

	groupNames := []string{"penalty_test1", "penalty_test2"}
	// Mock add 2 resource groups.
	group := &rmpb.ResourceGroup{
		Name: groupNames[0],
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
	for _, name := range groupNames {
		group.Name = name
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
	c, err := controller.NewResourceGroupController(suite.ctx, 1, cli, cfg, constants.NullKeyspaceID, controller.EnableSingleGroupByKeyspace())
	re.NoError(err)
	c.Start(suite.ctx)

	resourceGroupName := groupNames[0]
	// init
	req := controller.NewTestRequestInfo(false, 0, 2 /* store2 */, controller.AccessLocalZone)
	resp := controller.NewTestResponseInfo(0, time.Duration(30), true)
	_, penalty, _, _, err := c.OnRequestWait(suite.ctx, resourceGroupName, req)
	re.NoError(err)
	re.Zero(penalty.WriteBytes)
	re.Zero(penalty.TotalCpuTimeMs)
	_, err = c.OnResponse(resourceGroupName, req, resp)
	re.NoError(err)

	req = controller.NewTestRequestInfo(true, 60, 1 /* store1 */, controller.AccessLocalZone)
	resp = controller.NewTestResponseInfo(0, time.Duration(10), true)
	_, penalty, _, _, err = c.OnRequestWait(suite.ctx, resourceGroupName, req)
	re.NoError(err)
	re.Zero(penalty.WriteBytes)
	re.Zero(penalty.TotalCpuTimeMs)
	_, err = c.OnResponse(resourceGroupName, req, resp)
	re.NoError(err)

	// failed request, shouldn't be counted in penalty
	req = controller.NewTestRequestInfo(true, 20, 1 /* store1 */, controller.AccessLocalZone)
	resp = controller.NewTestResponseInfo(0, time.Duration(0), false)
	_, penalty, _, _, err = c.OnRequestWait(suite.ctx, resourceGroupName, req)
	re.NoError(err)
	re.Zero(penalty.WriteBytes)
	re.Zero(penalty.TotalCpuTimeMs)
	_, err = c.OnResponse(resourceGroupName, req, resp)
	re.NoError(err)

	// from same store, should be zero
	req1 := controller.NewTestRequestInfo(false, 0, 1 /* store1 */, controller.AccessLocalZone)
	resp1 := controller.NewTestResponseInfo(0, time.Duration(10), true)
	_, penalty, _, _, err = c.OnRequestWait(suite.ctx, resourceGroupName, req1)
	re.NoError(err)
	re.Zero(penalty.WriteBytes)
	_, err = c.OnResponse(resourceGroupName, req1, resp1)
	re.NoError(err)

	// from different store, should be non-zero
	req2 := controller.NewTestRequestInfo(true, 50, 2 /* store2 */, controller.AccessLocalZone)
	resp2 := controller.NewTestResponseInfo(0, time.Duration(10), true)
	_, penalty, _, _, err = c.OnRequestWait(suite.ctx, resourceGroupName, req2)
	re.NoError(err)
	re.Equal(60.0, penalty.WriteBytes)
	re.InEpsilon(10.0/1000.0/1000.0, penalty.TotalCpuTimeMs, 1e-6)
	_, err = c.OnResponse(resourceGroupName, req2, resp2)
	re.NoError(err)

	// from new store, should be zero
	req3 := controller.NewTestRequestInfo(true, 0, 3 /* store3 */, controller.AccessLocalZone)
	resp3 := controller.NewTestResponseInfo(0, time.Duration(10), true)
	_, penalty, _, _, err = c.OnRequestWait(suite.ctx, resourceGroupName, req3)
	re.NoError(err)
	re.Zero(penalty.WriteBytes)
	_, err = c.OnResponse(resourceGroupName, req3, resp3)
	re.NoError(err)

	// from different group, should be zero
	resourceGroupName = groupNames[1]
	req4 := controller.NewTestRequestInfo(true, 50, 1 /* store2 */, controller.AccessLocalZone)
	resp4 := controller.NewTestResponseInfo(0, time.Duration(10), true)
	_, penalty, _, _, err = c.OnRequestWait(suite.ctx, resourceGroupName, req4)
	re.NoError(err)
	re.Zero(penalty.WriteBytes)
	_, err = c.OnResponse(resourceGroupName, req4, resp4)
	re.NoError(err)

	err = c.Stop()
	re.NoError(err)
}

func (suite *resourceManagerClientTestSuite) TestAcquireTokenBucket() {
	re := suite.Require()
	cli := suite.client

	groups := suite.initGroups
	for _, group := range groups {
		resp, err := cli.AddResourceGroup(suite.ctx, group)
		re.NoError(err)
		re.Contains(resp, "Success!")
	}
	reqs := &rmpb.TokenBucketsRequest{
		Requests:              make([]*rmpb.TokenBucketRequest, 0),
		TargetRequestPeriodMs: uint64(time.Second * 10 / time.Millisecond),
	}
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/mcs/resourcemanager/server/fastPersist", `return(true)`))
	suite.resignAndWaitLeader(re)
	for range 3 {
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
			re.Equal(30000., resp.GrantedRUTokens[0].GrantedTokens.Tokens)
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
		suite.resignAndWaitLeader(re)
		gresp, err = cli.GetResourceGroup(suite.ctx, groups[0].GetName())
		re.NoError(err)
		checkFunc(gresp, groups[0])
	}

	// Test for background request upload.
	reqs.Requests = nil
	reqs.Requests = append(reqs.Requests, &rmpb.TokenBucketRequest{
		ResourceGroupName: "background_job",
		IsBackground:      true,
	})
	aresp, err := cli.AcquireTokenBuckets(suite.ctx, reqs)
	re.NoError(err)
	for _, resp := range aresp {
		re.Empty(resp.GrantedRUTokens)
	}

	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/mcs/resourcemanager/server/fastPersist"))
}

func (suite *resourceManagerClientTestSuite) TestBasicResourceGroupCURD() {
	re := suite.Require()
	cli := suite.client
	testCasesSet1 := []struct {
		name           string
		mode           rmpb.GroupMode
		isNewGroup     bool
		modifySuccess  bool
		expectMarshal  string
		modifySettings func(*rmpb.ResourceGroup)
	}{
		{"CRUD_test1", rmpb.GroupMode_RUMode, true, true,
			`{"name":"CRUD_test1","mode":1,"r_u_settings":{"r_u":{"settings":{"fill_rate":10000},"state":{"initialized":false}}},"priority":0}`,
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

		{"CRUD_test2", rmpb.GroupMode_RUMode, true, true,
			`{"name":"CRUD_test2","mode":1,"r_u_settings":{"r_u":{"settings":{"fill_rate":20000},"state":{"initialized":false}}},"priority":0,"runaway_settings":{"rule":{"exec_elapsed_time_ms":10000},"action":2},"background_settings":{"job_types":["test"]}}`,
			func(gs *rmpb.ResourceGroup) {
				gs.RUSettings = &rmpb.GroupRequestUnitSettings{
					RU: &rmpb.TokenBucket{
						Settings: &rmpb.TokenLimitSettings{
							FillRate: 20000,
						},
					},
				}
				gs.RunawaySettings = &rmpb.RunawaySettings{
					Rule: &rmpb.RunawayRule{
						ExecElapsedTimeMs: 10000,
					},
					Action: rmpb.RunawayAction_CoolDown,
				}
				gs.BackgroundSettings = &rmpb.BackgroundSettings{
					JobTypes: []string{"test"},
				}
			},
		},
		{"CRUD_test2", rmpb.GroupMode_RUMode, false, true,
			`{"name":"CRUD_test2","mode":1,"r_u_settings":{"r_u":{"settings":{"fill_rate":30000,"burst_limit":-1},"state":{"initialized":false}}},"priority":0,"runaway_settings":{"rule":{"exec_elapsed_time_ms":1000},"action":3,"watch":{"lasting_duration_ms":100000,"type":2}},"background_settings":{"job_types":["br","lightning"]}}`,
			func(gs *rmpb.ResourceGroup) {
				gs.RUSettings = &rmpb.GroupRequestUnitSettings{
					RU: &rmpb.TokenBucket{
						Settings: &rmpb.TokenLimitSettings{
							FillRate:   30000,
							BurstLimit: -1,
						},
					},
				}
				gs.RunawaySettings = &rmpb.RunawaySettings{
					Rule: &rmpb.RunawayRule{
						ExecElapsedTimeMs: 1000,
					},
					Action: rmpb.RunawayAction_Kill,
					Watch: &rmpb.RunawayWatch{
						Type:              rmpb.RunawayWatchType_Similar,
						LastingDurationMs: 100000,
					},
				}
				gs.BackgroundSettings = &rmpb.BackgroundSettings{
					JobTypes: []string{"br", "lightning"},
				}
			},
		},
		{"default", rmpb.GroupMode_RUMode, false, true,
			`{"name":"default","mode":1,"r_u_settings":{"r_u":{"settings":{"fill_rate":10000,"burst_limit":-1},"state":{"initialized":false}}},"priority":0,"background_settings":{"job_types":["br"]}}`,
			func(gs *rmpb.ResourceGroup) {
				gs.RUSettings = &rmpb.GroupRequestUnitSettings{
					RU: &rmpb.TokenBucket{
						Settings: &rmpb.TokenLimitSettings{
							FillRate:   10000,
							BurstLimit: -1,
						},
					},
				}
				gs.BackgroundSettings = &rmpb.BackgroundSettings{
					JobTypes: []string{"br"},
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

	finalNum := 1
	// Test Resource Group CURD via gRPC
	for i, tcase := range testCasesSet1 {
		group := &rmpb.ResourceGroup{
			Name: tcase.name,
			Mode: tcase.mode,
		}
		// Create Resource Group
		resp, err := cli.AddResourceGroup(suite.ctx, group)
		checkErr(err, true)
		if tcase.isNewGroup {
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
			re.Len(lresp, finalNum)

			for _, g := range lresp {
				// Delete Resource Group
				dresp, err := cli.DeleteResourceGroup(suite.ctx, g.Name)
				if g.Name == server.DefaultResourceGroupName {
					re.Contains(err.Error(), "cannot delete reserved group")
					continue
				}
				re.NoError(err)
				re.Contains(dresp, "Success!")
				_, err = cli.GetResourceGroup(suite.ctx, g.Name)
				re.EqualError(err, fmt.Sprintf("get resource group %v failed, rpc error: code = Unknown desc = [PD:resourcemanager:ErrGroupNotExists]the %v resource group does not exist", g.Name, g.Name))
			}

			// to test the deletion of persistence
			suite.resignAndWaitLeader(re)
			// List Resource Group
			lresp, err = cli.ListResourceGroups(suite.ctx)
			re.NoError(err)
			re.Len(lresp, 1)
		}
	}

	// Test Resource Group CURD via HTTP
	finalNum = 1
	getAddr := func(i int) string {
		server := suite.cluster.GetLeaderServer()
		if i%2 == 1 {
			server = suite.cluster.GetServer(suite.cluster.GetFollower())
		}
		return server.GetAddr()
	}
	for i, tcase := range testCasesSet1 {
		// Create Resource Group
		group := &rmpb.ResourceGroup{
			Name: tcase.name,
			Mode: tcase.mode,
		}
		createJSON, err := json.Marshal(group)
		re.NoError(err)
		resp, err := tests.TestDialClient.Post(getAddr(i)+"/resource-manager/api/v1/config/group", "application/json", strings.NewReader(string(createJSON)))
		re.NoError(err)
		resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)
		if tcase.isNewGroup {
			finalNum++
		}

		// Modify Resource Group
		tcase.modifySettings(group)
		modifyJSON, err := json.Marshal(group)
		re.NoError(err)
		req, err := http.NewRequest(http.MethodPut, getAddr(i+1)+"/resource-manager/api/v1/config/group", strings.NewReader(string(modifyJSON)))
		re.NoError(err)
		req.Header.Set("Content-Type", "application/json")
		resp, err = http.DefaultClient.Do(req)
		re.NoError(err)
		resp.Body.Close()
		if tcase.modifySuccess {
			re.Equal(http.StatusOK, resp.StatusCode)
		} else {
			re.Equal(http.StatusInternalServerError, resp.StatusCode)
		}

		// Get Resource Group
		resp, err = tests.TestDialClient.Get(getAddr(i) + "/resource-manager/api/v1/config/group/" + tcase.name)
		re.NoError(err)
		re.Equal(http.StatusOK, resp.StatusCode)
		respString, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		re.NoError(err)
		re.Contains(string(respString), tcase.name)
		if tcase.modifySuccess {
			re.JSONEq(string(respString), tcase.expectMarshal)
		}

		// Last one, Check list and delete all resource groups
		if i == len(testCasesSet1)-1 {
			resp, err := tests.TestDialClient.Get(getAddr(i) + "/resource-manager/api/v1/config/groups")
			re.NoError(err)
			re.Equal(http.StatusOK, resp.StatusCode)
			respString, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			re.NoError(err)
			groups := make([]*server.ResourceGroup, 0)
			err = json.Unmarshal(respString, &groups)
			re.NoError(err)
			re.Len(groups, finalNum)

			// Delete all resource groups
			for _, g := range groups {
				req, err := http.NewRequest(http.MethodDelete, getAddr(i+1)+"/resource-manager/api/v1/config/group/"+g.Name, http.NoBody)
				re.NoError(err)
				resp, err := http.DefaultClient.Do(req)
				re.NoError(err)
				respString, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				re.NoError(err)
				if g.Name == server.DefaultResourceGroupName {
					re.Contains(string(respString), "cannot delete reserved group")
					continue
				}
				re.Equal(http.StatusOK, resp.StatusCode)
				re.Contains(string(respString), "Success!")
			}

			// verify again
			resp1, err := tests.TestDialClient.Get(getAddr(i) + "/resource-manager/api/v1/config/groups")
			re.NoError(err)
			re.Equal(http.StatusOK, resp1.StatusCode)
			respString1, err := io.ReadAll(resp1.Body)
			resp1.Body.Close()
			re.NoError(err)
			groups1 := make([]server.ResourceGroup, 0)
			err = json.Unmarshal(respString1, &groups1)
			re.NoError(err)
			re.Len(groups1, 1)
		}
	}

	// test restart cluster
	groups, err := cli.ListResourceGroups(suite.ctx)
	re.NoError(err)
	servers := suite.cluster.GetServers()
	if suite.mode == resourceManagerStandaloneWithClientDiscovery {
		suite.stopMicroservicesForDiscovery()
	}
	re.NoError(suite.cluster.StopAll())
	cli.Close()
	serverList := make([]*tests.TestServer, 0, len(servers))
	for _, s := range servers {
		serverList = append(serverList, s)
	}
	re.NoError(tests.RunServers(serverList))
	re.NotEmpty(suite.cluster.WaitLeader())
	if suite.mode == resourceManagerStandaloneWithClientDiscovery {
		suite.startMicroservicesForDiscovery(re)
	}
	// re-connect client as well
	suite.client = suite.setupPDClient(re)
	cli = suite.client
	var newGroups []*rmpb.ResourceGroup
	testutil.Eventually(re, func() bool {
		var err error
		newGroups, err = cli.ListResourceGroups(suite.ctx)
		return err == nil
	}, testutil.WithWaitFor(time.Second))
	re.Equal(groups, newGroups)
}

func (suite *resourceManagerClientTestSuite) TestResourceGroupRUConsumption() {
	re := suite.Require()
	cli := suite.client
	group := &rmpb.ResourceGroup{
		Name: "test_ru_consumption",
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{
				FillRate:   10000,
				BurstLimit: 10000,
				MaxTokens:  20000.0,
			}},
		},
	}
	_, err := cli.AddResourceGroup(suite.ctx, group)
	re.NoError(err)

	g, err := cli.GetResourceGroup(suite.ctx, group.Name)
	re.NoError(err)
	re.Equal(group, g)

	// Test Resource Group Stats
	testConsumption := &rmpb.Consumption{
		RRU:               200.0,
		WRU:               100.0,
		ReadBytes:         1024,
		WriteBytes:        512,
		TotalCpuTimeMs:    50.0,
		SqlLayerCpuTimeMs: 40.0,
		KvReadRpcCount:    5,
		KvWriteRpcCount:   6,
	}
	_, err = cli.AcquireTokenBuckets(suite.ctx, &rmpb.TokenBucketsRequest{
		Requests: []*rmpb.TokenBucketRequest{
			{
				ResourceGroupName:           group.Name,
				ConsumptionSinceLastRequest: testConsumption,
			},
		},
		TargetRequestPeriodMs: 1000,
		ClientUniqueId:        1,
	})
	re.NoError(err)
	time.Sleep(10 * time.Millisecond)
	g, err = cli.GetResourceGroup(suite.ctx, group.Name, pd.WithRUStats)
	re.NoError(err)
	re.Equal(g.RUStats, testConsumption)

	// update resource group, ru stats not change
	g.RUSettings.RU.Settings.FillRate = 12345
	_, err = cli.ModifyResourceGroup(suite.ctx, g)
	re.NoError(err)
	g1, err := cli.GetResourceGroup(suite.ctx, group.Name, pd.WithRUStats)
	re.NoError(err)
	re.Equal(g1, g)

	// test leader change
	time.Sleep(250 * time.Millisecond)
	re.NoError(suite.cluster.GetLeaderServer().ResignLeader())
	suite.cluster.WaitLeader()
	// re-connect client as
	cli.Close()
	suite.client = suite.setupPDClient(re)
	cli = suite.client
	// check ru stats not loss after restart
	g, err = cli.GetResourceGroup(suite.ctx, group.Name, pd.WithRUStats)
	re.NoError(err)
	re.Equal(g.RUStats, testConsumption)
}

func (suite *resourceManagerClientTestSuite) TestResourceManagerClientFailover() {
	re := suite.Require()
	cli := suite.client

	group := &rmpb.ResourceGroup{
		Name: "failover_test",
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{}},
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
	for i := range 4 {
		group.RUSettings.RU.Settings.FillRate += uint64(i)
		modifyResp, err := cli.ModifyResourceGroup(suite.ctx, group)
		re.NoError(err)
		re.Contains(modifyResp, "Success!")
		suite.resignAndWaitLeader(re)
		getResp, err = cli.GetResourceGroup(suite.ctx, group.GetName())
		re.NoError(err)
		re.NotNil(getResp)
		re.Equal(group.RUSettings.RU.Settings.FillRate, getResp.RUSettings.RU.Settings.FillRate)
	}
}

func (suite *resourceManagerClientTestSuite) TestResourceManagerClientDegradedMode() {
	re := suite.Require()
	cli := suite.client

	groupName := "mode_test"
	group := &rmpb.ResourceGroup{
		Name: groupName,
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate:   10,
					BurstLimit: 10,
				},
				Tokens: 10,
			},
		},
	}
	addResp, err := cli.AddResourceGroup(suite.ctx, group)
	re.NoError(err)
	re.Contains(addResp, "Success!")

	cfg := &controller.RequestUnitConfig{
		ReadBaseCost:     1,
		ReadCostPerByte:  1,
		WriteBaseCost:    1,
		WriteCostPerByte: 1,
		CPUMsCost:        1,
	}
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/mcs/resourcemanager/server/acquireFailed", `return(true)`))
	re.NoError(failpoint.Enable("github.com/tikv/pd/client/resource_group/controller/degradedModeRU", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/mcs/resourcemanager/server/acquireFailed"))
		re.NoError(failpoint.Disable("github.com/tikv/pd/client/resource_group/controller/degradedModeRU"))
	}()
	controller, err := controller.NewResourceGroupController(suite.ctx, 1, cli, cfg, constants.NullKeyspaceID)
	re.NoError(err)
	controller.Start(suite.ctx)
	tc := tokenConsumptionPerSecond{
		rruTokensAtATime: 0,
		wruTokensAtATime: 10000,
	}
	tc2 := tokenConsumptionPerSecond{
		rruTokensAtATime: 0,
		wruTokensAtATime: 2,
	}
	_, _, _, _, err = controller.OnRequestWait(suite.ctx, groupName, tc.makeWriteRequest())
	re.NoError(err)
	time.Sleep(time.Second * 2)
	beginTime := time.Now()
	// This is used to make sure resource group in lowRU.
	for range 100 {
		_, _, _, _, err = controller.OnRequestWait(suite.ctx, groupName, tc2.makeWriteRequest())
		re.NoError(err)
	}
	for range 100 {
		_, _, _, _, err = controller.OnRequestWait(suite.ctx, groupName, tc.makeWriteRequest())
		re.NoError(err)
	}
	endTime := time.Now()
	// we can not check `inDegradedMode` because of data race.
	re.True(endTime.Before(beginTime.Add(time.Second)))
	err = controller.Stop()
	re.NoError(err)
}

func (suite *resourceManagerClientTestSuite) TestLoadRequestUnitConfig() {
	re := suite.Require()
	cli := suite.client
	// Test load from resource manager.
	ctr, err := controller.NewResourceGroupController(suite.ctx, 1, cli, nil, constants.NullKeyspaceID)
	re.NoError(err)
	config := ctr.GetConfig()
	re.NotNil(config)
	expectedConfig := controller.DefaultRUConfig()
	re.Equal(expectedConfig.ReadBaseCost, config.ReadBaseCost)
	re.Equal(expectedConfig.ReadBytesCost, config.ReadBytesCost)
	re.Equal(expectedConfig.WriteBaseCost, config.WriteBaseCost)
	re.Equal(expectedConfig.WriteBytesCost, config.WriteBytesCost)
	re.Equal(expectedConfig.CPUMsCost, config.CPUMsCost)
	// Test init from given config.
	ruConfig := &controller.RequestUnitConfig{
		ReadBaseCost:     1,
		ReadCostPerByte:  2,
		WriteBaseCost:    3,
		WriteCostPerByte: 4,
		CPUMsCost:        5,
	}
	ctr, err = controller.NewResourceGroupController(suite.ctx, 1, cli, ruConfig, constants.NullKeyspaceID)
	re.NoError(err)
	config = ctr.GetConfig()
	re.NotNil(config)
	controllerConfig := controller.DefaultConfig()
	controllerConfig.RequestUnit = *ruConfig
	expectedConfig = controller.GenerateRUConfig(controllerConfig)
	re.Equal(expectedConfig.ReadBaseCost, config.ReadBaseCost)
	re.Equal(expectedConfig.ReadBytesCost, config.ReadBytesCost)
	re.Equal(expectedConfig.WriteBaseCost, config.WriteBaseCost)
	re.Equal(expectedConfig.WriteBytesCost, config.WriteBytesCost)
	re.Equal(expectedConfig.CPUMsCost, config.CPUMsCost)
	// refer github.com/tikv/pd/pkg/mcs/resourcemanager/server/enableDegradedMode, check with 1s.
	re.Equal(time.Second, config.DegradedModeWaitDuration)
}

func (suite *resourceManagerClientTestSuite) TestRemoveStaleResourceGroup() {
	re := suite.Require()
	cli := suite.client

	// Mock add resource group.
	group := &rmpb.ResourceGroup{
		Name: "stale_test",
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{}},
		},
	}
	resp, err := cli.AddResourceGroup(suite.ctx, group)
	re.NoError(err)
	re.Contains(resp, "Success!")
	group2 := *group
	group2.Name = "tombstone_test"
	resp, err = cli.AddResourceGroup(suite.ctx, &group2)
	re.NoError(err)
	re.Contains(resp, "Success!")

	re.NoError(failpoint.Enable("github.com/tikv/pd/client/resource_group/controller/fastCleanup", `return(true)`))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/client/resource_group/controller/fastCleanup"))
	}()
	controller, err := controller.NewResourceGroupController(suite.ctx, 1, cli, nil, constants.NullKeyspaceID)
	re.NoError(err)
	controller.Start(suite.ctx)

	testConfig := struct {
		tcs   tokenConsumptionPerSecond
		times int
	}{
		tcs: tokenConsumptionPerSecond{
			rruTokensAtATime: 100,
		},
		times: 100,
	}
	// Mock client binds one resource group and then closed
	rreq := testConfig.tcs.makeReadRequest()
	rres := testConfig.tcs.makeReadResponse()
	for range testConfig.times {
		_, _, _, _, err = controller.OnRequestWait(suite.ctx, group.Name, rreq)
		re.NoError(err)
		_, err = controller.OnResponse(group.Name, rreq, rres)
		re.NoError(err)
		time.Sleep(100 * time.Microsecond)
	}
	testutil.Eventually(re, func() bool {
		meta := controller.GetActiveResourceGroup(group.Name)
		return meta == nil
	}, testutil.WithTickInterval(50*time.Millisecond))

	// Mock server deleted the resource group
	resp, err = cli.DeleteResourceGroup(suite.ctx, group2.Name)
	re.NoError(err)
	re.Contains(resp, "Success!")
	testutil.Eventually(re, func() bool {
		meta := controller.GetActiveResourceGroup(group2.Name)
		return meta == nil
	}, testutil.WithTickInterval(50*time.Millisecond))

	err = controller.Stop()
	re.NoError(err)
}

func (suite *resourceManagerClientTestSuite) TestCheckBackgroundJobs() {
	re := suite.Require()
	cli := suite.client

	enableBackgroundGroup := func(enable bool) string {
		if enable {
			return "background_enable"
		}
		return "background_unable"
	}
	// Mock add resource group.
	group := &rmpb.ResourceGroup{
		Name: enableBackgroundGroup(false),
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{}},
		},
	}
	resp, err := cli.AddResourceGroup(suite.ctx, group)
	re.NoError(err)
	re.Contains(resp, "Success!")
	group.Name = enableBackgroundGroup(true)
	group.BackgroundSettings = &rmpb.BackgroundSettings{JobTypes: []string{"br", "lightning"}}
	resp, err = cli.AddResourceGroup(suite.ctx, group)
	re.NoError(err)
	re.Contains(resp, "Success!")

	c, err := controller.NewResourceGroupController(suite.ctx, 1, cli, nil, constants.NullKeyspaceID)
	re.NoError(err)
	c.Start(suite.ctx)

	resourceGroupName := enableBackgroundGroup(false)
	re.False(c.IsBackgroundRequest(suite.ctx, resourceGroupName, "internal_default"))
	// test fallback for nil.
	re.False(c.IsBackgroundRequest(suite.ctx, resourceGroupName, "internal_lightning"))
	re.False(c.IsBackgroundRequest(suite.ctx, resourceGroupName, "internal_ddl"))
	re.False(c.IsBackgroundRequest(suite.ctx, resourceGroupName, ""))
	re.False(c.IsBackgroundRequest(suite.ctx, "none", "none"))

	resourceGroupName = enableBackgroundGroup(true)
	re.True(c.IsBackgroundRequest(suite.ctx, resourceGroupName, "internal_br"))
	re.True(c.IsBackgroundRequest(suite.ctx, resourceGroupName, "internal_lightning"))
	// test fallback for nil.
	re.False(c.IsBackgroundRequest(suite.ctx, resourceGroupName, "internal_ddl"))

	// modify `Default` to check fallback.
	resp, err = cli.ModifyResourceGroup(suite.ctx, &rmpb.ResourceGroup{
		Name: server.DefaultResourceGroupName,
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{}},
		},
		BackgroundSettings: &rmpb.BackgroundSettings{JobTypes: []string{"lightning", "ddl"}},
	})
	re.NoError(err)
	re.Contains(resp, "Success!")
	// wait for watch event modify.
	testutil.Eventually(re, func() bool {
		meta := c.GetActiveResourceGroup(server.DefaultResourceGroupName)
		if meta != nil && meta.BackgroundSettings != nil {
			return len(meta.BackgroundSettings.JobTypes) == 2
		}
		return false
	}, testutil.WithTickInterval(50*time.Millisecond))

	resourceGroupName = enableBackgroundGroup(false)
	re.False(c.IsBackgroundRequest(suite.ctx, resourceGroupName, "internal_default"))
	// test fallback for `"lightning", "ddl"`.
	re.True(c.IsBackgroundRequest(suite.ctx, resourceGroupName, "internal_lightning"))
	re.True(c.IsBackgroundRequest(suite.ctx, resourceGroupName, "internal_ddl"))
	re.False(c.IsBackgroundRequest(suite.ctx, resourceGroupName, ""))

	resourceGroupName = enableBackgroundGroup(true)
	re.True(c.IsBackgroundRequest(suite.ctx, resourceGroupName, "internal_br"))
	re.True(c.IsBackgroundRequest(suite.ctx, resourceGroupName, "internal_lightning"))
	// test fallback for `"lightning", "ddl"`.
	re.False(c.IsBackgroundRequest(suite.ctx, resourceGroupName, "internal_ddl"))

	err = c.Stop()
	re.NoError(err)
}

func (suite *resourceManagerClientTestSuite) TestResourceGroupControllerConfigChanged() {
	re := suite.Require()
	cli := suite.client
	// Mock add resource group.
	group := &rmpb.ResourceGroup{
		Name: "config_change_test",
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{}},
		},
	}
	resp, err := cli.AddResourceGroup(suite.ctx, group)
	re.NoError(err)
	re.Contains(resp, "Success!")

	c1, err := controller.NewResourceGroupController(suite.ctx, 1, cli, nil, constants.NullKeyspaceID)
	re.NoError(err)
	c1.Start(suite.ctx)
	// with client option
	c2, err := controller.NewResourceGroupController(suite.ctx, 2, cli, nil, constants.NullKeyspaceID, controller.WithMaxWaitDuration(time.Hour))
	re.NoError(err)
	c2.Start(suite.ctx)
	defer func() {
		err := c2.Stop()
		re.NoError(err)
	}()
	// helper function for sending HTTP requests and checking responses
	sendRequest := func(method, url string, body io.Reader) []byte {
		req, err := http.NewRequest(method, url, body)
		re.NoError(err)
		resp, err := http.DefaultClient.Do(req)
		re.NoError(err)
		defer resp.Body.Close()
		bytes, err := io.ReadAll(resp.Body)
		re.NoError(err)
		if resp.StatusCode != http.StatusOK {
			re.Fail(string(bytes))
		}
		return bytes
	}

	getAddr := func() string {
		server := suite.cluster.GetLeaderServer()
		if rand.IntN(100)%2 == 1 {
			server = suite.cluster.GetServer(suite.cluster.GetFollower())
		}
		return server.GetAddr()
	}

	configURL := "/resource-manager/api/v1/config/controller"
	waitDuration := 10 * time.Second
	tokenRPCMaxDelay := 2 * time.Second
	readBaseCost := 1.5
	defaultCfg := controller.DefaultConfig()
	expectCfg := server.ControllerConfig{
		// failpoint enableDegradedModeAndTraceLog will set it be 1s and enable trace log.
		DegradedModeWaitDuration: typeutil.NewDuration(time.Second),
		EnableControllerTraceLog: true,
		LTBMaxWaitDuration:       typeutil.Duration(defaultCfg.LTBMaxWaitDuration),
		LTBTokenRPCMaxDelay:      typeutil.Duration(defaultCfg.LTBTokenRPCMaxDelay),
		RequestUnit:              server.RequestUnitConfig(defaultCfg.RequestUnit),
	}
	expectRUCfg := controller.GenerateRUConfig(defaultCfg)
	expectRUCfg.DegradedModeWaitDuration = time.Second
	// initial config verification
	respString := sendRequest("GET", getAddr()+configURL, nil)
	expectStr, err := json.Marshal(expectCfg)
	re.NoError(err)
	re.JSONEq(string(respString), string(expectStr))
	re.Equal(expectRUCfg, c1.GetConfig())

	testCases := []struct {
		configJSON string
		value      any
		expected   func(ruConfig *controller.RUConfig)
	}{
		{
			configJSON: fmt.Sprintf(`{"degraded-mode-wait-duration": "%v"}`, waitDuration),
			value:      waitDuration,
			expected:   func(ruConfig *controller.RUConfig) { ruConfig.DegradedModeWaitDuration = waitDuration },
		},
		{
			configJSON: fmt.Sprintf(`{"ltb-token-rpc-max-delay": "%v"}`, tokenRPCMaxDelay),
			value:      waitDuration,
			expected: func(ruConfig *controller.RUConfig) {
				ruConfig.WaitRetryTimes = int(tokenRPCMaxDelay / ruConfig.WaitRetryInterval)
			},
		},
		{
			configJSON: fmt.Sprintf(`{"ltb-max-wait-duration": "%v"}`, waitDuration),
			value:      waitDuration,
			expected:   func(ruConfig *controller.RUConfig) { ruConfig.LTBMaxWaitDuration = waitDuration },
		},
		{
			configJSON: fmt.Sprintf(`{"read-base-cost": %v}`, readBaseCost),
			value:      readBaseCost,
			expected:   func(ruConfig *controller.RUConfig) { ruConfig.ReadBaseCost = controller.RequestUnit(readBaseCost) },
		},
		{
			configJSON: fmt.Sprintf(`{"write-base-cost": %v}`, readBaseCost*2),
			value:      readBaseCost * 2,
			expected:   func(ruConfig *controller.RUConfig) { ruConfig.WriteBaseCost = controller.RequestUnit(readBaseCost * 2) },
		},
		{
			// reset the degraded-mode-wait-duration to default in test.
			configJSON: fmt.Sprintf(`{"degraded-mode-wait-duration": "%v"}`, time.Second),
			value:      time.Second,
			expected:   func(ruConfig *controller.RUConfig) { ruConfig.DegradedModeWaitDuration = time.Second },
		},
	}
	// change properties one by one and verify each time
	for _, t := range testCases {
		sendRequest("POST", getAddr()+configURL, strings.NewReader(t.configJSON))
		time.Sleep(500 * time.Millisecond)
		t.expected(expectRUCfg)
		re.Equal(expectRUCfg, c1.GetConfig())

		expectRUCfg2 := *expectRUCfg
		// always apply the client option
		expectRUCfg2.LTBMaxWaitDuration = time.Hour
		re.Equal(&expectRUCfg2, c2.GetConfig())
	}
	// restart c1
	err = c1.Stop()
	re.NoError(err)
	c1, err = controller.NewResourceGroupController(suite.ctx, 1, cli, nil, constants.NullKeyspaceID)
	re.NoError(err)
	re.Equal(expectRUCfg, c1.GetConfig())
}

func (suite *resourceManagerClientTestSuite) TestResourceGroupCURDWithKeyspace() {
	re := suite.Require()
	cli := suite.client
	keyspaceID := uint32(1)
	clientKeyspace := suite.setupKeyspaceClient(re, keyspaceID)
	defer clientKeyspace.Close()

	// Add keyspace meta.
	keyspace := &keyspacepb.KeyspaceMeta{
		Id:   keyspaceID,
		Name: "keyspace_test",
	}
	storage := suite.cluster.GetLeaderServer().GetServer().GetStorage()
	err := storage.RunInTxn(suite.ctx, func(txn kv.Txn) error {
		return storage.SaveKeyspaceMeta(txn, keyspace)
	})
	re.NoError(err)
	// Add resource group with keyspace id
	group := &rmpb.ResourceGroup{
		Name: "keyspace_test",
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
	resp, err := clientKeyspace.AddResourceGroup(suite.ctx, group)
	re.NoError(err)
	re.Contains(resp, "Success!")

	// Get and List resource group without keyspace id
	rg, err := cli.GetResourceGroup(suite.ctx, group.Name)
	re.EqualError(err, fmt.Sprintf("get resource group %v failed, rpc error: code = Unknown desc = [PD:resourcemanager:ErrGroupNotExists]the %v resource group does not exist", group.Name, group.Name))
	re.Nil(rg)
	rgs, err := cli.ListResourceGroups(suite.ctx)
	re.NoError(err)
	re.Len(rgs, 1)
	re.Equal(server.DefaultResourceGroupName, rgs[0].Name)

	// Get and List resource group with keyspace id
	rg, err = clientKeyspace.GetResourceGroup(suite.ctx, group.Name, pd.WithRUStats)
	re.NoError(err)
	re.NotNil(rg)
	rgs, err = clientKeyspace.ListResourceGroups(suite.ctx, pd.WithRUStats)
	re.NoError(err)
	re.Len(rgs, 2) // Including the default resource group.
	for _, r := range rgs {
		re.NotNil(r.KeyspaceId)
		re.Equal(r.KeyspaceId.Value, keyspaceID)
		switch r.Name {
		case server.DefaultResourceGroupName:
		case group.Name:
			re.Equal(r.RUSettings.RU.Settings.FillRate, group.RUSettings.RU.Settings.FillRate)
			re.Equal(r.RUSettings.RU.Tokens, group.RUSettings.RU.Tokens)
		default:
			re.Fail("unknown resource group")
		}
	}

	// Modify resource group with keyspace id
	group.RUSettings.RU.Settings.FillRate = 1000
	resp, err = clientKeyspace.ModifyResourceGroup(suite.ctx, group)
	re.NoError(err)
	re.Contains(resp, "Success!")
	rg, err = clientKeyspace.GetResourceGroup(suite.ctx, group.Name, pd.WithRUStats)
	re.NoError(err)
	re.Equal(group.RUSettings.RU.Settings.FillRate, rg.RUSettings.RU.Settings.FillRate)

	// Test AcquireTokenBuckets without keyspace id
	testConsumption := &rmpb.Consumption{
		RRU:               200.0,
		WRU:               100.0,
		ReadBytes:         1024,
		WriteBytes:        512,
		TotalCpuTimeMs:    50.0,
		SqlLayerCpuTimeMs: 40.0,
		KvReadRpcCount:    5,
		KvWriteRpcCount:   6,
	}
	req := &rmpb.TokenBucketsRequest{
		Requests: []*rmpb.TokenBucketRequest{
			{
				ResourceGroupName:           group.Name,
				ConsumptionSinceLastRequest: testConsumption,
			},
		},
		TargetRequestPeriodMs: 1000,
		ClientUniqueId:        1,
	}
	_, err = clientKeyspace.AcquireTokenBuckets(suite.ctx, req)
	re.NoError(err)
	time.Sleep(10 * time.Millisecond)
	rg, err = clientKeyspace.GetResourceGroup(suite.ctx, group.Name, pd.WithRUStats)
	re.NoError(err)
	re.NotEqual(rg.RUStats, testConsumption)

	// Test AcquireTokenBuckets with keyspace id
	req.Requests[0].KeyspaceId = &rmpb.KeyspaceIDValue{Value: keyspaceID}
	_, err = clientKeyspace.AcquireTokenBuckets(suite.ctx, req)
	re.NoError(err)
	time.Sleep(10 * time.Millisecond)
	rg, err = clientKeyspace.GetResourceGroup(suite.ctx, group.Name, pd.WithRUStats)
	re.NoError(err)
	re.Equal(rg.RUStats, testConsumption)

	// Delete resource group without keyspace id
	_, err = cli.DeleteResourceGroup(suite.ctx, group.Name)
	re.Error(err)
	re.EqualError(err, "rpc error: code = Unknown desc = [PD:resourcemanager:ErrGroupNotExists]the keyspace_test resource group does not exist")
	rg, err = clientKeyspace.GetResourceGroup(suite.ctx, group.Name, pd.WithRUStats)
	re.NoError(err)
	re.NotNil(rg)

	// Delete resource group with keyspace id
	resp, err = clientKeyspace.DeleteResourceGroup(suite.ctx, group.Name)
	re.NoError(err)
	re.Contains(resp, "Success!")
	rg, err = clientKeyspace.GetResourceGroup(suite.ctx, group.Name, pd.WithRUStats)
	re.EqualError(err, fmt.Sprintf("get resource group %v failed, rpc error: code = Unknown desc = [PD:resourcemanager:ErrGroupNotExists]the %v resource group does not exist", group.Name, group.Name))
	re.Nil(rg)
}

func (suite *resourceManagerClientTestSuite) TestAcquireTokenBucketsWithMultiKeyspaces() {
	re := suite.Require()
	ctx := suite.ctx
	storage := suite.cluster.GetLeaderServer().GetServer().GetStorage()

	const numKeyspaces = 5
	var (
		groups       = make([]*rmpb.ResourceGroup, numKeyspaces)
		clients      = make([]pd.Client, numKeyspaces)
		consumptions = make([]*rmpb.Consumption, numKeyspaces)
	)

	// Setup: Use a loop to create 5 keyspaces, resource groups, and clients
	for i := range numKeyspaces {
		keyspaceID := uint32(i + 1)
		keyspaceName := fmt.Sprintf("keyspace%d_test", keyspaceID)
		groupName := fmt.Sprintf("rg_multi_%d", keyspaceID)
		// Create a specific client for this keyspace
		client := suite.setupKeyspaceClient(re, keyspaceID)
		clients[i] = client
		// Create and save keyspace metadata
		keyspaceMeta := &keyspacepb.KeyspaceMeta{Id: keyspaceID, Name: keyspaceName}
		err := storage.RunInTxn(ctx, func(txn kv.Txn) error {
			return storage.SaveKeyspaceMeta(txn, keyspaceMeta)
		})
		re.NoError(err)
		// Create resource group
		groups[i] = &rmpb.ResourceGroup{
			Name:       groupName,
			Mode:       rmpb.GroupMode_RUMode,
			RUSettings: &rmpb.GroupRequestUnitSettings{RU: &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{FillRate: 10000}, Tokens: 100000}},
			KeyspaceId: &rmpb.KeyspaceIDValue{Value: keyspaceID},
		}
		_, err = clients[i].AddResourceGroup(ctx, groups[i])
		re.NoError(err)
		// Prepare consumption data for later request
		consumptions[i] = &rmpb.Consumption{
			RRU: float64(100 * (i + 1)), // Use different values to distinguish
			WRU: float64(50 * (i + 1)),
		}
	}

	// Construct a single request containing items for all 5 keyspaces
	tokenBucketRequests := make([]*rmpb.TokenBucketRequest, numKeyspaces)
	for i := range numKeyspaces {
		tokenBucketRequests[i] = &rmpb.TokenBucketRequest{
			ResourceGroupName:           groups[i].Name,
			KeyspaceId:                  groups[i].KeyspaceId,
			ConsumptionSinceLastRequest: consumptions[i],
		}
	}
	req := &rmpb.TokenBucketsRequest{
		Requests:              tokenBucketRequests,
		TargetRequestPeriodMs: 1000,
		ClientUniqueId:        1,
	}

	// Send the request and verify the response
	resp, err := clients[0].AcquireTokenBuckets(ctx, req)
	re.NoError(err)
	re.NotNil(resp)
	re.Len(resp, numKeyspaces)
	expectedByKeyspace := make(map[uint32]*rmpb.ResourceGroup, numKeyspaces)
	for _, g := range groups {
		re.NotNil(g.KeyspaceId)
		expectedByKeyspace[g.KeyspaceId.GetValue()] = g
	}
	for _, tbResp := range resp {
		re.NotNil(tbResp.KeyspaceId)
		keyspaceID := tbResp.KeyspaceId.GetValue()
		expectedGroup, ok := expectedByKeyspace[keyspaceID]
		re.True(ok, "unexpected keyspace id in response: %d", keyspaceID)
		re.Equal(expectedGroup.Name, tbResp.ResourceGroupName)
		delete(expectedByKeyspace, keyspaceID)
	}
	re.Empty(expectedByKeyspace, "some keyspaces did not appear in response")

	// Verify state change using the keyspace-specific clients
	for i := range numKeyspaces {
		client := clients[i]
		groupName := groups[i].Name
		expectedConsumption := consumptions[i]
		testutil.Eventually(re, func() bool {
			rg, err := client.GetResourceGroup(ctx, groupName, pd.WithRUStats)
			re.NoError(err)
			re.NotNil(rg)
			return expectedConsumption.RRU == rg.RUStats.RRU &&
				expectedConsumption.WRU == rg.RUStats.WRU
		})
	}

	// Clean up
	for i := range numKeyspaces {
		_, err = clients[i].DeleteResourceGroup(ctx, groups[i].Name)
		re.NoError(err)
		clients[i].Close()
	}
}

func (suite *resourceManagerClientTestSuite) TestLoadAndWatchWithDifferentKeyspace() {
	re := suite.Require()
	keyspaces := []uint32{1, 2, constants.NullKeyspaceID}
	genGroupByKeyspace := func(keyspace uint32) *rmpb.ResourceGroup {
		return &rmpb.ResourceGroup{
			Name: fmt.Sprintf("keyspace_test_%d", keyspace),
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
	}

	// Create clients for different keyspaces
	clients := map[uint32]pd.Client{}
	for _, keyspace := range keyspaces {
		if keyspace == constants.NullKeyspaceID {
			clients[keyspace] = suite.client
			continue
		}
		cli := suite.setupKeyspaceClient(re, keyspace)
		clients[keyspace] = cli
	}

	// Add resource groups with different keyspaces
	for _, keyspace := range keyspaces {
		cli := clients[keyspace]
		group := genGroupByKeyspace(keyspace)
		resp, err := cli.AddResourceGroup(suite.ctx, group)
		re.NoError(err)
		re.Contains(resp, "Success!")
	}

	// Create controllers for different keyspaces
	// Test to load resource groups with different keyspaces
	clientID := uint64(1)
	tcs := tokenConsumptionPerSecond{rruTokensAtATime: 100}
	controllers := map[uint32]*controller.ResourceGroupsController{}
	for _, keyspace := range keyspaces {
		cli := clients[keyspace]
		c, err := controller.NewResourceGroupController(suite.ctx, clientID, cli, nil, keyspace)
		re.NoError(err)
		controllers[keyspace] = c
		c.Start(suite.ctx)
		for _, keyspaceToFind := range keyspaces {
			groupToFind := genGroupByKeyspace(keyspaceToFind)
			testutil.Eventually(re, func() bool {
				meta := c.GetActiveResourceGroup(groupToFind.Name)
				_, _, _, _, err := c.OnRequestWait(suite.ctx, groupToFind.Name, tcs.makeReadRequest())
				if keyspaceToFind == keyspace {
					re.NoError(err)
					return meta != nil &&
						meta.Name == groupToFind.Name &&
						meta.RUSettings.RU.Settings.FillRate == groupToFind.RUSettings.RU.Settings.FillRate
				}
				re.Error(err)
				return meta == nil
			})
		}
		clientID += 1
	}

	// Modify resource groups with different keyspaces
	fillRate := uint64(12345)
	for _, keyspace := range keyspaces {
		cli := clients[keyspace]
		group := genGroupByKeyspace(keyspace)
		group.RUSettings.RU.Settings.FillRate = fillRate
		resp, err := cli.ModifyResourceGroup(suite.ctx, group)
		re.NoError(err)
		re.Contains(resp, "Success!")
	}

	// Test to watch resource groups with different keyspaces
	for _, keyspace := range keyspaces {
		c := controllers[keyspace]
		group := genGroupByKeyspace(keyspace)
		testutil.Eventually(re, func() bool {
			meta := c.GetActiveResourceGroup(group.Name)
			return meta.RUSettings.RU.Settings.FillRate == fillRate
		})
	}

	// Delete resource groups with different keyspaces
	for _, keyspace := range keyspaces {
		cli := clients[keyspace]
		group := genGroupByKeyspace(keyspace)
		resp, err := cli.DeleteResourceGroup(suite.ctx, group.Name)
		re.NoError(err)
		re.Contains(resp, "Success!")
	}

	// Test to watch resource groups with different keyspaces
	for _, keyspace := range keyspaces {
		c := controllers[keyspace]
		group := genGroupByKeyspace(keyspace)
		testutil.Eventually(re, func() bool {
			meta := c.GetActiveResourceGroup(group.Name)
			return meta == nil
		})
	}

	// Stop controllers and close clients
	for _, keyspace := range keyspaces {
		err := controllers[keyspace].Stop()
		re.NoError(err)
		if keyspace == constants.NullKeyspaceID {
			continue
		}
		clients[keyspace].Close()
	}
}

func (suite *resourceManagerClientTestSuite) TestCannotModifyKeyspaceOfResourceGroup() {
	re := suite.Require()
	ctx := suite.ctx
	storage := suite.cluster.GetLeaderServer().GetServer().GetStorage()

	// Create two keyspaces
	keyspaceA := uint32(10)
	keyspaceB := uint32(11)
	err := storage.RunInTxn(ctx, func(txn kv.Txn) error {
		return storage.SaveKeyspaceMeta(txn, &keyspacepb.KeyspaceMeta{Id: keyspaceA, Name: "ks_A"})
	})
	re.NoError(err)
	err = storage.RunInTxn(ctx, func(txn kv.Txn) error {
		return storage.SaveKeyspaceMeta(txn, &keyspacepb.KeyspaceMeta{Id: keyspaceB, Name: "ks_B"})
	})
	re.NoError(err)

	// Create clients for keyspaceA
	clientA := suite.setupKeyspaceClient(re, keyspaceA)
	defer clientA.Close()

	// Add a resource group in Keyspace A and check
	groupName := "keyspace_test"
	originalGroup := &rmpb.ResourceGroup{
		Name: groupName,
		Mode: rmpb.GroupMode_RUMode,
	}
	resp, err := clientA.AddResourceGroup(ctx, originalGroup)
	re.NoError(err)
	re.Contains(resp, "Success!")
	g, err := clientA.GetResourceGroup(ctx, groupName)
	re.NoError(err)
	re.Equal(groupName, g.Name)
	re.NotNil(g.KeyspaceId)
	re.Equal(keyspaceA, g.KeyspaceId.Value)

	// Try to modify the group with a different keyspace ID using Client A
	modifiedGroup := &rmpb.ResourceGroup{
		Name:       groupName,
		Mode:       rmpb.GroupMode_RUMode,
		Priority:   5,
		KeyspaceId: &rmpb.KeyspaceIDValue{Value: keyspaceB},
	}

	// It should be failed because the keyspace ID does not match
	_, err = clientA.ModifyResourceGroup(ctx, modifiedGroup)
	re.Error(err)
	expectedErr := errs.ErrClientPutResourceGroupMismatchKeyspaceID.FastGenByArgs(keyspaceB, keyspaceA)
	re.EqualError(err, expectedErr.Error(), "The error should explicitly state the keyspace ID mismatch")

	// Check again and ensure the group in Keyspace A is unchanged
	g, err = clientA.GetResourceGroup(ctx, groupName)
	re.NoError(err)
	re.Equal(uint32(0), g.Priority, "Group in keyspace A should not be modified")
}
