// Copyright 2026 TiKV Project Authors.
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
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"

	"github.com/pingcap/failpoint"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"

	"github.com/tikv/pd/pkg/keyspace"
	rmserver "github.com/tikv/pd/pkg/mcs/resourcemanager/server"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

type resourceManagerWatcherMatrixTestSuite struct {
	suite.Suite
	ctx          context.Context
	cancel       context.CancelFunc
	pdCluster    *tests.TestCluster
	pdLeader     *tests.TestServer
	rmCluster    *tests.TestResourceManagerCluster
	keyspaceName string
	keyspaceID   uint32
}

func TestResourceManagerWatcherMatrix(t *testing.T) {
	suite.Run(t, new(resourceManagerWatcherMatrixTestSuite))
}

func (suite *resourceManagerWatcherMatrixTestSuite) SetupTest() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion", "return(true)"))

	var err error
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.pdCluster, err = tests.NewTestClusterWithKeyspaceGroup(suite.ctx, 1, func(conf *config.Config, _ string) {
		conf.Microservice.EnableResourceManagerFallback = false
	})
	re.NoError(err)
	re.NoError(suite.pdCluster.RunInitialServers())

	leaderName := suite.pdCluster.WaitLeader()
	re.NotEmpty(leaderName)
	suite.pdLeader = suite.pdCluster.GetServer(leaderName)
	re.NoError(suite.pdLeader.BootstrapCluster())

	suite.rmCluster, err = tests.NewTestResourceManagerCluster(suite.ctx, 2, suite.pdLeader.GetAddr())
	re.NoError(err)
	suite.waitForRMPrimary("")

	suite.keyspaceName = "watcher_matrix_ks"
	meta, err := suite.pdLeader.GetKeyspaceManager().CreateKeyspace(&keyspace.CreateKeyspaceRequest{Name: suite.keyspaceName})
	re.NoError(err)
	suite.keyspaceID = meta.GetId()
}

func (suite *resourceManagerWatcherMatrixTestSuite) TearDownTest() {
	re := suite.Require()
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion"))
	_ = failpoint.Disable("github.com/tikv/pd/pkg/utils/etcdutil/watchChanBlock")

	suite.cancel()
	suite.rmCluster.Destroy()
	suite.pdCluster.Destroy()
}

func (suite *resourceManagerWatcherMatrixTestSuite) TestWatcherKeepsLegacyKeyspacePrecedence() {
	re := suite.Require()
	primary := suite.waitForRMPrimary("")
	pdClient, pdConn := suite.newRMClient(suite.pdLeader.GetAddr())
	defer pdConn.Close()
	rmClient, rmConn := suite.newRMClient(primary.GetAddr())
	defer rmConn.Close()

	legacyGroup := newWatcherMatrixResourceGroup("shared_group", 1, 100, nil)
	keyspaceGroup := newWatcherMatrixResourceGroup("shared_group", 9, 900, &suite.keyspaceID)
	suite.putResourceGroup(re, pdClient, legacyGroup)
	suite.putResourceGroup(re, pdClient, keyspaceGroup)

	legacyReq := &rmpb.GetResourceGroupRequest{ResourceGroupName: legacyGroup.GetName()}
	keyspaceReq := &rmpb.GetResourceGroupRequest{
		ResourceGroupName: keyspaceGroup.GetName(),
		KeyspaceId:        &rmpb.KeyspaceIDValue{Value: suite.keyspaceID},
	}
	suite.waitForGroup(re, rmClient, legacyReq, 1, 100)
	suite.waitForGroup(re, rmClient, keyspaceReq, 9, 900)

	legacyGroup.Priority = 2
	legacyGroup.RUSettings.RU.Settings.FillRate = 120
	keyspaceGroup.Priority = 10
	keyspaceGroup.RUSettings.RU.Settings.FillRate = 1000
	suite.putResourceGroup(re, pdClient, legacyGroup)
	suite.putResourceGroup(re, pdClient, keyspaceGroup)

	suite.waitForGroup(re, rmClient, legacyReq, 2, 120)
	suite.waitForGroup(re, rmClient, keyspaceReq, 10, 1000)
}

func (suite *resourceManagerWatcherMatrixTestSuite) TestWatcherBootstrapsAfterRMPrimarySwitch() {
	re := suite.Require()
	initialPrimary := suite.waitForRMPrimary("")
	pdClient, pdConn := suite.newRMClient(suite.pdLeader.GetAddr())
	defer pdConn.Close()

	group := newWatcherMatrixResourceGroup("primary_switch_group", 7, 700, &suite.keyspaceID)
	suite.putResourceGroup(re, pdClient, group)

	initialRMClient, initialConn := suite.newRMClient(initialPrimary.GetAddr())
	defer initialConn.Close()
	req := &rmpb.GetResourceGroupRequest{
		ResourceGroupName: group.GetName(),
		KeyspaceId:        &rmpb.KeyspaceIDValue{Value: suite.keyspaceID},
	}
	suite.waitForGroup(re, initialRMClient, req, 7, 700)

	initialPrimary.Close()
	newPrimary := suite.waitForRMPrimary(initialPrimary.GetAddr())
	re.NotEqual(initialPrimary.GetAddr(), newPrimary.GetAddr())

	newRMClient, newConn := suite.newRMClient(newPrimary.GetAddr())
	defer newConn.Close()
	suite.waitForGroup(re, newRMClient, req, 7, 700)

	group.Priority = 8
	group.RUSettings.RU.Settings.FillRate = 800
	suite.putResourceGroup(re, pdClient, group)
	suite.waitForGroup(re, newRMClient, req, 8, 800)
}

func (suite *resourceManagerWatcherMatrixTestSuite) TestWatcherRecoversAfterCompaction() {
	re := suite.Require()
	primary := suite.waitForRMPrimary("")
	pdClient, pdConn := suite.newRMClient(suite.pdLeader.GetAddr())
	defer pdConn.Close()
	rmClient, rmConn := suite.newRMClient(primary.GetAddr())
	defer rmConn.Close()
	logFile := testutil.InitTempFileLogger("debug")
	defer os.Remove(logFile)

	group := newWatcherMatrixResourceGroup("compaction_group", 5, 500, &suite.keyspaceID)
	suite.putResourceGroup(re, pdClient, group)

	req := &rmpb.GetResourceGroupRequest{
		ResourceGroupName: group.GetName(),
		KeyspaceId:        &rmpb.KeyspaceIDValue{Value: suite.keyspaceID},
	}
	suite.waitForGroup(re, rmClient, req, 5, 500)

	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/utils/etcdutil/watchChanBlock", "return(true)"))

	group.Priority = 6
	group.RUSettings.RU.Settings.FillRate = 600
	suite.putResourceGroup(re, pdClient, group)
	suite.compactGroupSetting(re, group.GetName(), suite.keyspaceID)

	testutil.Eventually(re, func() bool {
		b, err := os.ReadFile(logFile)
		re.NoError(err)
		l := string(b)
		return strings.Contains(l, "resource-manager-metadata-watcher") &&
			strings.Contains(l, "watch channel is blocked for a long time")
	}, testutil.WithWaitFor(etcdutil.WatchChTimeoutDuration+2*etcdutil.RequestProgressInterval), testutil.WithTickInterval(100*time.Millisecond))
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/utils/etcdutil/watchChanBlock"))

	group.Priority = 7
	group.RUSettings.RU.Settings.FillRate = 700
	suite.putResourceGroup(re, pdClient, group)
	suite.waitForGroup(re, rmClient, req, 7, 700)
}

func newWatcherMatrixResourceGroup(name string, priority uint32, fillRate uint64, keyspaceID *uint32) *rmpb.ResourceGroup {
	group := &rmpb.ResourceGroup{
		Name:     name,
		Mode:     rmpb.GroupMode_RUMode,
		Priority: priority,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate:   fillRate,
					BurstLimit: 2 * int64(fillRate),
				},
			},
		},
	}
	if keyspaceID != nil {
		group.KeyspaceId = &rmpb.KeyspaceIDValue{Value: *keyspaceID}
	}
	return group
}

func (suite *resourceManagerWatcherMatrixTestSuite) newRMClient(addr string) (rmpb.ResourceManagerClient, *grpc.ClientConn) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := grpcutil.GetClientConn(ctx, addr, nil)
	suite.Require().NoError(err)
	return rmpb.NewResourceManagerClient(conn), conn
}

func (suite *resourceManagerWatcherMatrixTestSuite) waitForRMPrimary(excludeAddr string) *rmserver.Server {
	re := suite.Require()
	var primary *rmserver.Server
	testutil.Eventually(re, func() bool {
		for _, server := range suite.rmCluster.GetServers() {
			if server.IsServing() && server.GetAddr() != excludeAddr {
				primary = server
				return true
			}
		}
		return false
	}, testutil.WithWaitFor(30*time.Second), testutil.WithTickInterval(100*time.Millisecond))
	return primary
}

func (suite *resourceManagerWatcherMatrixTestSuite) putResourceGroup(re *require.Assertions, client rmpb.ResourceManagerClient, group *rmpb.ResourceGroup) {
	re.NotNil(group)
	addCtx, addCancel := context.WithTimeout(suite.ctx, 5*time.Second)
	defer addCancel()
	resp, err := client.AddResourceGroup(addCtx, &rmpb.PutResourceGroupRequest{Group: group})
	if err == nil && resp.GetError() == nil {
		re.Equal("Success!", resp.GetBody())
		return
	}
	modifyCtx, modifyCancel := context.WithTimeout(suite.ctx, 5*time.Second)
	defer modifyCancel()
	resp, err = client.ModifyResourceGroup(modifyCtx, &rmpb.PutResourceGroupRequest{Group: group})
	re.NoError(err)
	re.Nil(resp.GetError())
	re.Equal("Success!", resp.GetBody())
}

func (suite *resourceManagerWatcherMatrixTestSuite) waitForGroup(re *require.Assertions, client rmpb.ResourceManagerClient, req *rmpb.GetResourceGroupRequest, priority uint32, fillRate uint64) {
	testutil.Eventually(re, func() bool {
		ctx, cancel := context.WithTimeout(suite.ctx, 5*time.Second)
		defer cancel()
		resp, err := client.GetResourceGroup(ctx, req)
		if err != nil || resp.GetError() != nil || resp.GetGroup() == nil {
			return false
		}
		group := resp.GetGroup()
		if group.GetPriority() != priority {
			return false
		}
		settings := group.GetRUSettings().GetRU().GetSettings()
		return settings != nil && settings.GetFillRate() == fillRate
	}, testutil.WithWaitFor(30*time.Second), testutil.WithTickInterval(100*time.Millisecond))
}

func (suite *resourceManagerWatcherMatrixTestSuite) compactGroupSetting(re *require.Assertions, groupName string, keyspaceID uint32) {
	re.NotZero(keyspaceID)
	client := suite.pdCluster.GetEtcdClient()
	key := keypath.KeyspaceResourceGroupSettingPath(keyspaceID, groupName)
	resp, err := etcdutil.EtcdKVGet(client, key)
	re.NoError(err)
	re.NotEmpty(resp.Kvs)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = client.Compact(ctx, resp.Header.Revision)
	re.NoError(err)
}
