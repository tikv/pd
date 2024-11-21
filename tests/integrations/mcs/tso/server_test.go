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

package tso

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/caller"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mcs/discovery"
	tso "github.com/tikv/pd/pkg/mcs/tso/server"
	tsoapi "github.com/tikv/pd/pkg/mcs/tso/server/apis/v1"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/storage/endpoint"
	tsopkg "github.com/tikv/pd/pkg/tso"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/tsoutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/integrations/mcs"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

type tsoServerTestSuite struct {
	suite.Suite
	ctx              context.Context
	cancel           context.CancelFunc
	cluster          *tests.TestCluster
	pdLeader         *tests.TestServer
	backendEndpoints string
}

func TestTSOServerTestSuite(t *testing.T) {
	suite.Run(t, new(tsoServerTestSuite))
}

func (suite *tsoServerTestSuite) SetupSuite() {
	var err error
	re := suite.Require()

	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.cluster, err = tests.NewTestAPICluster(suite.ctx, 1)
	re.NoError(err)

	err = suite.cluster.RunInitialServers()
	re.NoError(err)

	leaderName := suite.cluster.WaitLeader()
	re.NotEmpty(leaderName)
	suite.pdLeader = suite.cluster.GetServer(leaderName)
	suite.backendEndpoints = suite.pdLeader.GetAddr()
	re.NoError(suite.pdLeader.BootstrapCluster())
}

func (suite *tsoServerTestSuite) TearDownSuite() {
	suite.cluster.Destroy()
	suite.cancel()
}

func (suite *tsoServerTestSuite) TestTSOServerStartAndStopNormally() {
	defer func() {
		if r := recover(); r != nil {
			suite.T().Log("Recovered from an unexpected panic", r)
			suite.T().Errorf("Expected no panic, but something bad occurred with")
		}
	}()

	re := suite.Require()
	s, cleanup := tests.StartSingleTSOTestServer(suite.ctx, re, suite.backendEndpoints, tempurl.Alloc())

	defer cleanup()
	testutil.Eventually(re, func() bool {
		return s.IsServing()
	}, testutil.WithWaitFor(5*time.Second), testutil.WithTickInterval(50*time.Millisecond))

	// Test registered GRPC Service
	cc, err := grpc.DialContext(suite.ctx, s.GetAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	re.NoError(err)
	cc.Close()

	url := s.GetAddr() + tsoapi.APIPathPrefix + "/admin/reset-ts"
	// Test reset ts
	input := []byte(`{"tso":"121312", "force-use-larger":true}`)
	err = testutil.CheckPostJSON(tests.TestDialClient, url, input,
		testutil.StatusOK(re), testutil.StringContain(re, "Reset ts successfully"))
	re.NoError(err)

	// Test reset ts with invalid tso
	input = []byte(`{}`)
	err = testutil.CheckPostJSON(tests.TestDialClient, suite.backendEndpoints+"/pd/api/v1/admin/reset-ts", input,
		testutil.StatusNotOK(re), testutil.StringContain(re, "invalid tso value"))
	re.NoError(err)
}

func (suite *tsoServerTestSuite) TestParticipantStartWithAdvertiseListenAddr() {
	re := suite.Require()

	cfg := tso.NewConfig()
	cfg.BackendEndpoints = suite.backendEndpoints
	cfg.ListenAddr = tempurl.Alloc()
	cfg.AdvertiseListenAddr = tempurl.Alloc()
	cfg, err := tso.GenerateConfig(cfg)
	re.NoError(err)

	// Setup the logger.
	err = tests.InitLogger(cfg.Log, cfg.Logger, cfg.LogProps, cfg.Security.RedactInfoLog)
	re.NoError(err)

	s, cleanup, err := tests.NewTSOTestServer(suite.ctx, cfg)
	re.NoError(err)
	defer cleanup()
	testutil.Eventually(re, func() bool {
		return s.IsServing()
	}, testutil.WithWaitFor(5*time.Second), testutil.WithTickInterval(50*time.Millisecond))

	member, err := s.GetMember(constant.DefaultKeyspaceID, constant.DefaultKeyspaceGroupID)
	re.NoError(err)
	re.Equal(fmt.Sprintf("%s-%05d", cfg.AdvertiseListenAddr, constant.DefaultKeyspaceGroupID), member.Name())
}

func TestTSOPath(t *testing.T) {
	re := require.New(t)
	checkTSOPath(re, true /*isAPIServiceMode*/)
	checkTSOPath(re, false /*isAPIServiceMode*/)
}

func checkTSOPath(re *require.Assertions, isAPIServiceMode bool) {
	var (
		cluster *tests.TestCluster
		err     error
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if isAPIServiceMode {
		cluster, err = tests.NewTestAPICluster(ctx, 1, func(conf *config.Config, _ string) {
			conf.MicroService.EnableTSODynamicSwitching = false
		})
	} else {
		cluster, err = tests.NewTestCluster(ctx, 1)
	}
	re.NoError(err)
	defer cluster.Destroy()
	err = cluster.RunInitialServers()
	re.NoError(err)
	leaderName := cluster.WaitLeader()
	re.NotEmpty(leaderName)
	pdLeader := cluster.GetServer(leaderName)
	re.NoError(pdLeader.BootstrapCluster())
	backendEndpoints := pdLeader.GetAddr()
	client := pdLeader.GetEtcdClient()
	if isAPIServiceMode {
		re.Equal(0, getEtcdTimestampKeyNum(re, client))
	} else {
		re.Equal(1, getEtcdTimestampKeyNum(re, client))
	}

	_, cleanup := tests.StartSingleTSOTestServer(ctx, re, backendEndpoints, tempurl.Alloc())
	defer cleanup()

	cli := mcs.SetupClientWithAPIContext(ctx, re, pd.NewAPIContextV2(""), []string{backendEndpoints})
	defer cli.Close()
	physical, logical, err := cli.GetTS(ctx)
	re.NoError(err)
	ts := tsoutil.ComposeTS(physical, logical)
	re.NotEmpty(ts)
	// After we request the tso server, etcd still has only one key related to the timestamp.
	re.Equal(1, getEtcdTimestampKeyNum(re, client))
}

func getEtcdTimestampKeyNum(re *require.Assertions, client *clientv3.Client) int {
	resp, err := etcdutil.EtcdKVGet(client, "/", clientv3.WithPrefix())
	re.NoError(err)
	var count int
	for _, kv := range resp.Kvs {
		key := strings.TrimSpace(string(kv.Key))
		if !strings.HasSuffix(key, keypath.TimestampKey) {
			continue
		}
		count++
	}
	return count
}

type APIServerForward struct {
	re               *require.Assertions
	ctx              context.Context
	cancel           context.CancelFunc
	cluster          *tests.TestCluster
	pdLeader         *tests.TestServer
	backendEndpoints string
	pdClient         pd.Client
}

func NewAPIServerForward(re *require.Assertions) APIServerForward {
	suite := APIServerForward{
		re: re,
	}
	var err error
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.cluster, err = tests.NewTestAPICluster(suite.ctx, 3)
	re.NoError(err)

	err = suite.cluster.RunInitialServers()
	re.NoError(err)

	leaderName := suite.cluster.WaitLeader()
	re.NotEmpty(leaderName)
	suite.pdLeader = suite.cluster.GetServer(leaderName)
	suite.backendEndpoints = suite.pdLeader.GetAddr()
	re.NoError(suite.pdLeader.BootstrapCluster())
	suite.addRegions()

	re.NoError(failpoint.Enable("github.com/tikv/pd/client/usePDServiceMode", "return(true)"))
	suite.pdClient, err = pd.NewClientWithContext(context.Background(),
		caller.TestComponent,
		[]string{suite.backendEndpoints}, pd.SecurityOption{}, opt.WithMaxErrorRetry(1))
	re.NoError(err)
	return suite
}

func (suite *APIServerForward) ShutDown() {
	suite.pdClient.Close()
	re := suite.re

	etcdClient := suite.pdLeader.GetEtcdClient()
	endpoints, err := discovery.Discover(etcdClient, constant.TSOServiceName)
	re.NoError(err)
	if len(endpoints) != 0 {
		endpoints, err = discovery.Discover(etcdClient, constant.TSOServiceName)
		re.NoError(err)
		re.Empty(endpoints)
	}
	suite.cluster.Destroy()
	suite.cancel()
	re.NoError(failpoint.Disable("github.com/tikv/pd/client/usePDServiceMode"))
}

func TestForwardTSORelated(t *testing.T) {
	re := require.New(t)
	suite := NewAPIServerForward(re)
	defer suite.ShutDown()
	leaderServer := suite.cluster.GetLeaderServer().GetServer()
	cfg := leaderServer.GetMicroServiceConfig().Clone()
	cfg.EnableTSODynamicSwitching = false
	leaderServer.SetMicroServiceConfig(*cfg)
	// Unable to use the tso-related interface without tso server
	suite.checkUnavailableTSO(re)
	tc, err := tests.NewTestTSOCluster(suite.ctx, 1, suite.backendEndpoints)
	re.NoError(err)
	defer tc.Destroy()
	tc.WaitForDefaultPrimaryServing(re)
	suite.checkAvailableTSO(re)
}

func TestForwardTSOWhenPrimaryChanged(t *testing.T) {
	re := require.New(t)
	suite := NewAPIServerForward(re)
	defer suite.ShutDown()

	tc, err := tests.NewTestTSOCluster(suite.ctx, 2, suite.backendEndpoints)
	re.NoError(err)
	defer tc.Destroy()
	tc.WaitForDefaultPrimaryServing(re)

	// can use the tso-related interface with old primary
	oldPrimary, exist := suite.pdLeader.GetServer().GetServicePrimaryAddr(suite.ctx, constant.TSOServiceName)
	re.True(exist)
	suite.checkAvailableTSO(re)

	// can use the tso-related interface with new primary
	tc.DestroyServer(oldPrimary)
	time.Sleep(time.Duration(constant.DefaultLeaderLease) * time.Second) // wait for leader lease timeout
	tc.WaitForDefaultPrimaryServing(re)
	primary, exist := suite.pdLeader.GetServer().GetServicePrimaryAddr(suite.ctx, constant.TSOServiceName)
	re.True(exist)
	re.NotEqual(oldPrimary, primary)
	suite.checkAvailableTSO(re)

	// can use the tso-related interface with old primary again
	tc.AddServer(oldPrimary)
	suite.checkAvailableTSO(re)
	for addr := range tc.GetServers() {
		if addr != oldPrimary {
			tc.DestroyServer(addr)
		}
	}
	tc.WaitForDefaultPrimaryServing(re)
	time.Sleep(time.Duration(constant.DefaultLeaderLease) * time.Second) // wait for leader lease timeout
	primary, exist = suite.pdLeader.GetServer().GetServicePrimaryAddr(suite.ctx, constant.TSOServiceName)
	re.True(exist)
	re.Equal(oldPrimary, primary)
	suite.checkAvailableTSO(re)
}

func TestResignTSOPrimaryForward(t *testing.T) {
	re := require.New(t)
	suite := NewAPIServerForward(re)
	defer suite.ShutDown()
	// TODO: test random kill primary with 3 nodes
	tc, err := tests.NewTestTSOCluster(suite.ctx, 2, suite.backendEndpoints)
	re.NoError(err)
	defer tc.Destroy()
	tc.WaitForDefaultPrimaryServing(re)

	for range 10 {
		tc.ResignPrimary(constant.DefaultKeyspaceID, constant.DefaultKeyspaceGroupID)
		tc.WaitForDefaultPrimaryServing(re)
		var err error
		for range 3 { // try 3 times
			_, _, err = suite.pdClient.GetTS(suite.ctx)
			if err == nil {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		re.NoError(err)
		suite.checkAvailableTSO(re)
	}
}

func TestResignAPIPrimaryForward(t *testing.T) {
	re := require.New(t)
	suite := NewAPIServerForward(re)
	defer suite.ShutDown()

	tc, err := tests.NewTestTSOCluster(suite.ctx, 2, suite.backendEndpoints)
	re.NoError(err)
	defer tc.Destroy()
	tc.WaitForDefaultPrimaryServing(re)

	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/member/skipCampaignLeaderCheck", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/member/skipCampaignLeaderCheck"))
	}()

	for range 10 {
		suite.pdLeader.ResignLeader()
		suite.pdLeader = suite.cluster.GetServer(suite.cluster.WaitLeader())
		suite.backendEndpoints = suite.pdLeader.GetAddr()
		_, _, err = suite.pdClient.GetTS(suite.ctx)
		re.NoError(err)
	}
}

func TestForwardTSOUnexpectedToFollower1(t *testing.T) {
	re := require.New(t)
	suite := NewAPIServerForward(re)
	defer suite.ShutDown()
	suite.checkForwardTSOUnexpectedToFollower(func() {
		// unary call will retry internally
		// try to update gc safe point
		min, err := suite.pdClient.UpdateServiceGCSafePoint(context.Background(), "a", 1000, 1)
		re.NoError(err)
		re.Equal(uint64(0), min)
	})
}

func TestForwardTSOUnexpectedToFollower2(t *testing.T) {
	re := require.New(t)
	suite := NewAPIServerForward(re)
	defer suite.ShutDown()
	suite.checkForwardTSOUnexpectedToFollower(func() {
		// unary call will retry internally
		// try to set external ts
		ts, err := suite.pdClient.GetExternalTimestamp(suite.ctx)
		re.NoError(err)
		err = suite.pdClient.SetExternalTimestamp(suite.ctx, ts+1)
		re.NoError(err)
	})
}

func TestForwardTSOUnexpectedToFollower3(t *testing.T) {
	re := require.New(t)
	suite := NewAPIServerForward(re)
	defer suite.ShutDown()
	suite.checkForwardTSOUnexpectedToFollower(func() {
		_, _, err := suite.pdClient.GetTS(suite.ctx)
		re.Error(err)
	})
}

func (suite *APIServerForward) checkForwardTSOUnexpectedToFollower(checkTSO func()) {
	re := suite.re
	tc, err := tests.NewTestTSOCluster(suite.ctx, 2, suite.backendEndpoints)
	re.NoError(err)
	tc.WaitForDefaultPrimaryServing(re)

	// get follower's address
	servers := tc.GetServers()
	oldPrimary := tc.GetPrimaryServer(constant.DefaultKeyspaceID, constant.DefaultKeyspaceGroupID).GetAddr()
	var follower string
	for addr := range servers {
		if addr != oldPrimary {
			follower = addr
			break
		}
	}
	re.NotEmpty(follower)

	// write follower's address to cache to simulate cache is not updated.
	suite.pdLeader.GetServer().SetServicePrimaryAddr(constant.TSOServiceName, follower)
	errorAddr, ok := suite.pdLeader.GetServer().GetServicePrimaryAddr(suite.ctx, constant.TSOServiceName)
	re.True(ok)
	re.Equal(follower, errorAddr)

	// test tso request
	checkTSO()

	// test tso request will success after cache is updated
	suite.checkAvailableTSO(re)
	newPrimary, exist2 := suite.pdLeader.GetServer().GetServicePrimaryAddr(suite.ctx, constant.TSOServiceName)
	re.True(exist2)
	re.NotEqual(errorAddr, newPrimary)
	re.Equal(oldPrimary, newPrimary)
	tc.Destroy()
}

func (suite *APIServerForward) addRegions() {
	leader := suite.cluster.GetServer(suite.cluster.WaitLeader())
	rc := leader.GetServer().GetRaftCluster()
	for i := range 3 {
		region := &metapb.Region{
			Id:       uint64(i*4 + 1),
			Peers:    []*metapb.Peer{{Id: uint64(i*4 + 2), StoreId: uint64(i*4 + 3)}},
			StartKey: []byte{byte(i)},
			EndKey:   []byte{byte(i + 1)},
		}
		rc.HandleRegionHeartbeat(core.NewRegionInfo(region, region.Peers[0]))
	}
}

func (suite *APIServerForward) checkUnavailableTSO(re *require.Assertions) {
	_, _, err := suite.pdClient.GetTS(suite.ctx)
	re.Error(err)
	// try to update gc safe point
	_, err = suite.pdClient.UpdateServiceGCSafePoint(suite.ctx, "a", 1000, 1)
	re.Error(err)
	// try to set external ts
	err = suite.pdClient.SetExternalTimestamp(suite.ctx, 1000)
	re.Error(err)
}

func (suite *APIServerForward) checkAvailableTSO(re *require.Assertions) {
	mcs.WaitForTSOServiceAvailable(suite.ctx, re, suite.pdClient)
	// try to get ts
	_, _, err := suite.pdClient.GetTS(suite.ctx)
	re.NoError(err)
	// try to update gc safe point
	min, err := suite.pdClient.UpdateServiceGCSafePoint(context.Background(), "a", 1000, 1)
	re.NoError(err)
	re.Equal(uint64(0), min)
	// try to set external ts
	ts, err := suite.pdClient.GetExternalTimestamp(suite.ctx)
	re.NoError(err)
	err = suite.pdClient.SetExternalTimestamp(suite.ctx, ts+1)
	re.NoError(err)
}

type CommonTestSuite struct {
	suite.Suite
	ctx        context.Context
	cancel     context.CancelFunc
	cluster    *tests.TestCluster
	tsoCluster *tests.TestTSOCluster
	pdLeader   *tests.TestServer
	// tsoDefaultPrimaryServer is the primary server of the default keyspace group
	tsoDefaultPrimaryServer *tso.Server
	backendEndpoints        string
}

func TestCommonTestSuite(t *testing.T) {
	suite.Run(t, new(CommonTestSuite))
}

func (suite *CommonTestSuite) SetupSuite() {
	var err error
	re := suite.Require()
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.cluster, err = tests.NewTestAPICluster(suite.ctx, 1)
	re.NoError(err)

	err = suite.cluster.RunInitialServers()
	re.NoError(err)

	leaderName := suite.cluster.WaitLeader()
	re.NotEmpty(leaderName)
	suite.pdLeader = suite.cluster.GetServer(leaderName)
	suite.backendEndpoints = suite.pdLeader.GetAddr()
	re.NoError(suite.pdLeader.BootstrapCluster())

	suite.tsoCluster, err = tests.NewTestTSOCluster(suite.ctx, 1, suite.backendEndpoints)
	re.NoError(err)
	suite.tsoCluster.WaitForDefaultPrimaryServing(re)
	suite.tsoDefaultPrimaryServer = suite.tsoCluster.GetPrimaryServer(constant.DefaultKeyspaceID, constant.DefaultKeyspaceGroupID)
}

func (suite *CommonTestSuite) TearDownSuite() {
	re := suite.Require()
	suite.tsoCluster.Destroy()
	etcdClient := suite.pdLeader.GetEtcdClient()
	endpoints, err := discovery.Discover(etcdClient, constant.TSOServiceName)
	re.NoError(err)
	if len(endpoints) != 0 {
		endpoints, err = discovery.Discover(etcdClient, constant.TSOServiceName)
		re.NoError(err)
		re.Empty(endpoints)
	}
	suite.cluster.Destroy()
	suite.cancel()
}

func (suite *CommonTestSuite) TestAdvertiseAddr() {
	re := suite.Require()

	conf := suite.tsoDefaultPrimaryServer.GetConfig()
	re.Equal(conf.GetListenAddr(), conf.GetAdvertiseListenAddr())
}

func (suite *CommonTestSuite) TestBootstrapDefaultKeyspaceGroup() {
	re := suite.Require()

	// check the default keyspace group and wait for alloc tso nodes for the default keyspace group
	check := func() {
		testutil.Eventually(re, func() bool {
			resp, err := tests.TestDialClient.Get(suite.pdLeader.GetServer().GetConfig().AdvertiseClientUrls + "/pd/api/v2/tso/keyspace-groups")
			re.NoError(err)
			defer resp.Body.Close()
			re.Equal(http.StatusOK, resp.StatusCode)
			respString, err := io.ReadAll(resp.Body)
			re.NoError(err)
			var kgs []*endpoint.KeyspaceGroup
			re.NoError(json.Unmarshal(respString, &kgs))
			re.Len(kgs, 1)
			re.Equal(constant.DefaultKeyspaceGroupID, kgs[0].ID)
			re.Equal(endpoint.Basic.String(), kgs[0].UserKind)
			re.Empty(kgs[0].SplitState)
			re.Empty(kgs[0].KeyspaceLookupTable)
			return len(kgs[0].Members) == 1
		})
	}
	check()

	s, err := suite.cluster.JoinAPIServer(suite.ctx)
	re.NoError(err)
	re.NoError(s.Run())

	// transfer leader to the new server
	suite.pdLeader.ResignLeader()
	suite.pdLeader = suite.cluster.GetServer(suite.cluster.WaitLeader())
	check()
	suite.pdLeader.ResignLeader()
	suite.pdLeader = suite.cluster.GetServer(suite.cluster.WaitLeader())
}

// TestTSOServiceSwitch tests the behavior of TSO service switching when `EnableTSODynamicSwitching` is enabled.
// Initially, the TSO service should be provided by PD. After starting a TSO server, the service should switch to the TSO server.
// When the TSO server is stopped, the PD should resume providing the TSO service if `EnableTSODynamicSwitching` is enabled.
// If `EnableTSODynamicSwitching` is disabled, the PD should not provide TSO service after the TSO server is stopped.
func TestTSOServiceSwitch(t *testing.T) {
	re := require.New(t)
	re.NoError(failpoint.Enable("github.com/tikv/pd/client/fastUpdateServiceMode", `return(true)`))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tc, err := tests.NewTestAPICluster(ctx, 1,
		func(conf *config.Config, _ string) {
			conf.MicroService.EnableTSODynamicSwitching = true
		},
	)
	re.NoError(err)
	defer tc.Destroy()

	err = tc.RunInitialServers()
	re.NoError(err)
	leaderName := tc.WaitLeader()
	re.NotEmpty(leaderName)
	pdLeader := tc.GetServer(leaderName)
	backendEndpoints := pdLeader.GetAddr()
	re.NoError(pdLeader.BootstrapCluster())
	pdClient, err := pd.NewClientWithContext(ctx, caller.TestComponent,
		[]string{backendEndpoints}, pd.SecurityOption{})
	re.NoError(err)
	re.NotNil(pdClient)
	defer pdClient.Close()

	var globalLastTS uint64
	// Initially, TSO service should be provided by PD
	re.NoError(checkTSOMonotonic(ctx, pdClient, &globalLastTS, 10))

	// Start TSO server
	tsoCluster, err := tests.NewTestTSOCluster(ctx, 1, pdLeader.GetAddr())
	re.NoError(err)
	tsoCluster.WaitForDefaultPrimaryServing(re)

	// Verify PD is not providing TSO service
	testutil.Eventually(re, func() bool {
		allocator, err := pdLeader.GetServer().GetTSOAllocatorManager().GetAllocator(tsopkg.GlobalDCLocation)
		if err != nil {
			return false
		}
		return !allocator.IsInitialize()
	})

	err = checkTSOMonotonic(ctx, pdClient, &globalLastTS, 10)
	re.NoError(err)

	// Disable TSO switching
	cfg := pdLeader.GetServer().GetMicroServiceConfig().Clone()
	cfg.EnableTSODynamicSwitching = false
	pdLeader.GetServer().SetMicroServiceConfig(*cfg)

	tsoCluster.Destroy()

	// Wait for the configuration change to take effect
	time.Sleep(300 * time.Millisecond)
	// Verify PD is not providing TSO service multiple times
	for range 10 {
		err = checkTSOMonotonic(ctx, pdClient, &globalLastTS, 1)
		re.Error(err, "TSO service should not be available")
		time.Sleep(10 * time.Millisecond)
	}

	// Now enable TSO switching
	cfg = pdLeader.GetServer().GetMicroServiceConfig().Clone()

	cfg.EnableTSODynamicSwitching = true
	pdLeader.GetServer().SetMicroServiceConfig(*cfg)

	// Wait for PD to detect the change
	time.Sleep(300 * time.Millisecond)

	// Verify PD is now providing TSO service and timestamps are monotonically increasing
	re.NoError(checkTSOMonotonic(ctx, pdClient, &globalLastTS, 10))
	re.NoError(failpoint.Disable("github.com/tikv/pd/client/fastUpdateServiceMode"))
}

func checkTSOMonotonic(ctx context.Context, pdClient pd.Client, globalLastTS *uint64, count int) error {
	for range count {
		physical, logical, err := pdClient.GetTS(ctx)
		if err != nil {
			return err
		}
		ts := (uint64(physical) << 18) + uint64(logical)
		if ts <= *globalLastTS {
			return fmt.Errorf("TSO is not globally increasing: last %d, current %d", globalLastTS, ts)
		}
		*globalLastTS = ts
	}
	return nil
}
