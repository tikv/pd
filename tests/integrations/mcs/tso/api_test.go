// Copyright 2023 TiKV Project Authors.
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

package tso

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	tso "github.com/tikv/pd/pkg/mcs/tso/server"
	apis "github.com/tikv/pd/pkg/mcs/tso/server/apis/v1"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/versioninfo"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

const (
	tsoKeyspaceGroupsPrefix = "/tso/api/v1/keyspace-groups"
)

type tsoAPITestSuite struct {
	suite.Suite
	ctx              context.Context
	cancel           context.CancelFunc
	pdCluster        *tests.TestCluster
	tsoCluster       *tests.TestTSOCluster
	backendEndpoints string
}

func TestTSOAPI(t *testing.T) {
	suite.Run(t, new(tsoAPITestSuite))
}

func (suite *tsoAPITestSuite) SetupTest() {
	re := suite.Require()

	var err error
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.pdCluster, err = tests.NewTestAPICluster(suite.ctx, 1)
	re.NoError(err)
	err = suite.pdCluster.RunInitialServers()
	re.NoError(err)
	leaderName := suite.pdCluster.WaitLeader()
	re.NotEmpty(leaderName)
	pdLeaderServer := suite.pdCluster.GetServer(leaderName)
	re.NoError(pdLeaderServer.BootstrapCluster())
	suite.backendEndpoints = pdLeaderServer.GetAddr()
	suite.tsoCluster, err = tests.NewTestTSOCluster(suite.ctx, 1, suite.backendEndpoints)
	re.NoError(err)
}

func (suite *tsoAPITestSuite) TearDownTest() {
	suite.cancel()
	suite.tsoCluster.Destroy()
	suite.pdCluster.Destroy()
}

func (suite *tsoAPITestSuite) TestGetKeyspaceGroupMembers() {
	re := suite.Require()

	primary := suite.tsoCluster.WaitForDefaultPrimaryServing(re)
	re.NotNil(primary)
	members := mustGetKeyspaceGroupMembers(re, primary)
	re.Len(members, 1)
	defaultGroupMember := members[constant.DefaultKeyspaceGroupID]
	re.NotNil(defaultGroupMember)
	re.Equal(constant.DefaultKeyspaceGroupID, defaultGroupMember.Group.ID)
	re.True(defaultGroupMember.IsPrimary)
	primaryMember, err := primary.GetMember(constant.DefaultKeyspaceID, constant.DefaultKeyspaceGroupID)
	re.NoError(err)
	re.Equal(primaryMember.GetLeaderID(), defaultGroupMember.PrimaryID)
}

func (suite *tsoAPITestSuite) TestForwardResetTS() {
	re := suite.Require()

	primary := suite.tsoCluster.WaitForDefaultPrimaryServing(re)
	re.NotNil(primary)
	url := suite.backendEndpoints + "/pd/api/v1/admin/reset-ts"

	// Test reset ts
	input := []byte(`{"tso":"121312", "force-use-larger":true}`)
	err := testutil.CheckPostJSON(tests.TestDialClient, url, input,
		testutil.StatusOK(re), testutil.StringContain(re, "Reset ts successfully"), testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)

	// Test reset ts with invalid tso
	input = []byte(`{}`)
	err = testutil.CheckPostJSON(tests.TestDialClient, url, input,
		testutil.StatusNotOK(re), testutil.StringContain(re, "invalid tso value"), testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)
}

func mustGetKeyspaceGroupMembers(re *require.Assertions, server *tso.Server) map[uint32]*apis.KeyspaceGroupMember {
	httpReq, err := http.NewRequest(http.MethodGet, server.GetAddr()+tsoKeyspaceGroupsPrefix+"/members", http.NoBody)
	re.NoError(err)
	httpResp, err := tests.TestDialClient.Do(httpReq)
	re.NoError(err)
	defer httpResp.Body.Close()
	data, err := io.ReadAll(httpResp.Body)
	re.NoError(err)
	re.Equal(http.StatusOK, httpResp.StatusCode, string(data))
	var resp map[uint32]*apis.KeyspaceGroupMember
	re.NoError(json.Unmarshal(data, &resp))
	return resp
}

func TestTSOServerStartFirst(t *testing.T) {
	re := require.New(t)
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/delayStartServerLoop", `return(true)`))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	apiCluster, err := tests.NewTestAPICluster(ctx, 1, func(conf *config.Config, _ string) {
		conf.Keyspace.PreAlloc = []string{"k1", "k2"}
	})
	defer apiCluster.Destroy()
	re.NoError(err)
	addr := apiCluster.GetConfig().GetClientURL()
	ch := make(chan struct{})
	defer close(ch)
	clusterCh := make(chan *tests.TestTSOCluster)
	defer close(clusterCh)
	go func() {
		tsoCluster, err := tests.NewTestTSOCluster(ctx, 2, addr)
		re.NoError(err)
		primary := tsoCluster.WaitForDefaultPrimaryServing(re)
		re.NotNil(primary)
		clusterCh <- tsoCluster
		ch <- struct{}{}
	}()
	err = apiCluster.RunInitialServers()
	re.NoError(err)
	leaderName := apiCluster.WaitLeader()
	re.NotEmpty(leaderName)
	pdLeaderServer := apiCluster.GetServer(leaderName)
	re.NoError(pdLeaderServer.BootstrapCluster())
	re.NoError(err)
	tsoCluster := <-clusterCh
	defer tsoCluster.Destroy()
	<-ch

	time.Sleep(time.Second * 1)
	input := make(map[string]any)
	input["new-id"] = 1
	input["keyspaces"] = []uint32{2}
	jsonBody, err := json.Marshal(input)
	re.NoError(err)
	httpReq, err := http.NewRequest(http.MethodPost, addr+"/pd/api/v2/tso/keyspace-groups/0/split", bytes.NewBuffer(jsonBody))
	re.NoError(err)
	httpResp, err := tests.TestDialClient.Do(httpReq)
	re.NoError(err)
	defer httpResp.Body.Close()
	re.Equal(http.StatusOK, httpResp.StatusCode)

	httpReq, err = http.NewRequest(http.MethodGet, addr+"/pd/api/v2/tso/keyspace-groups/0", http.NoBody)
	re.NoError(err)
	httpResp, err = tests.TestDialClient.Do(httpReq)
	re.NoError(err)
	data, err := io.ReadAll(httpResp.Body)
	re.NoError(err)
	defer httpResp.Body.Close()
	re.Equal(http.StatusOK, httpResp.StatusCode)

	var group endpoint.KeyspaceGroup
	re.NoError(json.Unmarshal(data, &group))
	re.Len(group.Keyspaces, 2)
	re.Len(group.Members, 2)

	re.NoError(failpoint.Disable("github.com/tikv/pd/server/delayStartServerLoop"))
}

func TestForwardOnlyTSONoScheduling(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tc, err := tests.NewTestAPICluster(ctx, 1)
	defer tc.Destroy()
	re.NoError(err)
	err = tc.RunInitialServers()
	re.NoError(err)
	pdAddr := tc.GetConfig().GetClientURL()
	ttc, err := tests.NewTestTSOCluster(ctx, 2, pdAddr)
	re.NoError(err)
	tc.WaitLeader()
	leaderServer := tc.GetLeaderServer()
	re.NoError(leaderServer.BootstrapCluster())

	urlPrefix := fmt.Sprintf("%s/pd/api/v1", pdAddr)

	// Test /operators, it should not forward when there is no scheduling server.
	var slice []string
	err = testutil.ReadGetJSON(re, tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "operators"), &slice,
		testutil.WithoutHeader(re, apiutil.XForwardedToMicroServiceHeader))
	re.NoError(err)
	re.Empty(slice)

	// Test admin/reset-ts, it should forward to tso server.
	input := []byte(`{"tso":"121312", "force-use-larger":true}`)
	err = testutil.CheckPostJSON(tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "admin/reset-ts"), input,
		testutil.StatusOK(re), testutil.StringContain(re, "Reset ts successfully"), testutil.WithHeader(re, apiutil.XForwardedToMicroServiceHeader, "true"))
	re.NoError(err)

	// If close tso server, it should try forward to tso server, but return error in api mode.
	ttc.Destroy()
	err = testutil.CheckPostJSON(tests.TestDialClient, fmt.Sprintf("%s/%s", urlPrefix, "admin/reset-ts"), input,
		testutil.Status(re, http.StatusInternalServerError), testutil.StringContain(re, "[PD:apiutil:ErrRedirect]redirect failed"))
	re.NoError(err)
}

func (suite *tsoAPITestSuite) TestMetrics() {
	re := suite.Require()

	primary := suite.tsoCluster.WaitForDefaultPrimaryServing(re)
	resp, err := tests.TestDialClient.Get(primary.GetConfig().GetAdvertiseListenAddr() + "/metrics")
	re.NoError(err)
	defer resp.Body.Close()
	re.Equal(http.StatusOK, resp.StatusCode)
	respBytes, err := io.ReadAll(resp.Body)
	re.NoError(err)
	re.Contains(string(respBytes), "pd_server_info")
}

func (suite *tsoAPITestSuite) TestStatus() {
	re := suite.Require()

	primary := suite.tsoCluster.WaitForDefaultPrimaryServing(re)
	resp, err := tests.TestDialClient.Get(primary.GetConfig().GetAdvertiseListenAddr() + "/status")
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

func (suite *tsoAPITestSuite) TestConfig() {
	re := suite.Require()

	primary := suite.tsoCluster.WaitForDefaultPrimaryServing(re)
	resp, err := tests.TestDialClient.Get(primary.GetConfig().GetAdvertiseListenAddr() + "/tso/api/v1/config")
	re.NoError(err)
	defer resp.Body.Close()
	re.Equal(http.StatusOK, resp.StatusCode)
	respBytes, err := io.ReadAll(resp.Body)
	re.NoError(err)
	var cfg tso.Config
	re.NoError(json.Unmarshal(respBytes, &cfg))
	re.Equal(cfg.GetListenAddr(), primary.GetConfig().GetListenAddr())
	re.Equal(cfg.GetTSOSaveInterval(), primary.GetConfig().GetTSOSaveInterval())
	re.Equal(cfg.IsLocalTSOEnabled(), primary.GetConfig().IsLocalTSOEnabled())
	re.Equal(cfg.GetTSOUpdatePhysicalInterval(), primary.GetConfig().GetTSOUpdatePhysicalInterval())
	re.Equal(cfg.GetMaxResetTSGap(), primary.GetConfig().GetMaxResetTSGap())
}
