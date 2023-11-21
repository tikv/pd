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
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	tso "github.com/tikv/pd/pkg/mcs/tso/server"
	apis "github.com/tikv/pd/pkg/mcs/tso/server/apis/v1"
	mcsutils "github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

const (
	tsoKeyspaceGroupsPrefix = "/tso/api/v1/keyspace-groups"
)

// dialClient used to dial http request.
var dialClient = &http.Client{
	Transport: &http.Transport{
		DisableKeepAlives: true,
	},
}

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
	defaultGroupMember := members[mcsutils.DefaultKeyspaceGroupID]
	re.NotNil(defaultGroupMember)
	re.Equal(mcsutils.DefaultKeyspaceGroupID, defaultGroupMember.Group.ID)
	re.True(defaultGroupMember.IsPrimary)
	primaryMember, err := primary.GetMember(mcsutils.DefaultKeyspaceID, mcsutils.DefaultKeyspaceGroupID)
	re.NoError(err)
	re.Equal(primaryMember.GetLeaderID(), defaultGroupMember.PrimaryID)
}

func (suite *tsoAPITestSuite) TestForwardResetTS() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/utils/apiutil/serverapi/checkHeader", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/utils/apiutil/serverapi/checkHeader"))
	}()

	primary := suite.tsoCluster.WaitForDefaultPrimaryServing(re)
	re.NotNil(primary)
	url := suite.backendEndpoints + "/pd/api/v1/admin/reset-ts"

	// Test reset ts
	input := []byte(`{"tso":"121312", "force-use-larger":true}`)
	err := testutil.CheckPostJSON(dialClient, url, input,
		testutil.StatusOK(re), testutil.StringContain(re, "Reset ts successfully"), testutil.WithHeader(re, apiutil.ForwardToMicroServiceHeader, "true"))
	suite.NoError(err)

	// Test reset ts with invalid tso
	input = []byte(`{}`)
	err = testutil.CheckPostJSON(dialClient, url, input,
		testutil.StatusNotOK(re), testutil.StringContain(re, "invalid tso value"), testutil.WithHeader(re, apiutil.ForwardToMicroServiceHeader, "true"))
	re.NoError(err)
}

func mustGetKeyspaceGroupMembers(re *require.Assertions, server *tso.Server) map[uint32]*apis.KeyspaceGroupMember {
	httpReq, err := http.NewRequest(http.MethodGet, server.GetAddr()+tsoKeyspaceGroupsPrefix+"/members", nil)
	re.NoError(err)
	httpResp, err := dialClient.Do(httpReq)
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

	apiCluster, err := tests.NewTestAPICluster(ctx, 1, func(conf *config.Config, serverName string) {
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
	pdLeaderServer := apiCluster.GetServer(leaderName)
	re.NoError(pdLeaderServer.BootstrapCluster())
	re.NoError(err)
	tsoCluster := <-clusterCh
	defer tsoCluster.Destroy()
	<-ch

	time.Sleep(time.Second * 1)
	input := make(map[string]interface{})
	input["new-id"] = 1
	input["keyspaces"] = []uint32{2}
	jsonBody, err := json.Marshal(input)
	re.NoError(err)
	httpReq, err := http.NewRequest(http.MethodPost, addr+"/pd/api/v2/tso/keyspace-groups/0/split", bytes.NewBuffer(jsonBody))
	re.NoError(err)
	httpResp, err := dialClient.Do(httpReq)
	re.NoError(err)
	defer httpResp.Body.Close()
	re.Equal(http.StatusOK, httpResp.StatusCode)

	httpReq, err = http.NewRequest(http.MethodGet, addr+"/pd/api/v2/tso/keyspace-groups/0", nil)
	re.NoError(err)
	httpResp, err = dialClient.Do(httpReq)
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
