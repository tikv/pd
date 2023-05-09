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
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	tso "github.com/tikv/pd/pkg/mcs/tso/server"
	apis "github.com/tikv/pd/pkg/mcs/tso/server/apis/v1"
	mcsutils "github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/integrations/mcs"
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
	ctx        context.Context
	cancel     context.CancelFunc
	pdCluster  *tests.TestCluster
	tsoCluster *mcs.TestTSOCluster
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
	suite.tsoCluster, err = mcs.NewTestTSOCluster(suite.ctx, 1, pdLeaderServer.GetAddr())
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
