// Copyright 2018 TiKV Project Authors.
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

package api

import (
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/api"
	"github.com/tikv/pd/tests"
)

type healthTestSuite struct {
	suite.Suite
	env *tests.SchedulingTestEnvironment
}

func TestHealthTestSuite(t *testing.T) {
	suite.Run(t, new(healthTestSuite))
}

func (suite *healthTestSuite) SetupSuite() {
	suite.env = tests.NewSchedulingTestEnvironment(suite.T())
	suite.env.PDCount = 3
}

func (suite *healthTestSuite) TearDownSuite() {
	suite.env.Cleanup()
}

func (suite *healthTestSuite) TestHealthSlice() {
	suite.env.RunTest(suite.checkHealthSlice)
}

func (suite *healthTestSuite) checkHealthSlice(cluster *tests.TestCluster) {
	re := suite.Require()
	servers := cluster.GetServers()

	var leader, follower *server.Server

	for _, server := range servers {
		svr := server.GetServer()
		if !svr.IsClosed() && svr.GetMember().IsLeader() {
			leader = svr
		} else {
			follower = svr
		}
	}
	addr := leader.GetAddr() + "/pd/api/v1/health"
	follower.Close()
	resp, err := tests.TestDialClient.Get(addr)
	re.NoError(err)
	defer resp.Body.Close()
	buf, err := io.ReadAll(resp.Body)
	re.NoError(err)
	checkSliceResponse(re, buf, servers, follower.GetConfig().Name)
}

func checkSliceResponse(re *require.Assertions, body []byte, servers map[string]*tests.TestServer, unhealthy string) {
	var got []api.Health
	re.NoError(json.Unmarshal(body, &got))
	re.Len(servers, len(got))

	for _, h := range got {
		for _, server := range servers {
			cfg := server.GetConfig()
			if h.Name != cfg.Name {
				continue
			}
			relaxEqualStings(re, h.ClientUrls, strings.Split(cfg.ClientUrls, ","))
		}
		if h.Name == unhealthy {
			re.False(h.Health)
			continue
		}
		re.True(h.Health)
	}
}
