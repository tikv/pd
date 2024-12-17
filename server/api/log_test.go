// Copyright 2020 TiKV Project Authors.
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
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/pingcap/log"

	tu "github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
)

type logTestSuite struct {
	suite.Suite
	svr       *server.Server
	cleanup   tu.CleanupFunc
	urlPrefix string
}

func TestLogTestSuite(t *testing.T) {
	suite.Run(t, new(logTestSuite))
}

func (suite *logTestSuite) SetupSuite() {
	re := suite.Require()
	suite.svr, suite.cleanup = mustNewServer(re)
	server.MustWaitLeader(re, []*server.Server{suite.svr})

	addr := suite.svr.GetAddr()
	suite.urlPrefix = fmt.Sprintf("%s%s/api/v1/admin", addr, apiPrefix)

	mustBootstrapCluster(re, suite.svr)
}

func (suite *logTestSuite) TearDownSuite() {
	suite.cleanup()
}

func (suite *logTestSuite) TestSetLogLevel() {
	re := suite.Require()
	level := "error"
	data, err := json.Marshal(level)
	re.NoError(err)
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/log", data, tu.StatusOK(re))
	re.NoError(err)
	re.Equal(level, log.GetLevel().String())
}
