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
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/pingcap/log"

	tu "github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/tests"
)

type logTestSuite struct {
	suite.Suite
	env *tests.SchedulingTestEnvironment
}

func TestLogTestSuite(t *testing.T) {
	suite.Run(t, new(logTestSuite))
}

func (suite *logTestSuite) SetupSuite() {
	suite.env = tests.NewSchedulingTestEnvironment(suite.T())
}

func (suite *logTestSuite) TearDownSuite() {
	suite.env.Cleanup()
}

func (suite *logTestSuite) TestSetLogLevel() {
	suite.env.RunTest(suite.checkSetLogLevel)
}

func (suite *logTestSuite) checkSetLogLevel(cluster *tests.TestCluster) {
	re := suite.Require()
	leader := cluster.GetLeaderServer()
	urlPrefix := leader.GetAddr() + "/pd/api/v1"

	level := "error"
	data, err := json.Marshal(level)
	re.NoError(err)
	err = tu.CheckPostJSON(tests.TestDialClient, urlPrefix+"/admin/log", data, tu.StatusOK(re))
	re.NoError(err)
	re.Equal(level, log.GetLevel().String())
}
