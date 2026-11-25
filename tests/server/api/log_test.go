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
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/config"
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
	err = testutil.CheckPostJSON(tests.TestDialClient, urlPrefix+"/admin/log", data, testutil.StatusOK(re))
	re.NoError(err)
	re.Equal(level, log.GetLevel().String())
}

func TestAuditLogLevelChange(t *testing.T) {
	for _, mode := range []logutil.RedactInfoLogType{logutil.RedactInfoLogOFF, logutil.RedactInfoLogON, logutil.RedactInfoLogMarker} {
		t.Run(fmt.Sprintf("redaction=%d", mode), func(t *testing.T) {
			re := require.New(t)
			cluster, err := tests.NewTestCluster(t.Context(), 1, func(cfg *config.Config, _ string) {
				cfg.Security.RedactInfoLog = mode
			})
			re.NoError(err)
			defer cluster.Destroy()
			re.NoError(cluster.RunInitialServers())
			re.NotEmpty(cluster.WaitLeader())
			leader := cluster.GetLeaderServer()
			re.NoError(leader.GetServer().SetAuditConfig(config.AuditConfig{EnableAudit: true}))
			for _, tc := range []struct{ path, body, service string }{
				{path: "/admin/log", body: `"error"`, service: "SetLogLevel"},
				{path: "/config", body: `{"log.level":"error"}`, service: "SetConfig"},
			} {
				t.Run(tc.service, func(t *testing.T) {
					re := require.New(t)
					logFile := testutil.InitTempFileLogger(t, "info")
					re.NoError(testutil.CheckPostJSON(tests.TestDialClient, leader.GetAddr()+"/pd/api/v1"+tc.path,
						[]byte(tc.body), testutil.StatusOK(re)))
					re.Equal(zap.ErrorLevel, log.GetLevel())
					data, err := os.ReadFile(logFile)
					re.NoError(err)
					re.Equal(1, strings.Count(string(data), "ServiceLabel:"+tc.service+","))
					re.Contains(string(data), "audit log")
					if mode == logutil.RedactInfoLogON {
						re.Contains(string(data), "BodyParam:?")
					} else {
						re.Contains(string(data), "error")
					}
				})
			}
		})
	}
}
