// Copyright 2021 TiKV Project Authors.
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
	"archive/zip"
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/tikv/pd/tests"
)

type pprofTestSuite struct {
	suite.Suite
	env *tests.SchedulingTestEnvironment
}

func TestPprofTestSuite(t *testing.T) {
	suite.Run(t, new(pprofTestSuite))
}

func (suite *pprofTestSuite) SetupSuite() {
	suite.env = tests.NewSchedulingTestEnvironment(suite.T())
}

func (suite *pprofTestSuite) TearDownSuite() {
	suite.env.Cleanup()
}

func (suite *pprofTestSuite) TestGetZip() {
	suite.env.RunTestInNonMicroserviceEnv(suite.checkGetZip)
}

func (suite *pprofTestSuite) checkGetZip(cluster *tests.TestCluster) {
	re := suite.Require()
	leader := cluster.GetLeaderServer()
	urlPrefix := leader.GetAddr() + "/pd/api/v1"

	rsp, err := tests.TestDialClient.Get(urlPrefix + "/debug/pprof/zip?" + "seconds=5")
	re.NoError(err)
	defer rsp.Body.Close()
	body, err := io.ReadAll(rsp.Body)
	re.NoError(err)
	re.NotNil(body)
	zipReader, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	re.NoError(err)
	re.Len(zipReader.File, 7)
}
