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
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
)

type profTestSuite struct {
	suite.Suite
	svr       *server.Server
	cleanup   testutil.CleanupFunc
	urlPrefix string
}

func TestProfTestSuite(t *testing.T) {
	suite.Run(t, new(profTestSuite))
}

func (suite *profTestSuite) SetupSuite() {
	re := suite.Require()
	suite.svr, suite.cleanup = mustNewServer(re)
	server.MustWaitLeader(re, []*server.Server{suite.svr})

	addr := suite.svr.GetAddr()
	suite.urlPrefix = fmt.Sprintf("%s%s/api/v1/debug", addr, apiPrefix)

	mustBootstrapCluster(re, suite.svr)
}

func (suite *profTestSuite) TearDownSuite() {
	suite.cleanup()
}

func (suite *profTestSuite) TestGetZip() {
	re := suite.Require()
	rsp, err := testDialClient.Get(suite.urlPrefix + "/pprof/zip?" + "seconds=5")
	re.NoError(err)
	defer rsp.Body.Close()
	body, err := io.ReadAll(rsp.Body)
	re.NoError(err)
	re.NotNil(body)
	zipReader, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	re.NoError(err)
	re.Len(zipReader.File, 7)
}
