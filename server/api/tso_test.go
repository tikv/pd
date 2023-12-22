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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	tu "github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
)

type tsoTestSuite struct {
	suite.Suite
	svr       *server.Server
	cleanup   tu.CleanupFunc
	urlPrefix string
}

func TestTSOTestSuite(t *testing.T) {
	suite.Run(t, new(tsoTestSuite))
}

func (suite *tsoTestSuite) SetupSuite() {
	re := suite.Require()
	suite.svr, suite.cleanup = mustNewServer(re, func(cfg *config.Config) {
		cfg.EnableLocalTSO = true
		cfg.Labels[config.ZoneLabel] = "dc-1"
	})
	server.MustWaitLeader(re, []*server.Server{suite.svr})

	addr := suite.svr.GetAddr()
	suite.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)
}

func (suite *tsoTestSuite) TearDownSuite() {
	suite.cleanup()
}

func (suite *tsoTestSuite) TestTransferAllocator() {
	re := suite.Require()
	tu.Eventually(re, func() bool {
		suite.svr.GetTSOAllocatorManager().ClusterDCLocationChecker()
		_, err := suite.svr.GetTSOAllocatorManager().GetAllocator("dc-1")
		return err == nil
	}, tu.WithWaitFor(15*time.Second), tu.WithTickInterval(3*time.Second))
	addr := suite.urlPrefix + "/tso/allocator/transfer/pd1?dcLocation=dc-1"
	err := tu.CheckPostJSON(testDialClient, addr, nil, tu.StatusOK(re))
	re.NoError(err)
}
