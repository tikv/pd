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
	"encoding/json"
	"fmt"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/unsaferecovery"
	tu "github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
)

type unsafeOperationTestSuite struct {
	suite.Suite
	svr       *server.Server
	cleanup   tu.CleanupFunc
	urlPrefix string
}

func TestUnsafeOperationTestSuite(t *testing.T) {
	suite.Run(t, new(unsafeOperationTestSuite))
}

func (suite *unsafeOperationTestSuite) SetupTest() {
	re := suite.Require()
	suite.svr, suite.cleanup = mustNewServer(re)
	server.MustWaitLeader(re, []*server.Server{suite.svr})

	addr := suite.svr.GetAddr()
	suite.urlPrefix = fmt.Sprintf("%s%s/api/v1/admin/unsafe", addr, apiPrefix)

	mustBootstrapCluster(re, suite.svr)
	mustPutStore(re, suite.svr, 1, metapb.StoreState_Offline, metapb.NodeState_Removing, nil)
}

func (suite *unsafeOperationTestSuite) TearDownTest() {
	suite.cleanup()
}

func (suite *unsafeOperationTestSuite) TestRemoveFailedStores() {
	re := suite.Require()

	input := map[string]any{"stores": []uint64{}}
	data, _ := json.Marshal(input)
	err := tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/remove-failed-stores", data, tu.StatusNotOK(re),
		tu.StringEqual(re, "\"[PD:unsaferecovery:ErrUnsafeRecoveryInvalidInput]invalid input no store specified\"\n"))
	re.NoError(err)

	input = map[string]any{"stores": []string{"abc", "def"}}
	data, _ = json.Marshal(input)
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/remove-failed-stores", data, tu.StatusNotOK(re),
		tu.StringEqual(re, "\"Store ids are invalid\"\n"))
	re.NoError(err)

	input = map[string]any{"stores": []uint64{1, 2}}
	data, _ = json.Marshal(input)
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/remove-failed-stores", data, tu.StatusNotOK(re),
		tu.StringEqual(re, "\"[PD:unsaferecovery:ErrUnsafeRecoveryInvalidInput]invalid input store 2 doesn't exist\"\n"))
	re.NoError(err)

	input = map[string]any{"stores": []uint64{1}}
	data, _ = json.Marshal(input)
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/remove-failed-stores", data, tu.StatusOK(re))
	re.NoError(err)

	// Test show
	var output []unsaferecovery.StageOutput
	err = tu.ReadGetJSON(re, testDialClient, suite.urlPrefix+"/remove-failed-stores/show", &output)
	re.NoError(err)
}

func (suite *unsafeOperationTestSuite) TestRemoveFailedStoresAutoDetect() {
	re := suite.Require()

	input := map[string]any{"auto-detect": false}
	data, _ := json.Marshal(input)
	err := tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/remove-failed-stores", data, tu.StatusNotOK(re),
		tu.StringEqual(re, "\"Store ids are invalid\"\n"))
	re.NoError(err)

	input = map[string]any{"auto-detect": true}
	data, _ = json.Marshal(input)
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/remove-failed-stores", data, tu.StatusOK(re))
	re.NoError(err)
}
