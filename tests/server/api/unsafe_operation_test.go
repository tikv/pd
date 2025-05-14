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
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/unsaferecovery"
	tu "github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/cluster"
	"github.com/tikv/pd/tests"
)

type unsafeOperationTestSuite struct {
	suite.Suite
	env *tests.SchedulingTestEnvironment
}

func TestUnsafeOperationTestSuite(t *testing.T) {
	suite.Run(t, new(unsafeOperationTestSuite))
}

func (suite *unsafeOperationTestSuite) SetupTest() {
	cluster.DefaultMinResolvedTSPersistenceInterval = time.Millisecond
	suite.env = tests.NewSchedulingTestEnvironment(suite.T())
}

func (suite *unsafeOperationTestSuite) TearDownTest() {
	suite.env.Cleanup()
}

func (suite *unsafeOperationTestSuite) TestRemoveFailedStores() {
	suite.env.RunTestInNonMicroserviceEnv(suite.checkRemoveFailedStores)
}

func (suite *unsafeOperationTestSuite) checkRemoveFailedStores(cluster *tests.TestCluster) {
	re := suite.Require()

	tests.MustPutStore(re, cluster, &metapb.Store{
		Id:            1,
		Address:       "mock://tikv-1:1",
		State:         metapb.StoreState_Offline,
		NodeState:     metapb.NodeState_Removing,
		LastHeartbeat: time.Now().UnixNano(),
	})

	leader := cluster.GetLeaderServer()
	urlPrefix := leader.GetAddr() + "/pd/api/v1/admin/unsafe"

	input := map[string]any{"stores": []uint64{}}
	data, _ := json.Marshal(input)
	err := tu.CheckPostJSON(tests.TestDialClient, urlPrefix+"/remove-failed-stores", data, tu.StatusNotOK(re),
		tu.StringEqual(re, "\"[PD:unsaferecovery:ErrUnsafeRecoveryInvalidInput]invalid input no store specified\"\n"))
	re.NoError(err)

	input = map[string]any{"stores": []string{"abc", "def"}}
	data, _ = json.Marshal(input)
	err = tu.CheckPostJSON(tests.TestDialClient, urlPrefix+"/remove-failed-stores", data, tu.StatusNotOK(re),
		tu.StringEqual(re, "\"Store ids are invalid\"\n"))
	re.NoError(err)

	input = map[string]any{"stores": []uint64{1, 2}}
	data, _ = json.Marshal(input)
	err = tu.CheckPostJSON(tests.TestDialClient, urlPrefix+"/remove-failed-stores", data, tu.StatusNotOK(re),
		tu.StringEqual(re, "\"[PD:unsaferecovery:ErrUnsafeRecoveryInvalidInput]invalid input store 2 doesn't exist\"\n"))
	re.NoError(err)

	input = map[string]any{"stores": []uint64{1}}
	data, _ = json.Marshal(input)
	err = tu.CheckPostJSON(tests.TestDialClient, urlPrefix+"/remove-failed-stores", data, tu.StatusOK(re))
	re.NoError(err)

	// Test show
	var output []unsaferecovery.StageOutput
	err = tu.ReadGetJSON(re, tests.TestDialClient, urlPrefix+"/remove-failed-stores/show", &output)
	re.NoError(err)
}

func (suite *unsafeOperationTestSuite) TestRemoveFailedStoresAutoDetect() {
	suite.env.RunTestInNonMicroserviceEnv(suite.checkRemoveFailedStoresAutoDetect)
}

func (suite *unsafeOperationTestSuite) checkRemoveFailedStoresAutoDetect(cluster *tests.TestCluster) {
	re := suite.Require()

	tests.MustPutStore(re, cluster, &metapb.Store{
		Id:            1,
		Address:       "mock://tikv-1:1",
		State:         metapb.StoreState_Offline,
		NodeState:     metapb.NodeState_Removing,
		LastHeartbeat: time.Now().UnixNano(),
	})
	leader := cluster.GetLeaderServer()
	urlPrefix := leader.GetAddr() + "/pd/api/v1/admin/unsafe"

	input := map[string]any{"auto-detect": false}
	data, _ := json.Marshal(input)
	err := tu.CheckPostJSON(tests.TestDialClient, urlPrefix+"/remove-failed-stores", data, tu.StatusNotOK(re),
		tu.StringEqual(re, "\"Store ids are invalid\"\n"))
	re.NoError(err)

	input = map[string]any{"auto-detect": true}
	data, _ = json.Marshal(input)
	err = tu.CheckPostJSON(tests.TestDialClient, urlPrefix+"/remove-failed-stores", data, tu.StatusOK(re))
	re.NoError(err)
}
