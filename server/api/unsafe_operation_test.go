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

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	tu "github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/cluster"
)

var _ = Suite(&testUnsafeAPISuite{})

type testUnsafeAPISuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func (s *testUnsafeAPISuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = mustNewServer(c)
	mustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1/admin/unsafe", addr, apiPrefix)

	mustBootstrapCluster(c, s.svr)
	mustPutStore(c, s.svr, 1, metapb.StoreState_Offline, metapb.NodeState_Removing, nil)
}

func (s *testUnsafeAPISuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testUnsafeAPISuite) TestRemoveFailedStores(c *C) {
	input := map[string]interface{}{"stores": []uint64{}}
	data, _ := json.Marshal(input)
	err := tu.CheckPostJSON(testDialClient, s.urlPrefix+"/remove-failed-stores", data, tu.StatusNotOK(c),
		tu.StringEqual(c, "\"[PD:unsaferecovery:ErrUnsafeRecoveryInvalidInput]invalid input no store specified\"\n"))
	c.Assert(err, IsNil)

	input = map[string]interface{}{"stores": []string{"abc", "def"}}
	data, _ = json.Marshal(input)
	err = tu.CheckPostJSON(testDialClient, s.urlPrefix+"/remove-failed-stores", data, tu.StatusNotOK(c),
		tu.StringEqual(c, "\"Store ids are invalid\"\n"))
	c.Assert(err, IsNil)

	input = map[string]interface{}{"stores": []uint64{1, 2}}
	data, _ = json.Marshal(input)
	err = tu.CheckPostJSON(testDialClient, s.urlPrefix+"/remove-failed-stores", data, tu.StatusNotOK(c),
		tu.StringEqual(c, "\"[PD:unsaferecovery:ErrUnsafeRecoveryInvalidInput]invalid input store 2 doesn't exist\"\n"))
	c.Assert(err, IsNil)

	input = map[string]interface{}{"stores": []uint64{1}}
	data, _ = json.Marshal(input)
	err = tu.CheckPostJSON(testDialClient, s.urlPrefix+"/remove-failed-stores", data, tu.StatusOK(c))
	c.Assert(err, IsNil)

	// Test show
	var output []cluster.StageOutput
	err = tu.ReadGetJSON(c, testDialClient, s.urlPrefix+"/remove-failed-stores/show", &output)
	c.Assert(err, IsNil)
}
