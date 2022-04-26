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
	"github.com/tikv/pd/server"
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
}

func (s *testUnsafeAPISuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testUnsafeAPISuite) TestRemoveFailedStores(c *C) {
	input := map[string]interface{}{"stores": []uint64{}}
	data, _ := json.Marshal(input)
	err := postJSON(testDialClient, s.urlPrefix+"/remove-failed-stores", data)
	c.Assert(err.Error(), Equals, "\"[PD:unsaferecovery:ErrUnsafeRecoveryInvalidInput]invalid input no store specified\"\n")
	input = map[string]interface{}{"stores": []string{"abc", "def"}}
	data, _ = json.Marshal(input)
	err = postJSON(testDialClient, s.urlPrefix+"/remove-failed-stores", data)
	c.Assert(err.Error(), Equals, "\"Store ids are invalid\"\n")
	input = map[string]interface{}{"stores": []uint64{1, 2}, "timeout": 3600}
	data, _ = json.Marshal(input)
	err = postJSON(testDialClient, s.urlPrefix+"/remove-failed-stores", data)
	c.Assert(err, IsNil)
	// Test show
	var output []string
	err = readJSON(testDialClient, s.urlPrefix+"/remove-failed-stores/show", &output)
	c.Assert(err, IsNil)
	// Test history
	err = readJSON(testDialClient, s.urlPrefix+"/remove-failed-stores/history", &output)
	c.Assert(err, IsNil)
}
