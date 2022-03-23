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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	tu "github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server"
)

var _ = Suite(&testCheckerSuite{})

type testCheckerSuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func (s *testCheckerSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = mustNewServer(c)
	mustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1/checker", addr, apiPrefix)

	mustBootstrapCluster(c, s.svr)
	mustPutStore(c, s.svr, 1, metapb.StoreState_Up, metapb.NodeState_Serving, nil)
	mustPutStore(c, s.svr, 2, metapb.StoreState_Up, metapb.NodeState_Serving, nil)
}

func (s *testCheckerSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testCheckerSuite) TestAPI(c *C) {
	s.testErrCases(c)

	cases := []struct {
		name string
	}{
		{name: "learner"},
		{name: "replica"},
		{name: "rule"},
		{name: "split"},
		{name: "merge"},
		{name: "joint-state"},
	}
	for _, ca := range cases {
		s.testGetStatus(ca.name, c)
		s.testPauseOrResume(ca.name, c)
	}
}

func (s *testCheckerSuite) testErrCases(c *C) {
	// missing args
	input := make(map[string]interface{})
	pauseArgs, err := json.Marshal(input)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, s.urlPrefix+"/merge", pauseArgs, tu.StatusNotOK(c))
	c.Assert(err, IsNil)

	// negative delay
	input["delay"] = -10
	pauseArgs, err = json.Marshal(input)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, s.urlPrefix+"/merge", pauseArgs, tu.StatusNotOK(c))
	c.Assert(err, IsNil)

	// wrong name
	name := "dummy"
	input["delay"] = 30
	pauseArgs, err = json.Marshal(input)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, s.urlPrefix+"/"+name, pauseArgs, tu.StatusNotOK(c))
	c.Assert(err, IsNil)
	input["delay"] = 0
	pauseArgs, err = json.Marshal(input)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, s.urlPrefix+"/"+name, pauseArgs, tu.StatusNotOK(c))
	c.Assert(err, IsNil)
}

func (s *testCheckerSuite) testGetStatus(name string, c *C) {
	handler := s.svr.GetHandler()

	// normal run
	resp := make(map[string]interface{})
	err := tu.ReadGetJSON(c, testDialClient, fmt.Sprintf("%s/%s", s.urlPrefix, name), &resp)
	c.Assert(err, IsNil)
	c.Assert(resp["paused"], IsFalse)
	// paused
	err = handler.PauseOrResumeChecker(name, 30)
	c.Assert(err, IsNil)
	resp = make(map[string]interface{})
	err = tu.ReadGetJSON(c, testDialClient, fmt.Sprintf("%s/%s", s.urlPrefix, name), &resp)
	c.Assert(err, IsNil)
	c.Assert(resp["paused"], IsTrue)
	// resumed
	err = handler.PauseOrResumeChecker(name, 1)
	c.Assert(err, IsNil)
	time.Sleep(time.Second)
	resp = make(map[string]interface{})
	err = tu.ReadGetJSON(c, testDialClient, fmt.Sprintf("%s/%s", s.urlPrefix, name), &resp)
	c.Assert(err, IsNil)
	c.Assert(resp["paused"], IsFalse)
}

func (s *testCheckerSuite) testPauseOrResume(name string, c *C) {
	handler := s.svr.GetHandler()
	input := make(map[string]interface{})

	// test pause.
	input["delay"] = 30
	pauseArgs, err := json.Marshal(input)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, s.urlPrefix+"/"+name, pauseArgs, tu.StatusOK(c))
	c.Assert(err, IsNil)
	isPaused, err := handler.IsCheckerPaused(name)
	c.Assert(err, IsNil)
	c.Assert(isPaused, IsTrue)
	input["delay"] = 1
	pauseArgs, err = json.Marshal(input)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, s.urlPrefix+"/"+name, pauseArgs, tu.StatusOK(c))
	c.Assert(err, IsNil)
	time.Sleep(time.Second)
	isPaused, err = handler.IsCheckerPaused(name)
	c.Assert(err, IsNil)
	c.Assert(isPaused, IsFalse)

	// test resume.
	input = make(map[string]interface{})
	input["delay"] = 30
	pauseArgs, err = json.Marshal(input)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, s.urlPrefix+"/"+name, pauseArgs, tu.StatusOK(c))
	c.Assert(err, IsNil)
	input["delay"] = 0
	pauseArgs, err = json.Marshal(input)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, s.urlPrefix+"/"+name, pauseArgs, tu.StatusOK(c))
	c.Assert(err, IsNil)
	isPaused, err = handler.IsCheckerPaused(name)
	c.Assert(err, IsNil)
	c.Assert(isPaused, IsFalse)
}
