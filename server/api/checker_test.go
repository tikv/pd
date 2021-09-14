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
	"github.com/tikv/pd/server"
	_ "github.com/tikv/pd/server/schedulers"
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
	mustPutStore(c, s.svr, 1, metapb.StoreState_Up, nil)
	mustPutStore(c, s.svr, 2, metapb.StoreState_Up, nil)
}

func (s *testCheckerSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testCheckerSuite) TestAPI(c *C) {
	type arg struct {
		opt   string
		value interface{}
	}
	cases := []struct {
		name          string
		args          []arg
		extraTestFunc func(name string, c *C)
	}{
		{name: "learner"},
		{name: "replica"},
		{name: "rule"},
		{name: "split"},
		{name: "merge"},
		{name: "joint-state"},
		{name: "priority"},
	}
	for _, ca := range cases {
		input := make(map[string]interface{})
		input["name"] = ca.name
		for _, a := range ca.args {
			input[a.opt] = a.value
		}
		body, err := json.Marshal(input)
		c.Assert(err, IsNil)
		s.testPauseOrResume(ca.name, body, ca.extraTestFunc, c)
	}
}

func (s *testCheckerSuite) testPauseOrResume(name string, body []byte, extraTest func(string, *C), c *C) {
	handler := s.svr.GetHandler()

	// test pause.
	input := make(map[string]interface{})
	input["delay"] = 30
	pauseArgs, err := json.Marshal(input)
	c.Assert(err, IsNil)
	err = postJSON(testDialClient, s.urlPrefix+"/"+name, pauseArgs)
	c.Assert(err, IsNil)
	isPaused, err := handler.IsCheckerPaused(name)
	c.Assert(err, IsNil)
	c.Assert(isPaused, Equals, true)
	input["delay"] = 1
	pauseArgs, err = json.Marshal(input)
	c.Assert(err, IsNil)
	err = postJSON(testDialClient, s.urlPrefix+"/"+name, pauseArgs)
	c.Assert(err, IsNil)
	time.Sleep(time.Second)
	isPaused, err = handler.IsCheckerPaused(name)
	c.Assert(err, IsNil)
	c.Assert(isPaused, Equals, false)

	// test resume.
	input = make(map[string]interface{})
	input["delay"] = 30
	pauseArgs, err = json.Marshal(input)
	c.Assert(err, IsNil)
	err = postJSON(testDialClient, s.urlPrefix+"/"+name, pauseArgs)
	c.Assert(err, IsNil)
	input["delay"] = 0
	pauseArgs, err = json.Marshal(input)
	c.Assert(err, IsNil)
	err = postJSON(testDialClient, s.urlPrefix+"/"+name, pauseArgs)
	c.Assert(err, IsNil)
	isPaused, err = handler.IsCheckerPaused(name)
	c.Assert(err, IsNil)
	c.Assert(isPaused, Equals, false)

	if extraTest != nil {
		extraTest(name, c)
	}
}
