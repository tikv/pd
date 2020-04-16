// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"encoding/json"
	"fmt"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/pd/v4/server"
)

var _ = Suite(&testComponentSuite{})

type testComponentSuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func (s *testComponentSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = mustNewServer(c)
	mustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)
}

func (s *testComponentSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testComponentSuite) TestComponent(c *C) {
	// register not happen
	addr := fmt.Sprintf("%s/component", s.urlPrefix)
	output := make(map[string][]string)
	err := readJSON(addr, &output)
	c.Assert(err, IsNil)
	c.Assert(len(output), Equals, 0)

	addr1 := fmt.Sprintf("%s/component/c1", s.urlPrefix)
	var output1 []string
	err = readJSON(addr1, &output)
	c.Assert(strings.Contains(err.Error(), "404"), IsTrue)
	c.Assert(len(output1), Equals, 0)

	// register 2 c1 and 1 c2
	reqs := []map[string]string{
		{"component": "c1", "addr": "127.0.0.1:1"},
		{"component": "c1", "addr": "127.0.0.1:2"},
		{"component": "c2", "addr": "127.0.0.1:3"},
		{"component": "c3", "addr": "example.com"},
	}
	for _, req := range reqs {
		postData, err := json.Marshal(req)
		c.Assert(err, IsNil)
		err = postJSON(addr, postData)
		c.Assert(err, IsNil)
	}

	// get all addresses
	expected := map[string][]string{
		"c1": {"127.0.0.1:1", "127.0.0.1:2"},
		"c2": {"127.0.0.1:3"},
		"c3": {"example.com"},
	}

	output = make(map[string][]string)
	err = readJSON(addr, &output)
	c.Assert(err, IsNil)
	c.Assert(output, DeepEquals, expected)

	// get the specific component addresses
	expected1 := []string{"127.0.0.1:1", "127.0.0.1:2"}
	var output2 []string
	err = readJSON(addr1, &output2)
	c.Assert(err, IsNil)
	c.Assert(output2, DeepEquals, expected1)

	addr2 := fmt.Sprintf("%s/component/c2", s.urlPrefix)
	expected2 := []string{"127.0.0.1:3"}
	var output3 []string
	err = readJSON(addr2, &output3)
	c.Assert(err, IsNil)
	c.Assert(output3, DeepEquals, expected2)

	// unregister address
	addr3 := fmt.Sprintf("%s/component/c1/127.0.0.1:1", s.urlPrefix)
	res, err := doDelete(addr3)
	c.Assert(err, IsNil)
	c.Assert(res.StatusCode, Equals, 200)

	expected3 := map[string][]string{
		"c1": {"127.0.0.1:2"},
		"c2": {"127.0.0.1:3"},
		"c3": {"example.com"},
	}
	output = make(map[string][]string)
	err = readJSON(addr, &output)
	c.Assert(err, IsNil)
	c.Assert(output, DeepEquals, expected3)

	addr4 := fmt.Sprintf("%s/component/c1/127.0.0.1:2", s.urlPrefix)
	res, err = doDelete(addr4)
	c.Assert(err, IsNil)
	c.Assert(res.StatusCode, Equals, 200)
	expected4 := map[string][]string{
		"c2": {"127.0.0.1:3"},
		"c3": {"example.com"},
	}
	output = make(map[string][]string)
	err = readJSON(addr, &output)
	c.Assert(err, IsNil)
	c.Assert(output, DeepEquals, expected4)
}
