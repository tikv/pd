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
	"io/ioutil"

	. "github.com/pingcap/check"
	"github.com/tikv/pd/server"
)

var _ = Suite(&ProfSuite{})

type ProfSuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func (s *ProfSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = mustNewServer(c)
	mustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1/debug", addr, apiPrefix)

	mustBootstrapCluster(c, s.svr)
}

func (s *ProfSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *ProfSuite) TestGetZip(c *C) {
	rsp, err := testDialClient.Get(s.urlPrefix + "/pprof/zip?" + "seconds=5s")
	c.Assert(err, IsNil)
	defer rsp.Body.Close()
	body, err := ioutil.ReadAll(rsp.Body)
	c.Assert(err, IsNil)
	c.Assert(body, NotNil)
	zipReader, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	c.Assert(err, IsNil)
	c.Assert(zipReader.File, HasLen, 7)
}
