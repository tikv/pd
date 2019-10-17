// Copyright 2019 PingCAP, Inc.
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
	"fmt"
	"strings"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server"
)

var _ = Suite(&testTSOSuite{})

type testTSOSuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func makeTS(offset time.Duration) int64 {
	physical := time.Now().Add(offset).UnixNano() / int64(time.Millisecond)
	return physical << 18
}

func (s *testTSOSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = mustNewServer(c)
	mustWaitLeader(c, []*server.Server{s.svr})
	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1/recovery/tso/", addr, apiPrefix)

	mustBootstrapCluster(c, s.svr)
	mustPutStore(c, s.svr, 1, metapb.StoreState_Up, nil)
}

func (s *testTSOSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testTSOSuite) TestResetTS(c *C) {
	t1 := makeTS(time.Hour)
	url := fmt.Sprintf("%s%d", s.urlPrefix, t1)
	err := postJSON(url, nil)
	c.Assert(err, IsNil)

	t2 := makeTS(32 * time.Hour)
	url = fmt.Sprintf("%s%d", s.urlPrefix, t2)
	err = postJSON(url, nil)
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "too large"), IsTrue)

	t3 := makeTS(-time.Hour)
	url = fmt.Sprintf("%s%d", s.urlPrefix, t3)
	err = postJSON(url, nil)
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "smaller"), IsTrue)
}
