package api

import (
	"encoding/json"
	"fmt"
	. "github.com/pingcap/check"
	"github.com/pingcap/log"
	"github.com/pingcap/pd/v4/server"
)

var _ = Suite(&testLogSuite{})

type testLogSuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func (s *testLogSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = mustNewServer(c)
	mustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1/admin", addr, apiPrefix)

	mustBootstrapCluster(c, s.svr)
}

func (s *testLogSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testLogSuite) TestSetLogLevel(c *C) {
	level := "error"
	if log.GetLevel().String() == level{
		level = "warn"
	}
	data, err := json.Marshal(level)
	c.Assert(err, IsNil)
	err = postJSON(testDialClient, s.urlPrefix+"/log", data)
	c.Assert(err, IsNil)
	c.Assert(log.GetLevel().String(), Equals, level)
}
