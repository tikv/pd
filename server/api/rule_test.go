package api

import (
	"fmt"
	. "github.com/pingcap/check"
	"github.com/pingcap/pd/v4/server"
	"github.com/pingcap/pd/v4/server/schedule/placement"
)

var _ = Suite(&testRuleSuite{})

type testRuleSuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func (s *testRuleSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = mustNewServer(c)
	mustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1/config", addr, apiPrefix)

	mustBootstrapCluster(c, s.svr)
}

func (s *testRuleSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testRuleSuite) TestGetAll(c *C) {
	stat :=  []*placement.Rule{}
	err := readJSON(testDialClient, s.urlPrefix+"/rules", &stat)
	c.Assert(err, IsNil)
}

func (s *testRuleSuite) TestGetAllByGroup(c *C) {
	stat :=  []*placement.Rule{}
	err := readJSON(testDialClient, s.urlPrefix+"/rules/group/a", &stat)
	c.Assert(err, IsNil)
}

func (s *testRuleSuite) TestGetAllByRegion(c *C) {
	stat :=  []*placement.Rule{}
	err := readJSON(testDialClient, s.urlPrefix+"/rules/region/10", &stat)
	c.Assert(err, IsNil)
}

func (s *testRuleSuite) TestGetAllByKey(c *C) {
	stat :=  []*placement.Rule{}
	err := readJSON(testDialClient, s.urlPrefix+"/rules/key/a", &stat)
	c.Assert(err, IsNil)
}

func (s *testRuleSuite) TestGet(c *C) {
	stat :=  placement.Rule{}
	err := readJSON(testDialClient, s.urlPrefix+"/rule/a/1", &stat)
	c.Assert(err, IsNil)
}

func (s *testRuleSuite) TestSet(c *C) {
	err := postJSON(testDialClient, s.urlPrefix+"/rule", nil)
	c.Assert(err, IsNil)
}

func (s *testRuleSuite) TestDelete(c *C) {
	//requestStatusBody(c, testDialClient, http.MethodDelete, s.urlPrefix+"/rule/a/1")
	doDelete(testDialClient, s.urlPrefix+"/rule/a/1")
}