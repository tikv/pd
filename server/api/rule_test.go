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

func (s *testRuleSuite) Testrule(c *C) {
	c.Assert(postJSON(testDialClient, s.urlPrefix, []byte(`{"enable-placement-rules":"true"}`)), IsNil)
	//Set
	err := postJSON(testDialClient, s.urlPrefix+"/rule", nil)
	c.Assert(err, IsNil)

	//Delete
	doDelete(testDialClient, s.urlPrefix+"/rule/a/1")

	//GetAll
	stat :=  []*placement.Rule{}
	err = readJSON(testDialClient, s.urlPrefix+"/rules", &stat)
	c.Assert(err, IsNil)

	//GetAllByGroup
	stat =  []*placement.Rule{}
	err = readJSON(testDialClient, s.urlPrefix+"/rules/group/a", &stat)
	c.Assert(err, IsNil)

	//GetAllByRegion
	stat =  []*placement.Rule{}
	err = readJSON(testDialClient, s.urlPrefix+"/rules/region/10", &stat)
	c.Assert(err, IsNil)

	//GetAllByKey
	stat =  []*placement.Rule{}
	err = readJSON(testDialClient, s.urlPrefix+"/rules/key/a", &stat)
	c.Assert(err, IsNil)

	//Get
	info :=  placement.Rule{}
	err = readJSON(testDialClient, s.urlPrefix+"/rule/a/1", &info)
	c.Assert(err, IsNil)

}
