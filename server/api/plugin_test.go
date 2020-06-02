package api

import (
	"encoding/json"
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/pd/v4/server"
)

var _ = Suite(&testPluginSuite{})

type testPluginSuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func (s *testPluginSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = mustNewServer(c)
	mustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1/plugin", addr, apiPrefix)

	mustBootstrapCluster(c, s.svr)
}

func (s *testPluginSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testPluginSuite) TestLoadPlugin(c *C) {
	labels := map[string]string{"a": "a"}
	data, err := json.Marshal(labels)
	c.Assert(err, IsNil)
	err = postJSON(testDialClient, s.urlPrefix, data)
	c.Assert(err, IsNil)
}

func (s *testPluginSuite) TestUnloadPlugin(c *C) {
	doDelete(testDialClient, s.urlPrefix)
}
