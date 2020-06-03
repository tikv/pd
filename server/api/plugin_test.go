package api

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

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

func (s *testPluginSuite) TestLoadandUnloadPlugin(c *C) {
	//load
	labels := map[string]string{"plugin-path": "./pd/plugin/scheduler_example/evict_leader.go"}
	data, err := json.Marshal(labels)
	c.Assert(err, IsNil)
	err = postJSON(testDialClient, s.urlPrefix, data)
	c.Assert(err, IsNil)

	//unload
	labels = map[string]string{"plugin-path": "./pd/plugin/scheduler_example/evict_leader.go"}
	data, err = json.Marshal(labels)
	c.Assert(err, IsNil)
	req, err := http.NewRequest(http.MethodDelete, s.urlPrefix, strings.NewReader(string(data)))
	c.Assert(err, IsNil)
	resp, err := testDialClient.Do(req)
	c.Assert(err, IsNil)
	_ , err = ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	err = resp.Body.Close()
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
}


