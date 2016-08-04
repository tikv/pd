// Copyright 2016 PingCAP, Inc.
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
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/pd/server"
)

func TestMemberAPI(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testMemberAPISuite{})

type testMemberAPISuite struct {
	hc *http.Client
}

func (s *testMemberAPISuite) SetUpSuite(c *C) {
	s.hc = newHTTPClient()
}

func newHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 10 * time.Second,
	}
}

type cleanUpFunc func()

func mustNewCluster(c *C, num int) ([]*server.Config, []*server.Server, cleanUpFunc) {
	dirs := make([]string, 0, num)
	svrs := make([]*server.Server, 0, num)
	cfgs := server.NewTestMultiConfig(num)

	ch := make(chan *server.Server, num)
	for _, cfg := range cfgs {
		dirs = append(dirs, cfg.DataDir)

		go func(cfg *server.Config) {
			s, e := server.NewServer(cfg)
			c.Assert(e, IsNil)
			go s.Run()
			go ServeHTTP(cfg.HTTPAddr, s)
			ch <- s
		}(cfg)
	}

	for i := 0; i < num; i++ {
		svr := <-ch
		svrs = append(svrs, svr)
	}
	close(ch)

	// wait etcds and http servers
	time.Sleep(5 * time.Second)

	// clean up
	clean := func() {
		for _, s := range svrs {
			s.Close()
		}
		for _, dir := range dirs {
			os.RemoveAll(dir)
		}
	}

	return cfgs, svrs, clean
}

func relaxEqualStings(c *C, a, b []string) {
	sort.Strings(a)
	sortedStringA := strings.Join(a, "")

	sort.Strings(b)
	sortedStringB := strings.Join(b, "")

	c.Assert(sortedStringA, Equals, sortedStringB)
}

func checkListResponse(c *C, body []byte, cfgs []*server.Config) {
	got := make(map[string][]memberInfo)
	json.Unmarshal(body, &got)

	c.Assert(len(got["members"]), Equals, len(cfgs))

	for _, memb := range got["members"] {
		for _, cfg := range cfgs {
			if memb.Name != cfg.Name {
				continue
			}

			relaxEqualStings(c, memb.ClientUrls, strings.Split(cfg.ClientUrls, ","))
			relaxEqualStings(c, memb.PeerUrls, strings.Split(cfg.PeerUrls, ","))
		}
	}
}

func (s *testMemberAPISuite) TestMemberList(c *C) {
	numbers := []int{1, 3}

	for _, num := range numbers {
		cfgs, _, clean := mustNewCluster(c, num)
		defer clean()

		parts := []string{"http://", cfgs[rand.Intn(len(cfgs))].HTTPAddr, "/api/v1/members"}
		resp, err := s.hc.Get(strings.Join(parts, ""))
		c.Assert(err, IsNil)
		buf, err := ioutil.ReadAll(resp.Body)
		c.Assert(err, IsNil)
		checkListResponse(c, buf, cfgs)
	}
}

func (s *testMemberAPISuite) TestMemberDelete(c *C) {
	cfgs, _, clean := mustNewCluster(c, 3)
	defer clean()

	someone := rand.Intn(len(cfgs))
	newCfgs := append(cfgs[:someone], cfgs[someone+1:]...)

	var table = []struct {
		name    string
		addr    string
		checker Checker
		status  int
	}{
		{
			// delete a nonexistent pd
			name:    fmt.Sprintf("pd%d", rand.Int63()),
			addr:    cfgs[rand.Intn(len(cfgs))].HTTPAddr,
			checker: Equals,
			status:  http.StatusNotFound,
		},
		{
			// delete a pd randomly
			name:    cfgs[someone].Name,
			addr:    cfgs[rand.Intn(len(cfgs))].HTTPAddr,
			checker: Equals,
			status:  http.StatusOK,
		},
		{
			// delete it again
			name:    cfgs[someone].Name,
			addr:    newCfgs[rand.Intn(len(newCfgs))].HTTPAddr,
			checker: Not(Equals),
			status:  http.StatusOK,
		},
	}

	for _, t := range table {
		parts := []string{"http://", t.addr, "/api/v1/members", "/" + t.name}
		req, err := http.NewRequest(http.MethodDelete, strings.Join(parts, ""), nil)
		c.Assert(err, IsNil)
		resp, err := s.hc.Do(req)
		c.Assert(err, IsNil)
		c.Assert(resp.StatusCode, t.checker, t.status)
	}

	parts := []string{"http://", cfgs[rand.Intn(len(newCfgs))].HTTPAddr, "/api/v1/members"}
	resp, err := s.hc.Get(strings.Join(parts, ""))
	c.Assert(err, IsNil)
	buf, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	checkListResponse(c, buf, newCfgs)
}
