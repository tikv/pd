// Copyright 2016 TiKV Project Authors.
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

package server

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"

	. "github.com/pingcap/check"
	"github.com/tikv/pd/pkg/etcdutil"
	"github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server/config"
	"go.etcd.io/etcd/embed"
	"go.etcd.io/etcd/pkg/types"
	"go.uber.org/goleak"
)

func TestServer(t *testing.T) {
	EnableZap = true
	TestingT(t)
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func mustWaitLeader(c *C, svrs []*Server) *Server {
	var leader *Server
	testutil.WaitUntil(c, func(c *C) bool {
		for _, s := range svrs {
			if !s.IsClosed() && s.member.IsLeader() {
				leader = s
				return true
			}
		}
		return false
	})
	return leader
}

var _ = Suite(&testLeaderServerSuite{})

type testLeaderServerSuite struct {
	ctx        context.Context
	cancel     context.CancelFunc
	svrs       map[string]*Server
	leaderPath string
}

func (s *testLeaderServerSuite) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.svrs = make(map[string]*Server)

	cfgs := NewTestMultiConfig(c, 3)

	ch := make(chan *Server, 3)
	for i := 0; i < 3; i++ {
		cfg := cfgs[i]

		go func() {
			svr, err := CreateServer(s.ctx, cfg)
			c.Assert(err, IsNil)
			err = svr.Run()
			c.Assert(err, IsNil)
			ch <- svr
		}()
	}

	for i := 0; i < 3; i++ {
		svr := <-ch
		s.svrs[svr.GetAddr()] = svr
		s.leaderPath = svr.GetMember().GetLeaderPath()
	}
}

func (s *testLeaderServerSuite) TearDownSuite(c *C) {
	s.cancel()
	for _, svr := range s.svrs {
		svr.Close()
		testutil.CleanServer(svr.cfg.DataDir)
	}
}

var _ = Suite(&testServerSuite{})

type testServerSuite struct{}

func newTestServersWithCfgs(ctx context.Context, c *C, cfgs []*config.Config) ([]*Server, CleanupFunc) {
	svrs := make([]*Server, 0, len(cfgs))

	ch := make(chan *Server)
	for _, cfg := range cfgs {
		go func(cfg *config.Config) {
			svr, err := CreateServer(ctx, cfg)
			// prevent blocking if Asserts fails
			failed := true
			defer func() {
				if failed {
					ch <- nil
				} else {
					ch <- svr
				}
			}()
			c.Assert(err, IsNil)
			err = svr.Run()
			c.Assert(err, IsNil)
			failed = false
		}(cfg)
	}

	for i := 0; i < len(cfgs); i++ {
		svr := <-ch
		c.Assert(svr, NotNil)
		svrs = append(svrs, svr)
	}
	mustWaitLeader(c, svrs)

	cleanup := func() {
		for _, svr := range svrs {
			svr.Close()
		}
		for _, cfg := range cfgs {
			testutil.CleanServer(cfg.DataDir)
		}
	}

	return svrs, cleanup
}

func (s *testServerSuite) TestCheckClusterID(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfgs := NewTestMultiConfig(c, 2)
	for i, cfg := range cfgs {
		cfg.DataDir = fmt.Sprintf("/tmp/test_pd_check_clusterID_%d", i)
		// Clean up before testing.
		testutil.CleanServer(cfg.DataDir)
	}
	originInitial := cfgs[0].InitialCluster
	for _, cfg := range cfgs {
		cfg.InitialCluster = fmt.Sprintf("%s=%s", cfg.Name, cfg.PeerUrls)
	}

	cfgA, cfgB := cfgs[0], cfgs[1]
	// Start a standalone cluster.
	svrsA, cleanA := newTestServersWithCfgs(ctx, c, []*config.Config{cfgA})
	defer cleanA()
	// Close it.
	for _, svr := range svrsA {
		svr.Close()
	}

	// Start another cluster.
	_, cleanB := newTestServersWithCfgs(ctx, c, []*config.Config{cfgB})
	defer cleanB()

	// Start previous cluster, expect an error.
	cfgA.InitialCluster = originInitial
	svr, err := CreateServer(ctx, cfgA)
	c.Assert(err, IsNil)

	etcd, err := embed.StartEtcd(svr.etcdCfg)
	c.Assert(err, IsNil)
	urlsMap, err := types.NewURLsMap(svr.cfg.InitialCluster)
	c.Assert(err, IsNil)
	tlsConfig, err := svr.cfg.Security.ToTLSConfig()
	c.Assert(err, IsNil)
	err = etcdutil.CheckClusterID(etcd.Server.Cluster().ID(), urlsMap, tlsConfig)
	c.Assert(err, NotNil)
	etcd.Close()
	testutil.CleanServer(cfgA.DataDir)
}

var _ = Suite(&testServerHandlerSuite{})

type testServerHandlerSuite struct{}

func (s *testServerHandlerSuite) TestRegisterServerHandler(c *C) {
	mokHandler := func(ctx context.Context, s *Server) (http.Handler, ServiceGroup, error) {
		mux := http.NewServeMux()
		mux.HandleFunc("/pd/apis/mok/v1/hello", func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintln(w, "Hello World")
		})
		info := ServiceGroup{
			Name:    "mok",
			Version: "v1",
		}
		return mux, info, nil
	}
	cfg := NewTestSingleConfig(c)
	ctx, cancel := context.WithCancel(context.Background())
	svr, err := CreateServer(ctx, cfg, mokHandler)
	c.Assert(err, IsNil)
	_, err = CreateServer(ctx, cfg, mokHandler, mokHandler)
	// Repeat register.
	c.Assert(err, NotNil)
	defer func() {
		cancel()
		svr.Close()
		testutil.CleanServer(svr.cfg.DataDir)
	}()
	err = svr.Run()
	c.Assert(err, IsNil)
	addr := fmt.Sprintf("%s/pd/apis/mok/v1/hello", svr.GetAddr())
	resp, err := http.Get(addr)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	bodyString := string(bodyBytes)
	c.Assert(bodyString, Equals, "Hello World\n")
}
