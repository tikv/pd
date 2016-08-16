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
	"net"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/pd/server"
)

func TestAPIServer(t *testing.T) {
	TestingT(t)
}

func unixDial(_, addr string) (net.Conn, error) {
	return net.Dial("unix", addr)
}

func newUnixSocketClient() *http.Client {
	tr := &http.Transport{
		Dial: unixDial,
	}
	client := &http.Client{
		Timeout:   10 * time.Second,
		Transport: tr,
	}

	return client
}

func unixAddrToHTTPAddr(addr string) (string, error) {
	ua, err := url.Parse(addr)
	if err != nil {
		return "", err
	}

	// Turn unix to http
	ua.Scheme = "http"
	return ua.String(), nil
}

func mustUnixAddrToHTTPAddr(c *C, addr string) string {
	u, err := unixAddrToHTTPAddr(addr)
	c.Assert(err, IsNil)
	return u
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
			s, e := server.CreateServer(cfg)
			c.Assert(e, IsNil)
			e = s.StartEtcd(NewHandler(s))
			c.Assert(e, IsNil)
			go s.Run()
			ch <- s
		}(cfg)
	}

	for i := 0; i < num; i++ {
		svr := <-ch
		svrs = append(svrs, svr)
	}
	close(ch)

	// wait etcds and http servers
	mustWaitLeader(svrs)

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

func mustWaitLeader(svrs []*server.Server) *server.Server {
	for i := 0; i < 100; i++ {
		for _, svr := range svrs {
			if svr.IsLeader() {
				return svr
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	panic("no leader")
}
