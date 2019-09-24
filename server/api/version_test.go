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
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/pd/pkg/testutil"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/config"
)

var _ = Suite(&testVersionSuite{})

type testVersionSuite struct{}

func (s *testVersionSuite) TestGetVersion(c *C) {
	fname := filepath.Join(os.TempDir(), "stdout")
	old := os.Stdout
	temp, _ := os.Create(fname)
	os.Stdout = temp

	cfg := server.NewTestSingleConfig(c)
	stopCh := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopCh:
				return
			default:
				func() {
					addr := cfg.ClientUrls + apiPrefix + "/api/v1/version"
					resp, err := dialClient.Get(addr)
					if resp == nil {
						return
					}
					c.Assert(err, IsNil)
					defer resp.Body.Close()
					_, err = ioutil.ReadAll(resp.Body)
					c.Assert(err, IsNil)
				}()
			}
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan *server.Server)
	go func(cfg *config.Config) {
		s, err := server.CreateServer(cfg, NewHandler)
		c.Assert(err, IsNil)
		err = s.Run(ctx)
		c.Assert(err, IsNil)
		ch <- s
	}(cfg)

	svr := <-ch
	close(ch)
	out, _ := ioutil.ReadFile(fname)
	c.Assert(strings.Contains(string(out), "PANIC"), IsFalse)
	stopCh <- struct{}{}

	// clean up
	func() {
		temp.Close()
		os.Stdout = old
		os.Remove(fname)
		svr.Close()
		cancel()
		testutil.CleanServer(cfg)
	}()
}
