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

package server_test

import (
	"sync"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/pd/tests"
)

func (s *serverTestSuite) TestMoveLeader(c *C) {
	c.Parallel()

	cluster, err := tests.NewTestCluster(5)
	c.Assert(err, IsNil)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()

	var wg sync.WaitGroup
	wg.Add(5)
	for _, s := range cluster.GetServers() {
		go func(s *tests.TestServer) {
			defer wg.Done()
			if s.IsLeader() {
				s.ResignLeader()
			} else {
				old, _ := s.GetEtcdLeaderID()
				s.MoveEtcdLeader(old, s.GetServerID())
			}
		}(s)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		c.Fatal("move etcd leader does not return in 10 seconds")
	}
}
