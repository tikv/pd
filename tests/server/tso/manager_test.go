// Copyright 2020 TiKV Project Authors.
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

package tso_test

import (
	"context"
	"time"

	. "github.com/pingcap/check"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

var _ = Suite(&testManagerSuite{})

type testManagerSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *testManagerSuite) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	server.EnableZap = true
}

func (s *testManagerSuite) TearDownSuite(c *C) {
	s.cancel()
}

// TestClusterDCLocations will write different dc-locations to each server
// and test whether we can get the whole dc-location config from each server.
func (s *testManagerSuite) TestClusterDCLocations(c *C) {
	testCase := struct {
		dcLocationNumber int
		dcLocationConfig map[string]string
	}{
		dcLocationNumber: 3,
		dcLocationConfig: map[string]string{
			"pd1": "dc-1",
			"pd2": "dc-1",
			"pd3": "dc-2",
			"pd4": "dc-2",
			"pd5": "dc-3",
			"pd6": "dc-3",
		},
	}
	serverNumber := len(testCase.dcLocationConfig)
	cluster, err := tests.NewTestCluster(s.ctx, serverNumber, func(conf *config.Config, serverName string) {
		conf.LocalTSO.EnableLocalTSO = true
		conf.LocalTSO.DCLocation = testCase.dcLocationConfig[serverName]
	})
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)

	serverNameMap := make(map[uint64]string)
	for _, server := range cluster.GetServers() {
		serverNameMap[server.GetServerID()] = server.GetServer().Name()
	}
	// Start to check every server's GetClusterDCLocations() result
	for _, server := range cluster.GetServers() {
		obtainedServerNumber := 0
		dcLocationMap, err := server.GetTSOAllocatorManager().GetClusterDCLocations()
		c.Assert(err, IsNil)
		c.Assert(len(dcLocationMap), Equals, testCase.dcLocationNumber)
		for obtainedDCLocation, serverIDs := range dcLocationMap {
			obtainedServerNumber += len(serverIDs)
			for _, serverID := range serverIDs {
				expectedDCLocation, exist := testCase.dcLocationConfig[serverNameMap[serverID]]
				c.Assert(exist, IsTrue)
				c.Assert(obtainedDCLocation, Equals, expectedDCLocation)
			}
		}
		c.Assert(obtainedServerNumber, Equals, serverNumber)
	}
}

const waitAllocatorPriorityCheckInterval = 3 * time.Minute

var _ = Suite(&testPrioritySuite{})

type testPrioritySuite struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *testPrioritySuite) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	server.EnableZap = true
}

func (s *testPrioritySuite) TearDownSuite(c *C) {
	s.cancel()
}

func (s *testPrioritySuite) TestAllocatorPriority(c *C) {
	dcLocationConfig := map[string]string{
		"pd1": "dc-1",
		"pd2": "dc-2",
		"pd3": "dc-3",
	}
	serverNumber := len(dcLocationConfig)
	cluster, err := tests.NewTestCluster(s.ctx, serverNumber, func(conf *config.Config, serverName string) {
		conf.LocalTSO.EnableLocalTSO = true
		conf.LocalTSO.DCLocation = dcLocationConfig[serverName]
	})
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)

	// Before the priority is checked, we may have allocators typology like this:
	// pd1: dc-1, dc-2 and dc-3 allocator leader
	// pd2: None
	// pd3: None
	// After the priority is checked, we should have allocators typology like this:
	// pd1: dc-1 allocator leader
	// pd2: dc-2 allocator leader
	// pd3: dc-3 allocator leader

	// Because the default priority checking period is 1 minute,
	// so we sleep longer here.
	time.Sleep(waitAllocatorPriorityCheckInterval)

	for serverName, dcLocation := range dcLocationConfig {
		currentLeaderName := cluster.WaitAllocatorLeader(dcLocation)
		c.Assert(currentLeaderName, Equals, serverName)
	}
}
