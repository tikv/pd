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
	"github.com/tikv/pd/pkg/slice"
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
		serverNumber     int
		dcLocationNumber int
		dcLocationConfig map[string]string
	}{
		serverNumber:     6,
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
	cluster, err := tests.NewTestCluster(s.ctx, testCase.serverNumber, func(conf *config.Config, serverName string) {
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
		c.Assert(obtainedServerNumber, Equals, testCase.serverNumber)
	}
}

// TestClusterDCLocationsChange is used to test the changing behavior
// of a cluster's dc-locations.
func (s *testManagerSuite) TestClusterDCLocationsChange(c *C) {
	testCase := struct {
		dcLocationNumber int
		dcLocationConfig map[string]string
		serversToDelete  []string
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
		serversToDelete: []string{"pd1", "pd2"},
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
	for i, serverToDelete := range testCase.serversToDelete {
		err = cluster.GetServer(serverToDelete).Destroy()
		c.Assert(err, IsNil)
		// Wait for the lease expiring
		time.Sleep(time.Second * 5)
		// Start to check every server's GetClusterDCLocations() result
		for _, server := range cluster.GetServers() {
			// Skip the servers which has been deleted already
			if slice.AnyOf(
				testCase.serversToDelete,
				func(i int) bool {
					return testCase.serversToDelete[i] == server.GetServer().Name()
				}) {
				continue
			}
			dcLocationMap, err := server.GetTSOAllocatorManager().GetClusterDCLocations()
			c.Assert(err, IsNil)
			// After deleting all of the servers from dc-1, the dcLocationNumber should be 2
			c.Assert(len(dcLocationMap), Equals, testCase.dcLocationNumber-i)
			c.Assert(len(dcLocationMap[testCase.dcLocationConfig[serverToDelete]]), Equals, 1-i)
		}
	}
}
