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

package tso

import (
	"context"
	"strconv"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/tikv/pd/pkg/etcdutil"
	"github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/tso"
	"github.com/tikv/pd/tests"
	"go.etcd.io/etcd/clientv3"
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
		conf.EnableLocalTSO = true
		conf.Labels[config.ZoneLabel] = testCase.dcLocationConfig[serverName]
	})
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)

	cluster.WaitAllLeaders(c, testCase.dcLocationConfig)
	serverNameMap := make(map[uint64]string)
	for _, server := range cluster.GetServers() {
		serverNameMap[server.GetServerID()] = server.GetServer().Name()
		// To speed up the test, we force to do the check
		server.GetTSOAllocatorManager().ClusterDCLocationChecker()
	}
	// Start to check every server's GetClusterDCLocations() result
	for _, server := range cluster.GetServers() {
		obtainedServerNumber := 0
		dcLocationMap := server.GetTSOAllocatorManager().GetClusterDCLocations()
		c.Assert(err, IsNil)
		c.Assert(len(dcLocationMap), Equals, testCase.dcLocationNumber)
		for obtainedDCLocation, info := range dcLocationMap {
			obtainedServerNumber += len(info.ServerIDs)
			for _, serverID := range info.ServerIDs {
				expectedDCLocation, exist := testCase.dcLocationConfig[serverNameMap[serverID]]
				c.Assert(exist, IsTrue)
				c.Assert(obtainedDCLocation, Equals, expectedDCLocation)
			}
		}
		c.Assert(obtainedServerNumber, Equals, serverNumber)
	}
}

func (s *testManagerSuite) TestLocalTSOSuffix(c *C) {
	testCase := struct {
		dcLocations      []string
		dcLocationConfig map[string]string
	}{
		dcLocations: []string{"dc-1", "dc-2", "dc-3"},
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
		conf.EnableLocalTSO = true
		conf.Labels[config.ZoneLabel] = testCase.dcLocationConfig[serverName]
	})
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)

	cluster.WaitAllLeaders(c, testCase.dcLocationConfig)

	tsoAllocatorManager := cluster.GetServer("pd1").GetTSOAllocatorManager()
	for _, dcLocation := range testCase.dcLocations {
		suffixResp, err := etcdutil.EtcdKVGet(
			cluster.GetEtcdClient(),
			tsoAllocatorManager.GetLocalTSOSuffixPath(dcLocation))
		c.Assert(err, IsNil)
		c.Assert(len(suffixResp.Kvs), Equals, 1)
		// Test the increment of the suffix
		allSuffixResp, err := etcdutil.EtcdKVGet(
			cluster.GetEtcdClient(),
			tsoAllocatorManager.GetLocalTSOSuffixPathPrefix(),
			clientv3.WithPrefix(),
			clientv3.WithSort(clientv3.SortByValue, clientv3.SortAscend))
		c.Assert(err, IsNil)
		c.Assert(len(allSuffixResp.Kvs), Equals, len(testCase.dcLocations))
		var lastSuffixNum int64
		for _, kv := range allSuffixResp.Kvs {
			suffixNum, err := strconv.ParseInt(string(kv.Value), 10, 64)
			c.Assert(err, IsNil)
			c.Assert(suffixNum, Greater, lastSuffixNum)
			lastSuffixNum = suffixNum
		}
	}
}

var _ = SerialSuites(&testLocalTSOSerialSuite{})

type testLocalTSOSerialSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *testLocalTSOSerialSuite) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	server.EnableZap = true
}

func (s *testLocalTSOSerialSuite) TearDownSuite(c *C) {
	s.cancel()
}

func (s *testLocalTSOSerialSuite) TestNextLeaderKey(c *C) {
	tso.PriorityCheck = 5 * time.Second
	defer func() {
		tso.PriorityCheck = 1 * time.Minute
	}()
	dcLocationConfig := map[string]string{
		"pd1": "dc-1",
		"pd2": "dc-1",
	}
	serverNum := len(dcLocationConfig)
	cluster, err := tests.NewTestCluster(s.ctx, serverNum, func(conf *config.Config, serverName string) {
		conf.EnableLocalTSO = true
		conf.Labels[config.ZoneLabel] = dcLocationConfig[serverName]
	})
	defer cluster.Destroy()
	c.Assert(err, IsNil)
	c.Assert(failpoint.Enable("github.com/tikv/pd/server/tso/injectNextLeaderKey", "return(true)"), IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)

	cluster.WaitLeader(tests.WithWaitInterval(5*time.Second), tests.WithRetryTimes(3))
	// To speed up the test, we force to do the check
	cluster.CheckClusterDCLocation()
	originName := cluster.WaitAllocatorLeader("dc-1", tests.WithRetryTimes(5), tests.WithWaitInterval(5*time.Second))
	c.Assert(originName, Equals, "")
	c.Assert(failpoint.Disable("github.com/tikv/pd/server/tso/injectNextLeaderKey"), IsNil)
	cluster.CheckClusterDCLocation()
	originName = cluster.WaitAllocatorLeader("dc-1")
	c.Assert(originName, Not(Equals), "")
	for name, server := range cluster.GetServers() {
		if name == originName {
			continue
		}
		err := server.GetTSOAllocatorManager().TransferAllocatorForDCLocation("dc-1", server.GetServer().GetMember().ID())
		c.Assert(err, IsNil)
		testutil.WaitUntil(c, func(c *C) bool {
			cluster.CheckClusterDCLocation()
			currName := cluster.WaitAllocatorLeader("dc-1")
			return currName == name
		}, testutil.WithSleepInterval(1*time.Second))
		return
	}
}
