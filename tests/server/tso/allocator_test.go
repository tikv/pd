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
	"strconv"
	"sync"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/pkg/etcdutil"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/tso"
	"github.com/tikv/pd/tests"
)

var _ = Suite(&testAllocatorSuite{})

type testAllocatorSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *testAllocatorSuite) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	server.EnableZap = true
}

func (s *testAllocatorSuite) TearDownSuite(c *C) {
	s.cancel()
}

// Make sure we have the correct number of Local TSO Allocator leaders.
func (s *testAllocatorSuite) TestAllocatorLeader(c *C) {
	// There will be three Local TSO Allocator leaders elected
	dcLocationConfig := map[string]string{
		"pd2": "dc-1",
		"pd4": "dc-2",
		"pd6": "dc-3",
	}
	dcLocationNum := len(dcLocationConfig)
	cluster, err := tests.NewTestCluster(s.ctx, dcLocationNum*2, func(conf *config.Config, serverName string) {
		if zoneLabel, ok := dcLocationConfig[serverName]; ok {
			conf.EnableLocalTSO = true
			conf.Labels[config.ZoneLabel] = zoneLabel
		}
	})
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)

	cluster.WaitAllLeaders(c, dcLocationConfig)
	// To check whether we have enough Local TSO Allocator leaders
	allAllocatorLeaders := make([]tso.Allocator, 0, dcLocationNum)
	for _, server := range cluster.GetServers() {
		// Filter out Global TSO Allocator and Local TSO Allocator followers
		allocators := server.GetTSOAllocatorManager().GetAllocators(
			tso.FilterDCLocation(tso.GlobalDCLocation),
			tso.FilterUnavailableLeadership(),
			tso.FilterUninitialized())
		// One PD server will have at most three initialized Local TSO Allocators,
		// which also means three allocator leaders
		c.Assert(len(allocators), LessEqual, dcLocationNum)
		if len(allocators) == 0 {
			continue
		}
		if len(allAllocatorLeaders) == 0 {
			allAllocatorLeaders = append(allAllocatorLeaders, allocators...)
			continue
		}
		for _, allocator := range allocators {
			if slice.NoneOf(allAllocatorLeaders, func(i int) bool { return allAllocatorLeaders[i] == allocator }) {
				allAllocatorLeaders = append(allAllocatorLeaders, allocator)
			}
		}
	}
	// At the end, we should have three initialized Local TSO Allocator,
	// i.e., the Local TSO Allocator leaders for all dc-locations in testDCLocations
	c.Assert(len(allAllocatorLeaders), Equals, dcLocationNum)
	allocatorLeaderMemberIDs := make([]uint64, 0, dcLocationNum)
	for _, allocator := range allAllocatorLeaders {
		allocatorLeader, _ := allocator.(*tso.LocalTSOAllocator)
		allocatorLeaderMemberIDs = append(allocatorLeaderMemberIDs, allocatorLeader.GetMember().GetMemberId())
	}
	for _, server := range cluster.GetServers() {
		// Filter out Global TSO Allocator
		allocators := server.GetTSOAllocatorManager().GetAllocators(tso.FilterDCLocation(tso.GlobalDCLocation))
		if _, ok := dcLocationConfig[server.GetServer().Name()]; !ok {
			c.Assert(len(allocators), Equals, 0)
			continue
		}
		c.Assert(len(allocators), Equals, dcLocationNum)
		for _, allocator := range allocators {
			allocatorFollower, _ := allocator.(*tso.LocalTSOAllocator)
			allocatorFollowerMemberID := allocatorFollower.GetAllocatorLeader().GetMemberId()
			c.Assert(
				slice.AnyOf(
					allocatorLeaderMemberIDs,
					func(i int) bool { return allocatorLeaderMemberIDs[i] == allocatorFollowerMemberID }),
				IsTrue)
		}
	}
}

func (s *testAllocatorSuite) TestPriorityAndDifferentLocalTSO(c *C) {
	dcLocationConfig := map[string]string{
		"pd1": "dc-1",
		"pd2": "dc-2",
		"pd3": "dc-3",
	}
	dcLocationNum := len(dcLocationConfig)
	cluster, err := tests.NewTestCluster(s.ctx, dcLocationNum, func(conf *config.Config, serverName string) {
		conf.EnableLocalTSO = true
		conf.Labels[config.ZoneLabel] = dcLocationConfig[serverName]
	})
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)

	cluster.WaitAllLeaders(c, dcLocationConfig)

	// Wait for all nodes becoming healthy.
	time.Sleep(time.Second * 5)

	// Join a new dc-location
	pd4, err := cluster.Join(s.ctx, func(conf *config.Config, serverName string) {
		conf.EnableLocalTSO = true
		conf.Labels[config.ZoneLabel] = "dc-4"
	})
	c.Assert(err, IsNil)
	err = pd4.Run()
	c.Assert(err, IsNil)
	dcLocationConfig["pd4"] = "dc-4"
	cluster.CheckClusterDCLocation()
	testutil.WaitUntil(c, func(c *C) bool {
		leaderName := cluster.WaitAllocatorLeader("dc-4")
		return leaderName != ""
	})

	// Scatter the Local TSO Allocators to different servers
	waitAllocatorPriorityCheck(cluster)
	cluster.WaitAllLeaders(c, dcLocationConfig)

	// Before the priority is checked, we may have allocators typology like this:
	// pd1: dc-1, dc-2 and dc-3 allocator leader
	// pd2: None
	// pd3: None
	// pd4: dc-4 allocator leader
	// After the priority is checked, we should have allocators typology like this:
	// pd1: dc-1 allocator leader
	// pd2: dc-2 allocator leader
	// pd3: dc-3 allocator leader
	// pd4: dc-4 allocator leader
	wg := sync.WaitGroup{}
	wg.Add(len(dcLocationConfig))
	for serverName, dcLocation := range dcLocationConfig {
		go func(serName, dc string) {
			defer wg.Done()
			testutil.WaitUntil(c, func(c *C) bool {
				leaderName := cluster.WaitAllocatorLeader(dc)
				return leaderName == serName
			}, testutil.WithRetryTimes(12), testutil.WithSleepInterval(5*time.Second))
		}(serverName, dcLocation)
	}
	wg.Wait()

	for serverName, server := range cluster.GetServers() {
		tsoAllocatorManager := server.GetTSOAllocatorManager()
		localAllocatorLeaders, err := tsoAllocatorManager.GetHoldingLocalAllocatorLeaders()
		c.Assert(err, IsNil)
		for _, localAllocatorLeader := range localAllocatorLeaders {
			s.testTSOSuffix(c, cluster, tsoAllocatorManager, localAllocatorLeader.GetDCLocation())
		}
		if serverName == cluster.GetLeader() {
			s.testTSOSuffix(c, cluster, tsoAllocatorManager, tso.GlobalDCLocation)
		}
	}
}

func waitAllocatorPriorityCheck(cluster *tests.TestCluster) {
	wg := sync.WaitGroup{}
	for _, server := range cluster.GetServers() {
		wg.Add(1)
		go func(ser *tests.TestServer) {
			ser.GetTSOAllocatorManager().PriorityChecker()
			wg.Done()
		}(server)
	}
	wg.Wait()
}

func (s *testAllocatorSuite) testTSOSuffix(c *C, cluster *tests.TestCluster, am *tso.AllocatorManager, dcLocation string) {
	suffixBits := am.GetSuffixBits()
	c.Assert(suffixBits, Greater, 0)
	var suffix int64
	// The suffix of a Global TSO will always be 0
	if dcLocation != tso.GlobalDCLocation {
		suffixResp, err := etcdutil.EtcdKVGet(
			cluster.GetEtcdClient(),
			am.GetLocalTSOSuffixPath(dcLocation))
		c.Assert(err, IsNil)
		c.Assert(len(suffixResp.Kvs), Equals, 1)
		suffix, err = strconv.ParseInt(string(suffixResp.Kvs[0].Value), 10, 64)
		c.Assert(err, IsNil)
		c.Assert(suffixBits, GreaterEqual, tso.CalSuffixBits(int32(suffix)))
	}
	allocator, err := am.GetAllocator(dcLocation)
	c.Assert(err, IsNil)
	var tso pdpb.Timestamp
	testutil.WaitUntil(c, func(c *C) bool {
		tso, err = allocator.GenerateTSO(1)
		c.Assert(err, IsNil)
		return tso.GetPhysical() != 0
	})
	// Test whether the TSO has the right suffix
	c.Assert(suffix, Equals, tso.Logical&((1<<suffixBits)-1))
}
