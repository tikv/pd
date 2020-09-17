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
	"github.com/tikv/pd/server/tso"
	"github.com/tikv/pd/tests"
)

const waitAllocatorCheckInterval = 2 * time.Second

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
		"pd1": "dc-1",
		"pd2": "dc-2",
		"pd3": "dc-3",
	}
	dcLocationNum := len(dcLocationConfig)
	cluster, err := tests.NewTestCluster(s.ctx, dcLocationNum, func(conf *config.Config, serverName string) {
		conf.LocalTSO.EnableLocalTSO = true
		conf.LocalTSO.DCLocation = dcLocationConfig[serverName]
	})
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)

	// Wait for a while to check
	time.Sleep(waitAllocatorCheckInterval)
	// To check whether we have enough Local TSO Allocator leaders
	allAllocatorLeaders := make([]tso.Allocator, 0, dcLocationNum)
	for _, server := range cluster.GetServers() {
		// Filter out Global TSO Allocator and Local TSO Allocator followers
		allocators := server.GetTSOAllocatorManager().GetAllocators(
			tso.FilterDCLocation(config.GlobalDCLocation),
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
		allocators := server.GetTSOAllocatorManager().GetAllocators(tso.FilterDCLocation(config.GlobalDCLocation))
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
