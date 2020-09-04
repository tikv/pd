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
	"path"
	"time"

	. "github.com/pingcap/check"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/election"
	"github.com/tikv/pd/server/tso"
	"github.com/tikv/pd/tests"
)

const waitAllocatorCheckInterval = 500 * time.Millisecond

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

func (s *testAllocatorSuite) TestLocalAllocatorLeader(c *C) {
	var err error
	cluster, err := tests.NewTestCluster(s.ctx, 3)
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)

	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()
	testDCLocation := "dc-1"
	for _, server := range cluster.GetServers() {
		tsoAllocatorManager := server.GetTSOAllocatorManager()
		leadership := election.NewLeadership(
			server.GetEtcdClient(),
			path.Join(server.GetServer().GetServerRootPath(), testDCLocation),
			"campaign-local-allocator-test")
		tsoAllocatorManager.SetUpAllocator(ctx, cancel, testDCLocation, leadership)
	}
	// To check whether we have only one Local TSO Allocator leader for dc-1
	allAllocators := make([]tso.Allocator, 0)
	for i := 0; i < 20; i++ {
		for _, server := range cluster.GetServers() {
			// Filter out Global TSO Allocator and uninitialized Local TSO Allocator
			allocators := server.GetTSOAllocatorManager().GetAllocators(false, true, true)
			c.Assert(len(allocators), LessEqual, 1)
			if len(allocators) == 0 {
				continue
			}
			if len(allAllocators) == 0 || slice.NoneOf(allAllocators, func(i int) bool { return allAllocators[i] == allocators[0] }) {
				allAllocators = append(allAllocators, allocators...)
			}
		}
		time.Sleep(waitAllocatorCheckInterval)
	}
	// At the end, we should only have one initialized Local TSO Allocator,
	// i.e., the Local TSO Allocator leader for dc-1
	c.Assert(len(allAllocators), Equals, 1)
	allocatorLeader, _ := allAllocators[0].(*tso.LocalTSOAllocator)
	for _, server := range cluster.GetServers() {
		// Filter out Global TSO Allocator
		allocators := server.GetTSOAllocatorManager().GetAllocators(false, true, false)
		c.Assert(len(allocators), Equals, 1)
		allocatorFollower, _ := allocators[0].(*tso.LocalTSOAllocator)
		// All followers sould have the same allocator leader
		c.Assert(allocatorFollower.GetAllocatorLeader().MemberId, Equals, allocatorLeader.GetMember().MemberId)
	}
}
