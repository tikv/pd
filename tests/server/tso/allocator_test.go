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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build tso_full_test || tso_function_test
// +build tso_full_test tso_function_test

package tso_test

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/pkg/tso"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

func TestAllocatorLeader(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// There will be three Local TSO Allocator leaders elected
	dcLocationConfig := map[string]string{
		"pd2": "dc-1",
		"pd4": "dc-2",
		"pd6": "leader", /* Test dc-location name is same as the special key */
	}
	dcLocationNum := len(dcLocationConfig)
	cluster, err := tests.NewTestCluster(ctx, dcLocationNum*2, func(conf *config.Config, serverName string) {
		if zoneLabel, ok := dcLocationConfig[serverName]; ok {
			conf.EnableLocalTSO = true
			conf.Labels[config.ZoneLabel] = zoneLabel
		}
	})
	re.NoError(err)
	defer cluster.Destroy()
	re.NoError(cluster.RunInitialServers())
	cluster.WaitAllLeaders(re, dcLocationConfig)
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
		re.LessOrEqual(len(allocators), dcLocationNum)
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
	re.Len(allAllocatorLeaders, dcLocationNum)
	allocatorLeaderMemberIDs := make([]uint64, 0, dcLocationNum)
	for _, allocator := range allAllocatorLeaders {
		allocatorLeader, _ := allocator.(*tso.LocalTSOAllocator)
		allocatorLeaderMemberIDs = append(allocatorLeaderMemberIDs, allocatorLeader.GetMember().ID())
	}
	for _, server := range cluster.GetServers() {
		// Filter out Global TSO Allocator
		allocators := server.GetTSOAllocatorManager().GetAllocators(tso.FilterDCLocation(tso.GlobalDCLocation))
		if _, ok := dcLocationConfig[server.GetServer().Name()]; !ok {
			re.Empty(allocators)
			continue
		}
		re.Len(allocators, dcLocationNum)
		for _, allocator := range allocators {
			allocatorFollower, _ := allocator.(*tso.LocalTSOAllocator)
			allocatorFollowerMemberID := allocatorFollower.GetAllocatorLeader().GetMemberId()
			re.True(
				slice.AnyOf(
					allocatorLeaderMemberIDs,
					func(i int) bool { return allocatorLeaderMemberIDs[i] == allocatorFollowerMemberID },
				),
			)
		}
	}
}

func TestPriorityAndDifferentLocalTSO(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dcLocationConfig := map[string]string{
		"pd1": "dc-1",
		"pd2": "dc-2",
		"pd3": "dc-3",
	}
	dcLocationNum := len(dcLocationConfig)
	cluster, err := tests.NewTestCluster(ctx, dcLocationNum, func(conf *config.Config, serverName string) {
		conf.EnableLocalTSO = true
		conf.Labels[config.ZoneLabel] = dcLocationConfig[serverName]
	})
	defer cluster.Destroy()
	re.NoError(err)
	re.NoError(cluster.RunInitialServers())

	cluster.WaitAllLeaders(re, dcLocationConfig)
	leaderServer := cluster.GetLeaderServer()
	re.NotNil(leaderServer)
	leaderServer.BootstrapCluster()

	// Wait for all nodes becoming healthy.
	time.Sleep(time.Second * 5)

	// Join a new dc-location
	pd4, err := cluster.Join(ctx, func(conf *config.Config, _ string) {
		conf.EnableLocalTSO = true
		conf.Labels[config.ZoneLabel] = "dc-4"
	})
	re.NoError(err)
	re.NoError(pd4.Run())
	dcLocationConfig["pd4"] = "dc-4"
	cluster.CheckClusterDCLocation()
	re.NotEqual("", cluster.WaitAllocatorLeader(
		"dc-4",
		tests.WithRetryTimes(90), tests.WithWaitInterval(time.Second),
	))

	// Scatter the Local TSO Allocators to different servers
	waitAllocatorPriorityCheck(cluster)
	cluster.WaitAllLeaders(re, dcLocationConfig)

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
		go func(name, dc string) {
			defer wg.Done()
			testutil.Eventually(re, func() bool {
				return cluster.WaitAllocatorLeader(dc) == name
			}, testutil.WithWaitFor(90*time.Second), testutil.WithTickInterval(time.Second))
		}(serverName, dcLocation)
	}
	wg.Wait()

	for serverName, server := range cluster.GetServers() {
		tsoAllocatorManager := server.GetTSOAllocatorManager()
		localAllocatorLeaders, err := tsoAllocatorManager.GetHoldingLocalAllocatorLeaders()
		re.NoError(err)
		for _, localAllocatorLeader := range localAllocatorLeaders {
			testTSOSuffix(re, cluster, tsoAllocatorManager, localAllocatorLeader.GetDCLocation())
		}
		if serverName == cluster.GetLeader() {
			testTSOSuffix(re, cluster, tsoAllocatorManager, tso.GlobalDCLocation)
		}
	}
}

func waitAllocatorPriorityCheck(cluster *tests.TestCluster) {
	wg := sync.WaitGroup{}
	for _, server := range cluster.GetServers() {
		wg.Add(1)
		go func(s *tests.TestServer) {
			s.GetTSOAllocatorManager().PriorityChecker()
			wg.Done()
		}(server)
	}
	wg.Wait()
}

func testTSOSuffix(re *require.Assertions, cluster *tests.TestCluster, am *tso.AllocatorManager, dcLocation string) {
	suffixBits := am.GetSuffixBits()
	re.Greater(suffixBits, 0)
	var suffix int64
	// The suffix of a Global TSO will always be 0
	if dcLocation != tso.GlobalDCLocation {
		suffixResp, err := etcdutil.EtcdKVGet(
			cluster.GetEtcdClient(),
			am.GetLocalTSOSuffixPath(dcLocation))
		re.NoError(err)
		re.Len(suffixResp.Kvs, 1)
		suffix, err = strconv.ParseInt(string(suffixResp.Kvs[0].Value), 10, 64)
		re.NoError(err)
		re.GreaterOrEqual(suffixBits, tso.CalSuffixBits(int32(suffix)))
	}
	allocator, err := am.GetAllocator(dcLocation)
	re.NoError(err)
	var tso pdpb.Timestamp
	testutil.Eventually(re, func() bool {
		tso, err = allocator.GenerateTSO(context.Background(), 1)
		re.NoError(err)
		return tso.GetPhysical() != 0
	})
	// Test whether the TSO has the right suffix
	re.Equal(suffix, tso.Logical&((1<<suffixBits)-1))
}
