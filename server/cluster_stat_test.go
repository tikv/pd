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
// limitations under the License

package server

import (
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

var _ = Suite(&testClusterStatSuite{})

type testClusterStatSuite struct {
}

func cpu(usage int64) []*pdpb.RecordPair {
	n := 10
	name := "cpu"
	pairs := make([]*pdpb.RecordPair, n)
	for i := 0; i < n; i++ {
		pairs[i] = &pdpb.RecordPair{
			Key:   fmt.Sprintf("%s:%d", name, i),
			Value: uint64(usage),
		}
	}
	return pairs
}

func (s *testClusterStatSuite) TestCPUStatEntriesAppend(c *C) {
	N := 10

	checkAppend := func(appended bool, usage int64, threads ...string) {
		entries := NewCPUStatEntries(N)
		c.Assert(entries, NotNil)
		for i := 0; i < N; i++ {
			entry := &StatEntry{
				CpuUsages: cpu(usage),
			}
			c.Assert(entries.Append(entry, threads...), Equals, appended)
		}
		c.Assert(entries.cpu.Get(), Equals, float64(usage))
	}

	checkAppend(true, 20)
	checkAppend(true, 20, "cpu")
	checkAppend(false, 0, "cup")
}

func (s *testClusterStatSuite) TestCPUStatEntriesCPU(c *C) {
	N := 10
	entries := NewCPUStatEntries(N)
	c.Assert(entries, NotNil)

	usages := cpu(20)
	for i := 0; i < N; i++ {
		entry := &StatEntry{
			CpuUsages: usages,
		}
		entries.Append(entry)
	}
	c.Assert(entries.CPU(), Equals, float64(20))
}

func (s *testClusterStatSuite) TestClusterStatEntriesAppend(c *C) {
	N := 10
	cst := NewClusterStatEntries(N)
	c.Assert(cst, NotNil)

	// fill 2*N entries, 2 entries for each store
	for i := 0; i < 2*N; i++ {
		entry := &StatEntry{
			StoreId:   uint64(i % N),
			CpuUsages: cpu(20),
		}
		cst.Append(entry)
	}

	// use i as the store ID
	for i := 0; i < N; i++ {
		c.Assert(cst.stats[uint64(i)].CPU(), Equals, float64(20))
	}
}

func (s *testClusterStatSuite) TestClusterStatCPU(c *C) {
	N := 10
	cst := NewClusterStatEntries(N)
	c.Assert(cst, NotNil)

	// heartbeat per 10s
	interval := &pdpb.TimeInterval{
		StartTimestamp: 1,
		EndTimestamp:   11,
	}
	// the average cpu usage is 20%
	usages := cpu(20)

	// 2 entries per store
	for i := 0; i < 2*N; i++ {
		entry := &StatEntry{
			StoreId:   uint64(i % N),
			Interval:  interval,
			CpuUsages: usages,
		}
		cst.Append(entry)
	}

	// the cpu usage of the whole cluster is 20%
	c.Assert(cst.CPU(), Equals, float64(20))
}

func (s *testClusterStatSuite) TestClusterStatState(c *C) {
	Load := func(usage int64) *ClusterState {
		cst := NewClusterStatEntries(10)
		c.Assert(cst, NotNil)

		usages := cpu(usage)

		for i := 0; i < NumberOfEntries; i++ {
			entry := &StatEntry{
				StoreId:   0,
				CpuUsages: usages,
			}
			cst.Append(entry)
		}
		return &ClusterState{cst}
	}
	c.Assert(Load(0).State(), Equals, LoadStateIdle)
	c.Assert(Load(20).State(), Equals, LoadStateLow)
	c.Assert(Load(50).State(), Equals, LoadStateNormal)
	c.Assert(Load(90).State(), Equals, LoadStateHigh)
}
