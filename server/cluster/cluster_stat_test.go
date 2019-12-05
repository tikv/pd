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

package cluster

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

func (s *testClusterStatSuite) TestCPUEntriesAppend(c *C) {
	N := 10

	checkAppend := func(appended bool, usage int64, threads ...string) {
		entries := NewCPUEntries(N)
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

func (s *testClusterStatSuite) TestCPUEntriesCPU(c *C) {
	N := 10
	entries := NewCPUEntries(N)
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

func (s *testClusterStatSuite) TestStatEntriesAppend(c *C) {
	N := 10
	cst := NewStatEntries(N)
	c.Assert(cst, NotNil)
	ThreadsCollected = []string{"cpu:"}

	// fill 2*N entries, 2 entries for each store
	for i := 0; i < 2*N; i++ {
		entry := &StatEntry{
			StoreId:   uint64(i % N),
			CpuUsages: cpu(20),
		}
		c.Assert(cst.Append(entry), IsTrue)
	}

	// use i as the store ID
	for i := 0; i < N; i++ {
		c.Assert(cst.stats[uint64(i)].CPU(), Equals, float64(20))
	}
}

func (s *testClusterStatSuite) TestStatEntriesCPU(c *C) {
	N := 10
	cst := NewStatEntries(N)
	c.Assert(cst, NotNil)

	// the average cpu usage is 20%
	usages := cpu(20)
	ThreadsCollected = []string{"cpu:"}

	// 2 entries per store
	for i := 0; i < 2*N; i++ {
		entry := &StatEntry{
			StoreId:   uint64(i % N),
			CpuUsages: usages,
		}
		c.Assert(cst.Append(entry), IsTrue)
	}

	c.Assert(cst.total, Equals, int64(2*N))
	// the cpu usage of the whole cluster is 20%
	c.Assert(cst.CPU(), Equals, float64(20))
}
func (s *testClusterStatSuite) TestStatEntriesCPUStale(c *C) {
	N := 10
	cst := NewStatEntries(N)
	// make all entries stale immediately
	cst.ttl = 0

	usages := cpu(20)
	ThreadsCollected = []string{"cpu:"}
	for i := 0; i < 2*N; i++ {
		entry := &StatEntry{
			StoreId:   uint64(i % N),
			CpuUsages: usages,
		}
		cst.Append(entry)
	}
	c.Assert(cst.CPU(), Equals, float64(0))
}

func (s *testClusterStatSuite) TestStatEntriesState(c *C) {
	Load := func(usage int64) *State {
		cst := NewStatEntries(10)
		c.Assert(cst, NotNil)

		usages := cpu(usage)
		ThreadsCollected = []string{"cpu:"}

		for i := 0; i < NumberOfEntries; i++ {
			entry := &StatEntry{
				StoreId:   0,
				CpuUsages: usages,
			}
			cst.Append(entry)
		}
		return &State{cst}
	}
	c.Assert(Load(0).State(), Equals, LoadStateIdle)
	c.Assert(Load(5).State(), Equals, LoadStateLow)
	c.Assert(Load(10).State(), Equals, LoadStateNormal)
	c.Assert(Load(30).State(), Equals, LoadStateHigh)
}
