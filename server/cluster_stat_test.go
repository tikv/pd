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
	"math/rand"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

var _ = Suite(&testClusterStatSuite{})

type testClusterStatSuite struct {
}

// randValues returns an array with values whose
// average is "avg"
func randValues(avg int64, n int) []int64 {
	total := avg * int64(n)
	values := make([]int64, n)
	for i := 0; i < n-1; i++ {
		val := int64(0)
		if total > 0 {
			val = rand.Int63n(total)
		}
		values[i] = val
		total -= val
	}
	values[n-1] = total
	return values
}
func cpu(usage int64) []*pdpb.RecordPair {
	n := 10
	name := "cpu"
	pairs := make([]*pdpb.RecordPair, n)
	values := randValues(usage, n)
	for i := 0; i < n; i++ {
		pairs[i] = &pdpb.RecordPair{
			Key:   fmt.Sprintf("%s:%d", name, i),
			Value: uint64(values[i]),
		}
	}
	return pairs
}

func (s *testClusterStatSuite) TestStatEntriesAppend(c *C) {
	N := 10
	entries := NewStatEntries(N)
	c.Assert(entries, NotNil)

	for i := 0; i < N; i++ {
		entry := &StatEntry{}
		entries.Append(entry)
		c.Assert(entries.total, Equals, i+1)
		c.Assert(entries.entries[i], Equals, entry)
	}

	// overwrite the first entry
	entry := &StatEntry{}
	entries.Append(entry)
	c.Assert(entries.total, Equals, 11)
	c.Assert(entries.entries[0], Equals, entry)
}

func (s *testClusterStatSuite) TestStatEntriesCPU(c *C) {
	N := 10
	entries := NewStatEntries(N)
	c.Assert(entries, NotNil)

	usages := cpu(20)
	// fill the first 5 entries
	for i := 0; i < N-5; i++ {
		entry := &StatEntry{
			CpuUsages: usages,
		}
		entries.Append(entry)
	}
	c.Assert(entries.CPU(N), Equals, float64(20))

	// fullfill the entries
	usages = cpu(40)
	for i := N - 5; i < N; i++ {
		entry := &StatEntry{
			CpuUsages: usages,
		}
		entries.Append(entry)
	}
	c.Assert(entries.CPU(N), Equals, float64(30))

	// overwrite some entries
	usages = cpu(40)
	for i := N; i < N+5; i++ {
		entry := &StatEntry{
			CpuUsages: usages,
		}
		entries.Append(entry)
	}
	c.Assert(entries.CPU(N), Equals, float64(40))
}

func (s *testClusterStatSuite) TestClusterStatEntriesAppend(c *C) {
	N := 10
	cst := NewClusterStatEntries(N)
	c.Assert(cst, NotNil)

	// fill 2*N entries, 2 entries for each store
	for i := 0; i < 2*N; i++ {
		entry := &StatEntry{
			StoreId: uint64(i % N),
		}
		cst.Append(entry)
	}

	// use i as the store ID
	for i := 0; i < N; i++ {
		c.Assert(cst.stats[uint64(i)].total, Equals, 2)
	}
}

func (s *testClusterStatSuite) TestClusterStatCPU(c *C) {
	N := 10
	cst := NewClusterStatEntries(N)
	c.Assert(cst, NotNil)

	// heartbeat per 10s
	interval := &pdpb.TimeInterval{
		StartTimestamp: 0,
		EndTimestamp:   10 * uint64(time.Second),
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
	c.Assert(cst.CPU(100*time.Second), Equals, float64(20))
}

func (s *testClusterStatSuite) TestClusterStatState(c *C) {
	CPU := func(usage int64) *ClusterState {
		cst := NewClusterStatEntries(10)
		c.Assert(cst, NotNil)

		// heartbeat per 10s
		interval := &pdpb.TimeInterval{
			StartTimestamp: 0,
			EndTimestamp:   10 * uint64(time.Second),
		}
		// the average cpu usage is 20%
		usages := cpu(usage)

		for i := 0; i < NumberOfEntries; i++ {
			entry := &StatEntry{
				StoreId:   0,
				Interval:  interval,
				CpuUsages: usages,
			}
			cst.Append(entry)
		}
		return &ClusterState{cst}
	}
	d := 60 * time.Second
	c.Assert(CPU(0).State(d), Equals, LoadStateIdle)
	c.Assert(CPU(20).State(d), Equals, LoadStateLow)
	c.Assert(CPU(50).State(d), Equals, LoadStateNormal)
	c.Assert(CPU(90).State(d), Equals, LoadStateHigh)
}
