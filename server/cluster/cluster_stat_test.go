// Copyright 2019 TiKV Project Authors.
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

package cluster

import (
	"fmt"
	"testing"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
)

func cpu(usage int64) []*pdpb.RecordPair {
	n := 10
	name := "cpu"
	pairs := make([]*pdpb.RecordPair, n)
	for i := range n {
		pairs[i] = &pdpb.RecordPair{
			Key:   fmt.Sprintf("%s:%d", name, i),
			Value: uint64(usage),
		}
	}
	return pairs
}

func TestCPUEntriesAppend(t *testing.T) {
	re := require.New(t)
	N := 10

	checkAppend := func(appended bool, usage int64, threads ...string) {
		entries := NewCPUEntries(N)
		re.NotNil(entries)
		for range N {
			entry := &StatEntry{
				CpuUsages: cpu(usage),
			}
			re.Equal(appended, entries.Append(entry, threads...))
		}
		re.Equal(float64(usage), entries.cpu.Get())
	}

	checkAppend(true, 20)
	checkAppend(true, 20, "cpu")
	checkAppend(false, 0, "cup")
}

func TestCPUEntriesCPU(t *testing.T) {
	re := require.New(t)
	N := 10
	entries := NewCPUEntries(N)
	re.NotNil(entries)

	usages := cpu(20)
	for range N {
		entry := &StatEntry{
			CpuUsages: usages,
		}
		entries.Append(entry)
	}
	re.Equal(float64(20), entries.CPU())
}

func TestStatEntriesAppend(t *testing.T) {
	re := require.New(t)
	N := 10
	cst := NewStatEntries(N)
	re.NotNil(cst)
	ThreadsCollected = []string{"cpu:"}

	// fill 2*N entries, 2 entries for each store
	for i := range 2 * N {
		entry := &StatEntry{
			StoreId:   uint64(i % N),
			CpuUsages: cpu(20),
		}
		re.True(cst.Append(entry))
	}

	// use i as the store ID
	for i := range N {
		re.Equal(float64(20), cst.stats[uint64(i)].CPU())
	}
}

func TestStatEntriesCPU(t *testing.T) {
	re := require.New(t)
	N := 10
	cst := NewStatEntries(N)
	re.NotNil(cst)

	// the average cpu usage is 20%
	usages := cpu(20)
	ThreadsCollected = []string{"cpu:"}

	// 2 entries per store
	for i := range 2 * N {
		entry := &StatEntry{
			StoreId:   uint64(i % N),
			CpuUsages: usages,
		}
		re.True(cst.Append(entry))
	}

	re.Equal(int64(2*N), cst.total)
	// the cpu usage of the whole cluster is 20%
	re.Equal(float64(20), cst.CPU())
}
func TestStatEntriesCPUStale(t *testing.T) {
	re := require.New(t)
	N := 10
	cst := NewStatEntries(N)
	// make all entries stale immediately
	cst.ttl = 0

	usages := cpu(20)
	ThreadsCollected = []string{"cpu:"}
	for i := range 2 * N {
		entry := &StatEntry{
			StoreId:   uint64(i % N),
			CpuUsages: usages,
		}
		cst.Append(entry)
	}
	re.Equal(float64(0), cst.CPU())
}

func TestStatEntriesState(t *testing.T) {
	re := require.New(t)
	Load := func(usage int64) *State {
		cst := NewStatEntries(10)
		re.NotNil(cst)

		usages := cpu(usage)
		ThreadsCollected = []string{"cpu:"}

		for range NumberOfEntries {
			entry := &StatEntry{
				StoreId:   0,
				CpuUsages: usages,
			}
			cst.Append(entry)
		}
		return &State{cst}
	}
	re.Equal(LoadStateIdle, Load(0).State())
	re.Equal(LoadStateLow, Load(5).State())
	re.Equal(LoadStateNormal, Load(10).State())
	re.Equal(LoadStateHigh, Load(30).State())
}
