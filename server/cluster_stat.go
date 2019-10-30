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
	"sync"

	"github.com/pingcap/kvproto/pkg/pdpb"
)

// Cluster State Statistics
//
// The target of cluster state statistics is to statistic the load state
// of a cluster given a time duration. The basic idea is to collect all
// the load information from every store at the same time duration and caculates
// the load for the whole cluster.
//
// Now we just support CPU as the measurement of the load. The CPU information
// is reported by each store with a hearbeat message which sending to PD every
// interval(10s). There is no synchronization between each store, so the stores
// could not send hearbeat messages at the same time, and the collected
// information has time shift.
//
// The diagram below demonstrates the time shift. "|" indicates the latest
// heartbeat.
//
// S1 ------------------------------|---------------------->
// S2 ---------------------------|------------------------->
// S3 ---------------------------------|------------------->
//
// The max time shift between 2 stores is 2*interval which is 20s here, and
// this is also the max time shift for the whole cluster. We assume that the
// time of starting to heartbeat is randomized, so the average time shift of
// the cluster is 10s. This is acceptable for statstics.
//
// Implementation
//
// Keep a 5min history statistics for each store, the history is stored in a
// circle array which evicting the oldest entry in a FIFO strategy. All the
// stores's histories combines into the cluster's history. So we can caculate
// any load value within 5 minutes. The algorithm for caculating is simple,
// Iterate each store's history from the latest entry with the same step and
// caculates the average CPU usage for the cluster.
//
// For example.
// To caculate the average load of the cluster within 3 minutes, start from the
// tail of circle array(which stores the history), and backward 18 steps to
// collect all the statistics that being accessed, then caculates the average
// CPU usage for this store. The average of all the stores CPU usage is the
// CPU usage of the whole cluster.
//

// LoadState indicates the load of a cluster or store
type LoadState int

// LoadStates that supported, None means no state determined
const (
	None LoadState = iota
	Idle
	LowLoad
	NormalLoad
	HighLoad
)

// StatEntry is an entry of store statistics
type StatEntry *pdpb.StoreStats

// StatEntries saves a history of store statistics
type StatEntries struct {
	// The index is used to indicate the latest position, there is no oldest
	// position so that it may waste some cpu to do caculation if the array
	// is not full. It will not affects the correctness because the empty
	// entry has empty values(Ex. 0 for CPU usage)
	index   int
	entries []StatEntry
}

// NewStatEntries returns the StateEntries with a fixed size
func NewStatEntries(size int) *StatEntries {
	return &StatEntries{
		index:   0,
		entries: make([]StatEntry, size, size),
	}
}

// Append a StatEntry
func (s *StatEntries) Append(stat StatEntry) {
	idx := (s.index + 1) % cap(s.entries)
	s.entries[idx] = stat
	s.index = idx
}

// CPU returns the cpu usage
func (s *StatEntries) CPU(steps int) float64 {
	cap := cap(s.entries)
	if steps > cap {
		steps = cap
	}

	usage := 0.0
	idx := s.index
	for i := 0; i < steps; i++ {
		stat := s.entries[idx]
		usage += cpuUsageAll(stat.CpuUsages)
		idx--
		if idx < 0 {
			idx += cap
		}
	}
	return usage / float64(steps)
}

func cpuUsageAll(usages []*pdpb.RecordPair) float64 {
	sum := 0.0
	for _, usage := range usages {
		sum += float64(usage.Value)
	}
	return sum / float64(len(usages))
}

// ClusterStatEntries saves the StatEntries for each store in the cluster
type ClusterStatEntries struct {
	m     sync.Mutex
	stats map[uint64]*StatEntries
	size  int // size of entries to keep for each store
}

// Append an store StatEntry
func (cst *ClusterStatEntries) Append(stat StatEntry) {
	cst.m.Lock()
	defer cst.m.Unlock()

	storeID := stat.StoreId
	entries, ok := cst.stats[storeID]
	if !ok {
		entries = NewStatEntries(cst.size)
		cst.stats[storeID] = entries
	}

	entries.Append(stat)
}

// CPU returns the cpu usage of the cluster
func (cst *ClusterStatEntries) CPU(steps int) float64 {
	sum := 0.0
	for _, stat := range cst.stats {
		sum += stat.CPU(steps)
	}
	return sum / float64(len(cst.stats))
}
