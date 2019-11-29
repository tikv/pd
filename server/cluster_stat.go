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
	"strings"
	"sync"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/pkg/slice"
	"github.com/pingcap/pd/server/statistics"
)

// Cluster State Statistics
//
// The target of cluster state statistics is to statistic the load state
// of a cluster given a time duration. The basic idea is to collect all
// the load information from every store at the same time duration and caculates
// the load for the whole cluster.
//
// Now we just support CPU as the measurement of the load. The CPU information
// is reported by each store with a heartbeat message which sending to PD every
// interval(10s). There is no synchronization between each store, so the stores
// could not send heartbeat messages at the same time, and the collected
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
	LoadStateNone LoadState = iota
	LoadStateIdle
	LoadStateLow
	LoadStateNormal
	LoadStateHigh
)

// String representation of LoadState
func (s LoadState) String() string {
	switch s {
	case LoadStateIdle:
		return "idle"
	case LoadStateLow:
		return "low"
	case LoadStateNormal:
		return "normal"
	case LoadStateHigh:
		return "high"
	}
	return "none"
}

// NumberOfEntries is the max number of StatEntry that preserved,
// it is the history of a store's heartbeats. The interval of store
// heartbeats from TiKV is 10s, so we can preserve 30 entries per
// store which is about 5 minutes.
const NumberOfEntries = 30

// StatEntry is an entry of store statistics
type StatEntry pdpb.StoreStats

// CPUStatEntries saves a history of store statistics
type CPUStatEntries struct {
	cpu statistics.MovingAvg
}

// NewCPUStatEntries returns the StateEntries with a fixed size
func NewCPUStatEntries(size int) *CPUStatEntries {
	return &CPUStatEntries{
		cpu: statistics.NewMedianFilter(size),
	}
}

// Append a StatEntry, it accepts an optional threads as a filter of CPU usage
func (s *CPUStatEntries) Append(stat *StatEntry, threads ...string) bool {
	usages := stat.CpuUsages
	// all gRPC fields are optional, so we must check the empty value
	if usages == nil {
		return false
	}

	cpu := float64(0)
	appended := 0
	for _, usage := range usages {
		name := usage.GetKey()
		value := usage.GetValue()
		if threads != nil && slice.NoneOf(threads, func(i int) bool {
			return strings.HasPrefix(name, threads[i])
		}) {
			continue
		}
		cpu += float64(value)
		appended++
	}
	if appended > 0 {
		s.cpu.Add(cpu / float64(appended))
		return true
	}
	return false
}

// CPU returns the cpu usage
func (s *CPUStatEntries) CPU() float64 {
	return s.cpu.Get()
}

// ClusterStatEntries saves the StatEntries for each store in the cluster
type ClusterStatEntries struct {
	m     sync.RWMutex
	stats map[uint64]*CPUStatEntries
	size  int   // size of entries to keep for each store
	total int64 // total of StatEntry appended
}

// NewClusterStatEntries returns a statistics object for the cluster
func NewClusterStatEntries(size int) *ClusterStatEntries {
	return &ClusterStatEntries{
		stats: make(map[uint64]*CPUStatEntries),
		size:  size,
	}
}

// Append an store StatEntry
func (cst *ClusterStatEntries) Append(stat *StatEntry) {
	cst.m.Lock()
	defer cst.m.Unlock()

	cst.total++

	// append the entry
	storeID := stat.StoreId
	entries, ok := cst.stats[storeID]
	if !ok {
		entries = NewCPUStatEntries(cst.size)
		cst.stats[storeID] = entries
	}

	entries.Append(stat)
}

func contains(slice []uint64, value uint64) bool {
	for i := range slice {
		if slice[i] == value {
			return true
		}
	}
	return false
}

// CPU returns the cpu usage of the cluster
func (cst *ClusterStatEntries) CPU(excludes ...uint64) float64 {
	cst.m.RLock()
	defer cst.m.RUnlock()

	// no entries have been collected
	if cst.total == 0 {
		return 0
	}

	sum := 0.0
	for sid, stat := range cst.stats {
		if contains(excludes, sid) {
			continue
		}
		sum += stat.CPU()
	}
	return sum / float64(len(cst.stats))
}

// ClusterState collects information from store heartbeat
// and caculates the load state of the cluster
type ClusterState struct {
	cst *ClusterStatEntries
}

// NewClusterState return the ClusterState object which collects
// information from store heartbeats and gives the current state of
// the cluster
func NewClusterState() *ClusterState {
	return &ClusterState{
		cst: NewClusterStatEntries(NumberOfEntries),
	}
}

// State returns the state of the cluster, excludes is the list of store ID
// to be excluded
func (cs *ClusterState) State(excludes ...uint64) LoadState {
	// Return LoadStateNone if there is not enough heartbeats
	// collected.
	if cs.cst.total < NumberOfEntries {
		return LoadStateNone
	}

	cpu := cs.cst.CPU(excludes...)
	switch {
	case cpu == 0:
		return LoadStateIdle
	case cpu > 0 && cpu < 30:
		return LoadStateLow
	case cpu >= 30 && cpu < 80:
		return LoadStateNormal
	case cpu >= 80:
		return LoadStateHigh
	}
	return LoadStateNone
}

// Collect statistics from store heartbeat
func (cs *ClusterState) Collect(stat *StatEntry) {
	cs.cst.Append(stat)
}
