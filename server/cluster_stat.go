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
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
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
	LoadStateNone LoadState = iota
	LoadStateIdle
	LoadStateLow
	LoadStateNormal
	LoadStateHigh
)

// DefaultIntervalHint is a hint of the interval of the store heartbeats,
// it is used when we can not infer the interval from heartbeats.
const DefaultIntervalHint = 10

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
// it is the history of a store's hearbeats. The interval of store
// hearbeats from TiKV is 10s, so we can preserve 30 entries per
// store which is about 5 minutes.
const NumberOfEntries = 30

// StatEntry is an entry of store statistics
type StatEntry pdpb.StoreStats

// StatEntries saves a history of store statistics
type StatEntries struct {
	total   int
	entries []*StatEntry
}

// NewStatEntries returns the StateEntries with a fixed size
func NewStatEntries(size int) *StatEntries {
	return &StatEntries{
		total:   0,
		entries: make([]*StatEntry, size, size),
	}
}

// Append a StatEntry
func (s *StatEntries) Append(stat *StatEntry) {
	idx := s.total % cap(s.entries)
	s.entries[idx] = stat
	s.total++
}

// CPU returns the cpu usage
func (s *StatEntries) CPU(steps int) float64 {
	cap := cap(s.entries)
	if steps > cap {
		steps = cap
	}
	if steps > s.total {
		steps = s.total
	}
	if steps == 0 {
		return float64(0)
	}

	log.Debug("caculate CPU", zap.Int("steps", steps))
	usage := 0.0
	idx := (s.total - 1) % cap
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
	if len(usages) == 0 {
		return sum
	}
	for _, usage := range usages {
		sum += float64(usage.Value)
	}
	return sum / float64(len(usages))
}

// Keys returns the average written and read keys duration
// an interval of heartbeats
func (s *StatEntries) Keys(steps int) (read int64, written int64) {
	cap := cap(s.entries)
	if steps > cap {
		steps = cap
	}
	if steps > s.total {
		steps = s.total
	}
	if steps == 0 {
		return 0, 0
	}

	idx := (s.total - 1) % cap
	for i := 0; i < steps; i++ {
		stat := s.entries[idx]
		read += int64(stat.KeysRead)
		written += int64(stat.KeysWritten)
		idx--
		if idx < 0 {
			idx += cap
		}
	}
	return read / int64(steps), written / int64(steps)
}

// Bytes returns the average written and read bytes duration
// an interval of heartbeats
func (s *StatEntries) Bytes(steps int) (read int64, written int64) {
	cap := cap(s.entries)
	if steps > cap {
		steps = cap
	}
	if steps > s.total {
		steps = s.total
	}
	if steps == 0 {
		return 0, 0
	}
	idx := (s.total - 1) % cap
	for i := 0; i < steps; i++ {
		stat := s.entries[idx]
		read += int64(stat.BytesRead)
		written += int64(stat.BytesWritten)
		idx--
		if idx < 0 {
			idx += cap
		}
	}
	return read / int64(steps), written / int64(steps)
}

// ClusterStatEntries saves the StatEntries for each store in the cluster
type ClusterStatEntries struct {
	m        sync.RWMutex
	stats    map[uint64]*StatEntries
	size     int   // size of entries to keep for each store
	interval int64 // average interval of heartbeats
	total    int64 // total of StatEntry appended
}

// NewClusterStatEntries returns a statistics object for the cluster
func NewClusterStatEntries(size int) *ClusterStatEntries {
	return &ClusterStatEntries{
		stats: make(map[uint64]*StatEntries),
		size:  size,
	}
}

// Append an store StatEntry
func (cst *ClusterStatEntries) Append(stat *StatEntry) {
	cst.m.Lock()
	defer cst.m.Unlock()

	// update interval
	start, end := stat.Interval.GetStartTimestamp(),
		stat.Interval.GetEndTimestamp()
	interval := int64(end - start)
	if start == 0 || end == 0 || interval == 0 {
		interval = DefaultIntervalHint
	}
	if cst.total == 0 {
		cst.interval = interval
	} else {
		cst.interval = cst.interval + (interval-cst.interval)/cst.total
	}
	cst.total++

	// append the entry
	storeID := stat.StoreId
	entries, ok := cst.stats[storeID]
	if !ok {
		entries = NewStatEntries(cst.size)
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
func (cst *ClusterStatEntries) CPU(d time.Duration, excludes ...uint64) float64 {
	cst.m.RLock()
	defer cst.m.RUnlock()

	// no entries have been collected
	if cst.total == 0 {
		return 0
	}

	steps := int64(d) / cst.interval

	sum := 0.0
	for sid, stat := range cst.stats {
		if contains(excludes, sid) {
			continue
		}
		sum += stat.CPU(int(steps))
	}
	return sum / float64(len(cst.stats))
}

// Keys returns the average written and read keys duration
// an interval of heartbeats for the cluster
func (cst *ClusterStatEntries) Keys(d time.Duration, excludes ...uint64) (read int64, written int64) {
	cst.m.RLock()
	defer cst.m.RUnlock()
	// no entries have been collected
	if cst.total == 0 {
		return 0, 0
	}

	steps := int64(d) / cst.interval

	for sid, stat := range cst.stats {
		if contains(excludes, sid) {
			continue
		}
		r, w := stat.Keys(int(steps))
		read += r
		written += w
	}
	return read / int64(len(cst.stats)), written / int64(len(cst.stats))
}

// Bytes returns the average written and read bytes duration
// an interval of heartbeats for the cluster
func (cst *ClusterStatEntries) Bytes(d time.Duration, excludes ...uint64) (read int64, written int64) {
	cst.m.RLock()
	defer cst.m.RUnlock()
	// no entries have been collected
	if cst.total == 0 {
		return 0, 0
	}

	steps := int64(d) / cst.interval

	for sid, stat := range cst.stats {
		if contains(excludes, sid) {
			continue
		}
		r, w := stat.Bytes(int(steps))
		read += r
		written += w
	}
	return read / int64(len(cst.stats)), written / int64(len(cst.stats))
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
func (cs *ClusterState) State(d time.Duration, excludes ...uint64) LoadState {
	// Return LoadStateNone if there is not enough hearbeats
	// collected.
	if cs.cst.total < NumberOfEntries {
		return LoadStateNone
	}
	read, written := cs.cst.Bytes(d, excludes...)
	if read == 0 && written == 0 {
		return LoadStateIdle
	}
	read, written = cs.cst.Keys(d, excludes...)
	if read == 0 && written == 0 {
		return LoadStateIdle
	}
	cpu := cs.cst.CPU(d, excludes...)
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
