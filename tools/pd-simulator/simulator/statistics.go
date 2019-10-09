// Copyright 2018 PingCAP, Inc.
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

package simulator

import (
	"fmt"
	"math"
	"sync"
)

type taskStatistics struct {
	sync.RWMutex
	addPeer        map[uint64]int
	removePeer     map[uint64]int
	addLearner     map[uint64]int
	promoteLeaner  map[uint64]int
	transferLeader map[uint64]map[uint64]int
	mergeRegion    int
}

func newTaskStatistics() *taskStatistics {
	return &taskStatistics{
		addPeer:        make(map[uint64]int),
		removePeer:     make(map[uint64]int),
		addLearner:     make(map[uint64]int),
		promoteLeaner:  make(map[uint64]int),
		transferLeader: make(map[uint64]map[uint64]int),
	}
}

func (t *taskStatistics) getStatistics() map[string]int {
	t.RLock()
	defer t.RUnlock()
	stats := make(map[string]int)
	addpeer := getSum(t.addPeer)
	removePeer := getSum(t.removePeer)
	addLearner := getSum(t.addLearner)
	promoteLeaner := getSum(t.promoteLeaner)

	var transferLeader int
	for _, to := range t.transferLeader {
		for _, v := range to {
			transferLeader += v
		}
	}

	stats["Add Peer (task)"] = addpeer
	stats["Remove Peer (task)"] = removePeer
	stats["Add Learner (task)"] = addLearner
	stats["Promote Learner (task)"] = promoteLeaner
	stats["Transfer Leader (task)"] = transferLeader
	stats["Merge Region (task)"] = t.mergeRegion

	return stats
}

func (t *taskStatistics) incAddPeer(regionID uint64) {
	t.Lock()
	defer t.Unlock()
	t.addPeer[regionID]++
}

func (t *taskStatistics) incAddLeaner(regionID uint64) {
	t.Lock()
	defer t.Unlock()
	t.addLearner[regionID]++
}

func (t *taskStatistics) incPromoteLeaner(regionID uint64) {
	t.Lock()
	defer t.Unlock()
	t.promoteLeaner[regionID]++
}

func (t *taskStatistics) incRemovePeer(regionID uint64) {
	t.Lock()
	defer t.Unlock()
	t.removePeer[regionID]++
}

func (t *taskStatistics) incMergeRegion() {
	t.Lock()
	defer t.Unlock()
	t.mergeRegion++
}

func (t *taskStatistics) incTransferLeader(fromPeerID, toPeerID uint64) {
	t.Lock()
	defer t.Unlock()
	_, ok := t.transferLeader[fromPeerID]
	if ok {
		t.transferLeader[fromPeerID][toPeerID]++
	} else {
		m := make(map[uint64]int)
		m[toPeerID]++
		t.transferLeader[fromPeerID] = m
	}
}

type snapshotStatistics struct {
	sync.RWMutex
	receive map[uint64]int
	send    map[uint64]int
}

func newSnapshotStatistics() *snapshotStatistics {
	return &snapshotStatistics{
		receive: make(map[uint64]int),
		send:    make(map[uint64]int),
	}
}

type schedulerStatistics struct {
	taskStats     *taskStatistics
	snapshotStats *snapshotStatistics
}

func newSchedulerStatistics() *schedulerStatistics {
	return &schedulerStatistics{
		taskStats:     newTaskStatistics(),
		snapshotStats: newSnapshotStatistics(),
	}
}

func (s *snapshotStatistics) getStatistics() map[string]int {
	s.RLock()
	defer s.RUnlock()
	maxSend := getMax(s.send)
	maxReceive := getMax(s.receive)
	minSend := getMin(s.send)
	minReceive := getMin(s.receive)

	stats := make(map[string]int)
	stats["Send Maximum (snapshot)"] = maxSend
	stats["Receive Maximum (snapshot)"] = maxReceive
	if minSend != math.MaxInt32 {
		stats["Send Minimum (snapshot)"] = minSend
	}
	if minReceive != math.MaxInt32 {
		stats["Receive Minimum (snapshot)"] = minReceive
	}

	return stats
}

func (s *snapshotStatistics) incSendSnapshot(storeID uint64) {
	s.Lock()
	defer s.Unlock()
	s.send[storeID]++
}

func (s *snapshotStatistics) incReceiveSnapshot(storeID uint64) {
	s.Lock()
	defer s.Unlock()
	s.receive[storeID]++
}

// PrintStatistics prints the statistics of the scheduler.
func (s *schedulerStatistics) PrintStatistics() {
	task := s.taskStats.getStatistics()
	snap := s.snapshotStats.getStatistics()
	for t, count := range task {
		fmt.Println(t, count)
	}
	for s, count := range snap {
		fmt.Println(s, count)
	}
}

func getMax(m map[uint64]int) int {
	var max int
	for _, v := range m {
		if v > max {
			max = v
		}
	}
	return max
}

func getMin(m map[uint64]int) int {
	min := math.MaxInt32
	for _, v := range m {
		if v < min {
			min = v
		}
	}
	return min
}

func getSum(m map[uint64]int) int {
	var sum int
	for _, v := range m {
		sum += v
	}
	return sum
}
