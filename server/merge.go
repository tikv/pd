// Copyright 2016 PingCAP, Inc.
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

package server

type mergeInfo struct {
	fromRegions     map[uint64]uint64 // from region id -> into region id
	intoRegions     map[uint64]uint64 // into region id -> from region id
	shutdownRegions map[uint64]struct{}
}

func newMergeInfo() *mergeInfo {
	return &mergeInfo{
		fromRegions:     make(map[uint64]uint64),
		intoRegions:     make(map[uint64]uint64),
		shutdownRegions: make(map[uint64]struct{}),
	}
}

func (m *mergeInfo) markRegionMerge(fromRegionID, intoRegionID uint64) {
	m.fromRegions[fromRegionID] = intoRegionID
	m.intoRegions[intoRegionID] = fromRegionID
}

func (m *mergeInfo) clearRegionMerge(fromRegionID uint64) {
	intoRegionID, ok := m.fromRegions[fromRegionID]
	if ok {
		delete(m.fromRegions, fromRegionID)
		delete(m.intoRegions, intoRegionID)
	}
}

func (m *mergeInfo) markRegionShutdown(regionID uint64) {
	m.shutdownRegions[regionID] = struct{}{}
}

func (m *mergeInfo) isRegionMerging(regionID uint64) bool {
	if _, ok := m.fromRegions[regionID]; ok {
		return true
	}
	if _, ok := m.intoRegions[regionID]; ok {
		return true
	}
	return false
}

func (m *mergeInfo) isRegionShutdown(regionID uint64) bool {
	_, ok := m.shutdownRegions[regionID]
	return ok
}

func (m *mergeInfo) getMergingRegions(regionID uint64) (uint64, uint64, bool) {
	if intoRegionID, ok := m.fromRegions[regionID]; ok {
		return regionID, intoRegionID, true
	}
	if fromRegionID, ok := m.intoRegions[regionID]; ok {
		return fromRegionID, regionID, true
	}
	return 0, 0, false
}

func (m *mergeInfo) getMergingRegionCount() int {
	return len(m.fromRegions) + len(m.intoRegions)
}

func (m *mergeInfo) getShutdownRegionCount() int {
	return len(m.shutdownRegions)
}
