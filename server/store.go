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

import (
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

// StoreInfo contains information about a store.
type StoreInfo struct {
	*metapb.Store
	Stats *pdpb.StoreStats
	// Blocked means that the store is blocked from balance.
	blocked         bool
	LeaderCount     int
	RegionCount     int
	LastHeartbeatTS time.Time
	LeaderWeight    float64
	RegionWeight    float64
}

func newStoreInfo(store *metapb.Store) *StoreInfo {
	return &StoreInfo{
		Store:        store,
		LeaderWeight: 1.0,
		RegionWeight: 1.0,
	}
}

func (s *StoreInfo) clone() *StoreInfo {
	return &StoreInfo{
		Store:           proto.Clone(s.Store).(*metapb.Store),
		Stats:           proto.Clone(s.Stats).(*pdpb.StoreStats),
		blocked:         s.blocked,
		LeaderCount:     s.LeaderCount,
		RegionCount:     s.RegionCount,
		LastHeartbeatTS: s.LastHeartbeatTS,
		LeaderWeight:    s.LeaderWeight,
		RegionWeight:    s.RegionWeight,
	}
}

func (s *StoreInfo) block() {
	s.blocked = true
}

func (s *StoreInfo) unblock() {
	s.blocked = false
}

func (s *StoreInfo) isBlocked() bool {
	return s.blocked
}

func (s *StoreInfo) isUp() bool {
	return s.GetState() == metapb.StoreState_Up
}

func (s *StoreInfo) isOffline() bool {
	return s.GetState() == metapb.StoreState_Offline
}

func (s *StoreInfo) isTombstone() bool {
	return s.GetState() == metapb.StoreState_Tombstone
}

func (s *StoreInfo) downTime() time.Duration {
	return time.Since(s.LastHeartbeatTS)
}

func (s *StoreInfo) leaderCount() uint64 {
	return uint64(s.LeaderCount)
}

const minWeight = 1e-6

// LeaderScore returns the store's leader score: leaderCount / leaderWeight.
func (s *StoreInfo) LeaderScore() float64 {
	if s.LeaderWeight <= 0 {
		return float64(s.LeaderCount) / minWeight
	}
	return float64(s.LeaderCount) / s.LeaderWeight
}

func (s *StoreInfo) regionCount() uint64 {
	return uint64(s.RegionCount)
}

// RegionScore returns the store's region score: regionCount / regionWeight.
func (s *StoreInfo) RegionScore() float64 {
	if s.RegionWeight <= 0 {
		return float64(s.RegionCount) / minWeight
	}
	return float64(s.RegionCount) / s.RegionWeight
}

func (s *StoreInfo) storageSize() uint64 {
	return s.Stats.GetUsedSize()
}

func (s *StoreInfo) availableRatio() float64 {
	if s.Stats.GetCapacity() == 0 {
		return 0
	}
	return float64(s.Stats.GetAvailable()) / float64(s.Stats.GetCapacity())
}

func (s *StoreInfo) resourceCount(kind ResourceKind) uint64 {
	switch kind {
	case LeaderKind:
		return s.leaderCount()
	case RegionKind:
		return s.regionCount()
	default:
		return 0
	}
}

func (s *StoreInfo) resourceScore(kind ResourceKind) float64 {
	switch kind {
	case LeaderKind:
		return s.LeaderScore()
	case RegionKind:
		return s.RegionScore()
	default:
		return 0
	}
}

// GetStartTS returns the start timestamp.
func (s *StoreInfo) GetStartTS() time.Time {
	return time.Unix(int64(s.Stats.GetStartTime()), 0)
}

// GetUptime returns the uptime.
func (s *StoreInfo) GetUptime() time.Duration {
	uptime := s.LastHeartbeatTS.Sub(s.GetStartTS())
	if uptime > 0 {
		return uptime
	}
	return 0
}

const defaultStoreDownTime = time.Minute

// IsDown returns whether the store is down
func (s *StoreInfo) IsDown() bool {
	return time.Now().Sub(s.LastHeartbeatTS) > defaultStoreDownTime
}

func (s *StoreInfo) getLabelValue(key string) string {
	for _, label := range s.GetLabels() {
		if label.GetKey() == key {
			return label.GetValue()
		}
	}
	return ""
}

// compareLocation compares 2 stores' labels and returns at which level their
// locations are different. It returns -1 if they are at the same location.
func (s *StoreInfo) compareLocation(other *StoreInfo, labels []string) int {
	for i, key := range labels {
		v1, v2 := s.getLabelValue(key), other.getLabelValue(key)
		// If label is not set, the store is considered at the same location
		// with any other store.
		if v1 != "" && v2 != "" && v1 != v2 {
			return i
		}
	}
	return -1
}

func (s *StoreInfo) mergeLabels(labels []*metapb.StoreLabel) {
L:
	for _, newLabel := range labels {
		for _, label := range s.Labels {
			if label.Key == newLabel.Key {
				label.Value = newLabel.Value
				continue L
			}
		}
		s.Labels = append(s.Labels, newLabel)
	}
}
