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

// storeInfo contains information about a store.
// TODO: Export this to API directly.
type storeInfo struct {
	*metapb.Store
	status *StoreStatus
}

func newStoreInfo(store *metapb.Store) *storeInfo {
	return &storeInfo{
		Store:  store,
		status: newStoreStatus(),
	}
}

func (s *storeInfo) clone() *storeInfo {
	return &storeInfo{
		Store:  proto.Clone(s.Store).(*metapb.Store),
		status: s.status.clone(),
	}
}

func (s *storeInfo) block() {
	s.status.blocked = true
}

func (s *storeInfo) unblock() {
	s.status.blocked = false
}

func (s *storeInfo) isBlocked() bool {
	return s.status.blocked
}

func (s *storeInfo) isUp() bool {
	return s.GetState() == metapb.StoreState_Up
}

func (s *storeInfo) isOffline() bool {
	return s.GetState() == metapb.StoreState_Offline
}

func (s *storeInfo) isTombstone() bool {
	return s.GetState() == metapb.StoreState_Tombstone
}

func (s *storeInfo) downTime() time.Duration {
	return time.Since(s.status.LastHeartbeatTS)
}

func (s *storeInfo) leaderCount() uint64 {
	return uint64(s.status.LeaderCount)
}

func (s *storeInfo) leaderScore() float64 {
	return float64(s.status.LeaderCount)
}

func (s *storeInfo) regionCount() uint64 {
	return uint64(s.status.RegionCount)
}

func (s *storeInfo) regionScore() float64 {
	if s.status.GetCapacity() == 0 {
		return 0
	}
	return float64(s.status.RegionCount) / float64(s.status.GetCapacity())
}

func (s *storeInfo) storageSize() uint64 {
	return s.status.UsedSize
}

func (s *storeInfo) availableRatio() float64 {
	if s.status.GetCapacity() == 0 {
		return 0
	}
	return float64(s.status.GetAvailable()) / float64(s.status.GetCapacity())
}

func (s *storeInfo) resourceCount(kind ResourceKind) uint64 {
	switch kind {
	case LeaderKind:
		return s.leaderCount()
	case RegionKind:
		return s.regionCount()
	default:
		return 0
	}
}

func (s *storeInfo) resourceScore(kind ResourceKind) float64 {
	switch kind {
	case LeaderKind:
		return s.leaderScore()
	case RegionKind:
		return s.regionScore()
	default:
		return 0
	}
}

func (s *storeInfo) getLabelValue(key string) string {
	for _, label := range s.GetLabels() {
		if label.GetKey() == key {
			return label.GetValue()
		}
	}
	return ""
}

// compareLocation compares 2 stores' labels and returns at which level their
// locations are different. It returns -1 if they are at the same location.
func (s *storeInfo) compareLocation(other *storeInfo, labels []string) int {
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

func (s *storeInfo) mergeLabels(labels []*metapb.StoreLabel) {
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

// StoreStatus contains information about a store's status.
type StoreStatus struct {
	*pdpb.StoreStats

	// Blocked means that the store is blocked from balance.
	blocked         bool
	LeaderCount     int
	RegionCount     int
	LastHeartbeatTS time.Time `json:"last_heartbeat_ts"`
}

func newStoreStatus() *StoreStatus {
	return &StoreStatus{
		StoreStats: &pdpb.StoreStats{},
	}
}

func (s *StoreStatus) clone() *StoreStatus {
	return &StoreStatus{
		StoreStats:      proto.Clone(s.StoreStats).(*pdpb.StoreStats),
		blocked:         s.blocked,
		LeaderCount:     s.LeaderCount,
		RegionCount:     s.RegionCount,
		LastHeartbeatTS: s.LastHeartbeatTS,
	}
}

// GetStartTS returns the start timestamp.
func (s *StoreStatus) GetStartTS() time.Time {
	return time.Unix(int64(s.GetStartTime()), 0)
}

// GetUptime returns the uptime.
func (s *StoreStatus) GetUptime() time.Duration {
	uptime := s.LastHeartbeatTS.Sub(s.GetStartTS())
	if uptime > 0 {
		return uptime
	}
	return 0
}

const defaultStoreDownTime = time.Minute

// IsDown returns whether the store is down
func (s *StoreStatus) IsDown() bool {
	return time.Now().Sub(s.LastHeartbeatTS) > defaultStoreDownTime
}
