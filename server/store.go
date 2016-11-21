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

// ResourceKind distinguishes different kinds of resources.
type ResourceKind int

const (
	leaderKind ResourceKind = iota + 1
	storageKind
)

// storeInfo contains information about a store.
// TODO: Export this to API directly.
type storeInfo struct {
	*metapb.Store
	stats *StoreStatus
}

func newStoreInfo(store *metapb.Store) *storeInfo {
	return &storeInfo{
		Store: store,
		stats: newStoreStatus(),
	}
}

func (s *storeInfo) clone() *storeInfo {
	return &storeInfo{
		Store: proto.Clone(s.Store).(*metapb.Store),
		stats: s.stats.clone(),
	}
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
	return time.Since(s.stats.LastHeartbeatTS)
}

func (s *storeInfo) leaderRatio() float64 {
	if s.stats.TotalRegionCount == 0 {
		return 0
	}
	return float64(s.stats.LeaderRegionCount) / float64(s.stats.TotalRegionCount)
}

func (s *storeInfo) storageRatio() float64 {
	if s.stats.GetCapacity() == 0 {
		return 0
	}
	return float64(s.stats.GetUsedSize()) / float64(s.stats.GetCapacity())
}

func (s *storeInfo) resourceRatio(kind ResourceKind) float64 {
	switch kind {
	case leaderKind:
		return s.leaderRatio()
	case storageKind:
		return s.storageRatio()
	default:
		return 0
	}
}

func (s *storeInfo) resourceScores() []int {
	var scores []int
	scores = append(scores, int(s.leaderRatio()*100))
	scores = append(scores, int(s.storageRatio()*100))
	return scores
}

func (s *storeInfo) getScore(kind ResourceKind) float64 {
	switch kind {
	case leaderKind:
		return s.leaderRatio()
	case storageKind:
		return s.usedRatio()
	default:
		return 0
	}
}

// StoreStatus contains information about a store's status.
type StoreStatus struct {
	*pdpb.StoreStats

	StartTS           time.Time `json:"start_ts"`
	LastHeartbeatTS   time.Time `json:"last_heartbeat_ts"`
	TotalRegionCount  int       `json:"total_region_count"`
	LeaderRegionCount int       `json:"leader_region_count"`
}

func newStoreStatus() *StoreStatus {
	return &StoreStatus{
		StoreStats: &pdpb.StoreStats{},
		StartTS:    time.Now(),
	}
}

func (s *StoreStatus) clone() *StoreStatus {
	return &StoreStatus{
		StoreStats:        proto.Clone(s.StoreStats).(*pdpb.StoreStats),
		StartTS:           s.StartTS,
		LastHeartbeatTS:   s.LastHeartbeatTS,
		TotalRegionCount:  s.TotalRegionCount,
		LeaderRegionCount: s.LeaderRegionCount,
	}
}

// GetUptime returns the uptime of the store.
func (s *StoreStatus) GetUptime() time.Duration {
	uptime := s.LastHeartbeatTS.Sub(s.StartTS)
	if uptime > 0 {
		return uptime
	}
	return 0
}

// GetUsedSize returns the used storage size.
func (s *StoreStatus) GetUsedSize() uint64 {
	return s.GetCapacity() - s.GetAvailable()
}
