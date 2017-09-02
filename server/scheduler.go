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
	"fmt"
	"math"

	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
)

// Scheduler is an interface to schedule resources.
type Scheduler interface {
	GetName() string
	GetResourceKind() core.ResourceKind
	GetResourceLimit() uint64
	Prepare(cluster *clusterInfo) error
	Cleanup(cluster *clusterInfo)
	Schedule(cluster *clusterInfo) schedule.Operator
}

// grantLeaderScheduler transfers all leaders to peers in the store.
type grantLeaderScheduler struct {
	opt     *scheduleOption
	name    string
	storeID uint64
}

func newGrantLeaderScheduler(opt *scheduleOption, storeID uint64) *grantLeaderScheduler {
	return &grantLeaderScheduler{
		opt:     opt,
		name:    fmt.Sprintf("grant-leader-scheduler-%d", storeID),
		storeID: storeID,
	}
}

func (s *grantLeaderScheduler) GetName() string {
	return s.name
}

func (s *grantLeaderScheduler) GetResourceKind() core.ResourceKind {
	return core.LeaderKind
}

func (s *grantLeaderScheduler) GetResourceLimit() uint64 {
	return s.opt.GetLeaderScheduleLimit()
}

func (s *grantLeaderScheduler) Prepare(cluster *clusterInfo) error {
	return errors.Trace(cluster.blockStore(s.storeID))
}

func (s *grantLeaderScheduler) Cleanup(cluster *clusterInfo) {
	cluster.unblockStore(s.storeID)
}

func (s *grantLeaderScheduler) Schedule(cluster *clusterInfo) schedule.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	region := cluster.randFollowerRegion(s.storeID)
	if region == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no_follower").Inc()
		return nil
	}
	schedulerCounter.WithLabelValues(s.GetName(), "new_operator").Inc()
	return newTransferLeader(region, region.GetStorePeer(s.storeID))
}

type evictLeaderScheduler struct {
	opt      *scheduleOption
	name     string
	storeID  uint64
	selector schedule.Selector
}

func newEvictLeaderScheduler(opt *scheduleOption, storeID uint64) *evictLeaderScheduler {
	filters := []schedule.Filter{
		schedule.NewStateFilter(opt),
		schedule.NewHealthFilter(opt),
	}

	return &evictLeaderScheduler{
		opt:      opt,
		name:     fmt.Sprintf("evict-leader-scheduler-%d", storeID),
		storeID:  storeID,
		selector: schedule.NewRandomSelector(filters),
	}
}

func (s *evictLeaderScheduler) GetName() string {
	return s.name
}

func (s *evictLeaderScheduler) GetResourceKind() core.ResourceKind {
	return core.LeaderKind
}

func (s *evictLeaderScheduler) GetResourceLimit() uint64 {
	return s.opt.GetLeaderScheduleLimit()
}

func (s *evictLeaderScheduler) Prepare(cluster *clusterInfo) error {
	return errors.Trace(cluster.blockStore(s.storeID))
}

func (s *evictLeaderScheduler) Cleanup(cluster *clusterInfo) {
	cluster.unblockStore(s.storeID)
}

func (s *evictLeaderScheduler) Schedule(cluster *clusterInfo) schedule.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	region := cluster.randLeaderRegion(s.storeID)
	if region == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no_leader").Inc()
		return nil
	}
	target := s.selector.SelectTarget(cluster.getFollowerStores(region))
	if target == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no_target_store").Inc()
		return nil
	}
	schedulerCounter.WithLabelValues(s.GetName(), "new_operator").Inc()
	return newTransferLeader(region, region.GetStorePeer(target.GetId()))
}

type shuffleLeaderScheduler struct {
	opt      *scheduleOption
	selector schedule.Selector
	selected *metapb.Peer
}

func newShuffleLeaderScheduler(opt *scheduleOption) *shuffleLeaderScheduler {
	filters := []schedule.Filter{
		schedule.NewStateFilter(opt),
		schedule.NewHealthFilter(opt),
	}

	return &shuffleLeaderScheduler{
		opt:      opt,
		selector: schedule.NewRandomSelector(filters),
	}
}

func (s *shuffleLeaderScheduler) GetName() string {
	return "shuffle-leader-scheduler"
}

func (s *shuffleLeaderScheduler) GetResourceKind() core.ResourceKind {
	return core.LeaderKind
}

func (s *shuffleLeaderScheduler) GetResourceLimit() uint64 {
	return s.opt.GetLeaderScheduleLimit()
}

func (s *shuffleLeaderScheduler) Prepare(cluster *clusterInfo) error { return nil }

func (s *shuffleLeaderScheduler) Cleanup(cluster *clusterInfo) {}

func (s *shuffleLeaderScheduler) Schedule(cluster *clusterInfo) schedule.Operator {
	// We shuffle leaders between stores:
	// 1. select a store randomly.
	// 2. transfer a leader from the store to another store.
	// 3. transfer a leader to the store from another store.
	// These will not change store's leader count, but swap leaders between stores.

	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	// Select a store and transfer a leader from it.
	if s.selected == nil {
		region, newLeader := scheduleTransferLeader(cluster, s.GetName(), s.selector)
		if region == nil {
			return nil
		}
		// Mark the selected store.
		s.selected = region.Leader
		schedulerCounter.WithLabelValues(s.GetName(), "new_operator").Inc()
		return newTransferLeader(region, newLeader)
	}

	// Reset the selected store.
	storeID := s.selected.GetStoreId()
	s.selected = nil

	// Transfer a leader to the selected store.
	region := cluster.randFollowerRegion(storeID)
	if region == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no_follower").Inc()
		return nil
	}
	schedulerCounter.WithLabelValues(s.GetName(), "new_operator").Inc()
	return newTransferLeader(region, region.GetStorePeer(storeID))
}

type shuffleRegionScheduler struct {
	opt      *scheduleOption
	selector schedule.Selector
}

func newShuffleRegionScheduler(opt *scheduleOption) *shuffleRegionScheduler {
	filters := []schedule.Filter{
		schedule.NewStateFilter(opt),
		schedule.NewHealthFilter(opt),
	}

	return &shuffleRegionScheduler{
		opt:      opt,
		selector: schedule.NewRandomSelector(filters),
	}
}

func (s *shuffleRegionScheduler) GetName() string {
	return "shuffle-region-scheduler"
}

func (s *shuffleRegionScheduler) GetResourceKind() core.ResourceKind {
	return core.RegionKind
}

func (s *shuffleRegionScheduler) GetResourceLimit() uint64 {
	return s.opt.GetRegionScheduleLimit()
}

func (s *shuffleRegionScheduler) Prepare(cluster *clusterInfo) error { return nil }

func (s *shuffleRegionScheduler) Cleanup(cluster *clusterInfo) {}

func (s *shuffleRegionScheduler) Schedule(cluster *clusterInfo) schedule.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	region, oldPeer := scheduleRemovePeer(cluster, s.GetName(), s.selector)
	if region == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no_region").Inc()
		return nil
	}

	excludedFilter := schedule.NewExcludedFilter(nil, region.GetStoreIds())
	newPeer := scheduleAddPeer(cluster, s.selector, excludedFilter)
	if newPeer == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no_new_peer").Inc()
		return nil
	}

	schedulerCounter.WithLabelValues(s.GetName(), "new_operator").Inc()
	return newTransferPeer(region, core.RegionKind, oldPeer, newPeer)
}

func newAddPeer(region *core.RegionInfo, peer *metapb.Peer) schedule.Operator {
	addPeer := schedule.NewAddPeerOperator(region.GetId(), peer)
	return schedule.NewRegionOperator(region, core.RegionKind, addPeer)
}

func newRemovePeer(region *core.RegionInfo, peer *metapb.Peer) schedule.Operator {
	removePeer := schedule.NewRemovePeerOperator(region.GetId(), peer)
	if region.Leader != nil && region.Leader.GetId() == peer.GetId() {
		if follower := region.GetFollower(); follower != nil {
			transferLeader := schedule.NewTransferLeaderOperator(region.GetId(), region.Leader, follower)
			return schedule.NewRegionOperator(region, core.RegionKind, transferLeader, removePeer)
		}
		return nil
	}
	return schedule.NewRegionOperator(region, core.RegionKind, removePeer)
}

func newTransferPeer(region *core.RegionInfo, kind core.ResourceKind, oldPeer, newPeer *metapb.Peer) schedule.Operator {
	addPeer := schedule.NewAddPeerOperator(region.GetId(), newPeer)
	removePeer := schedule.NewRemovePeerOperator(region.GetId(), oldPeer)
	if region.Leader != nil && region.Leader.GetId() == oldPeer.GetId() {
		newLeader := newPeer
		if follower := region.GetFollower(); follower != nil {
			newLeader = follower
		}
		transferLeader := schedule.NewTransferLeaderOperator(region.GetId(), region.Leader, newLeader)
		return schedule.NewRegionOperator(region, kind, addPeer, transferLeader, removePeer)
	}
	return schedule.NewRegionOperator(region, kind, addPeer, removePeer)
}

func newPriorityTransferLeader(region *core.RegionInfo, newLeader *metapb.Peer) schedule.Operator {
	transferLeader := schedule.NewTransferLeaderOperator(region.GetId(), region.Leader, newLeader)
	return schedule.NewRegionOperator(region, core.PriorityKind, transferLeader)
}

func newTransferLeader(region *core.RegionInfo, newLeader *metapb.Peer) schedule.Operator {
	transferLeader := schedule.NewTransferLeaderOperator(region.GetId(), region.Leader, newLeader)
	return schedule.NewRegionOperator(region, core.LeaderKind, transferLeader)
}

// scheduleAddPeer schedules a new peer.
func scheduleAddPeer(cluster *clusterInfo, s schedule.Selector, filters ...schedule.Filter) *metapb.Peer {
	stores := cluster.getStores()

	target := s.SelectTarget(stores, filters...)
	if target == nil {
		return nil
	}

	newPeer, err := cluster.allocPeer(target.GetId())
	if err != nil {
		log.Errorf("failed to allocate peer: %v", err)
		return nil
	}

	return newPeer
}

// scheduleRemovePeer schedules a region to remove the peer.
func scheduleRemovePeer(cluster *clusterInfo, schedulerName string, s schedule.Selector, filters ...schedule.Filter) (*core.RegionInfo, *metapb.Peer) {
	stores := cluster.getStores()

	source := s.SelectSource(stores, filters...)
	if source == nil {
		schedulerCounter.WithLabelValues(schedulerName, "no_store").Inc()
		return nil, nil
	}

	region := cluster.randFollowerRegion(source.GetId())
	if region == nil {
		region = cluster.randLeaderRegion(source.GetId())
	}
	if region == nil {
		schedulerCounter.WithLabelValues(schedulerName, "no_region").Inc()
		return nil, nil
	}

	return region, region.GetStorePeer(source.GetId())
}

// scheduleTransferLeader schedules a region to transfer leader to the peer.
func scheduleTransferLeader(cluster *clusterInfo, schedulerName string, s schedule.Selector, filters ...schedule.Filter) (*core.RegionInfo, *metapb.Peer) {
	stores := cluster.getStores()
	if len(stores) == 0 {
		schedulerCounter.WithLabelValues(schedulerName, "no_store").Inc()
		return nil, nil
	}

	var averageLeader float64
	for _, s := range stores {
		averageLeader += float64(s.LeaderScore()) / float64(len(stores))
	}

	mostLeaderStore := s.SelectSource(stores, filters...)
	leastLeaderStore := s.SelectTarget(stores, filters...)

	var mostLeaderDistance, leastLeaderDistance float64
	if mostLeaderStore != nil {
		mostLeaderDistance = math.Abs(mostLeaderStore.LeaderScore() - averageLeader)
	}
	if leastLeaderStore != nil {
		leastLeaderDistance = math.Abs(leastLeaderStore.LeaderScore() - averageLeader)
	}
	if mostLeaderDistance == 0 && leastLeaderDistance == 0 {
		schedulerCounter.WithLabelValues(schedulerName, "already_balanced").Inc()
		return nil, nil
	}

	if mostLeaderDistance > leastLeaderDistance {
		// Transfer a leader out of mostLeaderStore.
		region := cluster.randLeaderRegion(mostLeaderStore.GetId())
		if region == nil {
			schedulerCounter.WithLabelValues(schedulerName, "no_leader_region").Inc()
			return nil, nil
		}
		targetStores := cluster.getFollowerStores(region)
		target := s.SelectTarget(targetStores)
		if target == nil {
			schedulerCounter.WithLabelValues(schedulerName, "no_target_store").Inc()
			return nil, nil
		}

		return region, region.GetStorePeer(target.GetId())
	}

	// Transfer a leader into leastLeaderStore.
	region := cluster.randFollowerRegion(leastLeaderStore.GetId())
	if region == nil {
		schedulerCounter.WithLabelValues(schedulerName, "no_target_peer").Inc()
		return nil, nil
	}
	return region, region.GetStorePeer(leastLeaderStore.GetId())
}
