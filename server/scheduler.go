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

// Scheduler is an interface to schedule resources.
type Scheduler interface {
	GetName() string
	GetResourceKind() ResourceKind
	Schedule(cluster *clusterInfo) *balanceOperator
}

// grantLeaderScheduler transfers all leaders to peers in the store.
type grantLeaderScheduler struct {
	StoreID uint64 `json:"store_id"`
}

func newGrantLeaderScheduler(storeID uint64) *grantLeaderScheduler {
	return &grantLeaderScheduler{StoreID: storeID}
}

func (s *grantLeaderScheduler) GetName() string {
	return "grant-leader-scheduler"
}

func (s *grantLeaderScheduler) GetResourceKind() ResourceKind {
	return leaderKind
}

func (s *grantLeaderScheduler) Schedule(cluster *clusterInfo) *balanceOperator {
	region := cluster.randFollowerRegion(s.StoreID)
	if region == nil {
		return nil
	}
	return transferLeader(region, s.StoreID)
}

type shuffleLeaderScheduler struct {
	selector Selector
}

func newShuffleLeaderScheduler() *shuffleLeaderScheduler {
	return &shuffleLeaderScheduler{
		selector: newRandomSelector(leaderKind),
	}
}

func (s *shuffleLeaderScheduler) GetName() string {
	return "shuffle-leader-scheduler"
}

func (s *shuffleLeaderScheduler) GetResourceKind() ResourceKind {
	return leaderKind
}

func (s *shuffleLeaderScheduler) Schedule(cluster *clusterInfo) *balanceOperator {
	// We shuffle leaders between stores by:
	// 1. select a store as a source store randomly;
	// 2. transfer a leader from the store to another store;
	// 3. transfer a leader to the store from another store.
	// These will not change store's leader count, but swap leaders between stores.
	var ops []Operator

	// Transfer a leader from the source store.
	region, source, target := scheduleLeader(cluster, s.selector)
	if region == nil {
		return nil
	}
	ops = append(ops, transferLeader(region, target.GetId()))

	// Transfer a leader to the source store.
	region = cluster.randFollowerRegion(source.GetId())
	if region == nil {
		return nil
	}
	ops = append(ops, transferLeader(region, source.GetId()))

	return newBalanceOperator(region, balanceOP, ops...)
}

// transferLeader returns an operator to transfer leader to the store.
func transferLeader(region *regionInfo, storeID uint64) *balanceOperator {
	newLeader := region.GetStorePeer(storeID)
	if newLeader == nil {
		return nil
	}
	transferLeader := newTransferLeaderOperator(region.GetId(), region.Leader, newLeader)
	return newBalanceOperator(region, balanceOP, transferLeader)
}

// scheduleLeader schedules a region to transfer leader from the source store to the target store.
func scheduleLeader(cluster *clusterInfo, s Selector) (*regionInfo, *storeInfo, *storeInfo) {
	sourceStores := cluster.getStores()

	source := s.SelectSource(sourceStores)
	if source == nil {
		return nil, nil, nil
	}

	region := cluster.randLeaderRegion(source.GetId())
	if region == nil {
		return nil, nil, nil
	}

	targetStores := cluster.getFollowerStores(region)

	target := s.SelectTarget(targetStores)
	if target == nil {
		return nil, nil, nil
	}

	return region, source, target
}

// scheduleStorage schedules a region to transfer peer from the source store to the target store.
func scheduleStorage(cluster *clusterInfo, opt *scheduleOption, s Selector) (*regionInfo, *storeInfo, *storeInfo) {
	stores := cluster.getStores()

	source := s.SelectSource(stores)
	if source == nil {
		return nil, nil, nil
	}

	region := cluster.randFollowerRegion(source.GetId())
	if region == nil {
		region = cluster.randLeaderRegion(source.GetId())
	}
	if region == nil {
		return nil, nil, nil
	}

	excluded := newExcludedFilter(nil, region.GetStoreIds())
	result := opt.GetConstraints().Match(cluster.getRegionStores(region))
	constraint := newConstraintFilter(nil, result.stores[source.GetId()])

	target := s.SelectTarget(stores, excluded, constraint)
	if target == nil {
		return nil, nil, nil
	}

	return region, source, target
}
