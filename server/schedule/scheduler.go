// Copyright 2017 PingCAP, Inc.
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

package schedule

import (
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/core"
)

// Cluster provides an overview of a cluster's regions distribution.
type Cluster interface {
	RandFollowerRegion(storeID uint64) *core.RegionInfo
	RandLeaderRegion(storeID uint64) *core.RegionInfo

	GetStores() []*core.StoreInfo
	GetStore(id uint64) *core.StoreInfo
	GetRegion(id uint64) *core.RegionInfo
	GetRegionStores(region *core.RegionInfo) []*core.StoreInfo
	GetFollowerStores(region *core.RegionInfo) []*core.StoreInfo
	GetLeaderStore(region *core.RegionInfo) *core.StoreInfo

	BlockStore(id uint64) error
	UnblockStore(id uint64)

	IsRegionHot(id uint64) bool
	RegionWriteStats() []*core.RegionStat

	// TODO: it should be removed. Schedulers don't need to know anything
	// about peers.
	AllocPeer(storeID uint64) (*metapb.Peer, error)
}

// Scheduler is an interface to schedule resources.
type Scheduler interface {
	GetName() string
	GetResourceKind() core.ResourceKind
	GetResourceLimit() uint64
	Prepare(cluster Cluster) error
	Cleanup(cluster Cluster)
	Schedule(cluster Cluster) Operator
}
