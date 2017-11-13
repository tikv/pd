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

package faketikv

import (
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/core"
)

type ClusterInfo struct {
	*core.RegionsInfo
	Nodes       map[uint64]*Node
	firstRegion *core.RegionInfo
}

func (c *ClusterInfo) GetBootstrapInfo() (*metapb.Store, *metapb.Region) {
	storeID := c.firstRegion.Leader.GetStoreId()
	store := c.Nodes[storeID]
	return store.Store, c.firstRegion.Region
}

func (c *ClusterInfo) nodeHealth(storeID uint64) bool {
	n, ok := c.Nodes[storeID]
	if !ok {
		return false
	}
	return n.Store.GetState() == metapb.StoreState_Up
}

func (c *ClusterInfo) stepLeader(region *core.RegionInfo) {
	if c.nodeHealth(region.Leader.GetStoreId()) {
		return
	}
	var (
		newLeaderStoreID uint64
		unhealth         int
	)

	ids := region.GetStoreIds()
	for id, _ := range ids {
		if c.nodeHealth(id) {
			newLeaderStoreID = id
			break
		} else {
			unhealth++
		}
	}
	// TODO:records no leader region
	if unhealth > len(ids)/2 {
		return
	}
	for _, peer := range region.Peers {
		if peer.GetStoreId() == newLeaderStoreID {
			region.Leader = peer
			region.RegionEpoch.Version++
			break
		}
	}
	c.SetRegion(region)
}

func (c *ClusterInfo) addPeer() {}

func (c *ClusterInfo) deletePeer() {}

func (c *ClusterInfo) stepRegions() {
	regions := c.GetRegions()
	for _, region := range regions {
		c.stepLeader(region)
	}
}
func (c *ClusterInfo) Step() {
	c.stepRegions()
}
