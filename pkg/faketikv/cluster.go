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
	"context"
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/pkg/faketikv/cases"
)

// ClusterInfo records all cluster information.
type ClusterInfo struct {
	conf  *cases.Conf
	Nodes map[uint64]*Node
}

// NewClusterInfo creates the initialized cluster with config.
func NewClusterInfo(pdAddr string, conf *cases.Conf) (*ClusterInfo, error) {
	cluster := &ClusterInfo{
		conf:  conf,
		Nodes: make(map[uint64]*Node),
	}

	for _, store := range conf.Stores {
		node, err := NewNode(store.ID, fmt.Sprintf("mock:://tikv-%d", store.ID), pdAddr)
		if err != nil {
			return nil, errors.Trace(err)
		}
		cluster.Nodes[store.ID] = node
	}

	return cluster, nil
}

// GetBootstrapInfo returns a valid bootstrap store and region.
func (c *ClusterInfo) GetBootstrapInfo(r *RaftEngine) (*metapb.Store, *metapb.Region, error) {
	origin := r.RandRegion()
	if origin == nil {
		return nil, nil, errors.New("no region found for bootstrap")
	}
	region := origin.Clone()
	if region.Leader == nil {
		return nil, nil, errors.New("bootstrap region has no leader")
	}
	store := c.Nodes[region.Leader.GetStoreId()]
	if store == nil {
		return nil, nil, errors.Errorf("bootstrap store %v not found", region.Leader.GetStoreId())
	}
	region.StartKey, region.EndKey = []byte(""), []byte("")
	region.RegionEpoch = &metapb.RegionEpoch{}
	region.Peers = []*metapb.Peer{region.Leader}
	return store.Store, region.Region, nil
}

func (c *ClusterInfo) allocID(storeID uint64) (uint64, error) {
	node, ok := c.Nodes[storeID]
	if !ok {
		return 0, errors.Errorf("node %d not found", storeID)
	}
	id, err := node.client.AllocID(context.Background())
	return id, errors.Trace(err)
}
