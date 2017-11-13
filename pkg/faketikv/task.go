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
)

type Task interface {
	Step(cluster *ClusterInfo)
	IsFinished() bool
}

type addPeer struct {
	regionID uint64
	peerID   uint64
	storeID  uint64
	name     string
	size     uint64
	speed    uint64
}

func (a *addPeer) Step(cluster *ClusterInfo) {
	a.size -= a.speed
	if a.size < 0 {
		region := cluster.GetRegion(a.regionID)
		if region.GetPeer(a.peerID) == nil {
			peer := &metapb.Peer{
				Id:      a.peerID,
				StoreId: a.storeID,
			}
			region.Peers = append(region.Peers, peer)
			cluster.SetRegion(region)
		}
	}
}

func (a *addPeer) IsFinished() bool {
	return a.size < 0
}

type deletePeer struct {
	regionID uint64
	peerID   uint64
	name     string
	size     uint64
	speed    uint64
}

type transferLeader struct{}
