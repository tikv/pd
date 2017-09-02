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

package core

import (
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

// RegionInfo records detail region info.
type RegionInfo struct {
	*metapb.Region
	Leader       *metapb.Peer
	DownPeers    []*pdpb.PeerStats
	PendingPeers []*metapb.Peer
	WrittenBytes uint64
}

// NewRegionInfo creates RegionInfo with region's meta and leader peer.
func NewRegionInfo(region *metapb.Region, leader *metapb.Peer) *RegionInfo {
	return &RegionInfo{
		Region: region,
		Leader: leader,
	}
}

// Clone returns a copy of current regionInfo.
func (r *RegionInfo) Clone() *RegionInfo {
	downPeers := make([]*pdpb.PeerStats, 0, len(r.DownPeers))
	for _, peer := range r.DownPeers {
		downPeers = append(downPeers, proto.Clone(peer).(*pdpb.PeerStats))
	}
	pendingPeers := make([]*metapb.Peer, 0, len(r.PendingPeers))
	for _, peer := range r.PendingPeers {
		pendingPeers = append(pendingPeers, proto.Clone(peer).(*metapb.Peer))
	}
	return &RegionInfo{
		Region:       proto.Clone(r.Region).(*metapb.Region),
		Leader:       proto.Clone(r.Leader).(*metapb.Peer),
		DownPeers:    downPeers,
		PendingPeers: pendingPeers,
		WrittenBytes: r.WrittenBytes,
	}
}

// GetPeer returns the peer with specified peer id.
func (r *RegionInfo) GetPeer(peerID uint64) *metapb.Peer {
	for _, peer := range r.GetPeers() {
		if peer.GetId() == peerID {
			return peer
		}
	}
	return nil
}

// GetDownPeer returns the down peers with specified peer id.
func (r *RegionInfo) GetDownPeer(peerID uint64) *metapb.Peer {
	for _, down := range r.DownPeers {
		if down.GetPeer().GetId() == peerID {
			return down.GetPeer()
		}
	}
	return nil
}

// GetPendingPeer returns the pending peer with specified peer id.
func (r *RegionInfo) GetPendingPeer(peerID uint64) *metapb.Peer {
	for _, peer := range r.PendingPeers {
		if peer.GetId() == peerID {
			return peer
		}
	}
	return nil
}

// GetStorePeer returns the peer in specified store.
func (r *RegionInfo) GetStorePeer(storeID uint64) *metapb.Peer {
	for _, peer := range r.GetPeers() {
		if peer.GetStoreId() == storeID {
			return peer
		}
	}
	return nil
}

// RemoveStorePeer removes the peer in specified store.
func (r *RegionInfo) RemoveStorePeer(storeID uint64) {
	var peers []*metapb.Peer
	for _, peer := range r.GetPeers() {
		if peer.GetStoreId() != storeID {
			peers = append(peers, peer)
		}
	}
	r.Peers = peers
}

// GetStoreIds returns a map indicate the region distributed.
func (r *RegionInfo) GetStoreIds() map[uint64]struct{} {
	peers := r.GetPeers()
	stores := make(map[uint64]struct{}, len(peers))
	for _, peer := range peers {
		stores[peer.GetStoreId()] = struct{}{}
	}
	return stores
}

// GetFollowers returns a map indicate the follow peers distributed.
func (r *RegionInfo) GetFollowers() map[uint64]*metapb.Peer {
	peers := r.GetPeers()
	followers := make(map[uint64]*metapb.Peer, len(peers))
	for _, peer := range peers {
		if r.Leader == nil || r.Leader.GetId() != peer.GetId() {
			followers[peer.GetStoreId()] = peer
		}
	}
	return followers
}

// GetFollower randomly returns a follow peer.
func (r *RegionInfo) GetFollower() *metapb.Peer {
	for _, peer := range r.GetPeers() {
		if r.Leader == nil || r.Leader.GetId() != peer.GetId() {
			return peer
		}
	}
	return nil
}

// RegionStat records each hot region's statistics
type RegionStat struct {
	RegionID     uint64 `json:"region_id"`
	WrittenBytes uint64 `json:"written_bytes"`
	// HotDegree records the hot region update times
	HotDegree int `json:"hot_degree"`
	// LastUpdateTime used to calculate average write
	LastUpdateTime time.Time `json:"last_update_time"`
	StoreID        uint64    `json:"-"`
	// AntiCount used to eliminate some noise when remove region in cache
	AntiCount int
	// Version used to check the region split times
	Version uint64
}

// RegionsStat is a list of a group region state type
type RegionsStat []RegionStat

func (m RegionsStat) Len() int           { return len(m) }
func (m RegionsStat) Swap(i, j int)      { m[i], m[j] = m[j], m[i] }
func (m RegionsStat) Less(i, j int) bool { return m[i].WrittenBytes < m[j].WrittenBytes }

// HotRegionsStat records all hot regions statistics
type HotRegionsStat struct {
	WrittenBytes uint64      `json:"total_written_bytes"`
	RegionsCount int         `json:"regions_count"`
	RegionsStat  RegionsStat `json:"statistics"`
}
