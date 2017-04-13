// Copyright 2016 PingCAP, Inc.
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
	"bytes"

	"github.com/gogo/protobuf/proto"
	"github.com/google/btree"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

// RegionInfo record detail region info
type RegionInfo struct {
	*metapb.Region
	Leader       *metapb.Peer
	DownPeers    []*pdpb.PeerStats
	PendingPeers []*metapb.Peer
	WrittenBytes uint64
}

func newRegionInfo(region *metapb.Region, leader *metapb.Peer) *RegionInfo {
	return &RegionInfo{
		Region: region,
		Leader: leader,
	}
}

func (r *RegionInfo) clone() *RegionInfo {
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

// GetPeer return the peer with specified peer id
func (r *RegionInfo) GetPeer(peerID uint64) *metapb.Peer {
	for _, peer := range r.GetPeers() {
		if peer.GetId() == peerID {
			return peer
		}
	}
	return nil
}

// GetDownPeer return the down peers with specified peer id
func (r *RegionInfo) GetDownPeer(peerID uint64) *metapb.Peer {
	for _, down := range r.DownPeers {
		if down.GetPeer().GetId() == peerID {
			return down.GetPeer()
		}
	}
	return nil
}

// GetPendingPeer return the pending peer with specified peer id
func (r *RegionInfo) GetPendingPeer(peerID uint64) *metapb.Peer {
	for _, peer := range r.PendingPeers {
		if peer.GetId() == peerID {
			return peer
		}
	}
	return nil
}

// GetStorePeer return the peer in specified store
func (r *RegionInfo) GetStorePeer(storeID uint64) *metapb.Peer {
	for _, peer := range r.GetPeers() {
		if peer.GetStoreId() == storeID {
			return peer
		}
	}
	return nil
}

// RemoveStorePeer remove the peer in specified store
func (r *RegionInfo) RemoveStorePeer(storeID uint64) {
	var peers []*metapb.Peer
	for _, peer := range r.GetPeers() {
		if peer.GetStoreId() != storeID {
			peers = append(peers, peer)
		}
	}
	r.Peers = peers
}

// GetStoreIds return a map indicate the region distributed
func (r *RegionInfo) GetStoreIds() map[uint64]struct{} {
	peers := r.GetPeers()
	stores := make(map[uint64]struct{}, len(peers))
	for _, peer := range peers {
		stores[peer.GetStoreId()] = struct{}{}
	}
	return stores
}

// GetFollowers return a map indicate the follow peers distributed
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

// GetFollower randomly return a follow peer
func (r *RegionInfo) GetFollower() *metapb.Peer {
	for _, peer := range r.GetPeers() {
		if r.Leader == nil || r.Leader.GetId() != peer.GetId() {
			return peer
		}
	}
	return nil
}

var _ btree.Item = &regionItem{}

type regionItem struct {
	region *metapb.Region
}

// Less returns true if the region start key is greater than the other.
// So we will sort the region with start key reversely.
func (r *regionItem) Less(other btree.Item) bool {
	left := r.region.GetStartKey()
	right := other.(*regionItem).region.GetStartKey()
	return bytes.Compare(left, right) > 0
}

func (r *regionItem) Contains(key []byte) bool {
	start, end := r.region.GetStartKey(), r.region.GetEndKey()
	return bytes.Compare(key, start) >= 0 && (len(end) == 0 || bytes.Compare(key, end) < 0)
}

const (
	defaultBTreeDegree = 64
)

type regionTree struct {
	tree *btree.BTree
}

func newRegionTree() *regionTree {
	return &regionTree{
		tree: btree.New(defaultBTreeDegree),
	}
}

func (t *regionTree) length() int {
	return t.tree.Len()
}

// update updates the tree with the region.
// It finds and deletes all the overlapped regions first, and then
// insert the region.
func (t *regionTree) update(region *metapb.Region) {
	item := &regionItem{region: region}

	result := t.find(region)
	if result == nil {
		result = item
	}

	var overlaps []*regionItem
	t.tree.DescendLessOrEqual(result, func(i btree.Item) bool {
		over := i.(*regionItem)
		if len(region.EndKey) > 0 && bytes.Compare(region.EndKey, over.region.StartKey) <= 0 {
			return false
		}
		overlaps = append(overlaps, over)
		return true
	})

	for _, item := range overlaps {
		t.tree.Delete(item)
	}

	t.tree.ReplaceOrInsert(item)
}

// remove removes a region if the region is in the tree.
// It will do nothing if it cannot find the region or the found region
// is not the same with the region.
func (t *regionTree) remove(region *metapb.Region) {
	result := t.find(region)
	if result == nil || result.region.GetId() != region.GetId() {
		return
	}

	t.tree.Delete(result)
}

// search returns a region that contains the key.
func (t *regionTree) search(regionKey []byte) *metapb.Region {
	region := &metapb.Region{StartKey: regionKey}
	result := t.find(region)
	if result == nil {
		return nil
	}
	return result.region
}

// This is a helper function to find an item.
func (t *regionTree) find(region *metapb.Region) *regionItem {
	item := &regionItem{region: region}

	var result *regionItem
	t.tree.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem)
		return false
	})

	if result == nil || !result.Contains(region.StartKey) {
		return nil
	}

	return result
}
