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
	"bytes"
	"encoding/hex"
	"fmt"
	"math/rand"
	"reflect"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

// RegionInfo records detail region info.
// Read-Only once created.
type RegionInfo struct {
	meta            *metapb.Region
	learners        []*metapb.Peer
	voters          []*metapb.Peer
	leader          *metapb.Peer
	downPeers       []*pdpb.PeerStats
	pendingPeers    []*metapb.Peer
	writtenBytes    uint64
	writtenKeys     uint64
	readBytes       uint64
	readKeys        uint64
	approximateSize int64
	approximateKeys int64
	interval        *pdpb.TimeInterval
}

// NewRegionInfo creates RegionInfo with region's meta and leader peer.
func NewRegionInfo(region *metapb.Region, leader *metapb.Peer, opts ...RegionCreateOption) *RegionInfo {
	regionInfo := &RegionInfo{
		meta:   region,
		leader: leader,
	}

	for _, opt := range opts {
		opt(regionInfo)
	}
	classifyVoterAndLearner(regionInfo)
	return regionInfo
}

// classifyVoterAndLearner sorts out voter and learner from peers into different slice.
func classifyVoterAndLearner(region *RegionInfo) {
	learners := make([]*metapb.Peer, 0, 1)
	voters := make([]*metapb.Peer, 0, len(region.meta.Peers))
	for _, p := range region.meta.Peers {
		if p.IsLearner {
			learners = append(learners, p)
		} else {
			voters = append(voters, p)
		}
	}
	region.learners = learners
	region.voters = voters
}

// EmptyRegionApproximateSize is the region approximate size of an empty region
// (heartbeat size <= 1MB).
const EmptyRegionApproximateSize = 1

// RegionFromHeartbeat constructs a Region from region heartbeat.
func RegionFromHeartbeat(heartbeat *pdpb.RegionHeartbeatRequest) *RegionInfo {
	// Convert unit to MB.
	// If region is empty or less than 1MB, use 1MB instead.
	regionSize := heartbeat.GetApproximateSize() / (1 << 20)
	if regionSize < EmptyRegionApproximateSize {
		regionSize = EmptyRegionApproximateSize
	}

	region := &RegionInfo{
		meta:            heartbeat.GetRegion(),
		leader:          heartbeat.GetLeader(),
		downPeers:       heartbeat.GetDownPeers(),
		pendingPeers:    heartbeat.GetPendingPeers(),
		writtenBytes:    heartbeat.GetBytesWritten(),
		writtenKeys:     heartbeat.GetKeysWritten(),
		readBytes:       heartbeat.GetBytesRead(),
		readKeys:        heartbeat.GetKeysRead(),
		approximateSize: int64(regionSize),
		approximateKeys: int64(heartbeat.GetApproximateKeys()),
		interval:        heartbeat.GetInterval(),
	}

	classifyVoterAndLearner(region)
	return region
}

// Clone returns a copy of current regionInfo.
func (r *RegionInfo) Clone(opts ...RegionCreateOption) *RegionInfo {
	downPeers := make([]*pdpb.PeerStats, 0, len(r.downPeers))
	for _, peer := range r.downPeers {
		downPeers = append(downPeers, proto.Clone(peer).(*pdpb.PeerStats))
	}
	pendingPeers := make([]*metapb.Peer, 0, len(r.pendingPeers))
	for _, peer := range r.pendingPeers {
		pendingPeers = append(pendingPeers, proto.Clone(peer).(*metapb.Peer))
	}

	region := &RegionInfo{
		meta:            proto.Clone(r.meta).(*metapb.Region),
		leader:          proto.Clone(r.leader).(*metapb.Peer),
		downPeers:       downPeers,
		pendingPeers:    pendingPeers,
		writtenBytes:    r.writtenBytes,
		writtenKeys:     r.writtenKeys,
		readBytes:       r.readBytes,
		readKeys:        r.readKeys,
		approximateSize: r.approximateSize,
		approximateKeys: r.approximateKeys,
		interval:        proto.Clone(r.interval).(*pdpb.TimeInterval),
	}

	for _, opt := range opts {
		opt(region)
	}
	classifyVoterAndLearner(region)
	return region
}

// GetLearners returns the learners.
func (r *RegionInfo) GetLearners() []*metapb.Peer {
	return r.learners
}

// GetVoters returns the voters.
func (r *RegionInfo) GetVoters() []*metapb.Peer {
	return r.voters
}

// GetPeer returns the peer with specified peer id.
func (r *RegionInfo) GetPeer(peerID uint64) *metapb.Peer {
	for _, peer := range r.meta.GetPeers() {
		if peer.GetId() == peerID {
			return peer
		}
	}
	return nil
}

// GetDownPeer returns the down peer with specified peer id.
func (r *RegionInfo) GetDownPeer(peerID uint64) *metapb.Peer {
	for _, down := range r.downPeers {
		if down.GetPeer().GetId() == peerID {
			return down.GetPeer()
		}
	}
	return nil
}

// GetDownVoter returns the down voter with specified peer id.
func (r *RegionInfo) GetDownVoter(peerID uint64) *metapb.Peer {
	for _, down := range r.downPeers {
		if down.GetPeer().GetId() == peerID && !down.GetPeer().IsLearner {
			return down.GetPeer()
		}
	}
	return nil
}

// GetDownLearner returns the down learner with soecified peer id.
func (r *RegionInfo) GetDownLearner(peerID uint64) *metapb.Peer {
	for _, down := range r.downPeers {
		if down.GetPeer().GetId() == peerID && down.GetPeer().IsLearner {
			return down.GetPeer()
		}
	}
	return nil
}

// GetPendingPeer returns the pending peer with specified peer id.
func (r *RegionInfo) GetPendingPeer(peerID uint64) *metapb.Peer {
	for _, peer := range r.pendingPeers {
		if peer.GetId() == peerID {
			return peer
		}
	}
	return nil
}

// GetPendingVoter returns the pending voter with specified peer id.
func (r *RegionInfo) GetPendingVoter(peerID uint64) *metapb.Peer {
	for _, peer := range r.pendingPeers {
		if peer.GetId() == peerID && !peer.IsLearner {
			return peer
		}
	}
	return nil
}

// GetPendingLearner returns the pending learner peer with specified peer id.
func (r *RegionInfo) GetPendingLearner(peerID uint64) *metapb.Peer {
	for _, peer := range r.pendingPeers {
		if peer.GetId() == peerID && peer.IsLearner {
			return peer
		}
	}
	return nil
}

// GetStorePeer returns the peer in specified store.
func (r *RegionInfo) GetStorePeer(storeID uint64) *metapb.Peer {
	for _, peer := range r.meta.GetPeers() {
		if peer.GetStoreId() == storeID {
			return peer
		}
	}
	return nil
}

// GetStoreVoter returns the voter in specified store.
func (r *RegionInfo) GetStoreVoter(storeID uint64) *metapb.Peer {
	for _, peer := range r.voters {
		if peer.GetStoreId() == storeID {
			return peer
		}
	}
	return nil
}

// GetStoreLearner returns the learner peer in specified store.
func (r *RegionInfo) GetStoreLearner(storeID uint64) *metapb.Peer {
	for _, peer := range r.learners {
		if peer.GetStoreId() == storeID {
			return peer
		}
	}
	return nil
}

// GetStoreIds returns a map indicate the region distributed.
func (r *RegionInfo) GetStoreIds() map[uint64]struct{} {
	peers := r.meta.GetPeers()
	stores := make(map[uint64]struct{}, len(peers))
	for _, peer := range peers {
		stores[peer.GetStoreId()] = struct{}{}
	}
	return stores
}

// GetFollowers returns a map indicate the follow peers distributed.
func (r *RegionInfo) GetFollowers() map[uint64]*metapb.Peer {
	peers := r.GetVoters()
	followers := make(map[uint64]*metapb.Peer, len(peers))
	for _, peer := range peers {
		if r.leader == nil || r.leader.GetId() != peer.GetId() {
			followers[peer.GetStoreId()] = peer
		}
	}
	return followers
}

// GetFollower randomly returns a follow peer.
func (r *RegionInfo) GetFollower() *metapb.Peer {
	for _, peer := range r.GetVoters() {
		if r.leader == nil || r.leader.GetId() != peer.GetId() {
			return peer
		}
	}
	return nil
}

// GetDiffFollowers returns the followers which is not located in the same
// store as any other followers of the another specified region.
func (r *RegionInfo) GetDiffFollowers(other *RegionInfo) []*metapb.Peer {
	res := make([]*metapb.Peer, 0, len(r.meta.Peers))
	for _, p := range r.GetFollowers() {
		diff := true
		for _, o := range other.GetFollowers() {
			if p.GetStoreId() == o.GetStoreId() {
				diff = false
				break
			}
		}
		if diff {
			res = append(res, p)
		}
	}
	return res
}

// GetID returns the ID of the region.
func (r *RegionInfo) GetID() uint64 {
	return r.meta.GetId()
}

// GetMeta returns the meta information of the region.
func (r *RegionInfo) GetMeta() *metapb.Region {
	return r.meta
}

// GetApproximateSize returns the approximate size of the region.
func (r *RegionInfo) GetApproximateSize() int64 {
	return r.approximateSize
}

// GetApproximateKeys returns the approximate keys of the region.
func (r *RegionInfo) GetApproximateKeys() int64 {
	return r.approximateKeys
}

// GetInterval returns the interval information of the region.
func (r *RegionInfo) GetInterval() *pdpb.TimeInterval {
	return r.interval
}

// GetDownPeers returns the down peers of the region.
func (r *RegionInfo) GetDownPeers() []*pdpb.PeerStats {
	return r.downPeers
}

// GetPendingPeers returns the pending peers of the region.
func (r *RegionInfo) GetPendingPeers() []*metapb.Peer {
	return r.pendingPeers
}

// GetBytesRead returns the read bytes of the region.
func (r *RegionInfo) GetBytesRead() uint64 {
	return r.readBytes
}

// GetBytesWritten returns the written bytes of the region.
func (r *RegionInfo) GetBytesWritten() uint64 {
	return r.writtenBytes
}

// GetKeysWritten returns the written keys of the region.
func (r *RegionInfo) GetKeysWritten() uint64 {
	return r.writtenKeys
}

// GetKeysRead returns the read keys of the region.
func (r *RegionInfo) GetKeysRead() uint64 {
	return r.readKeys
}

// GetLeader returns the leader of the region.
func (r *RegionInfo) GetLeader() *metapb.Peer {
	return r.leader
}

// GetStartKey returns the start key of the region.
func (r *RegionInfo) GetStartKey() []byte {
	return r.meta.StartKey
}

// GetEndKey returns the end key of the region.
func (r *RegionInfo) GetEndKey() []byte {
	return r.meta.EndKey
}

// GetPeers returns the peers of the region.
func (r *RegionInfo) GetPeers() []*metapb.Peer {
	return r.meta.GetPeers()
}

// GetRegionEpoch returns the region epoch of the region.
func (r *RegionInfo) GetRegionEpoch() *metapb.RegionEpoch {
	return r.meta.RegionEpoch
}

// regionMap wraps a map[uint64]*core.RegionInfo and supports randomly pick a region.
type regionMap struct {
	m         map[uint64]*regionEntry
	ids       []uint64
	totalSize int64
	totalKeys int64
}

type regionEntry struct {
	*RegionInfo
	pos int
}

func newRegionMap() *regionMap {
	return &regionMap{
		m:         make(map[uint64]*regionEntry),
		totalSize: 0,
	}
}

func (rm *regionMap) Len() int {
	if rm == nil {
		return 0
	}
	return len(rm.m)
}

func (rm *regionMap) Get(id uint64) *RegionInfo {
	if rm == nil {
		return nil
	}
	if entry, ok := rm.m[id]; ok {
		return entry.RegionInfo
	}
	return nil
}

func (rm *regionMap) Put(region *RegionInfo) {
	if old, ok := rm.m[region.GetID()]; ok {
		rm.totalSize += region.approximateSize - old.approximateSize
		rm.totalKeys += region.approximateKeys - old.approximateKeys
		old.RegionInfo = region
		return
	}
	rm.m[region.GetID()] = &regionEntry{
		RegionInfo: region,
		pos:        len(rm.ids),
	}
	rm.ids = append(rm.ids, region.GetID())
	rm.totalSize += region.approximateSize
	rm.totalKeys += region.approximateKeys
}

func (rm *regionMap) RandomRegion() *RegionInfo {
	if rm.Len() == 0 {
		return nil
	}
	return rm.Get(rm.ids[rand.Intn(rm.Len())])
}

func (rm *regionMap) Delete(id uint64) {
	if rm == nil {
		return
	}
	if old, ok := rm.m[id]; ok {
		len := rm.Len()
		last := rm.m[rm.ids[len-1]]
		last.pos = old.pos
		rm.ids[last.pos] = last.GetID()
		delete(rm.m, id)
		rm.ids = rm.ids[:len-1]
		rm.totalSize -= old.approximateSize
		rm.totalKeys -= old.approximateKeys
	}
}

func (rm *regionMap) TotalSize() int64 {
	if rm.Len() == 0 {
		return 0
	}
	return rm.totalSize
}

// RegionsInfo for export
type RegionsInfo struct {
	tree         *regionTree
	regions      *regionMap            // regionID -> regionInfo
	leaders      map[uint64]*regionMap // storeID -> regionID -> regionInfo
	followers    map[uint64]*regionMap // storeID -> regionID -> regionInfo
	learners     map[uint64]*regionMap // storeID -> regionID -> regionInfo
	pendingPeers map[uint64]*regionMap // storeID -> regionID -> regionInfo
}

// NewRegionsInfo creates RegionsInfo with tree, regions, leaders and followers
func NewRegionsInfo() *RegionsInfo {
	return &RegionsInfo{
		tree:         newRegionTree(),
		regions:      newRegionMap(),
		leaders:      make(map[uint64]*regionMap),
		followers:    make(map[uint64]*regionMap),
		learners:     make(map[uint64]*regionMap),
		pendingPeers: make(map[uint64]*regionMap),
	}
}

// GetRegion returns the RegionInfo with regionID
func (r *RegionsInfo) GetRegion(regionID uint64) *RegionInfo {
	region := r.regions.Get(regionID)
	if region == nil {
		return nil
	}
	return region
}

// SetRegion sets the RegionInfo with regionID
func (r *RegionsInfo) SetRegion(region *RegionInfo) []*metapb.Region {
	if origin := r.regions.Get(region.GetID()); origin != nil {
		r.RemoveRegion(origin)
	}
	return r.AddRegion(region)
}

// Length returns the RegionsInfo length
func (r *RegionsInfo) Length() int {
	return r.regions.Len()
}

// TreeLength returns the RegionsInfo tree length(now only used in test)
func (r *RegionsInfo) TreeLength() int {
	return r.tree.length()
}

// GetOverlaps returns the regions which are overlapped with the specified region range.
func (r *RegionsInfo) GetOverlaps(region *RegionInfo) []*metapb.Region {
	return r.tree.getOverlaps(region.meta)
}

// AddRegion adds RegionInfo to regionTree and regionMap, also update leaders and followers by region peers
func (r *RegionsInfo) AddRegion(region *RegionInfo) []*metapb.Region {
	// Add to tree and regions.
	overlaps := r.tree.update(region.meta)
	for _, item := range overlaps {
		r.RemoveRegion(r.GetRegion(item.Id))
	}

	r.regions.Put(region)

	// Add to leaders and followers.
	for _, peer := range region.GetVoters() {
		storeID := peer.GetStoreId()
		if peer.GetId() == region.leader.GetId() {
			// Add leader peer to leaders.
			store, ok := r.leaders[storeID]
			if !ok {
				store = newRegionMap()
				r.leaders[storeID] = store
			}
			store.Put(region)
		} else {
			// Add follower peer to followers.
			store, ok := r.followers[storeID]
			if !ok {
				store = newRegionMap()
				r.followers[storeID] = store
			}
			store.Put(region)
		}
	}

	// Add to learners.
	for _, peer := range region.GetLearners() {
		storeID := peer.GetStoreId()
		store, ok := r.learners[storeID]
		if !ok {
			store = newRegionMap()
			r.learners[storeID] = store
		}
		store.Put(region)
	}

	for _, peer := range region.pendingPeers {
		storeID := peer.GetStoreId()
		store, ok := r.pendingPeers[storeID]
		if !ok {
			store = newRegionMap()
			r.pendingPeers[storeID] = store
		}
		store.Put(region)
	}

	return overlaps
}

// RemoveRegion removes RegionInfo from regionTree and regionMap
func (r *RegionsInfo) RemoveRegion(region *RegionInfo) {
	// Remove from tree and regions.
	r.tree.remove(region.meta)
	r.regions.Delete(region.GetID())
	// Remove from leaders and followers.
	for _, peer := range region.meta.GetPeers() {
		storeID := peer.GetStoreId()
		r.leaders[storeID].Delete(region.GetID())
		r.followers[storeID].Delete(region.GetID())
		r.learners[storeID].Delete(region.GetID())
		r.pendingPeers[storeID].Delete(region.GetID())
	}
}

// SearchRegion searches RegionInfo from regionTree
func (r *RegionsInfo) SearchRegion(regionKey []byte) *RegionInfo {
	metaRegion := r.tree.search(regionKey)
	if metaRegion == nil {
		return nil
	}
	return r.GetRegion(metaRegion.GetId())
}

// SearchPrevRegion searches previous RegionInfo from regionTree
func (r *RegionsInfo) SearchPrevRegion(regionKey []byte) *RegionInfo {
	metaRegion := r.tree.searchPrev(regionKey)
	if metaRegion == nil {
		return nil
	}
	return r.GetRegion(metaRegion.GetId())
}

// GetRegions gets all RegionInfo from regionMap
func (r *RegionsInfo) GetRegions() []*RegionInfo {
	regions := make([]*RegionInfo, 0, r.regions.Len())
	for _, region := range r.regions.m {
		regions = append(regions, region.RegionInfo)
	}
	return regions
}

// GetStoreRegions gets all RegionInfo with a given storeID
func (r *RegionsInfo) GetStoreRegions(storeID uint64) []*RegionInfo {
	regions := make([]*RegionInfo, 0, r.GetStoreLeaderCount(storeID)+r.GetStoreFollowerCount(storeID))
	if leaders, ok := r.leaders[storeID]; ok {
		for _, region := range leaders.m {
			regions = append(regions, region.RegionInfo)
		}
	}
	if followers, ok := r.followers[storeID]; ok {
		for _, region := range followers.m {
			regions = append(regions, region.RegionInfo)
		}
	}
	return regions
}

// GetStoreLeaderRegionSize get total size of store's leader regions
func (r *RegionsInfo) GetStoreLeaderRegionSize(storeID uint64) int64 {
	return r.leaders[storeID].TotalSize()
}

// GetStoreFollowerRegionSize get total size of store's follower regions
func (r *RegionsInfo) GetStoreFollowerRegionSize(storeID uint64) int64 {
	return r.followers[storeID].TotalSize()
}

// GetStoreLearnerRegionSize get total size of store's learner regions
func (r *RegionsInfo) GetStoreLearnerRegionSize(storeID uint64) int64 {
	return r.learners[storeID].TotalSize()
}

// GetStoreRegionSize get total size of store's regions
func (r *RegionsInfo) GetStoreRegionSize(storeID uint64) int64 {
	return r.GetStoreLeaderRegionSize(storeID) + r.GetStoreFollowerRegionSize(storeID) + r.GetStoreLearnerRegionSize(storeID)
}

// GetMetaRegions gets a set of metapb.Region from regionMap
func (r *RegionsInfo) GetMetaRegions() []*metapb.Region {
	regions := make([]*metapb.Region, 0, r.regions.Len())
	for _, region := range r.regions.m {
		regions = append(regions, proto.Clone(region.meta).(*metapb.Region))
	}
	return regions
}

// GetRegionCount gets the total count of RegionInfo of regionMap
func (r *RegionsInfo) GetRegionCount() int {
	return r.regions.Len()
}

// GetStoreRegionCount gets the total count of a store's leader and follower RegionInfo by storeID
func (r *RegionsInfo) GetStoreRegionCount(storeID uint64) int {
	return r.GetStoreLeaderCount(storeID) + r.GetStoreFollowerCount(storeID) + r.GetStoreLearnerCount(storeID)
}

// GetStorePendingPeerCount gets the total count of a store's region that includes pending peer
func (r *RegionsInfo) GetStorePendingPeerCount(storeID uint64) int {
	return r.pendingPeers[storeID].Len()
}

// GetStoreLeaderCount get the total count of a store's leader RegionInfo
func (r *RegionsInfo) GetStoreLeaderCount(storeID uint64) int {
	return r.leaders[storeID].Len()
}

// GetStoreFollowerCount get the total count of a store's follower RegionInfo
func (r *RegionsInfo) GetStoreFollowerCount(storeID uint64) int {
	return r.followers[storeID].Len()
}

// GetStoreLearnerCount get the total count of a store's learner RegionInfo
func (r *RegionsInfo) GetStoreLearnerCount(storeID uint64) int {
	return r.learners[storeID].Len()
}

// RandRegion get a region by random
func (r *RegionsInfo) RandRegion(opts ...RegionOption) *RegionInfo {
	return randRegion(r.regions, opts...)
}

// RandPendingRegion randomly gets a store's region with a pending peer.
func (r *RegionsInfo) RandPendingRegion(storeID uint64, opts ...RegionOption) *RegionInfo {
	return randRegion(r.pendingPeers[storeID], opts...)
}

// RandLeaderRegion randomly gets a store's leader region.
func (r *RegionsInfo) RandLeaderRegion(storeID uint64, opts ...RegionOption) *RegionInfo {
	return randRegion(r.leaders[storeID], opts...)
}

// RandFollowerRegion randomly gets a store's follower region.
func (r *RegionsInfo) RandFollowerRegion(storeID uint64, opts ...RegionOption) *RegionInfo {
	return randRegion(r.followers[storeID], opts...)
}

// GetLeader return leader RegionInfo by storeID and regionID(now only used in test)
func (r *RegionsInfo) GetLeader(storeID uint64, regionID uint64) *RegionInfo {
	return r.leaders[storeID].Get(regionID)
}

// GetFollower return follower RegionInfo by storeID and regionID(now only used in test)
func (r *RegionsInfo) GetFollower(storeID uint64, regionID uint64) *RegionInfo {
	return r.followers[storeID].Get(regionID)
}

// ScanRange scans regions intersecting [start key, end key), returns at most
// `limit` regions. limit <= 0 means no limit.
func (r *RegionsInfo) ScanRange(startKey, endKey []byte, limit int) []*RegionInfo {
	var res []*RegionInfo
	r.tree.scanRange(startKey, func(meta *metapb.Region) bool {
		if len(endKey) > 0 && bytes.Compare(meta.StartKey, endKey) >= 0 {
			return false
		}
		if limit > 0 && len(res) >= limit {
			return false
		}
		res = append(res, r.GetRegion(meta.GetId()))
		return true
	})
	return res
}

// ScanRangeWithIterator scans from the first region containing or behind start key,
// until iterator returns false.
func (r *RegionsInfo) ScanRangeWithIterator(startKey []byte, iterator func(metaRegion *metapb.Region) bool) {
	r.tree.scanRange(startKey, iterator)
}

// GetAdjacentRegions returns region's info that is adjacent with specific region
func (r *RegionsInfo) GetAdjacentRegions(region *RegionInfo) (*RegionInfo, *RegionInfo) {
	metaPrev, metaNext := r.tree.getAdjacentRegions(region.meta)
	var prev, next *RegionInfo
	// check key to avoid key range hole
	if metaPrev != nil && bytes.Equal(metaPrev.region.EndKey, region.meta.StartKey) {
		prev = r.GetRegion(metaPrev.region.GetId())
	}
	if metaNext != nil && bytes.Equal(region.meta.EndKey, metaNext.region.StartKey) {
		next = r.GetRegion(metaNext.region.GetId())
	}
	return prev, next
}

// GetAverageRegionSize returns the average region approximate size.
func (r *RegionsInfo) GetAverageRegionSize() int64 {
	if r.regions.Len() == 0 {
		return 0
	}
	return r.regions.TotalSize() / int64(r.regions.Len())
}

const randomRegionMaxRetry = 10

func randRegion(regions *regionMap, opts ...RegionOption) *RegionInfo {
	for i := 0; i < randomRegionMaxRetry; i++ {
		region := regions.RandomRegion()
		if region == nil {
			return nil
		}
		isSelect := true
		for _, opt := range opts {
			if !opt(region) {
				isSelect = false
				break
			}
		}
		if isSelect {
			return region
		}
	}
	return nil
}

// DiffRegionPeersInfo return the difference of peers info  between two RegionInfo
func DiffRegionPeersInfo(origin *RegionInfo, other *RegionInfo) string {
	var ret []string
	for _, a := range origin.meta.Peers {
		both := false
		for _, b := range other.meta.Peers {
			if reflect.DeepEqual(a, b) {
				both = true
				break
			}
		}
		if !both {
			ret = append(ret, fmt.Sprintf("Remove peer:{%v}", a))
		}
	}
	for _, b := range other.meta.Peers {
		both := false
		for _, a := range origin.meta.Peers {
			if reflect.DeepEqual(a, b) {
				both = true
				break
			}
		}
		if !both {
			ret = append(ret, fmt.Sprintf("Add peer:{%v}", b))
		}
	}
	return strings.Join(ret, ",")
}

// DiffRegionKeyInfo return the difference of key info between two RegionInfo
func DiffRegionKeyInfo(origin *RegionInfo, other *RegionInfo) string {
	var ret []string
	if !bytes.Equal(origin.meta.StartKey, other.meta.StartKey) {
		ret = append(ret, fmt.Sprintf("StartKey Changed:{%s} -> {%s}", HexRegionKey(origin.meta.StartKey), HexRegionKey(other.meta.StartKey)))
	} else {
		ret = append(ret, fmt.Sprintf("StartKey:{%s}", HexRegionKey(origin.meta.StartKey)))
	}
	if !bytes.Equal(origin.meta.EndKey, other.meta.EndKey) {
		ret = append(ret, fmt.Sprintf("EndKey Changed:{%s} -> {%s}", HexRegionKey(origin.meta.EndKey), HexRegionKey(other.meta.EndKey)))
	} else {
		ret = append(ret, fmt.Sprintf("EndKey:{%s}", HexRegionKey(origin.meta.EndKey)))
	}

	return strings.Join(ret, ", ")
}

// HexRegionKey converts region key to hex format. Used for formating region in
// logs.
func HexRegionKey(key []byte) []byte {
	return []byte(strings.ToUpper(hex.EncodeToString(key)))
}

// RegionToHexMeta converts a region meta's keys to hex format. Used for formating
// region in logs.
func RegionToHexMeta(meta *metapb.Region) HexRegionMeta {
	if meta == nil {
		return HexRegionMeta{}
	}
	meta = proto.Clone(meta).(*metapb.Region)
	meta.StartKey = HexRegionKey(meta.StartKey)
	meta.EndKey = HexRegionKey(meta.EndKey)
	return HexRegionMeta{meta}
}

// HexRegionMeta is a region meta in the hex format. Used for formating region in logs.
type HexRegionMeta struct {
	*metapb.Region
}

func (h HexRegionMeta) String() string {
	return strings.TrimSpace(proto.CompactTextString(h.Region))
}

// RegionsToHexMeta converts regions' meta keys to hex format. Used for formating
// region in logs.
func RegionsToHexMeta(regions []*metapb.Region) HexRegionsMeta {
	hexRegionMetas := make([]*metapb.Region, len(regions))
	for i, region := range regions {
		meta := proto.Clone(region).(*metapb.Region)
		meta.StartKey = HexRegionKey(meta.StartKey)
		meta.EndKey = HexRegionKey(meta.EndKey)

		hexRegionMetas[i] = meta
	}
	return HexRegionsMeta(hexRegionMetas)
}

// HexRegionsMeta is a slice of regions' meta in the hex format. Used for formating
// region in logs.
type HexRegionsMeta []*metapb.Region

func (h HexRegionsMeta) String() string {
	var b strings.Builder
	for _, r := range h {
		b.WriteString(proto.CompactTextString(r))
	}
	return strings.TrimSpace(b.String())
}
