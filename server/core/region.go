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
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

// RegionOption used to select region.
type RegionOption func(region *RegionInfo) bool

// HealthRegion checks if the region is healthy.
func HealthRegion() RegionOption {
	return func(region *RegionInfo) bool {
		return len(region.downPeers) == 0 && len(region.pendingPeers) == 0 && len(region.learners) == 0
	}
}

// RegionCreateOption used to create region.
type RegionCreateOption func(region *RegionInfo)

// WithDownPeers sets the down peers for the region.
func WithDownPeers(downPeers []*pdpb.PeerStats) RegionCreateOption {
	return func(region *RegionInfo) {
		region.downPeers = downPeers
	}
}

// WithPendingPeers sets the pending peers for the region.
func WithPendingPeers(pengdingPeers []*metapb.Peer) RegionCreateOption {
	return func(region *RegionInfo) {
		region.pendingPeers = pengdingPeers
	}
}

// WithLearners sets the learners for the region.
func WithLearners(learners []*metapb.Peer) RegionCreateOption {
	return func(region *RegionInfo) {
		region.learners = learners
	}
}

// WithLeader sets the leader for the region.
func WithLeader(leader *metapb.Peer) RegionCreateOption {
	return func(region *RegionInfo) {
		region.leader = leader
	}
}

// WithStartKey sets the start key for the region.
func WithStartKey(key []byte) RegionCreateOption {
	return func(region *RegionInfo) {
		region.meta.StartKey = key
	}
}

// WithEndKey sets the end key for the region.
func WithEndKey(key []byte) RegionCreateOption {
	return func(region *RegionInfo) {
		region.meta.StartKey = key
	}
}

// WithNewRegionID sets new id for the region.
func WithNewRegionID(id uint64) RegionCreateOption {
	return func(region *RegionInfo) {
		region.meta.Id = id
	}
}

// WithNewPeerIds sets new ids for peers.
func WithNewPeerIds(peerIds ...uint64) RegionCreateOption {
	return func(region *RegionInfo) {
		if len(peerIds) != len(region.meta.GetPeers()) {
			return
		}
		for i, p := range region.meta.GetPeers() {
			p.Id = peerIds[i]
		}
	}
}

// WithIncVersion increase the version of the region.
func WithIncVersion() RegionCreateOption {
	return func(region *RegionInfo) {
		e := region.meta.GetRegionEpoch()
		if e != nil {
			e.Version++
		}
	}
}

// WithIncConfVer increase the config version of the region.
func WithIncConfVer() RegionCreateOption {
	return func(region *RegionInfo) {
		e := region.meta.GetRegionEpoch()
		if e != nil {
			e.ConfVer++
		}
	}
}

// SetWrittenBytes sets the written bytes for the region.
func SetWrittenBytes(v uint64) RegionCreateOption {
	return func(region *RegionInfo) {
		region.writtenBytes = v
	}
}

// WithRemoveStorePeer remove the specified peer for the region.
func WithRemoveStorePeer(storeID uint64) RegionCreateOption {
	return func(region *RegionInfo) {
		var peers []*metapb.Peer
		for _, peer := range region.meta.GetPeers() {
			if peer.GetStoreId() != storeID {
				peers = append(peers, peer)
			}
		}
		region.meta.Peers = peers

	}
}

// SetReadBytes sets the read bytes for the region.
func SetReadBytes(v uint64) RegionCreateOption {
	return func(region *RegionInfo) {
		region.readBytes = v
	}
}

// SetApproximateSize sets the approximate size for the region.
func SetApproximateSize(v int64) RegionCreateOption {
	return func(region *RegionInfo) {
		region.approximateSize = v
	}
}

// SetApproximateKeys sets the approximate keys for the region.
func SetApproximateKeys(v int64) RegionCreateOption {
	return func(region *RegionInfo) {
		region.approximateKeys = v
	}
}

// SetRegionEpoch sets the epoch for the reigon.
func SetRegionEpoch(epoch *metapb.RegionEpoch) RegionCreateOption {
	return func(region *RegionInfo) {
		region.meta.RegionEpoch = epoch
	}
}

// SetPeers sets the peers for the region.
func SetPeers(peers []*metapb.Peer) RegionCreateOption {
	return func(region *RegionInfo) {
		region.meta.Peers = peers
	}
}

// WithAddPeer adds a peer for the region.
func WithAddPeer(peer *metapb.Peer) RegionCreateOption {
	return func(region *RegionInfo) {
		region.meta.Peers = append(region.meta.Peers, peer)
		if peer.IsLearner {
			region.learners = append(region.learners, peer)
		} else {
			region.voters = append(region.voters, peer)
		}
	}
}

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
	readBytes       uint64
	approximateSize int64
	approximateKeys int64
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
		readBytes:       heartbeat.GetBytesRead(),
		approximateSize: int64(regionSize),
		approximateKeys: int64(heartbeat.GetApproximateKeys()),
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
		readBytes:       r.readBytes,
		approximateSize: r.approximateSize,
		approximateKeys: r.approximateKeys,
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

// RemoveStorePeer removes the peer in specified store.
// Only for test.
func (r *RegionInfo) RemoveStorePeer(storeID uint64) {
	var peers []*metapb.Peer
	for _, peer := range r.meta.GetPeers() {
		if peer.GetStoreId() != storeID {
			peers = append(peers, peer)
		}
	}
	r.meta.Peers = peers
	classifyVoterAndLearner(r)
}

// AddPeer adds the peer in region info.
// Only for test.
func (r *RegionInfo) AddPeer(peer *metapb.Peer) {
	r.meta.Peers = append(r.meta.Peers, peer)
	if peer.IsLearner {
		r.learners = append(r.learners, peer)
	} else {
		r.voters = append(r.voters, peer)
	}
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

// SetPeers sets the peers of the region.
// Only for test and simulator.
func (r *RegionInfo) SetPeers(peers []*metapb.Peer) {
	r.meta.Peers = peers
}

// SetID sets the id of the region.
// Only for test and simulator.
func (r *RegionInfo) SetID(id uint64) {
	r.meta.Id = id
}

// RegionStat records each hot region's statistics
type RegionStat struct {
	RegionID  uint64 `json:"region_id"`
	FlowBytes uint64 `json:"flow_bytes"`
	// HotDegree records the hot region update times
	HotDegree int `json:"hot_degree"`
	// LastUpdateTime used to calculate average write
	LastUpdateTime time.Time `json:"last_update_time"`
	StoreID        uint64    `json:"-"`
	// AntiCount used to eliminate some noise when remove region in cache
	AntiCount int
	// Version used to check the region split times
	Version uint64
	// Stats is a rolling statistics, recording some recently added records.
	Stats *RollingStats
}

// NewRegionStat returns a RegionStat.
func NewRegionStat(region *RegionInfo, flowBytes uint64, antiCount int) *RegionStat {
	return &RegionStat{
		RegionID:       region.GetID(),
		FlowBytes:      flowBytes,
		LastUpdateTime: time.Now(),
		StoreID:        region.leader.GetStoreId(),
		Version:        region.meta.GetRegionEpoch().GetVersion(),
		AntiCount:      antiCount,
	}
}

// RegionsStat is a list of a group region state type
type RegionsStat []RegionStat

func (m RegionsStat) Len() int           { return len(m) }
func (m RegionsStat) Swap(i, j int)      { m[i], m[j] = m[j], m[i] }
func (m RegionsStat) Less(i, j int) bool { return m[i].FlowBytes < m[j].FlowBytes }

// HotRegionsStat records all hot regions statistics
type HotRegionsStat struct {
	TotalFlowBytes uint64      `json:"total_flow_bytes"`
	RegionsCount   int         `json:"regions_count"`
	RegionsStat    RegionsStat `json:"statistics"`
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

// GetRegion return the RegionInfo with regionID
func (r *RegionsInfo) GetRegion(regionID uint64) *RegionInfo {
	region := r.regions.Get(regionID)
	if region == nil {
		return nil
	}
	return region
}

// SetRegion set the RegionInfo with regionID
func (r *RegionsInfo) SetRegion(region *RegionInfo) []*metapb.Region {
	if origin := r.regions.Get(region.GetID()); origin != nil {
		r.RemoveRegion(origin)
	}
	return r.AddRegion(region)
}

// Length return the RegionsInfo length
func (r *RegionsInfo) Length() int {
	return r.regions.Len()
}

// TreeLength return the RegionsInfo tree length(now only used in test)
func (r *RegionsInfo) TreeLength() int {
	return r.tree.length()
}

// AddRegion add RegionInfo to regionTree and regionMap, also update leadres and followers by region peers
func (r *RegionsInfo) AddRegion(region *RegionInfo) []*metapb.Region {
	// Add to tree and regions.
	overlaps := r.tree.update(region.meta)
	for _, item := range overlaps {
		r.RemoveRegion(r.GetRegion(item.Id))
	}

	r.regions.Put(region)

	if region.leader == nil {
		return overlaps
	}

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

// RemoveRegion remove RegionInfo from regionTree and regionMap
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

// RandLeaderRegion get a store's leader region by random
func (r *RegionsInfo) RandLeaderRegion(storeID uint64, opts ...RegionOption) *RegionInfo {
	return randRegion(r.leaders[storeID], opts...)
}

// RandFollowerRegion get a store's follower region by random
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

// ScanRange scans region with start key, until number greater than limit.
func (r *RegionsInfo) ScanRange(startKey []byte, limit int) []*RegionInfo {
	res := make([]*RegionInfo, 0, limit)
	r.tree.scanRange(startKey, func(metaRegion *metapb.Region) bool {
		res = append(res, r.GetRegion(metaRegion.GetId()))
		return len(res) < limit
	})
	return res
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

// RegionStats records a list of regions' statistics and distribution status.
type RegionStats struct {
	Count            int              `json:"count"`
	EmptyCount       int              `json:"empty_count"`
	StorageSize      int64            `json:"storage_size"`
	StorageKeys      int64            `json:"storage_keys"`
	StoreLeaderCount map[uint64]int   `json:"store_leader_count"`
	StorePeerCount   map[uint64]int   `json:"store_peer_count"`
	StoreLeaderSize  map[uint64]int64 `json:"store_leader_size"`
	StoreLeaderKeys  map[uint64]int64 `json:"store_leader_keys"`
	StorePeerSize    map[uint64]int64 `json:"store_peer_size"`
	StorePeerKeys    map[uint64]int64 `json:"store_peer_keys"`
}

func newRegionStats() *RegionStats {
	return &RegionStats{
		StoreLeaderCount: make(map[uint64]int),
		StorePeerCount:   make(map[uint64]int),
		StoreLeaderSize:  make(map[uint64]int64),
		StoreLeaderKeys:  make(map[uint64]int64),
		StorePeerSize:    make(map[uint64]int64),
		StorePeerKeys:    make(map[uint64]int64),
	}
}

// Observe adds a region's statistics into RegionStats.
func (s *RegionStats) Observe(r *RegionInfo) {
	s.Count++
	if r.approximateSize <= EmptyRegionApproximateSize {
		s.EmptyCount++
	}
	s.StorageSize += r.approximateSize
	s.StorageKeys += r.approximateKeys
	if r.leader != nil {
		s.StoreLeaderCount[r.leader.GetStoreId()]++
		s.StoreLeaderSize[r.leader.GetStoreId()] += r.approximateSize
		s.StoreLeaderKeys[r.leader.GetStoreId()] += r.approximateKeys
	}
	for _, p := range r.meta.Peers {
		s.StorePeerCount[p.GetStoreId()]++
		s.StorePeerSize[p.GetStoreId()] += r.approximateSize
		s.StorePeerKeys[p.GetStoreId()] += r.approximateKeys
	}
}

// GetRegionStats scans regions that inside range [startKey, endKey) and sums up
// their statistics.
func (r *RegionsInfo) GetRegionStats(startKey, endKey []byte) *RegionStats {
	stats := newRegionStats()
	r.tree.scanRange(startKey, func(meta *metapb.Region) bool {
		if len(endKey) > 0 && (len(meta.EndKey) == 0 || bytes.Compare(meta.EndKey, endKey) >= 0) {
			return false
		}
		if region := r.GetRegion(meta.GetId()); region != nil {
			stats.Observe(region)
		}
		return true
	})
	return stats
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
		originKey := &metapb.Region{StartKey: origin.meta.StartKey}
		otherKey := &metapb.Region{StartKey: other.meta.StartKey}
		ret = append(ret, fmt.Sprintf("StartKey Changed:{%s} -> {%s}", originKey, otherKey))
	} else {
		ret = append(ret, fmt.Sprintf("StartKey:{%s}", origin.meta.StartKey))
	}
	if !bytes.Equal(origin.meta.EndKey, other.meta.EndKey) {
		originKey := &metapb.Region{EndKey: origin.meta.EndKey}
		otherKey := &metapb.Region{EndKey: other.meta.EndKey}
		ret = append(ret, fmt.Sprintf("EndKey Changed:{%s} -> {%s}, StartKey: {%s}", originKey, otherKey, origin.meta.StartKey))
	} else {
		ret = append(ret, fmt.Sprintf("EndKey:{%s}", origin.meta.EndKey))
	}

	return strings.Join(ret, ",")
}
