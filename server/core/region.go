// Copyright 2016 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"unsafe"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/replication_modepb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/logutil"
	"go.uber.org/zap"
)

// errRegionIsStale is error info for region is stale.
func errRegionIsStale(region *metapb.Region, origin *metapb.Region) error {
	return errors.Errorf("region is stale: region %v origin %v", region, origin)
}

// RegionInfo records detail region info.
// Read-Only once created.
type RegionInfo struct {
	term              uint64
	meta              *metapb.Region
	learners          []*metapb.Peer
	voters            []*metapb.Peer
	leader            *metapb.Peer
	downPeers         []*pdpb.PeerStats
	pendingPeers      []*metapb.Peer
	writtenBytes      uint64
	writtenKeys       uint64
	readBytes         uint64
	readKeys          uint64
	approximateSize   int64
	approximateKeys   int64
	interval          *pdpb.TimeInterval
	replicationStatus *replication_modepb.RegionReplicationStatus
	QueryStats        *pdpb.QueryStats
	flowRoundDivisor  uint64
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
		if IsLearner(p) {
			learners = append(learners, p)
		} else {
			voters = append(voters, p)
		}
	}
	sort.Sort(peerSlice(learners))
	sort.Sort(peerSlice(voters))
	region.learners = learners
	region.voters = voters
}

const (
	// EmptyRegionApproximateSize is the region approximate size of an empty region
	// (heartbeat size <= 1MB).
	EmptyRegionApproximateSize = 1
	// ImpossibleFlowSize is an impossible flow size (such as written_bytes, read_keys, etc.)
	// It may be caused by overflow, refer to https://github.com/tikv/pd/issues/3379.
	// They need to be filtered so as not to affect downstream.
	// (flow size >= 1024TB)
	ImpossibleFlowSize = 1 << 50
	// Only statistics within this interval limit are valid.
	statsReportMinInterval = 3      // 3s
	statsReportMaxInterval = 5 * 60 // 5min
)

// RegionFromHeartbeat constructs a Region from region heartbeat.
func RegionFromHeartbeat(heartbeat *pdpb.RegionHeartbeatRequest, opts ...RegionCreateOption) *RegionInfo {
	// Convert unit to MB.
	// If region isn't empty and less than 1MB, use 1MB instead.
	// The size of empty region will be correct by the previous RegionInfo.
	regionSize := heartbeat.GetApproximateSize() / (1 << 20)
	if heartbeat.GetApproximateSize() > 0 && regionSize < EmptyRegionApproximateSize {
		regionSize = EmptyRegionApproximateSize
	}

	region := &RegionInfo{
		term:              heartbeat.GetTerm(),
		meta:              heartbeat.GetRegion(),
		leader:            heartbeat.GetLeader(),
		downPeers:         heartbeat.GetDownPeers(),
		pendingPeers:      heartbeat.GetPendingPeers(),
		writtenBytes:      heartbeat.GetBytesWritten(),
		writtenKeys:       heartbeat.GetKeysWritten(),
		readBytes:         heartbeat.GetBytesRead(),
		readKeys:          heartbeat.GetKeysRead(),
		approximateSize:   int64(regionSize),
		approximateKeys:   int64(heartbeat.GetApproximateKeys()),
		interval:          heartbeat.GetInterval(),
		replicationStatus: heartbeat.GetReplicationStatus(),
		QueryStats:        heartbeat.GetQueryStats(),
	}

	for _, opt := range opts {
		opt(region)
	}

	if region.writtenKeys >= ImpossibleFlowSize || region.writtenBytes >= ImpossibleFlowSize {
		region.writtenKeys = 0
		region.writtenBytes = 0
	}
	if region.readKeys >= ImpossibleFlowSize || region.readBytes >= ImpossibleFlowSize {
		region.readKeys = 0
		region.readBytes = 0
	}

	sort.Sort(peerStatsSlice(region.downPeers))
	sort.Sort(peerSlice(region.pendingPeers))

	classifyVoterAndLearner(region)
	return region
}

// CorrectApproximateSize correct approximate size by the previous size if here exists an reported RegionInfo.
//
// See https://github.com/tikv/tikv/issues/11114
func (r *RegionInfo) CorrectApproximateSize(origin *RegionInfo) {
	if r.approximateSize != 0 {
		return
	}

	if origin != nil {
		r.approximateSize = origin.approximateSize
	} else {
		r.approximateSize = EmptyRegionApproximateSize
	}
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
		term:              r.term,
		meta:              proto.Clone(r.meta).(*metapb.Region),
		leader:            proto.Clone(r.leader).(*metapb.Peer),
		downPeers:         downPeers,
		pendingPeers:      pendingPeers,
		writtenBytes:      r.writtenBytes,
		writtenKeys:       r.writtenKeys,
		readBytes:         r.readBytes,
		readKeys:          r.readKeys,
		approximateSize:   r.approximateSize,
		approximateKeys:   r.approximateKeys,
		interval:          proto.Clone(r.interval).(*pdpb.TimeInterval),
		replicationStatus: r.replicationStatus,
	}

	for _, opt := range opts {
		opt(region)
	}
	classifyVoterAndLearner(region)
	return region
}

// GetTerm returns the current term of the region
func (r *RegionInfo) GetTerm() uint64 {
	return r.term
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
		if down.GetPeer().GetId() == peerID && !IsLearner(down.GetPeer()) {
			return down.GetPeer()
		}
	}
	return nil
}

// GetDownLearner returns the down learner with soecified peer id.
func (r *RegionInfo) GetDownLearner(peerID uint64) *metapb.Peer {
	for _, down := range r.downPeers {
		if down.GetPeer().GetId() == peerID && IsLearner(down.GetPeer()) {
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
		if peer.GetId() == peerID && !IsLearner(peer) {
			return peer
		}
	}
	return nil
}

// GetPendingLearner returns the pending learner peer with specified peer id.
func (r *RegionInfo) GetPendingLearner(peerID uint64) *metapb.Peer {
	for _, peer := range r.pendingPeers {
		if peer.GetId() == peerID && IsLearner(peer) {
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
	if r == nil {
		return nil
	}
	return r.meta
}

// GetStat returns the statistics of the region.
func (r *RegionInfo) GetStat() *pdpb.RegionStat {
	if r == nil {
		return nil
	}
	return &pdpb.RegionStat{
		BytesWritten: r.writtenBytes,
		BytesRead:    r.readBytes,
		KeysWritten:  r.writtenKeys,
		KeysRead:     r.readKeys,
	}
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

// GetRoundBytesRead returns the read bytes of the region.
func (r *RegionInfo) GetRoundBytesRead() uint64 {
	if r.flowRoundDivisor == 0 {
		return r.readBytes
	}
	return ((r.readBytes + r.flowRoundDivisor/2) / r.flowRoundDivisor) * r.flowRoundDivisor
}

// GetBytesWritten returns the written bytes of the region.
func (r *RegionInfo) GetBytesWritten() uint64 {
	return r.writtenBytes
}

// GetRoundBytesWritten returns the written bytes of the region.
func (r *RegionInfo) GetRoundBytesWritten() uint64 {
	if r.flowRoundDivisor == 0 {
		return r.writtenBytes
	}
	return ((r.writtenBytes + r.flowRoundDivisor/2) / r.flowRoundDivisor) * r.flowRoundDivisor
}

// GetKeysWritten returns the written keys of the region.
func (r *RegionInfo) GetKeysWritten() uint64 {
	return r.writtenKeys
}

// GetKeysRead returns the read keys of the region.
func (r *RegionInfo) GetKeysRead() uint64 {
	return r.readKeys
}

// GetWriteRate returns the write rate of the region.
func (r *RegionInfo) GetWriteRate() (bytesRate, keysRate float64) {
	reportInterval := r.GetInterval()
	interval := reportInterval.GetEndTimestamp() - reportInterval.GetStartTimestamp()
	if interval >= statsReportMinInterval && interval <= statsReportMaxInterval {
		return float64(r.writtenBytes) / float64(interval), float64(r.writtenKeys) / float64(interval)
	}
	return 0, 0
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

// GetReplicationStatus returns the region's replication status.
func (r *RegionInfo) GetReplicationStatus() *replication_modepb.RegionReplicationStatus {
	return r.replicationStatus
}

// RegionGuideFunc is a function that determines which follow-up operations need to be performed based on the origin
// and new region information.
type RegionGuideFunc func(region, origin *RegionInfo) (isNew, saveKV, saveCache, needSync bool)

// GenerateRegionGuideFunc is used to generate a RegionGuideFunc. Control the log output by specifying the log function.
// nil means do not print the log.
func GenerateRegionGuideFunc(enableLog bool) RegionGuideFunc {
	noLog := func(msg string, fields ...zap.Field) {}
	debug, info := noLog, noLog
	if enableLog {
		debug = log.Debug
		info = log.Info
	}
	// Save to storage if meta is updated.
	// Save to cache if meta or leader is updated, or contains any down/pending peer.
	// Mark isNew if the region in cache does not have leader.
	return func(region, origin *RegionInfo) (isNew, saveKV, saveCache, needSync bool) {
		if origin == nil {
			debug("insert new region",
				zap.Uint64("region-id", region.GetID()),
				logutil.ZapRedactStringer("meta-region", RegionToHexMeta(region.GetMeta())))
			saveKV, saveCache, isNew = true, true, true
		} else {
			r := region.GetRegionEpoch()
			o := origin.GetRegionEpoch()
			if r.GetVersion() > o.GetVersion() {
				info("region Version changed",
					zap.Uint64("region-id", region.GetID()),
					logutil.ZapRedactString("detail", DiffRegionKeyInfo(origin, region)),
					zap.Uint64("old-version", o.GetVersion()),
					zap.Uint64("new-version", r.GetVersion()),
				)
				saveKV, saveCache = true, true
			}
			if r.GetConfVer() > o.GetConfVer() {
				info("region ConfVer changed",
					zap.Uint64("region-id", region.GetID()),
					zap.String("detail", DiffRegionPeersInfo(origin, region)),
					zap.Uint64("old-confver", o.GetConfVer()),
					zap.Uint64("new-confver", r.GetConfVer()),
				)
				saveKV, saveCache = true, true
			}
			if region.GetLeader().GetId() != origin.GetLeader().GetId() {
				if origin.GetLeader().GetId() == 0 {
					isNew = true
				} else {
					info("leader changed",
						zap.Uint64("region-id", region.GetID()),
						zap.Uint64("from", origin.GetLeader().GetStoreId()),
						zap.Uint64("to", region.GetLeader().GetStoreId()),
					)
				}
				saveCache, needSync = true, true
			}
			if !SortedPeersStatsEqual(region.GetDownPeers(), origin.GetDownPeers()) {
				debug("down-peers changed", zap.Uint64("region-id", region.GetID()))
				saveCache, needSync = true, true
			}
			if !SortedPeersEqual(region.GetPendingPeers(), origin.GetPendingPeers()) {
				debug("pending-peers changed", zap.Uint64("region-id", region.GetID()))
				saveCache, needSync = true, true
			}
			if len(region.GetPeers()) != len(origin.GetPeers()) {
				saveKV, saveCache = true, true
			}

			if region.GetApproximateSize() != origin.GetApproximateSize() ||
				region.GetApproximateKeys() != origin.GetApproximateKeys() {
				saveCache = true
			}
			// Once flow has changed, will update the cache.
			// Because keys and bytes are strongly related, only bytes are judged.
			if region.GetRoundBytesWritten() != origin.GetRoundBytesWritten() ||
				region.GetRoundBytesRead() != origin.GetRoundBytesRead() ||
				region.flowRoundDivisor < origin.flowRoundDivisor {
				saveCache, needSync = true, true
			}

			if region.GetReplicationStatus().GetState() != replication_modepb.RegionReplicationState_UNKNOWN &&
				(region.GetReplicationStatus().GetState() != origin.GetReplicationStatus().GetState() ||
					region.GetReplicationStatus().GetStateId() != origin.GetReplicationStatus().GetStateId()) {
				saveCache = true
			}
		}
		return
	}
}

// regionMap wraps a map[uint64]*regionItem and supports randomly pick a region. They are the leaves of regionTree.
type regionMap map[uint64]*regionItem

func newRegionMap() regionMap {
	return make(map[uint64]*regionItem)
}

func (rm regionMap) Len() int {
	return len(rm)
}

func (rm regionMap) Get(id uint64) *regionItem {
	return rm[id]
}

// AddNew uses RegionInfo to generate a new regionItem.
// If the regionItem already exists, it will be overwritten.
// Note: Do not use this function when you only need to update the RegionInfo and do not need a new regionItem.
func (rm regionMap) AddNew(region *RegionInfo) *regionItem {
	item := &regionItem{region: region}
	rm[region.GetID()] = item
	return item
}

func (rm regionMap) Delete(id uint64) {
	delete(rm, id)
}

// RegionsInfo for export
type RegionsInfo struct {
	tree         *regionTree
	regions      regionMap              // regionID -> regionInfo
	leaders      map[uint64]*regionTree // storeID -> sub regionTree
	followers    map[uint64]*regionTree // storeID -> sub regionTree
	learners     map[uint64]*regionTree // storeID -> sub regionTree
	pendingPeers map[uint64]*regionTree // storeID -> sub regionTree
}

// NewRegionsInfo creates RegionsInfo with tree, regions, leaders and followers
func NewRegionsInfo() *RegionsInfo {
	return &RegionsInfo{
		tree:         newRegionTree(),
		regions:      newRegionMap(),
		leaders:      make(map[uint64]*regionTree),
		followers:    make(map[uint64]*regionTree),
		learners:     make(map[uint64]*regionTree),
		pendingPeers: make(map[uint64]*regionTree),
	}
}

// GetRegion returns the RegionInfo with regionID
func (r *RegionsInfo) GetRegion(regionID uint64) *RegionInfo {
	if item := r.regions.Get(regionID); item != nil {
		return item.region
	}
	return nil
}

// SetRegion sets the RegionInfo to regionTree and regionMap, also update leaders and followers by region peers
// overlaps: Other regions that overlap with the specified region, excluding itself.
func (r *RegionsInfo) SetRegion(region *RegionInfo) (overlaps []*RegionInfo) {
	var item *regionItem   // Pointer to the *RegionInfo of this ID.
	var origin *RegionInfo // This is the original region information of this ID.
	var rangeChanged bool  // This Region is new, or its range has changed.
	var peersChanged bool  // This Region is new, or its peers have changed, including leader-change/pending/down.

	if item = r.regions.Get(region.GetID()); item != nil {
		// If this ID already exists, use the existing regionItem and pick out the origin.
		origin = item.region
		rangeChanged = !bytes.Equal(origin.GetStartKey(), region.GetStartKey()) ||
			!bytes.Equal(origin.GetEndKey(), region.GetEndKey())
		if rangeChanged {
			// Delete itself in regionTree so that overlaps will not contain itself.
			// Because the regionItem is reused, there is no need to delete it in the regionMap.
			r.tree.remove(origin)
			// A change in the range is equivalent to a change in all peers.
			peersChanged = true
		} else {
			peersChanged = r.shouldRemoveFromSubTree(region, origin)
		}
		// If the peers have changed, the sub regionTree needs to be cleaned up.
		if peersChanged {
			// TODO: Improve performance by deleting only the different peers.
			r.removeRegionFromSubTree(origin)
		}
		// Update the RegionInfo in the regionItem.
		item.region = region
	} else {
		// If this ID does not exist, generate a new regionItem and save it in the regionMap.
		rangeChanged = true
		peersChanged = true
		item = r.regions.AddNew(region)
	}

	if !rangeChanged {
		// If the range is not changed, only the statistical on the regionTree needs to be updated.
		r.tree.updateStat(origin, region)
	} else {
		// It has been removed and all information needs to be updated again.
		overlaps = r.tree.update(item)
		for _, old := range overlaps {
			r.RemoveRegion(r.GetRegion(old.GetID()))
		}
	}

	if !peersChanged {
		// If the peers are not changed, only the statistical on the sub regionTree needs to be updated.
		r.updateSubTreeStat(origin, region)
	} else {
		// It has been removed and all information needs to be updated again.

		// Add to leaders and followers.
		for _, peer := range region.GetVoters() {
			storeID := peer.GetStoreId()
			if peer.GetId() == region.leader.GetId() {
				// Add leader peer to leaders.
				store, ok := r.leaders[storeID]
				if !ok {
					store = newRegionTree()
					r.leaders[storeID] = store
				}
				store.update(item)
			} else {
				// Add follower peer to followers.
				store, ok := r.followers[storeID]
				if !ok {
					store = newRegionTree()
					r.followers[storeID] = store
				}
				store.update(item)
			}
		}
		// Add to learners.
		for _, peer := range region.GetLearners() {
			storeID := peer.GetStoreId()
			store, ok := r.learners[storeID]
			if !ok {
				store = newRegionTree()
				r.learners[storeID] = store
			}
			store.update(item)
		}
		// Add to PendingPeers
		for _, peer := range region.GetPendingPeers() {
			storeID := peer.GetStoreId()
			store, ok := r.pendingPeers[storeID]
			if !ok {
				store = newRegionTree()
				r.pendingPeers[storeID] = store
			}
			store.update(item)
		}
	}

	return
}

// Len returns the RegionsInfo length
func (r *RegionsInfo) Len() int {
	return r.regions.Len()
}

// TreeLen returns the RegionsInfo tree length(now only used in test)
func (r *RegionsInfo) TreeLen() int {
	return r.tree.length()
}

func (r *RegionsInfo) updateSubTreeStat(origin *RegionInfo, region *RegionInfo) {
	for _, peer := range region.GetVoters() {
		storeID := peer.GetStoreId()
		if peer.GetId() == region.leader.GetId() {
			if tree, ok := r.leaders[storeID]; ok {
				tree.updateStat(origin, region)
			}
		} else {
			if tree, ok := r.followers[storeID]; ok {
				tree.updateStat(origin, region)
			}
		}
	}
	for _, peer := range region.GetLearners() {
		if tree, ok := r.learners[peer.GetStoreId()]; ok {
			tree.updateStat(origin, region)
		}
	}
	for _, peer := range region.GetPendingPeers() {
		if tree, ok := r.pendingPeers[peer.GetStoreId()]; ok {
			tree.updateStat(origin, region)
		}
	}
}

// GetOverlaps returns the regions which are overlapped with the specified region range.
func (r *RegionsInfo) GetOverlaps(region *RegionInfo) []*RegionInfo {
	return r.tree.getOverlaps(region)
}

// RemoveRegion removes RegionInfo from regionTree and regionMap
func (r *RegionsInfo) RemoveRegion(region *RegionInfo) {
	// Remove from tree and regions.
	r.tree.remove(region)
	r.regions.Delete(region.GetID())
	// Remove from leaders and followers.
	r.removeRegionFromSubTree(region)
}

// removeRegionFromSubTree removes RegionInfo from regionSubTrees
func (r *RegionsInfo) removeRegionFromSubTree(region *RegionInfo) {
	// Remove from leaders and followers.
	for _, peer := range region.meta.GetPeers() {
		storeID := peer.GetStoreId()
		r.leaders[storeID].remove(region)
		r.followers[storeID].remove(region)
		r.learners[storeID].remove(region)
		r.pendingPeers[storeID].remove(region)
	}
}

type peerSlice []*metapb.Peer

func (s peerSlice) Len() int {
	return len(s)
}
func (s peerSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s peerSlice) Less(i, j int) bool {
	return s[i].GetId() < s[j].GetId()
}

// SortedPeersEqual judges whether two sorted `peerSlice` are equal
func SortedPeersEqual(peersA, peersB []*metapb.Peer) bool {
	if len(peersA) != len(peersB) {
		return false
	}
	for i, peerA := range peersA {
		peerB := peersB[i]
		if peerA.GetStoreId() != peerB.GetStoreId() || peerA.GetId() != peerB.GetId() {
			return false
		}
	}
	return true
}

type peerStatsSlice []*pdpb.PeerStats

func (s peerStatsSlice) Len() int {
	return len(s)
}
func (s peerStatsSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s peerStatsSlice) Less(i, j int) bool {
	return s[i].GetPeer().GetId() < s[j].GetPeer().GetId()
}

// SortedPeersStatsEqual judges whether two sorted `peerStatsSlice` are equal
func SortedPeersStatsEqual(peersA, peersB []*pdpb.PeerStats) bool {
	if len(peersA) != len(peersB) {
		return false
	}
	for i, peerStatsA := range peersA {
		peerA := peerStatsA.GetPeer()
		peerB := peersB[i].GetPeer()
		if peerA.GetStoreId() != peerB.GetStoreId() || peerA.GetId() != peerB.GetId() {
			return false
		}
	}
	return true
}

// shouldRemoveFromSubTree return true when the region leader changed, peer transferred,
// new peer was created, learners changed, pendingPeers changed, and so on.
func (r *RegionsInfo) shouldRemoveFromSubTree(region *RegionInfo, origin *RegionInfo) bool {
	return origin.leader.GetId() != region.leader.GetId() ||
		!SortedPeersEqual(origin.GetVoters(), region.GetVoters()) ||
		!SortedPeersEqual(origin.GetLearners(), region.GetLearners()) ||
		!SortedPeersEqual(origin.GetPendingPeers(), region.GetPendingPeers())
}

// SearchRegion searches RegionInfo from regionTree
func (r *RegionsInfo) SearchRegion(regionKey []byte) *RegionInfo {
	region := r.tree.search(regionKey)
	if region == nil {
		return nil
	}
	return r.GetRegion(region.GetID())
}

// SearchPrevRegion searches previous RegionInfo from regionTree
func (r *RegionsInfo) SearchPrevRegion(regionKey []byte) *RegionInfo {
	region := r.tree.searchPrev(regionKey)
	if region == nil {
		return nil
	}
	return r.GetRegion(region.GetID())
}

// GetRegions gets all RegionInfo from regionMap
func (r *RegionsInfo) GetRegions() []*RegionInfo {
	regions := make([]*RegionInfo, 0, r.regions.Len())
	for _, item := range r.regions {
		regions = append(regions, item.region)
	}
	return regions
}

// GetStoreRegions gets all RegionInfo with a given storeID
func (r *RegionsInfo) GetStoreRegions(storeID uint64) []*RegionInfo {
	regions := make([]*RegionInfo, 0, r.GetStoreRegionCount(storeID))
	if leaders, ok := r.leaders[storeID]; ok {
		regions = append(regions, leaders.scanRanges()...)
	}
	if followers, ok := r.followers[storeID]; ok {
		regions = append(regions, followers.scanRanges()...)
	}
	if learners, ok := r.learners[storeID]; ok {
		regions = append(regions, learners.scanRanges()...)
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

// GetStoreLeaderWriteRate get total write rate of store's leaders
func (r *RegionsInfo) GetStoreLeaderWriteRate(storeID uint64) (bytesRate, keysRate float64) {
	return r.leaders[storeID].TotalWriteRate()
}

// GetStoreWriteRate get total write rate of store's regions
func (r *RegionsInfo) GetStoreWriteRate(storeID uint64) (bytesRate, keysRate float64) {
	storeBytesRate, storeKeysRate := r.leaders[storeID].TotalWriteRate()
	bytesRate += storeBytesRate
	keysRate += storeKeysRate
	storeBytesRate, storeKeysRate = r.followers[storeID].TotalWriteRate()
	bytesRate += storeBytesRate
	keysRate += storeKeysRate
	storeBytesRate, storeKeysRate = r.learners[storeID].TotalWriteRate()
	bytesRate += storeBytesRate
	keysRate += storeKeysRate
	return
}

// GetMetaRegions gets a set of metapb.Region from regionMap
func (r *RegionsInfo) GetMetaRegions() []*metapb.Region {
	regions := make([]*metapb.Region, 0, r.regions.Len())
	for _, item := range r.regions {
		regions = append(regions, proto.Clone(item.region.meta).(*metapb.Region))
	}
	return regions
}

// GetRegionCount gets the total count of RegionInfo of regionMap
func (r *RegionsInfo) GetRegionCount() int {
	return r.regions.Len()
}

// GetStoreRegionCount gets the total count of a store's leader, follower and learner RegionInfo by storeID
func (r *RegionsInfo) GetStoreRegionCount(storeID uint64) int {
	return r.GetStoreLeaderCount(storeID) + r.GetStoreFollowerCount(storeID) + r.GetStoreLearnerCount(storeID)
}

// GetStorePendingPeerCount gets the total count of a store's region that includes pending peer
func (r *RegionsInfo) GetStorePendingPeerCount(storeID uint64) int {
	return r.pendingPeers[storeID].length()
}

// GetStoreLeaderCount get the total count of a store's leader RegionInfo
func (r *RegionsInfo) GetStoreLeaderCount(storeID uint64) int {
	return r.leaders[storeID].length()
}

// GetStoreFollowerCount get the total count of a store's follower RegionInfo
func (r *RegionsInfo) GetStoreFollowerCount(storeID uint64) int {
	return r.followers[storeID].length()
}

// GetStoreLearnerCount get the total count of a store's learner RegionInfo
func (r *RegionsInfo) GetStoreLearnerCount(storeID uint64) int {
	return r.learners[storeID].length()
}

// RandPendingRegion randomly gets a store's region with a pending peer.
func (r *RegionsInfo) RandPendingRegion(storeID uint64, ranges []KeyRange) *RegionInfo {
	return r.pendingPeers[storeID].RandomRegion(ranges)
}

// RandPendingRegions randomly gets a store's n regions with a pending peer.
func (r *RegionsInfo) RandPendingRegions(storeID uint64, ranges []KeyRange, n int) []*RegionInfo {
	return r.pendingPeers[storeID].RandomRegions(n, ranges)
}

// RandLeaderRegion randomly gets a store's leader region.
func (r *RegionsInfo) RandLeaderRegion(storeID uint64, ranges []KeyRange) *RegionInfo {
	return r.leaders[storeID].RandomRegion(ranges)
}

// RandLeaderRegions randomly gets a store's n leader regions.
func (r *RegionsInfo) RandLeaderRegions(storeID uint64, ranges []KeyRange, n int) []*RegionInfo {
	return r.leaders[storeID].RandomRegions(n, ranges)
}

// RandFollowerRegion randomly gets a store's follower region.
func (r *RegionsInfo) RandFollowerRegion(storeID uint64, ranges []KeyRange) *RegionInfo {
	return r.followers[storeID].RandomRegion(ranges)
}

// RandFollowerRegions randomly gets a store's n follower regions.
func (r *RegionsInfo) RandFollowerRegions(storeID uint64, ranges []KeyRange, n int) []*RegionInfo {
	return r.followers[storeID].RandomRegions(n, ranges)
}

// RandLearnerRegion randomly gets a store's learner region.
func (r *RegionsInfo) RandLearnerRegion(storeID uint64, ranges []KeyRange) *RegionInfo {
	return r.learners[storeID].RandomRegion(ranges)
}

// RandLearnerRegions randomly gets a store's n learner regions.
func (r *RegionsInfo) RandLearnerRegions(storeID uint64, ranges []KeyRange, n int) []*RegionInfo {
	return r.learners[storeID].RandomRegions(n, ranges)
}

// GetLeader returns leader RegionInfo by storeID and regionID (now only used in test)
func (r *RegionsInfo) GetLeader(storeID uint64, region *RegionInfo) *RegionInfo {
	if leaders, ok := r.leaders[storeID]; ok {
		return leaders.find(region).region
	}
	return nil
}

// GetFollower returns follower RegionInfo by storeID and regionID (now only used in test)
func (r *RegionsInfo) GetFollower(storeID uint64, region *RegionInfo) *RegionInfo {
	if followers, ok := r.followers[storeID]; ok {
		return followers.find(region).region
	}
	return nil
}

// GetReadQueryNum returns read query num from this region
func (r *RegionInfo) GetReadQueryNum() uint64 {
	return GetReadQueryNum(r.QueryStats)
}

// GetWriteQueryNum returns write query num from this region
func (r *RegionInfo) GetWriteQueryNum() uint64 {
	return GetWriteQueryNum(r.QueryStats)
}

// GetReadQueryNum returns read query num from this QueryStats
func GetReadQueryNum(stats *pdpb.QueryStats) uint64 {
	if stats == nil {
		return 0
	}
	return stats.Coprocessor + stats.Get + stats.Scan
}

// GetWriteQueryNum returns write query num from this QueryStats
func GetWriteQueryNum(stats *pdpb.QueryStats) uint64 {
	if stats == nil {
		return 0
	}
	return stats.Put + stats.Delete + stats.DeleteRange + // raw
		stats.AcquirePessimisticLock + stats.Commit + stats.Prewrite + stats.Rollback // txn
}

// GetLoads returns loads from region
func (r *RegionInfo) GetLoads() []float64 {
	return []float64{
		float64(r.GetBytesRead()),
		float64(r.GetKeysRead()),
		float64(r.GetReadQueryNum()),
		float64(r.GetBytesWritten()),
		float64(r.GetKeysWritten()),
		float64(r.GetWriteQueryNum()),
	}
}

// GetWriteLoads returns write loads from region
func (r *RegionInfo) GetWriteLoads() []float64 {
	return []float64{
		0,
		0,
		0,
		float64(r.GetBytesWritten()),
		float64(r.GetKeysWritten()),
		float64(r.GetWriteQueryNum()),
	}
}

// ScanRange scans regions intersecting [start key, end key), returns at most
// `limit` regions. limit <= 0 means no limit.
func (r *RegionsInfo) ScanRange(startKey, endKey []byte, limit int) []*RegionInfo {
	var res []*RegionInfo
	r.tree.scanRange(startKey, func(region *RegionInfo) bool {
		if len(endKey) > 0 && bytes.Compare(region.GetStartKey(), endKey) >= 0 {
			return false
		}
		if limit > 0 && len(res) >= limit {
			return false
		}
		res = append(res, r.GetRegion(region.GetID()))
		return true
	})
	return res
}

// ScanRangeWithIterator scans from the first region containing or behind start key,
// until iterator returns false.
func (r *RegionsInfo) ScanRangeWithIterator(startKey []byte, iterator func(region *RegionInfo) bool) {
	r.tree.scanRange(startKey, iterator)
}

// GetAdjacentRegions returns region's info that is adjacent with specific region
func (r *RegionsInfo) GetAdjacentRegions(region *RegionInfo) (*RegionInfo, *RegionInfo) {
	p, n := r.tree.getAdjacentRegions(region)
	var prev, next *RegionInfo
	// check key to avoid key range hole
	if p != nil && bytes.Equal(p.region.GetEndKey(), region.GetStartKey()) {
		prev = r.GetRegion(p.region.GetID())
	}
	if n != nil && bytes.Equal(region.GetEndKey(), n.region.GetStartKey()) {
		next = r.GetRegion(n.region.GetID())
	}
	return prev, next
}

// GetRangeHoles returns all range holes, i.e the key ranges without any region info.
func (r *RegionsInfo) GetRangeHoles() [][]string {
	var (
		rangeHoles = make([][]string, 0)
		lastEndKey = []byte("")
	)
	// Start from the zero byte.
	r.tree.scanRange(lastEndKey, func(region *RegionInfo) bool {
		startKey := region.GetStartKey()
		// The last end key should equal to the next start key.
		// Otherwise it would mean there is a range hole between them.
		if !bytes.Equal(lastEndKey, startKey) {
			rangeHoles = append(rangeHoles, []string{HexRegionKeyStr(lastEndKey), HexRegionKeyStr(startKey)})
		}
		lastEndKey = region.GetEndKey()
		return true
	})
	return rangeHoles
}

// GetAverageRegionSize returns the average region approximate size.
func (r *RegionsInfo) GetAverageRegionSize() int64 {
	if r.tree.length() == 0 {
		return 0
	}
	return r.tree.TotalSize() / int64(r.tree.length())
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

func isInvolved(region *RegionInfo, startKey, endKey []byte) bool {
	return bytes.Compare(region.GetStartKey(), startKey) >= 0 && (len(endKey) == 0 || (len(region.GetEndKey()) > 0 && bytes.Compare(region.GetEndKey(), endKey) <= 0))
}

// String converts slice of bytes to string without copy.
func String(b []byte) (s string) {
	if len(b) == 0 {
		return ""
	}
	pbytes := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	pstring := (*reflect.StringHeader)(unsafe.Pointer(&s))
	pstring.Data = pbytes.Data
	pstring.Len = pbytes.Len
	return
}

// ToUpperASCIIInplace bytes.ToUpper but zero-cost
func ToUpperASCIIInplace(s []byte) []byte {
	hasLower := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		hasLower = hasLower || ('a' <= c && c <= 'z')
	}

	if !hasLower {
		return s
	}
	var c byte
	for i := 0; i < len(s); i++ {
		c = s[i]
		if 'a' <= c && c <= 'z' {
			c -= 'a' - 'A'
		}
		s[i] = c
	}
	return s
}

// EncodeToString overrides hex.EncodeToString implementation. Difference: returns []byte, not string
func EncodeToString(src []byte) []byte {
	dst := make([]byte, hex.EncodedLen(len(src)))
	hex.Encode(dst, src)
	return dst
}

// HexRegionKey converts region key to hex format. Used for formating region in
// logs.
func HexRegionKey(key []byte) []byte {
	return ToUpperASCIIInplace(EncodeToString(key))
}

// HexRegionKeyStr converts region key to hex format. Used for formating region in
// logs.
func HexRegionKeyStr(key []byte) string {
	return String(HexRegionKey(key))
}

// RegionToHexMeta converts a region meta's keys to hex format. Used for formating
// region in logs.
func RegionToHexMeta(meta *metapb.Region) HexRegionMeta {
	if meta == nil {
		return HexRegionMeta{}
	}
	return HexRegionMeta{meta}
}

// HexRegionMeta is a region meta in the hex format. Used for formating region in logs.
type HexRegionMeta struct {
	*metapb.Region
}

func (h HexRegionMeta) String() string {
	var meta = proto.Clone(h.Region).(*metapb.Region)
	meta.StartKey = HexRegionKey(meta.StartKey)
	meta.EndKey = HexRegionKey(meta.EndKey)
	return strings.TrimSpace(proto.CompactTextString(meta))
}

// RegionsToHexMeta converts regions' meta keys to hex format. Used for formating
// region in logs.
func RegionsToHexMeta(regions []*metapb.Region) HexRegionsMeta {
	hexRegionMetas := make([]*metapb.Region, len(regions))
	copy(hexRegionMetas, regions)
	return hexRegionMetas
}

// HexRegionsMeta is a slice of regions' meta in the hex format. Used for formating
// region in logs.
type HexRegionsMeta []*metapb.Region

func (h HexRegionsMeta) String() string {
	var b strings.Builder
	for _, r := range h {
		meta := proto.Clone(r).(*metapb.Region)
		meta.StartKey = HexRegionKey(meta.StartKey)
		meta.EndKey = HexRegionKey(meta.EndKey)

		b.WriteString(proto.CompactTextString(meta))
	}
	return strings.TrimSpace(b.String())
}
