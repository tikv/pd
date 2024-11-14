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
	"sync/atomic"
	"unsafe"

	"github.com/docker/go-units"
	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/replication_modepb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/logutil"
	"github.com/tikv/pd/pkg/syncutil"
	"github.com/tikv/pd/pkg/typeutil"
	"go.uber.org/zap"
)

const (
	randomRegionMaxRetry = 10
	scanRegionLimit      = 1000
)

// errRegionIsStale is error info for region is stale.
func errRegionIsStale(region *metapb.Region, origin *metapb.Region) error {
	return errors.Errorf("region is stale: region %v origin %v", region, origin)
}

// RegionInfo records detail region info.
// the properties are Read-Only once created except buckets.
// the `buckets` could be modified by the request `report buckets` with greater version.
type RegionInfo struct {
	term              uint64
	meta              *metapb.Region
	learners          []*metapb.Peer
	witnesses         []*metapb.Peer
	voters            []*metapb.Peer
	leader            *metapb.Peer
	downPeers         []*pdpb.PeerStats
	pendingPeers      []*metapb.Peer
	cpuUsage          uint64
	writtenBytes      uint64
	writtenKeys       uint64
	readBytes         uint64
	readKeys          uint64
	approximateSize   int64
	approximateKeys   int64
	interval          *pdpb.TimeInterval
	replicationStatus *replication_modepb.RegionReplicationStatus
	queryStats        *pdpb.QueryStats
	flowRoundDivisor  uint64
	// buckets is not thread unsafe, it should be accessed by the request `report buckets` with greater version.
	buckets unsafe.Pointer
	// source is used to indicate region's source, such as Storage/Sync/Heartbeat.
	source RegionSource
}

// RegionSource is the source of region.
type RegionSource uint32

const (
	// Storage means this region's meta info might be stale.
	Storage RegionSource = iota
	// Sync means this region's meta info is relatively fresher.
	Sync
	// Heartbeat means this region's meta info is relatively fresher.
	Heartbeat
)

// LoadedFromStorage means this region's meta info loaded from storage.
func (r *RegionInfo) LoadedFromStorage() bool {
	return r.source == Storage
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
	witnesses := make([]*metapb.Peer, 0, 1)
	for _, p := range region.meta.Peers {
		if IsLearner(p) {
			learners = append(learners, p)
		} else {
			voters = append(voters, p)
		}
		// Whichever peer role can be a witness
		if IsWitness(p) {
			witnesses = append(witnesses, p)
		}
	}
	sort.Sort(peerSlice(learners))
	sort.Sort(peerSlice(voters))
	sort.Sort(peerSlice(witnesses))
	region.learners = learners
	region.voters = voters
	region.witnesses = witnesses
}

// peersEqualTo returns true when the peers are not changed, which may caused by: the region leader not changed,
// peer transferred, new peer was created, learners changed, pendingPeers changed.
func (r *RegionInfo) peersEqualTo(region *RegionInfo) bool {
	return r.leader.GetId() == region.leader.GetId() &&
		SortedPeersEqual(r.GetVoters(), region.GetVoters()) &&
		SortedPeersEqual(r.GetLearners(), region.GetLearners()) &&
		SortedPeersEqual(r.GetWitnesses(), region.GetWitnesses()) &&
		SortedPeersEqual(r.GetPendingPeers(), region.GetPendingPeers())
}

// rangeEqualsTo returns true when the start_key and end_key are the same.
func (r *RegionInfo) rangeEqualsTo(region *RegionInfo) bool {
	return bytes.Equal(r.GetStartKey(), region.GetStartKey()) && bytes.Equal(r.GetEndKey(), region.GetEndKey())
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
	// InitClusterRegionThreshold is a threshold which represent a new cluster.
	InitClusterRegionThreshold = 50
)

// RegionFromHeartbeat constructs a Region from region heartbeat.
func RegionFromHeartbeat(heartbeat *pdpb.RegionHeartbeatRequest, opts ...RegionCreateOption) *RegionInfo {
	// Convert unit to MB.
	// If region isn't empty and less than 1MB, use 1MB instead.
	// The size of empty region will be correct by the previous RegionInfo.
	regionSize := heartbeat.GetApproximateSize() / units.MiB
	if heartbeat.GetApproximateSize() > 0 && regionSize < EmptyRegionApproximateSize {
		regionSize = EmptyRegionApproximateSize
	}

	region := &RegionInfo{
		term:              heartbeat.GetTerm(),
		meta:              heartbeat.GetRegion(),
		leader:            heartbeat.GetLeader(),
		downPeers:         heartbeat.GetDownPeers(),
		pendingPeers:      heartbeat.GetPendingPeers(),
		cpuUsage:          heartbeat.GetCpuUsage(),
		writtenBytes:      heartbeat.GetBytesWritten(),
		writtenKeys:       heartbeat.GetKeysWritten(),
		readBytes:         heartbeat.GetBytesRead(),
		readKeys:          heartbeat.GetKeysRead(),
		approximateSize:   int64(regionSize),
		approximateKeys:   int64(heartbeat.GetApproximateKeys()),
		interval:          heartbeat.GetInterval(),
		replicationStatus: heartbeat.GetReplicationStatus(),
		queryStats:        heartbeat.GetQueryStats(),
		source:            Heartbeat,
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

// Inherit inherits the buckets and region size from the parent region if bucket enabled.
// correct approximate size and buckets by the previous size if here exists a reported RegionInfo.
// See https://github.com/tikv/tikv/issues/11114
func (r *RegionInfo) Inherit(origin *RegionInfo, bucketEnable bool) {
	// regionSize should not be zero if region is not empty.
	if r.GetApproximateSize() == 0 {
		if origin != nil {
			r.approximateSize = origin.approximateSize
		} else {
			r.approximateSize = EmptyRegionApproximateSize
		}
	}
	if bucketEnable && origin != nil && r.buckets == nil {
		r.buckets = origin.buckets
	}
}

// Clone returns a copy of current regionInfo.
func (r *RegionInfo) Clone(opts ...RegionCreateOption) *RegionInfo {
	downPeers := make([]*pdpb.PeerStats, 0, len(r.downPeers))
	for _, peer := range r.downPeers {
		downPeers = append(downPeers, typeutil.DeepClone(peer, PeerStatsFactory))
	}
	pendingPeers := make([]*metapb.Peer, 0, len(r.pendingPeers))
	for _, peer := range r.pendingPeers {
		pendingPeers = append(pendingPeers, typeutil.DeepClone(peer, RegionPeerFactory))
	}

	region := &RegionInfo{
		term:              r.term,
		meta:              typeutil.DeepClone(r.meta, RegionFactory),
		leader:            typeutil.DeepClone(r.leader, RegionPeerFactory),
		downPeers:         downPeers,
		pendingPeers:      pendingPeers,
		cpuUsage:          r.cpuUsage,
		writtenBytes:      r.writtenBytes,
		writtenKeys:       r.writtenKeys,
		readBytes:         r.readBytes,
		readKeys:          r.readKeys,
		approximateSize:   r.approximateSize,
		approximateKeys:   r.approximateKeys,
		interval:          typeutil.DeepClone(r.interval, TimeIntervalFactory),
		replicationStatus: r.replicationStatus,
		buckets:           r.buckets,
		queryStats:        typeutil.DeepClone(r.queryStats, QueryStatsFactory),
	}

	for _, opt := range opts {
		opt(region)
	}
	classifyVoterAndLearner(region)
	return region
}

// NeedMerge returns true if size is less than merge size and keys is less than mergeKeys.
func (r *RegionInfo) NeedMerge(mergeSize int64, mergeKeys int64) bool {
	return r.GetApproximateSize() <= mergeSize && r.GetApproximateKeys() <= mergeKeys
}

// IsOversized indicates whether the region is oversized.
func (r *RegionInfo) IsOversized(maxSize int64, maxKeys int64) bool {
	return r.GetApproximateSize() >= maxSize || r.GetApproximateKeys() >= maxKeys
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

// GetWitnesses returns the witnesses.
func (r *RegionInfo) GetWitnesses() []*metapb.Peer {
	return r.witnesses
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

// GetStoreIDs returns a map indicate the region distributed.
func (r *RegionInfo) GetStoreIDs() map[uint64]struct{} {
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

// UpdateBuckets sets the buckets of the region.
func (r *RegionInfo) UpdateBuckets(buckets, old *metapb.Buckets) bool {
	if buckets == nil {
		atomic.StorePointer(&r.buckets, nil)
		return true
	}
	// only need to update bucket keys, versions.
	newBuckets := &metapb.Buckets{
		RegionId: buckets.GetRegionId(),
		Version:  buckets.GetVersion(),
		Keys:     buckets.GetKeys(),
	}
	return atomic.CompareAndSwapPointer(&r.buckets, unsafe.Pointer(old), unsafe.Pointer(newBuckets))
}

// GetBuckets returns the buckets of the region.
func (r *RegionInfo) GetBuckets() *metapb.Buckets {
	if r == nil {
		return nil
	}
	buckets := atomic.LoadPointer(&r.buckets)
	return (*metapb.Buckets)(buckets)
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

// GetCPUUsage returns the CPU usage of the region since the last heartbeat.
// The number range is [0, N * 100], where N is the number of CPU cores.
// However, since the TiKV basically only meters the CPU usage inside the
// Unified Read Pool, it should be considered as an indicator of Region read
// CPU overhead for now.
func (r *RegionInfo) GetCPUUsage() uint64 {
	return r.cpuUsage
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

// IsFlashbackChanged returns true if flashback changes.
func (r *RegionInfo) IsFlashbackChanged(l *RegionInfo) bool {
	return r.meta.IsInFlashback != l.meta.IsInFlashback
}

func (r *RegionInfo) isInvolved(startKey, endKey []byte) bool {
	return bytes.Compare(r.GetStartKey(), startKey) >= 0 && (len(endKey) == 0 || (len(r.GetEndKey()) > 0 && bytes.Compare(r.GetEndKey(), endKey) <= 0))
}

func (r *RegionInfo) isRegionRecreated() bool {
	// Regions recreated by online unsafe recover have both ver and conf ver equal to 1. To
	// prevent stale bootstrap region (first region in a cluster which covers the entire key
	// range) from reporting stale info, we exclude regions that covers the entire key range
	// here. Technically, it is possible for unsafe recover to recreate such region, but that
	// means the entire key range is unavailable, and we don't expect unsafe recover to perform
	// better than recreating the cluster.
	return r.GetRegionEpoch().GetVersion() == 1 && r.GetRegionEpoch().GetConfVer() == 1 && (len(r.GetStartKey()) != 0 || len(r.GetEndKey()) != 0)
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
			if len(region.GetBuckets().GetKeys()) != len(origin.GetBuckets().GetKeys()) {
				debug("bucket key changed", zap.Uint64("region-id", region.GetID()))
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
			// Do not save to kv, because 1) flashback will be eventually set to
			// false, 2) flashback changes almost all regions in a cluster.
			// Saving kv may downgrade PD performance when there are many regions.
			if region.IsFlashbackChanged(origin) {
				saveCache = true
			}
			if origin.LoadedFromStorage() {
				isNew = true
			}
		}
		return
	}
}

// RegionsInfo for export
type RegionsInfo struct {
	t            syncutil.RWMutex
	tree         *regionTree
	regions      map[uint64]*regionItem // regionID -> regionInfo
	st           syncutil.RWMutex
	subRegions   map[uint64]*regionItem // regionID -> regionInfo
	leaders      map[uint64]*regionTree // storeID -> sub regionTree
	followers    map[uint64]*regionTree // storeID -> sub regionTree
	learners     map[uint64]*regionTree // storeID -> sub regionTree
	witnesses    map[uint64]*regionTree // storeID -> sub regionTree
	pendingPeers map[uint64]*regionTree // storeID -> sub regionTree
}

// NewRegionsInfo creates RegionsInfo with tree, regions, leaders and followers
func NewRegionsInfo() *RegionsInfo {
	return &RegionsInfo{
		tree:         newRegionTree(),
		regions:      make(map[uint64]*regionItem),
		subRegions:   make(map[uint64]*regionItem),
		leaders:      make(map[uint64]*regionTree),
		followers:    make(map[uint64]*regionTree),
		learners:     make(map[uint64]*regionTree),
		witnesses:    make(map[uint64]*regionTree),
		pendingPeers: make(map[uint64]*regionTree),
	}
}

// GetRegion returns the RegionInfo with regionID
func (r *RegionsInfo) GetRegion(regionID uint64) *RegionInfo {
	r.t.RLock()
	defer r.t.RUnlock()

	return r.getRegionLocked(regionID)
}

func (r *RegionsInfo) getRegionLocked(regionID uint64) *RegionInfo {
	if item := r.regions[regionID]; item != nil {
		return item.RegionInfo
	}
	return nil
}

// AtomicCheckAndPutRegion checks if the region is valid to put, if valid then put.
func (r *RegionsInfo) AtomicCheckAndPutRegion(region *RegionInfo) ([]*RegionInfo, error) {
	r.t.Lock()
	var overlaps []*RegionInfo
	origin := r.getRegionLocked(region.GetID())
	if origin == nil || !bytes.Equal(origin.GetStartKey(), region.GetStartKey()) || !bytes.Equal(origin.GetEndKey(), region.GetEndKey()) {
		overlaps = r.tree.getOverlaps(region)
	}
	_, err := check(region, origin, overlaps)
	if err != nil {
		r.t.Unlock()
		return nil, err
	}
	origin, overlaps, rangeChanged := r.setRegionLocked(region)
	r.t.Unlock()
	r.UpdateSubTree(region, origin, overlaps, rangeChanged)
	return overlaps, nil
}

// GetRelevantRegions returns the relevant regions for a given region.
func (r *RegionsInfo) GetRelevantRegions(region *RegionInfo) (origin *RegionInfo, overlaps []*RegionInfo) {
	r.t.RLock()
	defer r.t.RUnlock()
	origin = r.getRegionLocked(region.GetID())
	if origin == nil || !bytes.Equal(origin.GetStartKey(), region.GetStartKey()) || !bytes.Equal(origin.GetEndKey(), region.GetEndKey()) {
		overlaps = r.tree.getOverlaps(region)
	}
	return
}

func check(region, origin *RegionInfo, overlaps []*RegionInfo) (*RegionInfo, error) {
	for _, item := range overlaps {
		// PD ignores stale regions' heartbeats, unless it is recreated recently by unsafe recover operation.
		if region.GetRegionEpoch().GetVersion() < item.GetRegionEpoch().GetVersion() && !region.isRegionRecreated() {
			return nil, errRegionIsStale(region.GetMeta(), item.GetMeta())
		}
	}
	if origin == nil {
		return nil, nil
	}

	r := region.GetRegionEpoch()
	o := origin.GetRegionEpoch()
	// TiKV reports term after v3.0
	isTermBehind := region.GetTerm() > 0 && region.GetTerm() < origin.GetTerm()
	// Region meta is stale, return an error.
	if (isTermBehind || r.GetVersion() < o.GetVersion() || r.GetConfVer() < o.GetConfVer()) && !region.isRegionRecreated() {
		return origin, errRegionIsStale(region.GetMeta(), origin.GetMeta())
	}

	return origin, nil
}

// SetRegion sets the RegionInfo to regionTree and regionMap, also update leaders and followers by region peers
// overlaps: Other regions that overlap with the specified region, excluding itself.
func (r *RegionsInfo) SetRegion(region *RegionInfo) []*RegionInfo {
	r.t.Lock()
	defer r.t.Unlock()
	_, overlaps, _ := r.setRegionLocked(region)
	return overlaps
}

// SetRegionWithUpdate sets the RegionInfo to regionTree and regionMap and return the update info of subtree.
func (r *RegionsInfo) SetRegionWithUpdate(region *RegionInfo) (*RegionInfo, []*RegionInfo, bool) {
	r.t.Lock()
	defer r.t.Unlock()
	return r.setRegionLocked(region)
}

func (r *RegionsInfo) setRegionLocked(region *RegionInfo) (*RegionInfo, []*RegionInfo, bool) {
	var (
		item   *regionItem // Pointer to the *RegionInfo of this ID.
		origin *RegionInfo
	)
	rangeChanged := true // This Region is new, or its range has changed.

	if item = r.regions[region.GetID()]; item != nil {
		// If this ID already exists, use the existing regionItem and pick out the origin.
		origin = item.RegionInfo
		rangeChanged = !origin.rangeEqualsTo(region)
		if rangeChanged {
			// Delete itself in regionTree so that overlaps will not contain itself.
			// Because the regionItem is reused, there is no need to delete it in the regionMap.
			r.tree.remove(origin)
			// Update the RegionInfo in the regionItem.
			item.RegionInfo = region
		} else {
			// If the range is not changed, only the statistical on the regionTree needs to be updated.
			r.tree.updateStat(origin, region)
			// Update the RegionInfo in the regionItem.
			item.RegionInfo = region
			return origin, nil, rangeChanged
		}
	} else {
		// If this ID does not exist, generate a new regionItem and save it in the regionMap.
		item = &regionItem{RegionInfo: region}
		r.regions[region.GetID()] = item
	}

	var overlaps []*RegionInfo
	if rangeChanged {
		// It has been removed and all information needs to be updated again.
		overlaps = r.tree.update(item)
		for _, old := range overlaps {
			o := r.getRegionLocked(old.GetID())
			// Remove from tree and regions.
			r.tree.remove(o)
			delete(r.regions, o.GetID())
		}
	}
	return origin, overlaps, rangeChanged
}

// UpdateSubTree updates the subtree.
func (r *RegionsInfo) UpdateSubTree(region, origin *RegionInfo, toRemove []*RegionInfo, rangeChanged bool) {
	r.st.Lock()
	defer r.st.Unlock()
	if origin != nil {
		if rangeChanged || !origin.peersEqualTo(region) {
			// If the range or peers have changed, the sub regionTree needs to be cleaned up.
			// TODO: Improve performance by deleting only the different peers.
			r.removeRegionFromSubTreeLocked(origin)
		} else {
			r.updateSubTreeStat(origin, region)
			r.subRegions[region.GetID()].RegionInfo = region
			return
		}
	}
	if rangeChanged {
		for _, re := range toRemove {
			r.removeRegionFromSubTreeLocked(re)
		}
	}

	item := &regionItem{region}
	r.subRegions[region.GetID()] = item
	// It has been removed and all information needs to be updated again.
	// Set peers then.
	setPeer := func(peersMap map[uint64]*regionTree, storeID uint64, item *regionItem) {
		store, ok := peersMap[storeID]
		if !ok {
			store = newRegionTree()
			peersMap[storeID] = store
		}
		store.update(item)
	}

	// Add to leaders and followers.
	for _, peer := range region.GetVoters() {
		storeID := peer.GetStoreId()
		if peer.GetId() == region.leader.GetId() {
			// Add leader peer to leaders.
			setPeer(r.leaders, storeID, item)
		} else {
			// Add follower peer to followers.
			setPeer(r.followers, storeID, item)
		}
	}

	setPeers := func(peersMap map[uint64]*regionTree, peers []*metapb.Peer) {
		for _, peer := range peers {
			storeID := peer.GetStoreId()
			setPeer(peersMap, storeID, item)
		}
	}
	// Add to learners.
	setPeers(r.learners, region.GetLearners())
	// Add to witnesses.
	setPeers(r.witnesses, region.GetWitnesses())
	// Add to PendingPeers
	setPeers(r.pendingPeers, region.GetPendingPeers())
}

func (r *RegionsInfo) updateSubTreeStat(origin *RegionInfo, region *RegionInfo) {
	updatePeerStat := func(peersMap map[uint64]*regionTree, storeID uint64) {
		if tree, ok := peersMap[storeID]; ok {
			tree.updateStat(origin, region)
		}
	}
	for _, peer := range region.GetVoters() {
		storeID := peer.GetStoreId()
		if peer.GetId() == region.leader.GetId() {
			updatePeerStat(r.leaders, storeID)
		} else {
			updatePeerStat(r.followers, storeID)
		}
	}

	updatePeersStat := func(peersMap map[uint64]*regionTree, peers []*metapb.Peer) {
		for _, peer := range peers {
			updatePeerStat(peersMap, peer.GetStoreId())
		}
	}
	updatePeersStat(r.learners, region.GetLearners())
	updatePeersStat(r.witnesses, region.GetWitnesses())
	updatePeersStat(r.pendingPeers, region.GetPendingPeers())
}

// TreeLen returns the RegionsInfo tree length(now only used in test)
func (r *RegionsInfo) TreeLen() int {
	r.t.RLock()
	defer r.t.RUnlock()
	return r.tree.length()
}

// GetOverlaps returns the regions which are overlapped with the specified region range.
func (r *RegionsInfo) GetOverlaps(region *RegionInfo) []*RegionInfo {
	r.t.RLock()
	defer r.t.RUnlock()
	return r.tree.getOverlaps(region)
}

// RemoveRegion removes RegionInfo from regionTree and regionMap
func (r *RegionsInfo) RemoveRegion(region *RegionInfo) {
	r.t.Lock()
	defer r.t.Unlock()
	// Remove from tree and regions.
	r.tree.remove(region)
	delete(r.regions, region.GetID())
}

// Reset resets the regions info.
func (r *RegionsInfo) Reset() {
	r.t.Lock()
	r.tree = newRegionTree()
	r.regions = make(map[uint64]*regionItem)
	r.t.Unlock()
	r.st.Lock()
	defer r.st.Unlock()
	r.leaders = make(map[uint64]*regionTree)
	r.followers = make(map[uint64]*regionTree)
	r.learners = make(map[uint64]*regionTree)
	r.witnesses = make(map[uint64]*regionTree)
	r.pendingPeers = make(map[uint64]*regionTree)
}

// RemoveRegionFromSubTree removes RegionInfo from regionSubTrees
func (r *RegionsInfo) RemoveRegionFromSubTree(region *RegionInfo) {
	r.st.Lock()
	defer r.st.Unlock()
	// Remove from leaders and followers.
	r.removeRegionFromSubTreeLocked(region)
}

// removeRegionFromSubTreeLocked removes RegionInfo from regionSubTrees
func (r *RegionsInfo) removeRegionFromSubTreeLocked(region *RegionInfo) {
	// Remove from leaders and followers.
	for _, peer := range region.GetMeta().GetPeers() {
		storeID := peer.GetStoreId()
		r.leaders[storeID].remove(region)
		r.followers[storeID].remove(region)
		r.learners[storeID].remove(region)
		r.witnesses[storeID].remove(region)
		r.pendingPeers[storeID].remove(region)
	}
	delete(r.subRegions, region.GetMeta().GetId())
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

// GetRegionByKey searches RegionInfo from regionTree
func (r *RegionsInfo) GetRegionByKey(regionKey []byte) *RegionInfo {
	r.t.RLock()
	defer r.t.RUnlock()
	region := r.tree.search(regionKey)
	if region == nil {
		return nil
	}
	return r.getRegionLocked(region.GetID())
}

// GetPrevRegionByKey searches previous RegionInfo from regionTree
func (r *RegionsInfo) GetPrevRegionByKey(regionKey []byte) *RegionInfo {
	r.t.RLock()
	defer r.t.RUnlock()
	region := r.tree.searchPrev(regionKey)
	if region == nil {
		return nil
	}
	return r.getRegionLocked(region.GetID())
}

// GetRegions gets all RegionInfo from regionMap
func (r *RegionsInfo) GetRegions() []*RegionInfo {
	r.t.RLock()
	defer r.t.RUnlock()
	regions := make([]*RegionInfo, 0, len(r.regions))
	for _, item := range r.regions {
		regions = append(regions, item.RegionInfo)
	}
	return regions
}

// GetStoreRegions gets all RegionInfo with a given storeID
func (r *RegionsInfo) GetStoreRegions(storeID uint64) []*RegionInfo {
	r.st.RLock()
	defer r.st.RUnlock()
	regions := make([]*RegionInfo, 0, r.getStoreRegionCountLocked(storeID))
	if leaders, ok := r.leaders[storeID]; ok {
		regions = append(regions, leaders.scanRanges()...)
	}
	if followers, ok := r.followers[storeID]; ok {
		regions = append(regions, followers.scanRanges()...)
	}
	if learners, ok := r.learners[storeID]; ok {
		regions = append(regions, learners.scanRanges()...)
	}
	// no need to consider witness, as it is already included in leaders, followers and learners
	return regions
}

// GetStoreLeaderRegionSize get total size of store's leader regions
func (r *RegionsInfo) GetStoreLeaderRegionSize(storeID uint64) int64 {
	r.st.RLock()
	defer r.st.RUnlock()
	return r.leaders[storeID].TotalSize()
}

// GetStoreFollowerRegionSize get total size of store's follower regions
func (r *RegionsInfo) GetStoreFollowerRegionSize(storeID uint64) int64 {
	r.st.RLock()
	defer r.st.RUnlock()
	return r.followers[storeID].TotalSize()
}

// GetStoreLearnerRegionSize get total size of store's learner regions
func (r *RegionsInfo) GetStoreLearnerRegionSize(storeID uint64) int64 {
	r.st.RLock()
	defer r.st.RUnlock()
	return r.learners[storeID].TotalSize()
}

// GetStoreRegionSize get total size of store's regions
func (r *RegionsInfo) GetStoreRegionSize(storeID uint64) int64 {
	r.st.RLock()
	defer r.st.RUnlock()
	return r.getStoreRegionSizeLocked(storeID)
}

// getStoreRegionSizeLocked get total size of store's regions
func (r *RegionsInfo) getStoreRegionSizeLocked(storeID uint64) int64 {
	return r.leaders[storeID].TotalSize() + r.followers[storeID].TotalSize() + r.learners[storeID].TotalSize()
}

// GetStoreLeaderWriteRate get total write rate of store's leaders
func (r *RegionsInfo) GetStoreLeaderWriteRate(storeID uint64) (bytesRate, keysRate float64) {
	r.st.RLock()
	defer r.st.RUnlock()
	return r.leaders[storeID].TotalWriteRate()
}

// GetStoreWriteRate get total write rate of store's regions
func (r *RegionsInfo) GetStoreWriteRate(storeID uint64) (bytesRate, keysRate float64) {
	r.st.RLock()
	defer r.st.RUnlock()
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

// GetClusterNotFromStorageRegionsCnt gets the total count of regions that not loaded from storage anymore
func (r *RegionsInfo) GetClusterNotFromStorageRegionsCnt() int {
	r.t.RLock()
	defer r.t.RUnlock()
	return r.tree.notFromStorageRegionsCnt
}

// GetMetaRegions gets a set of metapb.Region from regionMap
func (r *RegionsInfo) GetMetaRegions() []*metapb.Region {
	r.t.RLock()
	defer r.t.RUnlock()
	regions := make([]*metapb.Region, 0, len(r.regions))
	for _, item := range r.regions {
		regions = append(regions, typeutil.DeepClone(item.meta, RegionFactory))
	}
	return regions
}

// GetStoreStats returns the store stats.
func (r *RegionsInfo) GetStoreStats(storeID uint64) (leader, region, witness, learner, pending int, leaderSize, regionSize int64) {
	r.st.RLock()
	defer r.st.RUnlock()
	return r.leaders[storeID].length(), r.getStoreRegionCountLocked(storeID), r.witnesses[storeID].length(),
		r.learners[storeID].length(), r.pendingPeers[storeID].length(), r.leaders[storeID].TotalSize(), r.getStoreRegionSizeLocked(storeID)
}

// GetRegionCount gets the total count of RegionInfo of regionMap
func (r *RegionsInfo) GetRegionCount() int {
	r.t.RLock()
	defer r.t.RUnlock()
	return len(r.regions)
}

// GetStoreRegionCount gets the total count of a store's leader, follower and learner RegionInfo by storeID
func (r *RegionsInfo) GetStoreRegionCount(storeID uint64) int {
	r.st.RLock()
	defer r.st.RUnlock()
	return r.getStoreRegionCountLocked(storeID)
}

// GetStoreRegionCount gets the total count of a store's leader, follower and learner RegionInfo by storeID
func (r *RegionsInfo) getStoreRegionCountLocked(storeID uint64) int {
	return r.leaders[storeID].length() + r.followers[storeID].length() + r.learners[storeID].length()
}

// GetStorePendingPeerCount gets the total count of a store's region that includes pending peer
func (r *RegionsInfo) GetStorePendingPeerCount(storeID uint64) int {
	r.st.RLock()
	defer r.st.RUnlock()
	return r.pendingPeers[storeID].length()
}

// GetStoreLeaderCount get the total count of a store's leader RegionInfo
func (r *RegionsInfo) GetStoreLeaderCount(storeID uint64) int {
	r.st.RLock()
	defer r.st.RUnlock()
	return r.leaders[storeID].length()
}

// GetStoreFollowerCount get the total count of a store's follower RegionInfo
func (r *RegionsInfo) GetStoreFollowerCount(storeID uint64) int {
	r.st.RLock()
	defer r.st.RUnlock()
	return r.followers[storeID].length()
}

// GetStoreLearnerCount get the total count of a store's learner RegionInfo
func (r *RegionsInfo) GetStoreLearnerCount(storeID uint64) int {
	r.st.RLock()
	defer r.st.RUnlock()
	return r.learners[storeID].length()
}

// GetStoreWitnessCount get the total count of a store's witness RegionInfo
func (r *RegionsInfo) GetStoreWitnessCount(storeID uint64) int {
	r.st.RLock()
	defer r.st.RUnlock()
	return r.witnesses[storeID].length()
}

// RandPendingRegion randomly gets a store's region with a pending peer.
func (r *RegionsInfo) RandPendingRegion(storeID uint64, ranges []KeyRange) *RegionInfo {
	r.st.RLock()
	defer r.st.RUnlock()
	return r.pendingPeers[storeID].RandomRegion(ranges)
}

// RandPendingRegions randomly gets a store's n regions with a pending peer.
func (r *RegionsInfo) RandPendingRegions(storeID uint64, ranges []KeyRange) []*RegionInfo {
	r.st.RLock()
	defer r.st.RUnlock()
	return r.pendingPeers[storeID].RandomRegions(randomRegionMaxRetry, ranges)
}

// RandLeaderRegion randomly gets a store's leader region.
func (r *RegionsInfo) RandLeaderRegion(storeID uint64, ranges []KeyRange) *RegionInfo {
	r.st.RLock()
	defer r.st.RUnlock()
	return r.leaders[storeID].RandomRegion(ranges)
}

// RandLeaderRegions randomly gets a store's n leader regions.
func (r *RegionsInfo) RandLeaderRegions(storeID uint64, ranges []KeyRange) []*RegionInfo {
	r.st.RLock()
	defer r.st.RUnlock()
	return r.leaders[storeID].RandomRegions(randomRegionMaxRetry, ranges)
}

// RandFollowerRegion randomly gets a store's follower region.
func (r *RegionsInfo) RandFollowerRegion(storeID uint64, ranges []KeyRange) *RegionInfo {
	r.st.RLock()
	defer r.st.RUnlock()
	return r.followers[storeID].RandomRegion(ranges)
}

// RandFollowerRegions randomly gets a store's n follower regions.
func (r *RegionsInfo) RandFollowerRegions(storeID uint64, ranges []KeyRange) []*RegionInfo {
	r.st.RLock()
	defer r.st.RUnlock()
	return r.followers[storeID].RandomRegions(randomRegionMaxRetry, ranges)
}

// RandLearnerRegion randomly gets a store's learner region.
func (r *RegionsInfo) RandLearnerRegion(storeID uint64, ranges []KeyRange) *RegionInfo {
	r.st.RLock()
	defer r.st.RUnlock()
	return r.learners[storeID].RandomRegion(ranges)
}

// RandLearnerRegions randomly gets a store's n learner regions.
func (r *RegionsInfo) RandLearnerRegions(storeID uint64, ranges []KeyRange) []*RegionInfo {
	r.st.RLock()
	defer r.st.RUnlock()
	return r.learners[storeID].RandomRegions(randomRegionMaxRetry, ranges)
}

// GetLeader returns leader RegionInfo by storeID and regionID (now only used in test)
func (r *RegionsInfo) GetLeader(storeID uint64, region *RegionInfo) *RegionInfo {
	r.st.RLock()
	defer r.st.RUnlock()
	if leaders, ok := r.leaders[storeID]; ok {
		return leaders.find(&regionItem{RegionInfo: region}).RegionInfo
	}
	return nil
}

// GetFollower returns follower RegionInfo by storeID and regionID (now only used in test)
func (r *RegionsInfo) GetFollower(storeID uint64, region *RegionInfo) *RegionInfo {
	r.st.RLock()
	defer r.st.RUnlock()
	if followers, ok := r.followers[storeID]; ok {
		return followers.find(&regionItem{RegionInfo: region}).RegionInfo
	}
	return nil
}

// GetReadQueryNum returns read query num from this region
func (r *RegionInfo) GetReadQueryNum() uint64 {
	return GetReadQueryNum(r.queryStats)
}

// GetWriteQueryNum returns write query num from this region
func (r *RegionInfo) GetWriteQueryNum() uint64 {
	return GetWriteQueryNum(r.queryStats)
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

// GetRangeCount returns the number of regions that overlap with the range [startKey, endKey).
func (r *RegionsInfo) GetRangeCount(startKey, endKey []byte) int {
	r.t.RLock()
	defer r.t.RUnlock()
	start := &regionItem{&RegionInfo{meta: &metapb.Region{StartKey: startKey}}}
	end := &regionItem{&RegionInfo{meta: &metapb.Region{StartKey: endKey}}}
	// it returns 0 if startKey is nil.
	_, startIndex := r.tree.tree.GetWithIndex(start)
	var endIndex int
	var item *regionItem
	// it should return the length of the tree if endKey is nil.
	if len(endKey) == 0 {
		endIndex = r.tree.tree.Len() - 1
	} else {
		item, endIndex = r.tree.tree.GetWithIndex(end)
		// it should return the endIndex - 1 if the endKey is the startKey of a region.
		if item != nil && bytes.Equal(item.GetStartKey(), endKey) {
			endIndex--
		}
	}
	return endIndex - startIndex + 1
}

// ScanRange scans regions intersecting [start key, end key), returns at most
// `limit` regions. limit <= 0 means no limit.
func (r *RegionsInfo) ScanRange(startKey, endKey []byte, limit int) []*RegionInfo {
	r.t.RLock()
	defer r.t.RUnlock()
	var res []*RegionInfo
	r.tree.scanRange(startKey, func(region *RegionInfo) bool {
		if len(endKey) > 0 && bytes.Compare(region.GetStartKey(), endKey) >= 0 {
			return false
		}
		if limit > 0 && len(res) >= limit {
			return false
		}
		res = append(res, region)
		return true
	})
	return res
}

// ScanRangeWithIterator scans from the first region containing or behind start key,
// until iterator returns false.
func (r *RegionsInfo) ScanRangeWithIterator(startKey []byte, iterator func(region *RegionInfo) bool) {
	r.t.RLock()
	defer r.t.RUnlock()
	r.tree.scanRange(startKey, iterator)
}

// GetRegionSizeByRange scans regions intersecting [start key, end key), returns the total region size of this range.
func (r *RegionsInfo) GetRegionSizeByRange(startKey, endKey []byte) int64 {
	var size int64
	for {
		r.t.RLock()
		var cnt int
		r.tree.scanRange(startKey, func(region *RegionInfo) bool {
			if len(endKey) > 0 && bytes.Compare(region.GetStartKey(), endKey) >= 0 {
				return false
			}
			if cnt >= scanRegionLimit {
				return false
			}
			cnt++
			startKey = region.GetEndKey()
			size += region.GetApproximateSize()
			return true
		})
		r.t.RUnlock()
		if cnt == 0 {
			break
		}
		if len(startKey) == 0 {
			break
		}
	}

	return size
}

// GetAdjacentRegions returns region's info that is adjacent with specific region
func (r *RegionsInfo) GetAdjacentRegions(region *RegionInfo) (*RegionInfo, *RegionInfo) {
	r.t.RLock()
	defer r.t.RUnlock()
	p, n := r.tree.getAdjacentRegions(region)
	var prev, next *RegionInfo
	// check key to avoid key range hole
	if p != nil && bytes.Equal(p.GetEndKey(), region.GetStartKey()) {
		prev = r.getRegionLocked(p.GetID())
	}
	if n != nil && bytes.Equal(region.GetEndKey(), n.GetStartKey()) {
		next = r.getRegionLocked(n.GetID())
	}
	return prev, next
}

// GetRangeHoles returns all range holes, i.e the key ranges without any region info.
func (r *RegionsInfo) GetRangeHoles() [][]string {
	r.t.RLock()
	defer r.t.RUnlock()
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
	// If the last end key is not empty, it means there is a range hole at the end.
	if len(lastEndKey) > 0 {
		rangeHoles = append(rangeHoles, []string{HexRegionKeyStr(lastEndKey), ""})
	}
	return rangeHoles
}

// GetAverageRegionSize returns the average region approximate size.
func (r *RegionsInfo) GetAverageRegionSize() int64 {
	r.t.RLock()
	defer r.t.RUnlock()
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
	return string(HexRegionKey(key))
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
	meta := typeutil.DeepClone(h.Region, RegionFactory)
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
		meta := typeutil.DeepClone(r, RegionFactory)
		meta.StartKey = HexRegionKey(meta.StartKey)
		meta.EndKey = HexRegionKey(meta.EndKey)
		b.WriteString(proto.CompactTextString(meta))
	}
	return strings.TrimSpace(b.String())
}

// NeedTransferWitnessLeader is used to judge if the region's leader is a witness
func NeedTransferWitnessLeader(region *RegionInfo) bool {
	if region == nil || region.GetLeader() == nil {
		return false
	}
	return region.GetLeader().IsWitness
}
