// Copyright 2018 TiKV Project Authors.
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
	"math"
	"math/rand"
	"sort"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/replication_modepb"
)

// RegionCreateOption used to create region.
type RegionCreateOption func(region *RegionInfo)

// WithDownPeers sets the down peers for the region.
func WithDownPeers(downPeers []*pdpb.PeerStats) RegionCreateOption {
	return func(region *RegionInfo) {
		region.downPeers = append(downPeers[:0:0], downPeers...)
		sort.Sort(peerStatsSlice(region.downPeers))
	}
}

// WithFlowRoundByDigit set the digit, which use to round to the nearest number
func WithFlowRoundByDigit(digit int) RegionCreateOption {
	flowRoundDivisor := uint64(math.Pow10(digit))
	return func(region *RegionInfo) {
		region.flowRoundDivisor = flowRoundDivisor
	}
}

// WithPendingPeers sets the pending peers for the region.
func WithPendingPeers(pendingPeers []*metapb.Peer) RegionCreateOption {
	return func(region *RegionInfo) {
		region.pendingPeers = append(pendingPeers[:0:0], pendingPeers...)
		sort.Sort(peerSlice(region.pendingPeers))
	}
}

// WithWitness sets the witness for the region.
func WithWitness(peerID uint64) RegionCreateOption {
	return func(region *RegionInfo) {
		for _, p := range region.GetPeers() {
			if p.GetId() == peerID {
				p.IsWitness = true
			}
		}
	}
}

// WithWitnesses sets the witnesses for the region.
func WithWitnesses(witnesses []*metapb.Peer) RegionCreateOption {
	return func(region *RegionInfo) {
		peers := region.meta.GetPeers()
		for i := range peers {
			for _, l := range witnesses {
				if peers[i].GetId() == l.GetId() {
					peers[i].IsWitness = true
					break
				}
			}
		}
	}
}

// WithLearners sets the learners for the region.
func WithLearners(learners []*metapb.Peer) RegionCreateOption {
	return func(region *RegionInfo) {
		peers := region.meta.GetPeers()
		for i := range peers {
			for _, l := range learners {
				if peers[i].GetId() == l.GetId() {
					peers[i] = &metapb.Peer{Id: l.GetId(), StoreId: l.GetStoreId(), Role: metapb.PeerRole_Learner}
					break
				}
			}
		}
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
		region.meta.EndKey = key
	}
}

// WithNewRegionID sets new id for the region.
func WithNewRegionID(id uint64) RegionCreateOption {
	return func(region *RegionInfo) {
		region.meta.Id = id
	}
}

// WithNewPeerIDs sets new ids for peers.
func WithNewPeerIDs(peerIDs ...uint64) RegionCreateOption {
	return func(region *RegionInfo) {
		if len(peerIDs) != len(region.meta.GetPeers()) {
			return
		}
		for i, p := range region.meta.GetPeers() {
			p.Id = peerIDs[i]
		}
	}
}

// WithIncVersion increases the version of the region.
func WithIncVersion() RegionCreateOption {
	return func(region *RegionInfo) {
		e := region.meta.GetRegionEpoch()
		if e != nil {
			e.Version++
		} else {
			region.meta.RegionEpoch = &metapb.RegionEpoch{
				ConfVer: 0,
				Version: 1,
			}
		}
	}
}

// WithDecVersion decreases the version of the region.
func WithDecVersion() RegionCreateOption {
	return func(region *RegionInfo) {
		e := region.meta.GetRegionEpoch()
		if e != nil {
			e.Version--
		}
	}
}

// WithIncConfVer increases the config version of the region.
func WithIncConfVer() RegionCreateOption {
	return func(region *RegionInfo) {
		e := region.meta.GetRegionEpoch()
		if e != nil {
			e.ConfVer++
		} else {
			region.meta.RegionEpoch = &metapb.RegionEpoch{
				ConfVer: 1,
				Version: 0,
			}
		}
	}
}

// WithDecConfVer decreases the config version of the region.
func WithDecConfVer() RegionCreateOption {
	return func(region *RegionInfo) {
		e := region.meta.GetRegionEpoch()
		if e != nil {
			e.ConfVer--
		}
	}
}

// WithFlashback set region flashback states.
func WithFlashback(isInFlashback bool, flashbackTS uint64) RegionCreateOption {
	return func(region *RegionInfo) {
		region.meta.FlashbackStartTs = flashbackTS
		region.meta.IsInFlashback = isInFlashback
	}
}

// SetCPUUsage sets the CPU usage of the region.
func SetCPUUsage(v uint64) RegionCreateOption {
	return func(region *RegionInfo) {
		region.cpuUsage = v
	}
}

// SetWrittenBytes sets the written bytes for the region.
func SetWrittenBytes(v uint64) RegionCreateOption {
	return func(region *RegionInfo) {
		region.writtenBytes = v
	}
}

// SetWrittenKeys sets the written keys for the region.
func SetWrittenKeys(v uint64) RegionCreateOption {
	return func(region *RegionInfo) {
		region.writtenKeys = v
	}
}

// WithRemoveStorePeer removes the specified peer for the region.
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

// SetBuckets sets the buckets for the region, only use test.
func SetBuckets(buckets *metapb.Buckets) RegionCreateOption {
	return func(region *RegionInfo) {
		region.UpdateBuckets(buckets, region.GetBuckets())
	}
}

// SetReadBytes sets the read bytes for the region.
func SetReadBytes(v uint64) RegionCreateOption {
	return func(region *RegionInfo) {
		region.readBytes = v
	}
}

// SetReadKeys sets the read keys for the region.
func SetReadKeys(v uint64) RegionCreateOption {
	return func(region *RegionInfo) {
		region.readKeys = v
	}
}

// SetReadQuery sets the read query for the region, only used for unit test.
func SetReadQuery(v uint64) RegionCreateOption {
	return func(region *RegionInfo) {
		resetReadQuery(region.queryStats)
		region.queryStats = mergeQueryStat(region.queryStats, RandomKindReadQuery(v))
	}
}

// SetWrittenQuery sets the write query for the region, only used for unit test.
func SetWrittenQuery(v uint64) RegionCreateOption {
	return func(region *RegionInfo) {
		resetWriteQuery(region.queryStats)
		region.queryStats = mergeQueryStat(region.queryStats, RandomKindWriteQuery(v))
	}
}

// SetQueryStats sets the query stats for the region, it will cover previous statistic.
// This func is only used for unit test.
// It will cover previous statistic.
func SetQueryStats(v *pdpb.QueryStats) RegionCreateOption {
	return func(region *RegionInfo) {
		region.queryStats = v
	}
}

// AddQueryStats sets the query stats for the region, it will preserve previous statistic.
// This func is only used for test and simulator.
// It will preserve previous statistic.
func AddQueryStats(v *pdpb.QueryStats) RegionCreateOption {
	return func(region *RegionInfo) {
		q := mergeQueryStat(region.queryStats, v)
		region.queryStats = q
	}
}

// SetApproximateSize sets the approximate size for the region.
func SetApproximateSize(v int64) RegionCreateOption {
	return func(region *RegionInfo) {
		region.approximateSize = v
	}
}

// SetApproximateKvSize sets the approximate size for the region.
func SetApproximateKvSize(v int64) RegionCreateOption {
	return func(region *RegionInfo) {
		region.approximateKvSize = v
	}
}

// SetApproximateKeys sets the approximate keys for the region.
func SetApproximateKeys(v int64) RegionCreateOption {
	return func(region *RegionInfo) {
		region.approximateKeys = v
	}
}

// SetReportInterval sets the report interval for the region.
// This func is only used for test.
func SetReportInterval(start, end uint64) RegionCreateOption {
	return func(region *RegionInfo) {
		region.interval = &pdpb.TimeInterval{StartTimestamp: start, EndTimestamp: end}
	}
}

// SetRegionConfVer sets the config version for the region.
func SetRegionConfVer(confVer uint64) RegionCreateOption {
	return func(region *RegionInfo) {
		if region.meta.RegionEpoch == nil {
			region.meta.RegionEpoch = &metapb.RegionEpoch{ConfVer: confVer, Version: 1}
		} else {
			region.meta.RegionEpoch.ConfVer = confVer
		}
	}
}

// SetRegionVersion sets the version for the region.
func SetRegionVersion(version uint64) RegionCreateOption {
	return func(region *RegionInfo) {
		if region.meta.RegionEpoch == nil {
			region.meta.RegionEpoch = &metapb.RegionEpoch{ConfVer: 1, Version: version}
		} else {
			region.meta.RegionEpoch.Version = version
		}
	}
}

// SetPeers sets the peers for the region.
func SetPeers(peers []*metapb.Peer) RegionCreateOption {
	return func(region *RegionInfo) {
		region.meta.Peers = peers
	}
}

// SetReplicationStatus sets the region's replication status.
func SetReplicationStatus(status *replication_modepb.RegionReplicationStatus) RegionCreateOption {
	return func(region *RegionInfo) {
		region.replicationStatus = status
	}
}

// WithAddPeer adds a peer for the region.
func WithAddPeer(peer *metapb.Peer) RegionCreateOption {
	return func(region *RegionInfo) {
		region.meta.Peers = append(region.meta.Peers, peer)
		if IsLearner(peer) {
			region.learners = append(region.learners, peer)
		} else {
			region.voters = append(region.voters, peer)
		}
	}
}

// WithRole changes the role.
func WithRole(peerID uint64, role metapb.PeerRole) RegionCreateOption {
	return func(region *RegionInfo) {
		for _, p := range region.GetPeers() {
			if p.GetId() == peerID {
				p.Role = role
			}
		}
	}
}

// WithReplacePeerStore replaces a peer's storeID with another ID.
func WithReplacePeerStore(oldStoreID, newStoreID uint64) RegionCreateOption {
	return func(region *RegionInfo) {
		for _, p := range region.GetPeers() {
			if p.GetStoreId() == oldStoreID {
				p.StoreId = newStoreID
			}
		}
	}
}

// WithInterval sets the interval
func WithInterval(interval *pdpb.TimeInterval) RegionCreateOption {
	return func(region *RegionInfo) {
		region.interval = interval
	}
}

// SetSource sets the region info's come from.
func SetSource(source RegionSource) RegionCreateOption {
	return func(region *RegionInfo) {
		region.source = source
	}
}

// RandomKindReadQuery returns query stat with random query kind, only used for unit test.
func RandomKindReadQuery(queryRead uint64) *pdpb.QueryStats {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	switch r.Intn(3) {
	case 0:
		return &pdpb.QueryStats{
			Coprocessor: queryRead,
		}
	case 1:
		return &pdpb.QueryStats{
			Scan: queryRead,
		}
	case 2:
		return &pdpb.QueryStats{
			Get: queryRead,
		}
	default:
		return &pdpb.QueryStats{}
	}
}

// RandomKindWriteQuery returns query stat with random query kind, only used for unit test.
func RandomKindWriteQuery(queryWrite uint64) *pdpb.QueryStats {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	switch r.Intn(7) {
	case 0:
		return &pdpb.QueryStats{
			Put: queryWrite,
		}
	case 1:
		return &pdpb.QueryStats{
			Delete: queryWrite,
		}
	case 2:
		return &pdpb.QueryStats{
			DeleteRange: queryWrite,
		}
	case 3:
		return &pdpb.QueryStats{
			AcquirePessimisticLock: queryWrite,
		}
	case 4:
		return &pdpb.QueryStats{
			Rollback: queryWrite,
		}
	case 5:
		return &pdpb.QueryStats{
			Prewrite: queryWrite,
		}
	case 6:
		return &pdpb.QueryStats{
			Commit: queryWrite,
		}
	default:
		return &pdpb.QueryStats{}
	}
}

func mergeQueryStat(q1, q2 *pdpb.QueryStats) *pdpb.QueryStats {
	if q1 == nil && q2 == nil {
		return &pdpb.QueryStats{}
	}
	if q1 == nil {
		return q2
	}
	if q2 == nil {
		return q1
	}
	q2.GC += q1.GC
	q2.Get += q1.Get
	q2.Scan += q1.Scan
	q2.Coprocessor += q1.Coprocessor
	q2.Delete += q1.Delete
	q2.DeleteRange += q1.DeleteRange
	q2.Put += q1.Put
	q2.Prewrite += q1.Prewrite
	q2.AcquirePessimisticLock += q1.AcquirePessimisticLock
	q2.Commit += q1.Commit
	q2.Rollback += q1.Rollback
	return q2
}

func resetReadQuery(q *pdpb.QueryStats) {
	if q == nil {
		return
	}
	q.Get = 0
	q.Scan = 0
	q.Coprocessor = 0
}

func resetWriteQuery(q *pdpb.QueryStats) {
	if q == nil {
		return
	}
	q.Put = 0
	q.Delete = 0
	q.DeleteRange = 0
	q.AcquirePessimisticLock = 0
	q.Rollback = 0
	q.Prewrite = 0
	q.Commit = 0
}
