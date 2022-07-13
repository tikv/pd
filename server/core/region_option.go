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
	"sort"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/replication_modepb"
)

// RegionOption is used to select region.
type RegionOption func(region *RegionInfo) bool

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
	q := RandomKindReadQuery(v)
	return SetQueryStats(q)
}

// SetWrittenQuery sets the write query for the region, only used for unit test.
func SetWrittenQuery(v uint64) RegionCreateOption {
	q := RandomKindWriteQuery(v)
	return SetQueryStats(q)
}

// SetQueryStats sets the query stats for the region.
func SetQueryStats(v *pdpb.QueryStats) RegionCreateOption {
	return func(region *RegionInfo) {
		region.QueryStats = v
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

// SetReportInterval sets the report interval for the region.
func SetReportInterval(v uint64) RegionCreateOption {
	return func(region *RegionInfo) {
		region.interval = &pdpb.TimeInterval{StartTimestamp: 0, EndTimestamp: v}
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

// WithPromoteLearner promotes the learner.
func WithPromoteLearner(peerID uint64) RegionCreateOption {
	return func(region *RegionInfo) {
		for _, p := range region.GetPeers() {
			if p.GetId() == peerID {
				p.Role = metapb.PeerRole_Voter
			}
		}
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

// SetFromHeartbeat sets if the region info comes from the region heartbeat.
func SetFromHeartbeat(fromHeartbeat bool) RegionCreateOption {
	return func(region *RegionInfo) {
		region.fromHeartbeat = fromHeartbeat
	}
}
