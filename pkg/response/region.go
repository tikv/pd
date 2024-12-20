// Copyright 2024 TiKV Project Authors.
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

package response

import (
	"context"

	"github.com/mailru/easyjson/jwriter"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/replication_modepb"

	"github.com/tikv/pd/pkg/core"
)

// MetaPeer is api compatible with *metapb.Peer.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type MetaPeer struct {
	*metapb.Peer
	// RoleName is `Role.String()`.
	// Since Role is serialized as int by json by default,
	// introducing it will make the output of pd-ctl easier to identify Role.
	RoleName string `json:"role_name"`
	// IsLearner is `Role == "Learner"`.
	// Since IsLearner was changed to Role in kvproto in 5.0, this field was introduced to ensure api compatibility.
	IsLearner bool `json:"is_learner,omitempty"`
}

func (m *MetaPeer) setDefaultIfNil() {
	if m.Peer == nil {
		m.Peer = &metapb.Peer{
			Id:        m.GetId(),
			StoreId:   m.GetStoreId(),
			Role:      m.GetRole(),
			IsWitness: m.GetIsWitness(),
		}
	}
}

// PDPeerStats is api compatible with *pdpb.PeerStats.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type PDPeerStats struct {
	*pdpb.PeerStats
	Peer MetaPeer `json:"peer"`
}

func (s *PDPeerStats) setDefaultIfNil() {
	if s.PeerStats == nil {
		s.PeerStats = &pdpb.PeerStats{
			Peer:        s.GetPeer(),
			DownSeconds: s.GetDownSeconds(),
		}
	}
	s.Peer.setDefaultIfNil()
}

func fromPeer(peer *metapb.Peer) MetaPeer {
	if peer == nil {
		return MetaPeer{}
	}
	return MetaPeer{
		Peer:      peer,
		RoleName:  peer.GetRole().String(),
		IsLearner: core.IsLearner(peer),
	}
}

func fromPeerSlice(peers []*metapb.Peer) []MetaPeer {
	if peers == nil {
		return nil
	}
	slice := make([]MetaPeer, len(peers))
	for i, peer := range peers {
		slice[i] = fromPeer(peer)
	}
	return slice
}

func fromPeerStats(peer *pdpb.PeerStats) PDPeerStats {
	return PDPeerStats{
		PeerStats: peer,
		Peer:      fromPeer(peer.Peer),
	}
}

func fromPeerStatsSlice(peers []*pdpb.PeerStats) []PDPeerStats {
	if peers == nil {
		return nil
	}
	slice := make([]PDPeerStats, len(peers))
	for i, peer := range peers {
		slice[i] = fromPeerStats(peer)
	}
	return slice
}

// RegionInfo records detail region info for api usage.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
// easyjson:json
type RegionInfo struct {
	ID          uint64              `json:"id"`
	StartKey    string              `json:"start_key"`
	EndKey      string              `json:"end_key"`
	RegionEpoch *metapb.RegionEpoch `json:"epoch,omitempty"`
	Peers       []MetaPeer          `json:"peers,omitempty"`

	Leader            MetaPeer      `json:"leader,omitempty"`
	DownPeers         []PDPeerStats `json:"down_peers,omitempty"`
	PendingPeers      []MetaPeer    `json:"pending_peers,omitempty"`
	CPUUsage          uint64        `json:"cpu_usage"`
	WrittenBytes      uint64        `json:"written_bytes"`
	ReadBytes         uint64        `json:"read_bytes"`
	WrittenKeys       uint64        `json:"written_keys"`
	ReadKeys          uint64        `json:"read_keys"`
	ApproximateSize   int64         `json:"approximate_size"`
	ApproximateKeys   int64         `json:"approximate_keys"`
	ApproximateKvSize int64         `json:"approximate_kv_size"`
	Buckets           []string      `json:"buckets,omitempty"`

	ReplicationStatus *ReplicationStatus `json:"replication_status,omitempty"`
}

// ReplicationStatus represents the replication mode status of the region.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type ReplicationStatus struct {
	State   string `json:"state"`
	StateID uint64 `json:"state_id"`
}

func fromPBReplicationStatus(s *replication_modepb.RegionReplicationStatus) *ReplicationStatus {
	if s == nil {
		return nil
	}
	return &ReplicationStatus{
		State:   s.GetState().String(),
		StateID: s.GetStateId(),
	}
}

// NewAPIRegionInfo create a new API RegionInfo.
func NewAPIRegionInfo(r *core.RegionInfo) *RegionInfo {
	return InitRegion(r, &RegionInfo{})
}

// InitRegion init a new API RegionInfo from the core.RegionInfo.
func InitRegion(r *core.RegionInfo, s *RegionInfo) *RegionInfo {
	if r == nil {
		return nil
	}

	s.ID = r.GetID()
	s.StartKey = core.HexRegionKeyStr(r.GetStartKey())
	s.EndKey = core.HexRegionKeyStr(r.GetEndKey())
	s.RegionEpoch = r.GetRegionEpoch()
	s.Peers = fromPeerSlice(r.GetPeers())
	s.Leader = fromPeer(r.GetLeader())
	s.DownPeers = fromPeerStatsSlice(r.GetDownPeers())
	s.PendingPeers = fromPeerSlice(r.GetPendingPeers())
	s.CPUUsage = r.GetCPUUsage()
	s.WrittenBytes = r.GetBytesWritten()
	s.WrittenKeys = r.GetKeysWritten()
	s.ReadBytes = r.GetBytesRead()
	s.ReadKeys = r.GetKeysRead()
	s.ApproximateSize = r.GetApproximateSize()
	s.ApproximateKeys = r.GetApproximateKeys()
	s.ApproximateKvSize = r.GetApproximateKvSize()
	s.ReplicationStatus = fromPBReplicationStatus(r.GetReplicationStatus())
	s.Buckets = nil

	keys := r.GetBuckets().GetKeys()
	if len(keys) > 0 {
		s.Buckets = make([]string, len(keys))
		for i, key := range keys {
			s.Buckets[i] = core.HexRegionKeyStr(key)
		}
	}
	return s
}

// Adjust is only used in testing, in order to compare the data from json deserialization.
func (r *RegionInfo) Adjust() {
	for _, peer := range r.DownPeers {
		// Since api.PDPeerStats uses the api.MetaPeer type variable Peer to overwrite PeerStats.Peer,
		// it needs to be restored after deserialization to be completely consistent with the original.
		peer.PeerStats.Peer = peer.Peer.Peer
	}
}

// RegionsInfo contains some regions with the detailed region info.
type RegionsInfo struct {
	Count   int          `json:"count"`
	Regions []RegionInfo `json:"regions"`
}

// Adjust is only used in testing, in order to compare the data from json deserialization.
func (s *RegionsInfo) Adjust() {
	for _, r := range s.Regions {
		r.Adjust()
	}
}

// MarshalRegionInfoJSON marshals region to bytes in `RegionInfo`'s JSON format.
// It is used to reduce the cost of JSON serialization.
func MarshalRegionInfoJSON(ctx context.Context, r *core.RegionInfo) ([]byte, error) {
	out := &jwriter.Writer{}

	region := &RegionInfo{}
	select {
	case <-ctx.Done():
		// Return early, avoid the unnecessary computation.
		// See more details in https://github.com/tikv/pd/issues/6835
		return nil, ctx.Err()
	default:
	}

	covertAPIRegionInfo(r, region, out)
	return out.Buffer.BuildBytes(), out.Error
}

// MarshalRegionsInfoJSON marshals regions to bytes in `RegionsInfo`'s JSON format.
// It is used to reduce the cost of JSON serialization.
func MarshalRegionsInfoJSON(ctx context.Context, regions []*core.RegionInfo) ([]byte, error) {
	out := &jwriter.Writer{}
	out.RawByte('{')

	out.RawString("\"count\":")
	out.Int(len(regions))

	out.RawString(",\"regions\":")
	out.RawByte('[')
	region := &RegionInfo{}
	for i, r := range regions {
		select {
		case <-ctx.Done():
			// Return early, avoid the unnecessary computation.
			// See more details in https://github.com/tikv/pd/issues/6835
			return nil, ctx.Err()
		default:
		}
		if i > 0 {
			out.RawByte(',')
		}
		covertAPIRegionInfo(r, region, out)
	}
	out.RawByte(']')

	out.RawByte('}')
	return out.Buffer.BuildBytes(), out.Error
}

func covertAPIRegionInfo(r *core.RegionInfo, region *RegionInfo, out *jwriter.Writer) {
	InitRegion(r, region)
	// EasyJSON will not check anonymous struct pointer field and will panic if the field is nil.
	// So we need to set the field to default value explicitly when the anonymous struct pointer is nil.
	region.Leader.setDefaultIfNil()
	for i := range region.Peers {
		region.Peers[i].setDefaultIfNil()
	}
	for i := range region.PendingPeers {
		region.PendingPeers[i].setDefaultIfNil()
	}
	for i := range region.DownPeers {
		region.DownPeers[i].setDefaultIfNil()
	}
	region.MarshalEasyJSON(out)
}
