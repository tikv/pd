// Copyright 2022 TiKV Project Authors.
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

package handlers

import (
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/replication_modepb"
	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
)

const (
	AliveStatusName        = "Alive"
	DisconnectedStatusName = "Disconnected"
	DownStatusName         = "Down"
)

// StoresInfo records stores' info.
type StoresInfo struct {
	Count  int          `json:"count"`
	Stores []*StoreInfo `json:"stores"`
}

// MetaStore contains meta information about a store.
type MetaStore struct {
	ID          uint64               `json:"id,omitempty"`
	Address     string               `json:"address,omitempty"`
	Labels      []*metapb.StoreLabel `json:"labels,omitempty"`
	Version     string               `json:"version,omitempty"`
	PeerAddress string               `json:"peer_address,omitempty"`
	// Status address provides the HTTP service for external components
	StatusAddress string `json:"status_address,omitempty"`
	GitHash       string `json:"git_hash,omitempty"`
	// The start timestamp of the current store
	StartTimestamp int64  `json:"start_timestamp,omitempty"`
	DeployPath     string `json:"deploy_path,omitempty"`
	// The last heartbeat timestamp of the store.
	LastHeartbeat int64 `json:"last_heartbeat,omitempty"`
	// If the store is physically destroyed, which means it can never up again.
	PhysicallyDestroyed bool             `json:"physically_destroyed,omitempty"`
	NodeState           metapb.NodeState `json:"node_state"`
	HeartbeatStatus     string           `json:"heartbeat_status"`
}

func NewMetaStore(store *metapb.Store, heartbeatStatus string) *MetaStore {
	metaStore := &MetaStore{HeartbeatStatus: heartbeatStatus}
	metaStore.ID = store.GetId()
	metaStore.Address = store.GetAddress()
	metaStore.Labels = store.GetLabels()
	metaStore.Version = store.GetVersion()
	metaStore.PeerAddress = store.GetPeerAddress()
	metaStore.StatusAddress = store.GetStatusAddress()
	metaStore.GitHash = store.GetGitHash()
	metaStore.StartTimestamp = store.GetStartTimestamp()
	metaStore.DeployPath = store.GetDeployPath()
	metaStore.LastHeartbeat = store.GetLastHeartbeat()
	metaStore.PhysicallyDestroyed = store.GetPhysicallyDestroyed()
	metaStore.NodeState = store.GetNodeState()
	return metaStore
}

// StoreStatus contains status about a store.
type StoreStatus struct {
	Capacity        typeutil.ByteSize  `json:"capacity"`
	Available       typeutil.ByteSize  `json:"available"`
	UsedSize        typeutil.ByteSize  `json:"used_size"`
	LeaderCount     int                `json:"leader_count"`
	LeaderWeight    float64            `json:"leader_weight"`
	LeaderScore     float64            `json:"leader_score"`
	LeaderSize      int64              `json:"leader_size"`
	RegionCount     int                `json:"region_count"`
	RegionWeight    float64            `json:"region_weight"`
	RegionScore     float64            `json:"region_score"`
	RegionSize      int64              `json:"region_size"`
	SlowScore       uint64             `json:"slow_score"`
	StartTS         *time.Time         `json:"start_ts,omitempty"`
	LastHeartbeatTS *time.Time         `json:"last_heartbeat_ts,omitempty"`
	Uptime          *typeutil.Duration `json:"uptime,omitempty"`
}

// StoreInfo contains information about a store.
type StoreInfo struct {
	Store  *MetaStore   `json:"store"`
	Status *StoreStatus `json:"status"`
}

func newStoreInfo(cfg *config.ScheduleConfig, store *core.StoreInfo) *StoreInfo {
	if store == nil {
		return nil
	}
	s := &StoreInfo{
		Store: NewMetaStore(store.GetMeta(), AliveStatusName),
		Status: &StoreStatus{
			Capacity:     typeutil.ByteSize(store.GetCapacity()),
			Available:    typeutil.ByteSize(store.GetAvailable()),
			UsedSize:     typeutil.ByteSize(store.GetUsedSize()),
			LeaderCount:  store.GetLeaderCount(),
			LeaderWeight: store.GetLeaderWeight(),
			LeaderScore:  store.LeaderScore(core.StringToSchedulePolicy(cfg.LeaderSchedulePolicy), 0),
			LeaderSize:   store.GetLeaderSize(),
			RegionCount:  store.GetRegionCount(),
			RegionWeight: store.GetRegionWeight(),
			RegionScore:  store.RegionScore(cfg.RegionScoreFormulaVersion, cfg.HighSpaceRatio, cfg.LowSpaceRatio, 0),
			RegionSize:   store.GetRegionSize(),
			SlowScore:    store.GetSlowScore(),
		},
	}

	if store.GetStoreStats() != nil {
		startTS := store.GetStartTime()
		s.Status.StartTS = &startTS
	}
	if lastHeartbeat := store.GetLastHeartbeatTS(); !lastHeartbeat.IsZero() {
		s.Status.LastHeartbeatTS = &lastHeartbeat
	}
	if upTime := store.GetUptime(); upTime > 0 {
		duration := typeutil.NewDuration(upTime)
		s.Status.Uptime = &duration
	}

	if store.DownTime() > cfg.MaxStoreDownTime.Duration {
		s.Store.HeartbeatStatus = DownStatusName
	} else if store.IsDisconnected() {
		s.Store.HeartbeatStatus = DisconnectedStatusName
	}

	return s
}

// RegionsInfo records regions' info.
type RegionsInfo struct {
	Count   int           `json:"count"`
	Regions []*RegionInfo `json:"regions"`
}

// RegionInfo records detail region info for api usage.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type RegionInfo struct {
	ID          uint64              `json:"id"`
	StartKey    string              `json:"start_key"`
	EndKey      string              `json:"end_key"`
	RegionEpoch *metapb.RegionEpoch `json:"epoch,omitempty"`
	Peers       []MetaPeer          `json:"peers,omitempty"`

	Leader          MetaPeer      `json:"leader,omitempty"`
	DownPeers       []PDPeerStats `json:"down_peers,omitempty"`
	PendingPeers    []MetaPeer    `json:"pending_peers,omitempty"`
	WrittenBytes    uint64        `json:"written_bytes"`
	ReadBytes       uint64        `json:"read_bytes"`
	WrittenKeys     uint64        `json:"written_keys"`
	ReadKeys        uint64        `json:"read_keys"`
	ApproximateSize uint64        `json:"approximate_size"`
	ApproximateKeys uint64        `json:"approximate_keys"`

	ReplicationStatus *ReplicationStatus `json:"replication_status,omitempty"`
}

// MetaPeer is converted from *metapb.Peer.
type MetaPeer struct {
	ID      uint64 `json:"id"`
	StoreID uint64 `json:"store_id"`
	// RoleName is `Role.String()`.
	// Since Role is serialized as int by json by default,
	// introducing it will make the output of pd-ctl easier to identify Role.
	RoleName string `json:"role_name"`
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

// PDPeerStats is api compatible with *pdpb.PeerStats.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type PDPeerStats struct {
	*pdpb.PeerStats
	Peer MetaPeer `json:"peer"`
}

func fromPeer(peer *metapb.Peer) MetaPeer {
	return MetaPeer{
		ID:       peer.GetId(),
		StoreID:  peer.GetStoreId(),
		RoleName: peer.GetRole().String(),
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

func newRegionInfo(r *core.RegionInfo) *RegionInfo {
	if r == nil {
		return nil
	}
	return &RegionInfo{
		ID:                r.GetID(),
		StartKey:          core.HexRegionKeyStr(r.GetStartKey()),
		EndKey:            core.HexRegionKeyStr(r.GetEndKey()),
		RegionEpoch:       r.GetRegionEpoch(),
		Peers:             fromPeerSlice(r.GetPeers()),
		Leader:            fromPeer(r.GetLeader()),
		DownPeers:         fromPeerStatsSlice(r.GetDownPeers()),
		PendingPeers:      fromPeerSlice(r.GetPendingPeers()),
		WrittenBytes:      r.GetBytesWritten(),
		WrittenKeys:       r.GetKeysWritten(),
		ReadBytes:         r.GetBytesRead(),
		ReadKeys:          r.GetKeysRead(),
		ApproximateSize:   uint64(r.GetApproximateSize()),
		ApproximateKeys:   uint64(r.GetApproximateKeys()),
		ReplicationStatus: fromPBReplicationStatus(r.GetReplicationStatus()),
	}
}
