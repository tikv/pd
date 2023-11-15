// Copyright 2023 TiKV Project Authors.
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

package http

import "time"

// NOTICE: the structures below are copied from the PD API definitions.
// Please make sure the consistency if any change happens to the PD API.

// RegionInfo stores the information of one region.
type RegionInfo struct {
	ID              int64            `json:"id"`
	StartKey        string           `json:"start_key"`
	EndKey          string           `json:"end_key"`
	Epoch           RegionEpoch      `json:"epoch"`
	Peers           []RegionPeer     `json:"peers"`
	Leader          RegionPeer       `json:"leader"`
	DownPeers       []RegionPeerStat `json:"down_peers"`
	PendingPeers    []RegionPeer     `json:"pending_peers"`
	WrittenBytes    uint64           `json:"written_bytes"`
	ReadBytes       uint64           `json:"read_bytes"`
	ApproximateSize int64            `json:"approximate_size"`
	ApproximateKeys int64            `json:"approximate_keys"`

	ReplicationStatus *ReplicationStatus `json:"replication_status,omitempty"`
}

// GetStartKey gets the start key of the region.
func (r *RegionInfo) GetStartKey() string { return r.StartKey }

// GetEndKey gets the end key of the region.
func (r *RegionInfo) GetEndKey() string { return r.EndKey }

// RegionEpoch stores the information about its epoch.
type RegionEpoch struct {
	ConfVer int64 `json:"conf_ver"`
	Version int64 `json:"version"`
}

// RegionPeer stores information of one peer.
type RegionPeer struct {
	ID        int64 `json:"id"`
	StoreID   int64 `json:"store_id"`
	IsLearner bool  `json:"is_learner"`
}

// RegionPeerStat stores one field `DownSec` which indicates how long it's down than `RegionPeer`.
type RegionPeerStat struct {
	Peer    RegionPeer `json:"peer"`
	DownSec int64      `json:"down_seconds"`
}

// ReplicationStatus represents the replication mode status of the region.
type ReplicationStatus struct {
	State   string `json:"state"`
	StateID int64  `json:"state_id"`
}

// RegionsInfo stores the information of regions.
type RegionsInfo struct {
	Count   int64        `json:"count"`
	Regions []RegionInfo `json:"regions"`
}

// Merge merges two RegionsInfo together and returns a new one.
func (ri *RegionsInfo) Merge(other *RegionsInfo) *RegionsInfo {
	newRegionsInfo := &RegionsInfo{
		Regions: make([]RegionInfo, 0, ri.Count+other.Count),
	}
	m := make(map[int64]RegionInfo, ri.Count+other.Count)
	for _, region := range ri.Regions {
		m[region.ID] = region
	}
	for _, region := range other.Regions {
		m[region.ID] = region
	}
	for _, region := range m {
		newRegionsInfo.Regions = append(newRegionsInfo.Regions, region)
	}
	newRegionsInfo.Count = int64(len(newRegionsInfo.Regions))
	return newRegionsInfo
}

// StoreHotPeersInfos is used to get human-readable description for hot regions.
type StoreHotPeersInfos struct {
	AsPeer   StoreHotPeersStat `json:"as_peer"`
	AsLeader StoreHotPeersStat `json:"as_leader"`
}

// StoreHotPeersStat is used to record the hot region statistics group by store.
type StoreHotPeersStat map[uint64]*HotPeersStat

// HotPeersStat records all hot regions statistics
type HotPeersStat struct {
	StoreByteRate  float64           `json:"store_bytes"`
	StoreKeyRate   float64           `json:"store_keys"`
	StoreQueryRate float64           `json:"store_query"`
	TotalBytesRate float64           `json:"total_flow_bytes"`
	TotalKeysRate  float64           `json:"total_flow_keys"`
	TotalQueryRate float64           `json:"total_flow_query"`
	Count          int               `json:"regions_count"`
	Stats          []HotPeerStatShow `json:"statistics"`
}

// HotPeerStatShow records the hot region statistics for output
type HotPeerStatShow struct {
	StoreID        uint64    `json:"store_id"`
	Stores         []uint64  `json:"stores"`
	IsLeader       bool      `json:"is_leader"`
	IsLearner      bool      `json:"is_learner"`
	RegionID       uint64    `json:"region_id"`
	HotDegree      int       `json:"hot_degree"`
	ByteRate       float64   `json:"flow_bytes"`
	KeyRate        float64   `json:"flow_keys"`
	QueryRate      float64   `json:"flow_query"`
	AntiCount      int       `json:"anti_count"`
	LastUpdateTime time.Time `json:"last_update_time,omitempty"`
}

// StoresInfo represents the information of all TiKV/TiFlash stores.
type StoresInfo struct {
	Count  int         `json:"count"`
	Stores []StoreInfo `json:"stores"`
}

// StoreInfo represents the information of one TiKV/TiFlash store.
type StoreInfo struct {
	Store  MetaStore   `json:"store"`
	Status StoreStatus `json:"status"`
}

// MetaStore represents the meta information of one store.
type MetaStore struct {
	ID             int64        `json:"id"`
	Address        string       `json:"address"`
	State          int64        `json:"state"`
	StateName      string       `json:"state_name"`
	Version        string       `json:"version"`
	Labels         []StoreLabel `json:"labels"`
	StatusAddress  string       `json:"status_address"`
	GitHash        string       `json:"git_hash"`
	StartTimestamp int64        `json:"start_timestamp"`
}

// StoreLabel stores the information of one store label.
type StoreLabel struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// StoreStatus stores the detail information of one store.
type StoreStatus struct {
	Capacity        string    `json:"capacity"`
	Available       string    `json:"available"`
	LeaderCount     int64     `json:"leader_count"`
	LeaderWeight    float64   `json:"leader_weight"`
	LeaderScore     float64   `json:"leader_score"`
	LeaderSize      int64     `json:"leader_size"`
	RegionCount     int64     `json:"region_count"`
	RegionWeight    float64   `json:"region_weight"`
	RegionScore     float64   `json:"region_score"`
	RegionSize      int64     `json:"region_size"`
	StartTS         time.Time `json:"start_ts"`
	LastHeartbeatTS time.Time `json:"last_heartbeat_ts"`
	Uptime          string    `json:"uptime"`
}

// RegionStats stores the statistics of regions.
type RegionStats struct {
	Count            int            `json:"count"`
	EmptyCount       int            `json:"empty_count"`
	StorageSize      int64          `json:"storage_size"`
	StorageKeys      int64          `json:"storage_keys"`
	StoreLeaderCount map[uint64]int `json:"store_leader_count"`
	StorePeerCount   map[uint64]int `json:"store_peer_count"`
}

// PeerRoleType is the expected peer type of the placement rule.
type PeerRoleType string

const (
	// Voter can either match a leader peer or follower peer
	Voter PeerRoleType = "voter"
	// Leader matches a leader.
	Leader PeerRoleType = "leader"
	// Follower matches a follower.
	Follower PeerRoleType = "follower"
	// Learner matches a learner.
	Learner PeerRoleType = "learner"
)

// LabelConstraint is used to filter store when trying to place peer of a region.
type LabelConstraint struct {
	Key    string            `json:"key,omitempty"`
	Op     LabelConstraintOp `json:"op,omitempty"`
	Values []string          `json:"values,omitempty"`
}

// LabelConstraintOp defines how a LabelConstraint matches a store. It can be one of
// 'in', 'notIn', 'exists', or 'notExists'.
type LabelConstraintOp string

const (
	// In restricts the store label value should in the value list.
	// If label does not exist, `in` is always false.
	In LabelConstraintOp = "in"
	// NotIn restricts the store label value should not in the value list.
	// If label does not exist, `notIn` is always true.
	NotIn LabelConstraintOp = "notIn"
	// Exists restricts the store should have the label.
	Exists LabelConstraintOp = "exists"
	// NotExists restricts the store should not have the label.
	NotExists LabelConstraintOp = "notExists"
)

// Rule is the placement rule that can be checked against a region. When
// applying rules (apply means schedule regions to match selected rules), the
// apply order is defined by the tuple [GroupIndex, GroupID, Index, ID].
type Rule struct {
	GroupID        string          `json:"group_id"`
	ID             string          `json:"id"`
	Index          int             `json:"index,omitempty"`
	Override       bool            `json:"override,omitempty"`
	StartKeyHex    string          `json:"start_key"`
	EndKeyHex      string          `json:"end_key"`
	Role           PeerRoleType    `json:"role"`
	Count          int             `json:"count"`
	Constraints    LabelConstraint `json:"label_constraints,omitempty"`
	LocationLabels []string        `json:"location_labels,omitempty"`
}
