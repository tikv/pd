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

import (
	"encoding/hex"
	"encoding/json"
	"net/url"
	"time"

	"github.com/pingcap/kvproto/pkg/encryptionpb"
)

// KeyRange defines a range of keys in bytes.
type KeyRange struct {
	startKey []byte
	endKey   []byte
}

// NewKeyRange creates a new key range structure with the given start key and end key bytes.
// Notice: the actual encoding of the key range is not specified here. It should be either UTF-8 or hex.
//   - UTF-8 means the key has already been encoded into a string with UTF-8 encoding, like:
//     []byte{52 56 54 53 54 99 54 99 54 102 50 48 53 55 54 102 55 50 54 99 54 52}, which will later be converted to "48656c6c6f20576f726c64"
//     by using `string()` method.
//   - Hex means the key is just a raw hex bytes without encoding to a UTF-8 string, like:
//     []byte{72, 101, 108, 108, 111, 32, 87, 111, 114, 108, 100}, which will later be converted to "48656c6c6f20576f726c64"
//     by using `hex.EncodeToString()` method.
func NewKeyRange(startKey, endKey []byte) *KeyRange {
	return &KeyRange{startKey, endKey}
}

// EscapeAsUTF8Str returns the URL escaped key strings as they are UTF-8 encoded.
func (r *KeyRange) EscapeAsUTF8Str() (startKeyStr, endKeyStr string) {
	startKeyStr = url.QueryEscape(string(r.startKey))
	endKeyStr = url.QueryEscape(string(r.endKey))
	return
}

// EscapeAsHexStr returns the URL escaped key strings as they are hex encoded.
func (r *KeyRange) EscapeAsHexStr() (startKeyStr, endKeyStr string) {
	startKeyStr = url.QueryEscape(hex.EncodeToString(r.startKey))
	endKeyStr = url.QueryEscape(hex.EncodeToString(r.endKey))
	return
}

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

// HistoryHotRegionsRequest wrap the request conditions.
type HistoryHotRegionsRequest struct {
	StartTime      int64    `json:"start_time,omitempty"`
	EndTime        int64    `json:"end_time,omitempty"`
	RegionIDs      []uint64 `json:"region_ids,omitempty"`
	StoreIDs       []uint64 `json:"store_ids,omitempty"`
	PeerIDs        []uint64 `json:"peer_ids,omitempty"`
	IsLearners     []bool   `json:"is_learners,omitempty"`
	IsLeaders      []bool   `json:"is_leaders,omitempty"`
	HotRegionTypes []string `json:"hot_region_type,omitempty"`
}

// HistoryHotRegions wraps historyHotRegion
type HistoryHotRegions struct {
	HistoryHotRegion []*HistoryHotRegion `json:"history_hot_region"`
}

// HistoryHotRegion wraps hot region info
// it is storage format of hot_region_storage
type HistoryHotRegion struct {
	UpdateTime    int64   `json:"update_time"`
	RegionID      uint64  `json:"region_id"`
	PeerID        uint64  `json:"peer_id"`
	StoreID       uint64  `json:"store_id"`
	IsLeader      bool    `json:"is_leader"`
	IsLearner     bool    `json:"is_learner"`
	HotRegionType string  `json:"hot_region_type"`
	HotDegree     int64   `json:"hot_degree"`
	FlowBytes     float64 `json:"flow_bytes"`
	KeyRate       float64 `json:"key_rate"`
	QueryRate     float64 `json:"query_rate"`
	StartKey      string  `json:"start_key"`
	EndKey        string  `json:"end_key"`
	// Encryption metadata for start_key and end_key. encryption_meta.iv is IV for start_key.
	// IV for end_key is calculated from (encryption_meta.iv + len(start_key)).
	// The field is only used by PD and should be ignored otherwise.
	// If encryption_meta is empty (i.e. nil), it means start_key and end_key are unencrypted.
	EncryptionMeta *encryptionpb.EncryptionMeta `json:"encryption_meta,omitempty"`
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
	GroupID          string            `json:"group_id"`                    // mark the source that add the rule
	ID               string            `json:"id"`                          // unique ID within a group
	Index            int               `json:"index,omitempty"`             // rule apply order in a group, rule with less ID is applied first when indexes are equal
	Override         bool              `json:"override,omitempty"`          // when it is true, all rules with less indexes are disabled
	StartKey         []byte            `json:"-"`                           // range start key
	StartKeyHex      string            `json:"start_key"`                   // hex format start key, for marshal/unmarshal
	EndKey           []byte            `json:"-"`                           // range end key
	EndKeyHex        string            `json:"end_key"`                     // hex format end key, for marshal/unmarshal
	Role             PeerRoleType      `json:"role"`                        // expected role of the peers
	IsWitness        bool              `json:"is_witness"`                  // when it is true, it means the role is also a witness
	Count            int               `json:"count"`                       // expected count of the peers
	LabelConstraints []LabelConstraint `json:"label_constraints,omitempty"` // used to select stores to place peers
	LocationLabels   []string          `json:"location_labels,omitempty"`   // used to make peers isolated physically
	IsolationLevel   string            `json:"isolation_level,omitempty"`   // used to isolate replicas explicitly and forcibly
	Version          uint64            `json:"version,omitempty"`           // only set at runtime, add 1 each time rules updated, begin from 0.
	CreateTimestamp  uint64            `json:"create_timestamp,omitempty"`  // only set at runtime, recorded rule create timestamp
}

// String returns the string representation of this rule.
func (r *Rule) String() string {
	b, _ := json.Marshal(r)
	return string(b)
}

// Clone returns a copy of Rule.
func (r *Rule) Clone() *Rule {
	var clone Rule
	json.Unmarshal([]byte(r.String()), &clone)
	clone.StartKey = append(r.StartKey[:0:0], r.StartKey...)
	clone.EndKey = append(r.EndKey[:0:0], r.EndKey...)
	return &clone
}

var (
	_ json.Marshaler   = (*Rule)(nil)
	_ json.Unmarshaler = (*Rule)(nil)
)

// This is a helper struct used to customizing the JSON marshal/unmarshal methods of `Rule`.
type rule struct {
	GroupID          string            `json:"group_id"`
	ID               string            `json:"id"`
	Index            int               `json:"index,omitempty"`
	Override         bool              `json:"override,omitempty"`
	StartKeyHex      string            `json:"start_key"`
	EndKeyHex        string            `json:"end_key"`
	Role             PeerRoleType      `json:"role"`
	IsWitness        bool              `json:"is_witness"`
	Count            int               `json:"count"`
	LabelConstraints []LabelConstraint `json:"label_constraints,omitempty"`
	LocationLabels   []string          `json:"location_labels,omitempty"`
	IsolationLevel   string            `json:"isolation_level,omitempty"`
}

// MarshalJSON implements `json.Marshaler` interface to make sure we could set the correct start/end key.
func (r *Rule) MarshalJSON() ([]byte, error) {
	tempRule := &rule{
		GroupID:          r.GroupID,
		ID:               r.ID,
		Index:            r.Index,
		Override:         r.Override,
		StartKeyHex:      r.StartKeyHex,
		EndKeyHex:        r.EndKeyHex,
		Role:             r.Role,
		IsWitness:        r.IsWitness,
		Count:            r.Count,
		LabelConstraints: r.LabelConstraints,
		LocationLabels:   r.LocationLabels,
		IsolationLevel:   r.IsolationLevel,
	}
	// Converts the start/end key to hex format if the corresponding hex field is empty.
	if len(r.StartKey) > 0 && len(r.StartKeyHex) == 0 {
		tempRule.StartKeyHex = rawKeyToKeyHexStr(r.StartKey)
	}
	if len(r.EndKey) > 0 && len(r.EndKeyHex) == 0 {
		tempRule.EndKeyHex = rawKeyToKeyHexStr(r.EndKey)
	}
	return json.Marshal(tempRule)
}

// UnmarshalJSON implements `json.Unmarshaler` interface to make sure we could get the correct start/end key.
func (r *Rule) UnmarshalJSON(bytes []byte) error {
	var tempRule rule
	err := json.Unmarshal(bytes, &tempRule)
	if err != nil {
		return err
	}
	newRule := Rule{
		GroupID:          tempRule.GroupID,
		ID:               tempRule.ID,
		Index:            tempRule.Index,
		Override:         tempRule.Override,
		StartKeyHex:      tempRule.StartKeyHex,
		EndKeyHex:        tempRule.EndKeyHex,
		Role:             tempRule.Role,
		IsWitness:        tempRule.IsWitness,
		Count:            tempRule.Count,
		LabelConstraints: tempRule.LabelConstraints,
		LocationLabels:   tempRule.LocationLabels,
		IsolationLevel:   tempRule.IsolationLevel,
	}
	newRule.StartKey, err = keyHexStrToRawKey(newRule.StartKeyHex)
	if err != nil {
		return err
	}
	newRule.EndKey, err = keyHexStrToRawKey(newRule.EndKeyHex)
	if err != nil {
		return err
	}
	*r = newRule
	return nil
}

// RuleOpType indicates the operation type
type RuleOpType string

const (
	// RuleOpAdd a placement rule, only need to specify the field *Rule
	RuleOpAdd RuleOpType = "add"
	// RuleOpDel a placement rule, only need to specify the field `GroupID`, `ID`, `MatchID`
	RuleOpDel RuleOpType = "del"
)

// RuleOp is for batching placement rule actions.
// The action type is distinguished by the field `Action`.
type RuleOp struct {
	*Rule                       // information of the placement rule to add/delete the operation type
	Action           RuleOpType `json:"action"`
	DeleteByIDPrefix bool       `json:"delete_by_id_prefix"` // if action == delete, delete by the prefix of id
}

func (r RuleOp) String() string {
	b, _ := json.Marshal(r)
	return string(b)
}

var (
	_ json.Marshaler   = (*RuleOp)(nil)
	_ json.Unmarshaler = (*RuleOp)(nil)
)

// This is a helper struct used to customizing the JSON marshal/unmarshal methods of `RuleOp`.
type ruleOp struct {
	GroupID          string            `json:"group_id"`
	ID               string            `json:"id"`
	Index            int               `json:"index,omitempty"`
	Override         bool              `json:"override,omitempty"`
	StartKeyHex      string            `json:"start_key"`
	EndKeyHex        string            `json:"end_key"`
	Role             PeerRoleType      `json:"role"`
	IsWitness        bool              `json:"is_witness"`
	Count            int               `json:"count"`
	LabelConstraints []LabelConstraint `json:"label_constraints,omitempty"`
	LocationLabels   []string          `json:"location_labels,omitempty"`
	IsolationLevel   string            `json:"isolation_level,omitempty"`
	Action           RuleOpType        `json:"action"`
	DeleteByIDPrefix bool              `json:"delete_by_id_prefix"`
}

// MarshalJSON implements `json.Marshaler` interface to make sure we could set the correct start/end key.
func (r *RuleOp) MarshalJSON() ([]byte, error) {
	tempRuleOp := &ruleOp{
		GroupID:          r.GroupID,
		ID:               r.ID,
		Index:            r.Index,
		Override:         r.Override,
		StartKeyHex:      r.StartKeyHex,
		EndKeyHex:        r.EndKeyHex,
		Role:             r.Role,
		IsWitness:        r.IsWitness,
		Count:            r.Count,
		LabelConstraints: r.LabelConstraints,
		LocationLabels:   r.LocationLabels,
		IsolationLevel:   r.IsolationLevel,
		Action:           r.Action,
		DeleteByIDPrefix: r.DeleteByIDPrefix,
	}
	// Converts the start/end key to hex format if the corresponding hex field is empty.
	if len(r.StartKey) > 0 && len(r.StartKeyHex) == 0 {
		tempRuleOp.StartKeyHex = rawKeyToKeyHexStr(r.StartKey)
	}
	if len(r.EndKey) > 0 && len(r.EndKeyHex) == 0 {
		tempRuleOp.EndKeyHex = rawKeyToKeyHexStr(r.EndKey)
	}
	return json.Marshal(tempRuleOp)
}

// UnmarshalJSON implements `json.Unmarshaler` interface to make sure we could get the correct start/end key.
func (r *RuleOp) UnmarshalJSON(bytes []byte) error {
	var tempRuleOp ruleOp
	err := json.Unmarshal(bytes, &tempRuleOp)
	if err != nil {
		return err
	}
	newRuleOp := RuleOp{
		Rule: &Rule{
			GroupID:          tempRuleOp.GroupID,
			ID:               tempRuleOp.ID,
			Index:            tempRuleOp.Index,
			Override:         tempRuleOp.Override,
			StartKeyHex:      tempRuleOp.StartKeyHex,
			EndKeyHex:        tempRuleOp.EndKeyHex,
			Role:             tempRuleOp.Role,
			IsWitness:        tempRuleOp.IsWitness,
			Count:            tempRuleOp.Count,
			LabelConstraints: tempRuleOp.LabelConstraints,
			LocationLabels:   tempRuleOp.LocationLabels,
			IsolationLevel:   tempRuleOp.IsolationLevel,
		},
		Action:           tempRuleOp.Action,
		DeleteByIDPrefix: tempRuleOp.DeleteByIDPrefix,
	}
	newRuleOp.StartKey, err = keyHexStrToRawKey(newRuleOp.StartKeyHex)
	if err != nil {
		return err
	}
	newRuleOp.EndKey, err = keyHexStrToRawKey(newRuleOp.EndKeyHex)
	if err != nil {
		return err
	}
	*r = newRuleOp
	return nil
}

// RuleGroup defines properties of a rule group.
type RuleGroup struct {
	ID       string `json:"id,omitempty"`
	Index    int    `json:"index,omitempty"`
	Override bool   `json:"override,omitempty"`
}

func (g *RuleGroup) String() string {
	b, _ := json.Marshal(g)
	return string(b)
}

// GroupBundle represents a rule group and all rules belong to the group.
type GroupBundle struct {
	ID       string  `json:"group_id"`
	Index    int     `json:"group_index"`
	Override bool    `json:"group_override"`
	Rules    []*Rule `json:"rules"`
}

// RegionLabel is the label of a region.
type RegionLabel struct {
	Key     string `json:"key"`
	Value   string `json:"value"`
	TTL     string `json:"ttl,omitempty"`
	StartAt string `json:"start_at,omitempty"`
}

// LabelRule is the rule to assign labels to a region.
type LabelRule struct {
	ID       string        `json:"id"`
	Index    int           `json:"index"`
	Labels   []RegionLabel `json:"labels"`
	RuleType string        `json:"rule_type"`
	Data     interface{}   `json:"data"`
}

// LabelRulePatch is the patch to update the label rules.
type LabelRulePatch struct {
	SetRules    []*LabelRule `json:"sets"`
	DeleteRules []string     `json:"deletes"`
}
