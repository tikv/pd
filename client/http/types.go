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
	"go.uber.org/zap"
	"net/url"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
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

// following struct definitions are copied from github.com/pingcap/pd/server/api/store
// these are not exported by that package

// HealthInfo define PD's healthy info
type HealthInfo struct {
	Healths []MemberHealth
}

// MemberHealth define a pd member's healthy info
type MemberHealth struct {
	Name       string   `json:"name"`
	MemberID   uint64   `json:"member_id"`
	ClientUrls []string `json:"client_urls"`
	Health     bool     `json:"health"`
}

// MembersInfo is PD members info returned from PD RESTful interface
// type Members map[string][]*pdpb.Member
type MembersInfo struct {
	Header     *pdpb.ResponseHeader `json:"header,omitempty"`
	Members    []*pdpb.Member       `json:"members,omitempty"`
	Leader     *pdpb.Member         `json:"leader,omitempty"`
	EtcdLeader *pdpb.Member         `json:"etcd_leader,omitempty"`
}

// EvictLeaderSchedulerConfig holds configuration for evict leader
// https://github.com/pingcap/pd/blob/b21855a3aeb787c71b0819743059e432be217dcd/server/schedulers/evict_leader.go#L81-L86
// note that we use `interface{}` as the type of value because we don't care
// about the value for now
type EvictLeaderSchedulerConfig struct {
	StoreIDWithRanges map[uint64]interface{} `json:"store-id-ranges"`
}

// Strategy within an HTTP request provides rules and resources to help make decision for auto scaling.
type Strategy struct {
	Rules     []*StrategyRule `json:"rules"`
	Resources []*Resource     `json:"resources"`
}

// StrategyRule is a set of constraints for a kind of component.
type StrategyRule struct {
	Component   string       `json:"component"`
	CPURule     *CPURule     `json:"cpu_rule,omitempty"`
	StorageRule *StorageRule `json:"storage_rule,omitempty"`
}

// CPURule is the constraints about CPU.
type CPURule struct {
	MaxThreshold  float64  `json:"max_threshold"`
	MinThreshold  float64  `json:"min_threshold"`
	ResourceTypes []string `json:"resource_types"`
}

// StorageRule is the constraints about storage.
type StorageRule struct {
	MinThreshold  float64  `json:"min_threshold"`
	ResourceTypes []string `json:"resource_types"`
}

// Resource represents a kind of resource set including CPU, memory, storage.
type Resource struct {
	ResourceType string `json:"resource_type"`
	// The basic unit of CPU is milli-core.
	CPU uint64 `json:"cpu"`
	// The basic unit of memory is byte.
	Memory uint64 `json:"memory"`
	// The basic unit of storage is byte.
	Storage uint64 `json:"storage"`
	// If count is not set, it indicates no limit.
	Count *uint64 `json:"count,omitempty"`
}

// Plan is the final result of auto-scaling, which indicates how to scale in or scale out.
type Plan struct {
	Component    string            `json:"component"`
	Count        uint64            `json:"count"`
	ResourceType string            `json:"resource_type"`
	Labels       map[string]string `json:"labels"`
}

// ReplicationConfig is the replication configuration.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type ReplicationConfig struct {
	// MaxReplicas is the number of replicas for each region.
	MaxReplicas uint64 `toml:"max-replicas" json:"max-replicas"`

	// The label keys specified the location of a store.
	// The placement priorities is implied by the order of label keys.
	// For example, ["zone", "rack"] means that we should place replicas to
	// different zones first, then to different racks if we don't have enough zones.
	LocationLabels StringSlice `toml:"location-labels" json:"location-labels"`
	// StrictlyMatchLabel strictly checks if the label of TiKV is matched with LocationLabels.
	StrictlyMatchLabel bool `toml:"strictly-match-label" json:"strictly-match-label,string"`

	// When PlacementRules feature is enabled. MaxReplicas, LocationLabels and IsolationLabels are not used any more.
	EnablePlacementRules bool `toml:"enable-placement-rules" json:"enable-placement-rules,string"`

	// EnablePlacementRuleCache controls whether use cache during rule checker
	EnablePlacementRulesCache bool `toml:"enable-placement-rules-cache" json:"enable-placement-rules-cache,string"`

	// IsolationLevel is used to isolate replicas explicitly and forcibly if it's not empty.
	// Its value must be empty or one of LocationLabels.
	// Example:
	// location-labels = ["zone", "rack", "host"]
	// isolation-level = "zone"
	// With configuration like above, PD ensure that all replicas be placed in different zones.
	// Even if a zone is down, PD will not try to make up replicas in other zone
	// because other zones already have replicas on it.
	IsolationLevel string `toml:"isolation-level" json:"isolation-level"`
}

// StringSlice is more friendly to json encode/decode
type StringSlice []string

// ServerConfig is the pd server configuration.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type ServerConfig struct {
	// Config is the pd server configuration.
	// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
	ClientUrls          string `toml:"client-urls" json:"client-urls"`
	PeerUrls            string `toml:"peer-urls" json:"peer-urls"`
	AdvertiseClientUrls string `toml:"advertise-client-urls" json:"advertise-client-urls"`
	AdvertisePeerUrls   string `toml:"advertise-peer-urls" json:"advertise-peer-urls"`

	Name              string `toml:"name" json:"name"`
	DataDir           string `toml:"data-dir" json:"data-dir"`
	ForceNewCluster   bool   `json:"force-new-cluster"`
	EnableGRPCGateway bool   `json:"enable-grpc-gateway"`

	InitialCluster      string `toml:"initial-cluster" json:"initial-cluster"`
	InitialClusterState string `toml:"initial-cluster-state" json:"initial-cluster-state"`
	InitialClusterToken string `toml:"initial-cluster-token" json:"initial-cluster-token"`

	// Join to an existing pd cluster, a string of endpoints.
	Join string `toml:"join" json:"join"`

	// LeaderLease time, if leader doesn't update its TTL
	// in etcd after lease time, etcd will expire the leader key
	// and other servers can campaign the leader again.
	// Etcd only supports seconds TTL, so here is second too.
	LeaderLease int64 `toml:"lease" json:"lease"`

	// Log related config.
	Log log.Config `toml:"log" json:"log"`

	// Backward compatibility.
	LogFileDeprecated  string `toml:"log-file" json:"log-file,omitempty"`
	LogLevelDeprecated string `toml:"log-level" json:"log-level,omitempty"`

	// MaxConcurrentTSOProxyStreamings is the maximum number of concurrent TSO proxy streaming process routines allowed.
	// Exceeding this limit will result in an error being returned to the client when a new client starts a TSO streaming.
	// Set this to 0 will disable TSO Proxy.
	// Set this to the negative value to disable the limit.
	MaxConcurrentTSOProxyStreamings int `toml:"max-concurrent-tso-proxy-streamings" json:"max-concurrent-tso-proxy-streamings"`
	// TSOProxyRecvFromClientTimeout is the timeout for the TSO proxy to receive a tso request from a client via grpc TSO stream.
	// After the timeout, the TSO proxy will close the grpc TSO stream.
	TSOProxyRecvFromClientTimeout Duration `toml:"tso-proxy-recv-from-client-timeout" json:"tso-proxy-recv-from-client-timeout"`

	// TSOSaveInterval is the interval to save timestamp.
	TSOSaveInterval Duration `toml:"tso-save-interval" json:"tso-save-interval"`

	// The interval to update physical part of timestamp. Usually, this config should not be set.
	// At most 1<<18 (262144) TSOs can be generated in the interval. The smaller the value, the
	// more TSOs provided, and at the same time consuming more CPU time.
	// This config is only valid in 1ms to 10s. If it's configured too long or too short, it will
	// be automatically clamped to the range.
	TSOUpdatePhysicalInterval Duration `toml:"tso-update-physical-interval" json:"tso-update-physical-interval"`

	// EnableLocalTSO is used to enable the Local TSO Allocator feature,
	// which allows the PD server to generate Local TSO for certain DC-level transactions.
	// To make this feature meaningful, user has to set the "zone" label for the PD server
	// to indicate which DC this PD belongs to.
	EnableLocalTSO bool `toml:"enable-local-tso" json:"enable-local-tso"`

	Metric MetricConfig `toml:"metric" json:"metric"`

	Schedule ScheduleConfig `toml:"schedule" json:"schedule"`

	Replication ReplicationConfig `toml:"replication" json:"replication"`

	PDServerCfg PDServerConfig `toml:"pd-server" json:"pd-server"`

	ClusterVersion semver.Version `toml:"cluster-version" json:"cluster-version"`

	// Labels indicates the labels set for **this** PD server. The labels describe some specific properties
	// like `zone`/`rack`/`host`. Currently, labels won't affect the PD server except for some special
	// label keys. Now we have following special keys:
	// 1. 'zone' is a special key that indicates the DC location of this PD server. If it is set, the value for this
	// will be used to determine which DC's Local TSO service this PD will provide with if EnableLocalTSO is true.
	Labels map[string]string `toml:"labels" json:"labels"`

	// QuotaBackendBytes Raise alarms when backend size exceeds the given quota. 0 means use the default quota.
	// the default size is 2GB, the maximum is 8GB.
	QuotaBackendBytes ByteSize `toml:"quota-backend-bytes" json:"quota-backend-bytes"`
	// AutoCompactionMode is either 'periodic' or 'revision'. The default value is 'periodic'.
	AutoCompactionMode string `toml:"auto-compaction-mode" json:"auto-compaction-mode"`
	// AutoCompactionRetention is either duration string with time unit
	// (e.g. '5m' for 5-minute), or revision unit (e.g. '5000').
	// If no time unit is provided and compaction mode is 'periodic',
	// the unit defaults to hour. For example, '5' translates into 5-hour.
	// The default retention is 1 hour.
	// Before etcd v3.3.x, the type of retention is int. We add 'v2' suffix to make it backward compatible.
	AutoCompactionRetention string `toml:"auto-compaction-retention" json:"auto-compaction-retention-v2"`

	// TickInterval is the interval for etcd Raft tick.
	TickInterval Duration `toml:"tick-interval"`
	// ElectionInterval is the interval for etcd Raft election.
	ElectionInterval Duration `toml:"election-interval"`
	// Prevote is true to enable Raft Pre-Vote.
	// If enabled, Raft runs an additional election phase
	// to check whether it would get enough votes to win
	// an election, thus minimizing disruptions.
	PreVote bool `toml:"enable-prevote"`

	MaxRequestBytes uint `toml:"max-request-bytes" json:"max-request-bytes"`

	Security SecurityConfig `toml:"security" json:"security"`

	LabelProperty LabelPropertyConfig `toml:"label-property" json:"label-property"`

	// For all warnings during parsing.
	WarningMsgs []string

	DisableStrictReconfigCheck bool

	HeartbeatStreamBindInterval Duration
	LeaderPriorityCheckInterval Duration

	Logger   *zap.Logger        `json:"-"`
	LogProps *log.ZapProperties `json:"-"`

	Dashboard DashboardConfig `toml:"dashboard" json:"dashboard"`

	ReplicationMode ReplicationModeConfig `toml:"replication-mode" json:"replication-mode"`

	Keyspace KeyspaceConfig `toml:"keyspace" json:"keyspace"`

	Controller ControllerConfig `toml:"controller" json:"controller"`
}

// PDServerConfig is the configuration for pd server.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type PDServerConfig struct {
	// UseRegionStorage enables the independent region storage.
	UseRegionStorage bool `toml:"use-region-storage" json:"use-region-storage,string"`
	// MaxResetTSGap is the max gap to reset the TSO.
	MaxResetTSGap Duration `toml:"max-gap-reset-ts" json:"max-gap-reset-ts"`
	// KeyType is option to specify the type of keys.
	// There are some types supported: ["table", "raw", "txn"], default: "table"
	KeyType string `toml:"key-type" json:"key-type"`
	// RuntimeServices is the running extension services.
	RuntimeServices StringSlice `toml:"runtime-services" json:"runtime-services"`
	// MetricStorage is the cluster metric storage.
	// Currently, we use prometheus as metric storage, we may use PD/TiKV as metric storage later.
	MetricStorage string `toml:"metric-storage" json:"metric-storage"`
	// There are some values supported: "auto", "none", or a specific address, default: "auto"
	DashboardAddress string `toml:"dashboard-address" json:"dashboard-address"`
	// TraceRegionFlow the option to update flow information of regions.
	// WARN: TraceRegionFlow is deprecated.
	TraceRegionFlow bool `toml:"trace-region-flow" json:"trace-region-flow,string,omitempty"`
	// FlowRoundByDigit used to discretization processing flow information.
	FlowRoundByDigit int `toml:"flow-round-by-digit" json:"flow-round-by-digit"`
	// MinResolvedTSPersistenceInterval is the interval to save the min resolved ts.
	MinResolvedTSPersistenceInterval Duration `toml:"min-resolved-ts-persistence-interval" json:"min-resolved-ts-persistence-interval"`
	// ServerMemoryLimit indicates the memory limit of current process.
	ServerMemoryLimit float64 `toml:"server-memory-limit" json:"server-memory-limit"`
	// ServerMemoryLimitGCTrigger indicates the gc percentage of the ServerMemoryLimit.
	ServerMemoryLimitGCTrigger float64 `toml:"server-memory-limit-gc-trigger" json:"server-memory-limit-gc-trigger"`
	// EnableGOGCTuner is to enable GOGC tuner. it can tuner GOGC.
	EnableGOGCTuner bool `toml:"enable-gogc-tuner" json:"enable-gogc-tuner,string"`
	// GCTunerThreshold is the threshold of GC tuner.
	GCTunerThreshold float64 `toml:"gc-tuner-threshold" json:"gc-tuner-threshold"`
	// BlockSafePointV1 is used to control gc safe point v1 and service safe point v1 can not be updated.
	BlockSafePointV1 bool `toml:"block-safe-point-v1" json:"block-safe-point-v1,string"`
}

// ScheduleConfig is the schedule configuration.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type ScheduleConfig struct {
	// If the snapshot count of one store is greater than this value,
	// it will never be used as a source or target store.
	MaxSnapshotCount    uint64 `toml:"max-snapshot-count" json:"max-snapshot-count"`
	MaxPendingPeerCount uint64 `toml:"max-pending-peer-count" json:"max-pending-peer-count"`
	// If both the size of region is smaller than MaxMergeRegionSize
	// and the number of rows in region is smaller than MaxMergeRegionKeys,
	// it will try to merge with adjacent regions.
	MaxMergeRegionSize uint64 `toml:"max-merge-region-size" json:"max-merge-region-size"`
	MaxMergeRegionKeys uint64 `toml:"max-merge-region-keys" json:"max-merge-region-keys"`
	// SplitMergeInterval is the minimum interval time to permit merge after split.
	SplitMergeInterval Duration `toml:"split-merge-interval" json:"split-merge-interval"`
	// SwitchWitnessInterval is the minimum interval that allows a peer to become a witness again after it is promoted to non-witness.
	SwitchWitnessInterval Duration `toml:"switch-witness-interval" json:"switch-witness-interval"`
	// EnableOneWayMerge is the option to enable one way merge. This means a Region can only be merged into the next region of it.
	EnableOneWayMerge bool `toml:"enable-one-way-merge" json:"enable-one-way-merge,string"`
	// EnableCrossTableMerge is the option to enable cross table merge. This means two Regions can be merged with different table IDs.
	// This option only works when key type is "table".
	EnableCrossTableMerge bool `toml:"enable-cross-table-merge" json:"enable-cross-table-merge,string"`
	// PatrolRegionInterval is the interval for scanning region during patrol.
	PatrolRegionInterval Duration `toml:"patrol-region-interval" json:"patrol-region-interval"`
	// MaxStoreDownTime is the max duration after which
	// a store will be considered to be down if it hasn't reported heartbeats.
	MaxStoreDownTime Duration `toml:"max-store-down-time" json:"max-store-down-time"`
	// MaxStorePreparingTime is the max duration after which
	// a store will be considered to be preparing.
	MaxStorePreparingTime Duration `toml:"max-store-preparing-time" json:"max-store-preparing-time"`
	// LeaderScheduleLimit is the max coexist leader schedules.
	LeaderScheduleLimit uint64 `toml:"leader-schedule-limit" json:"leader-schedule-limit"`
	// LeaderSchedulePolicy is the option to balance leader, there are some policies supported: ["count", "size"], default: "count"
	LeaderSchedulePolicy string `toml:"leader-schedule-policy" json:"leader-schedule-policy"`
	// RegionScheduleLimit is the max coexist region schedules.
	RegionScheduleLimit uint64 `toml:"region-schedule-limit" json:"region-schedule-limit"`
	// WitnessScheduleLimit is the max coexist witness schedules.
	WitnessScheduleLimit uint64 `toml:"witness-schedule-limit" json:"witness-schedule-limit"`
	// ReplicaScheduleLimit is the max coexist replica schedules.
	ReplicaScheduleLimit uint64 `toml:"replica-schedule-limit" json:"replica-schedule-limit"`
	// MergeScheduleLimit is the max coexist merge schedules.
	MergeScheduleLimit uint64 `toml:"merge-schedule-limit" json:"merge-schedule-limit"`
	// HotRegionScheduleLimit is the max coexist hot region schedules.
	HotRegionScheduleLimit uint64 `toml:"hot-region-schedule-limit" json:"hot-region-schedule-limit"`
	// HotRegionCacheHitThreshold is the cache hits threshold of the hot region.
	// If the number of times a region hits the hot cache is greater than this
	// threshold, it is considered a hot region.
	HotRegionCacheHitsThreshold uint64 `toml:"hot-region-cache-hits-threshold" json:"hot-region-cache-hits-threshold"`
	// StoreBalanceRate is the maximum of balance rate for each store.
	// WARN: StoreBalanceRate is deprecated.
	StoreBalanceRate float64 `toml:"store-balance-rate" json:"store-balance-rate,omitempty"`
	// StoreLimit is the limit of scheduling for stores.
	StoreLimit map[uint64]StoreLimitConfig `toml:"store-limit" json:"store-limit"`
	// TolerantSizeRatio is the ratio of buffer size for balance scheduler.
	TolerantSizeRatio float64 `toml:"tolerant-size-ratio" json:"tolerant-size-ratio"`
	//
	//      high space stage         transition stage           low space stage
	//   |--------------------|-----------------------------|-------------------------|
	//   ^                    ^                             ^                         ^
	//   0       HighSpaceRatio * capacity       LowSpaceRatio * capacity          capacity
	//
	// LowSpaceRatio is the lowest usage ratio of store which regraded as low space.
	// When in low space, store region score increases to very large and varies inversely with available size.
	LowSpaceRatio float64 `toml:"low-space-ratio" json:"low-space-ratio"`
	// HighSpaceRatio is the highest usage ratio of store which regraded as high space.
	// High space means there is a lot of spare capacity, and store region score varies directly with used size.
	HighSpaceRatio float64 `toml:"high-space-ratio" json:"high-space-ratio"`
	// RegionScoreFormulaVersion is used to control the formula used to calculate region score.
	RegionScoreFormulaVersion string `toml:"region-score-formula-version" json:"region-score-formula-version"`
	// SchedulerMaxWaitingOperator is the max coexist operators for each scheduler.
	SchedulerMaxWaitingOperator uint64 `toml:"scheduler-max-waiting-operator" json:"scheduler-max-waiting-operator"`
	// WARN: DisableLearner is deprecated.
	// DisableLearner is the option to disable using AddLearnerNode instead of AddNode.
	DisableLearner bool `toml:"disable-raft-learner" json:"disable-raft-learner,string,omitempty"`
	// DisableRemoveDownReplica is the option to prevent replica checker from
	// removing down replicas.
	// WARN: DisableRemoveDownReplica is deprecated.
	DisableRemoveDownReplica bool `toml:"disable-remove-down-replica" json:"disable-remove-down-replica,string,omitempty"`
	// DisableReplaceOfflineReplica is the option to prevent replica checker from
	// replacing offline replicas.
	// WARN: DisableReplaceOfflineReplica is deprecated.
	DisableReplaceOfflineReplica bool `toml:"disable-replace-offline-replica" json:"disable-replace-offline-replica,string,omitempty"`
	// DisableMakeUpReplica is the option to prevent replica checker from making up
	// replicas when replica count is less than expected.
	// WARN: DisableMakeUpReplica is deprecated.
	DisableMakeUpReplica bool `toml:"disable-make-up-replica" json:"disable-make-up-replica,string,omitempty"`
	// DisableRemoveExtraReplica is the option to prevent replica checker from
	// removing extra replicas.
	// WARN: DisableRemoveExtraReplica is deprecated.
	DisableRemoveExtraReplica bool `toml:"disable-remove-extra-replica" json:"disable-remove-extra-replica,string,omitempty"`
	// DisableLocationReplacement is the option to prevent replica checker from
	// moving replica to a better location.
	// WARN: DisableLocationReplacement is deprecated.
	DisableLocationReplacement bool `toml:"disable-location-replacement" json:"disable-location-replacement,string,omitempty"`

	// EnableRemoveDownReplica is the option to enable replica checker to remove down replica.
	EnableRemoveDownReplica bool `toml:"enable-remove-down-replica" json:"enable-remove-down-replica,string"`
	// EnableReplaceOfflineReplica is the option to enable replica checker to replace offline replica.
	EnableReplaceOfflineReplica bool `toml:"enable-replace-offline-replica" json:"enable-replace-offline-replica,string"`
	// EnableMakeUpReplica is the option to enable replica checker to make up replica.
	EnableMakeUpReplica bool `toml:"enable-make-up-replica" json:"enable-make-up-replica,string"`
	// EnableRemoveExtraReplica is the option to enable replica checker to remove extra replica.
	EnableRemoveExtraReplica bool `toml:"enable-remove-extra-replica" json:"enable-remove-extra-replica,string"`
	// EnableLocationReplacement is the option to enable replica checker to move replica to a better location.
	EnableLocationReplacement bool `toml:"enable-location-replacement" json:"enable-location-replacement,string"`
	// EnableDebugMetrics is the option to enable debug metrics.
	EnableDebugMetrics bool `toml:"enable-debug-metrics" json:"enable-debug-metrics,string"`
	// EnableJointConsensus is the option to enable using joint consensus as an operator step.
	EnableJointConsensus bool `toml:"enable-joint-consensus" json:"enable-joint-consensus,string"`
	// EnableTiKVSplitRegion is the option to enable tikv split region.
	// on ebs-based BR we need to disable it with TTL
	EnableTiKVSplitRegion bool `toml:"enable-tikv-split-region" json:"enable-tikv-split-region,string"`

	// Schedulers support for loading customized schedulers
	Schedulers SchedulerConfigs `toml:"schedulers" json:"schedulers-v2"` // json v2 is for the sake of compatible upgrade

	// Only used to display
	SchedulersPayload map[string]interface{} `toml:"schedulers-payload" json:"schedulers-payload"`

	// Controls the time interval between write hot regions info into leveldb.
	HotRegionsWriteInterval Duration `toml:"hot-regions-write-interval" json:"hot-regions-write-interval"`

	// The day of hot regions data to be reserved. 0 means close.
	HotRegionsReservedDays uint64 `toml:"hot-regions-reserved-days" json:"hot-regions-reserved-days"`

	// MaxMovableHotPeerSize is the threshold of region size for balance hot region and split bucket scheduler.
	// Hot region must be split before moved if it's region size is greater than MaxMovableHotPeerSize.
	MaxMovableHotPeerSize int64 `toml:"max-movable-hot-peer-size" json:"max-movable-hot-peer-size,omitempty"`

	// EnableDiagnostic is the option to enable using diagnostic
	EnableDiagnostic bool `toml:"enable-diagnostic" json:"enable-diagnostic,string"`

	// EnableWitness is the option to enable using witness
	EnableWitness bool `toml:"enable-witness" json:"enable-witness,string"`

	// SlowStoreEvictingAffectedStoreRatioThreshold is the affected ratio threshold when judging a store is slow
	// A store's slowness must affect more than `store-count * SlowStoreEvictingAffectedStoreRatioThreshold` to trigger evicting.
	SlowStoreEvictingAffectedStoreRatioThreshold float64 `toml:"slow-store-evicting-affected-store-ratio-threshold" json:"slow-store-evicting-affected-store-ratio-threshold,omitempty"`

	// StoreLimitVersion is the version of store limit.
	// v1: which is based on the region count by rate limit.
	// v2: which is based on region size by window size.
	StoreLimitVersion string `toml:"store-limit-version" json:"store-limit-version,omitempty"`

	// HaltScheduling is the option to halt the scheduling. Once it's on, PD will halt the scheduling,
	// and any other scheduling configs will be ignored.
	HaltScheduling bool `toml:"halt-scheduling" json:"halt-scheduling,string,omitempty"`
}

// SchedulerConfigs is a slice of customized scheduler configuration.
type SchedulerConfigs []SchedulerConfig

// SchedulerConfig is customized scheduler configuration
type SchedulerConfig struct {
	Type        string   `toml:"type" json:"type"`
	Args        []string `toml:"args" json:"args"`
	Disable     bool     `toml:"disable" json:"disable"`
	ArgsPayload string   `toml:"args-payload" json:"args-payload"`
}

// StoreLimitConfig is a config about scheduling rate limit of different types for a store.
type StoreLimitConfig struct {
	AddPeer    float64 `toml:"add-peer" json:"add-peer"`
	RemovePeer float64 `toml:"remove-peer" json:"remove-peer"`
}

// SecurityConfig indicates the security configuration
type SecurityConfig struct {
	TLSConfig
	// RedactInfoLog indicates that whether enabling redact log
	RedactInfoLog bool             `toml:"redact-info-log" json:"redact-info-log"`
	Encryption    encryptionConfig `toml:"encryption" json:"encryption"`
}

// TLSConfig is the configuration for supporting tls.
type TLSConfig struct {
	// CAPath is the path of file that contains list of trusted SSL CAs. if set, following four settings shouldn't be empty
	CAPath string `toml:"cacert-path" json:"cacert-path"`
	// CertPath is the path of file that contains X509 certificate in PEM format.
	CertPath string `toml:"cert-path" json:"cert-path"`
	// KeyPath is the path of file that contains X509 key in PEM format.
	KeyPath string `toml:"key-path" json:"key-path"`
	// CertAllowedCN is a CN which must be provided by a client
	CertAllowedCN []string `toml:"cert-allowed-cn" json:"cert-allowed-cn"`

	SSLCABytes   []byte
	SSLCertBytes []byte
	SSLKEYBytes  []byte
}

// Config define the encryption config structure.
type encryptionConfig struct {
	// Encryption method to use for PD data.
	DataEncryptionMethod string `toml:"data-encryption-method" json:"data-encryption-method"`
	// Specifies how often PD rotates data encryption key.
	DataKeyRotationPeriod Duration `toml:"data-key-rotation-period" json:"data-key-rotation-period"`
	// Specifies master key if encryption is enabled.
	MasterKey MasterKeyConfig `toml:"master-key" json:"master-key"`
}

// MasterKeyConfig defines master key config structure.
type MasterKeyConfig struct {
	// Master key type, one of "plaintext", "kms" or "file".
	Type string `toml:"type" json:"type"`

	MasterKeyKMSConfig
	MasterKeyFileConfig
}

// MasterKeyKMSConfig defines a KMS master key config structure.
type MasterKeyKMSConfig struct {
	// KMS CMK key id.
	KmsKeyID string `toml:"key-id" json:"key-id"`
	// KMS region of the CMK.
	KmsRegion string `toml:"region" json:"region"`
	// Custom endpoint to access KMS.
	KmsEndpoint string `toml:"endpoint" json:"endpoint"`
}

// MasterKeyFileConfig defines a file-based master key config structure.
type MasterKeyFileConfig struct {
	// Master key file path.
	FilePath string `toml:"path" json:"path"`
}

// MetricConfig is the metric configuration.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type MetricConfig struct {
	PushJob      string   `toml:"job" json:"job"`
	PushAddress  string   `toml:"address" json:"address"`
	PushInterval Duration `toml:"interval" json:"interval"`
}

// LabelPropertyConfig is the config section to set properties to store labels.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type LabelPropertyConfig map[string][]StoreLabel

// DashboardConfig is the configuration for tidb-dashboard.
type DashboardConfig struct {
	TiDBCAPath         string `toml:"tidb-cacert-path" json:"tidb-cacert-path"`
	TiDBCertPath       string `toml:"tidb-cert-path" json:"tidb-cert-path"`
	TiDBKeyPath        string `toml:"tidb-key-path" json:"tidb-key-path"`
	PublicPathPrefix   string `toml:"public-path-prefix" json:"public-path-prefix"`
	InternalProxy      bool   `toml:"internal-proxy" json:"internal-proxy"`
	EnableTelemetry    bool   `toml:"enable-telemetry" json:"enable-telemetry"`
	EnableExperimental bool   `toml:"enable-experimental" json:"enable-experimental"`
}

// ReplicationModeConfig is the configuration for the replication policy.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type ReplicationModeConfig struct {
	ReplicationMode string                      `toml:"replication-mode" json:"replication-mode"` // can be 'dr-auto-sync' or 'majority', default value is 'majority'
	DRAutoSync      DRAutoSyncReplicationConfig `toml:"dr-auto-sync" json:"dr-auto-sync"`         // used when ReplicationMode is 'dr-auto-sync'
}

// DRAutoSyncReplicationConfig is the configuration for auto sync mode between 2 data centers.
type DRAutoSyncReplicationConfig struct {
	LabelKey           string   `toml:"label-key" json:"label-key"`
	Primary            string   `toml:"primary" json:"primary"`
	DR                 string   `toml:"dr" json:"dr"`
	PrimaryReplicas    int      `toml:"primary-replicas" json:"primary-replicas"`
	DRReplicas         int      `toml:"dr-replicas" json:"dr-replicas"`
	WaitStoreTimeout   Duration `toml:"wait-store-timeout" json:"wait-store-timeout"`
	WaitRecoverTimeout Duration `toml:"wait-recover-timeout" json:"wait-recover-timeout"`
	PauseRegionSplit   bool     `toml:"pause-region-split" json:"pause-region-split,string"`
}

// KeyspaceConfig is the configuration for keyspace management.
type KeyspaceConfig struct {
	// PreAlloc contains the keyspace to be allocated during keyspace manager initialization.
	PreAlloc []string `toml:"pre-alloc" json:"pre-alloc"`
	// WaitRegionSplit indicates whether to wait for the region split to complete
	WaitRegionSplit bool `toml:"wait-region-split" json:"wait-region-split"`
	// WaitRegionSplitTimeout indicates the max duration to wait region split.
	WaitRegionSplitTimeout Duration `toml:"wait-region-split-timeout" json:"wait-region-split-timeout"`
	// CheckRegionSplitInterval indicates the interval to check whether the region split is complete
	CheckRegionSplitInterval Duration `toml:"check-region-split-interval" json:"check-region-split-interval"`
}

// ControllerConfig is the configuration of the resource manager controller which includes some option for client needed.
type ControllerConfig struct {
	// EnableDegradedMode is to control whether resource control client enable degraded mode when server is disconnect.
	DegradedModeWaitDuration Duration `toml:"degraded-mode-wait-duration" json:"degraded-mode-wait-duration"`

	// LTBMaxWaitDuration is the max wait time duration for local token bucket.
	LTBMaxWaitDuration Duration `toml:"ltb-max-wait-duration" json:"ltb-max-wait-duration"`

	// RequestUnit is the configuration determines the coefficients of the RRU and WRU cost.
	// This configuration should be modified carefully.
	RequestUnit RequestUnitConfig `toml:"request-unit" json:"request-unit"`
}

// RequestUnitConfig is the configuration of the request units, which determines the coefficients of
// the RRU and WRU cost. This configuration should be modified carefully.
type RequestUnitConfig struct {
	// ReadBaseCost is the base cost for a read request. No matter how many bytes read/written or
	// the CPU times taken for a request, this cost is inevitable.
	ReadBaseCost float64 `toml:"read-base-cost" json:"read-base-cost"`
	// ReadPerBatchBaseCost is the base cost for a read request with batch.
	ReadPerBatchBaseCost float64 `toml:"read-per-batch-base-cost" json:"read-per-batch-base-cost"`
	// ReadCostPerByte is the cost for each byte read. It's 1 RU = 64 KiB by default.
	ReadCostPerByte float64 `toml:"read-cost-per-byte" json:"read-cost-per-byte"`
	// WriteBaseCost is the base cost for a write request. No matter how many bytes read/written or
	// the CPU times taken for a request, this cost is inevitable.
	WriteBaseCost float64 `toml:"write-base-cost" json:"write-base-cost"`
	// WritePerBatchBaseCost is the base cost for a write request with batch.
	WritePerBatchBaseCost float64 `toml:"write-per-batch-base-cost" json:"write-per-batch-base-cost"`
	// WriteCostPerByte is the cost for each byte written. It's 1 RU = 1 KiB by default.
	WriteCostPerByte float64 `toml:"write-cost-per-byte" json:"write-cost-per-byte"`
	// CPUMsCost is the cost for each millisecond of CPU time taken.
	// It's 1 RU = 3 millisecond by default.
	CPUMsCost float64 `toml:"read-cpu-ms-cost" json:"read-cpu-ms-cost"`
}
