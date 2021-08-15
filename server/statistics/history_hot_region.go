package statistics

import "github.com/pingcap/kvproto/pkg/encryptionpb"

// HistoryHotRegions wraps historyHotRegion
// it will return to tidb
type HistoryHotRegions struct {
	HistoryHotRegion []*HistoryHotRegion `json:"history_hot_region"`
}

// HistoryHotRegion wraps hot region info
// it is storage format of hot_region_storage
type HistoryHotRegion struct {
	UpdateTime    int64   `json:"update_time,omitempty"`
	RegionID      uint64  `json:"region_id,omitempty"`
	PeerID        uint64  `json:"peer_id,omitempty"`
	StoreID       uint64  `json:"store_id,omitempty"`
	HotRegionType string  `json:"hot_region_type,omitempty"`
	HotDegree     int64   `json:"hot_degree,omitempty"`
	FlowBytes     float64 `json:"flow_bytes,omitempty"`
	KeyRate       float64 `json:"key_rate,omitempty"`
	QueryRate     float64 `json:"query_rate,omitempty"`
	StartKey      []byte  `json:"start_key,omitempty"`
	EndKey        []byte  `json:"end_key,omitempty"`
	// Encryption metadata for start_key and end_key. encryption_meta.iv is IV for start_key.
	// IV for end_key is calculated from (encryption_meta.iv + len(start_key)).
	// The field is only used by PD and should be ignored otherwise.
	// If encryption_meta is empty (i.e. nil), it means start_key and end_key are unencrypted.
	EncryptionMeta *encryptionpb.EncryptionMeta `json:"encryption_meta,omitempty"`
}

// HistoryHotRegionsRequest wrap request condition from tidb.
//it is request from tidb
//TODO
//find a better place to put this struct
type HistoryHotRegionsRequest struct {
	StartTime      int64    `json:"start_time,omitempty"`
	EndTime        int64    `json:"end_time,omitempty"`
	RegionIDs      []uint64 `json:"region_ids,omitempty"`
	StoreIDs       []uint64 `json:"store_ids,omitempty"`
	PeerIDs        []uint64 `json:"peer_ids,omitempty"`
	HotRegionTypes []string `json:"hot_region_type,omitempty"`
	LowHotDegree   int64    `json:"low_hot_degree,omitempty"`
	HighHotDegree  int64    `json:"high_hot_degree,omitempty"`
	LowFlowBytes   float64  `json:"low_flow_bytes,omitempty"`
	HighFlowBytes  float64  `json:"high_flow_bytes,omitempty"`
	LowKeyRate     float64  `json:"low_key_rate,omitempty"`
	HighKeyRate    float64  `json:"high_key_rate,omitempty"`
	LowQueryRate   float64  `json:"low_query_rate,omitempty"`
	HighQueryRate  float64  `json:"high_query_rate,omitempty"`
}
