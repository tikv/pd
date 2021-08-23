// Copyright 2021 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

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
	UpdateTime int64  `json:"update_time,omitempty"`
	RegionID   uint64 `json:"region_id,omitempty"`
	PeerID     uint64 `json:"peer_id,omitempty"`
	StoreID    uint64 `json:"store_id,omitempty"`
	//0 means not leader,1 means leader
	IsLeader      int64   `json:"is_leader,omitempty"`
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
