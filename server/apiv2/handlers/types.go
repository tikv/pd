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
	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
)

const (
	// StoreIDParamKey is the key for getting store ID query parameter.
	StoreIDParamKey = "id"
)

const (
	// AliveStatusName is normal status of the store heartbeat.
	AliveStatusName = "Alive"
	// DisconnectedStatusName is abnormal status that indicates the store has lost heartbeat for 20s.
	DisconnectedStatusName = "Disconnected"
	// DownStatusName is abnormal status that indicates the store has lost heartbeat for the configured down time setting.
	DownStatusName = "Down"
)

// StoresInfo records stores' info.
type StoresInfo struct {
	Count  int          `json:"count"`
	Stores []*StoreInfo `json:"stores"`
}

// MetaStore contains meta information about a store.
type MetaStore struct {
	ID      uint64 `json:"id,omitempty"`
	Address string `json:"address,omitempty"`
	// Status address provides the HTTP service for external components
	StatusAddress string               `json:"status_address,omitempty"`
	Labels        []*metapb.StoreLabel `json:"labels,omitempty"`
	Version       string               `json:"version,omitempty"`
	GitHash       string               `json:"git_hash,omitempty"`
	DeployPath    string               `json:"deploy_path,omitempty"`
}

// NewMetaStore create a new meta store.
func NewMetaStore(store *metapb.Store) *MetaStore {
	return &MetaStore{
		ID:            store.GetId(),
		Address:       store.GetAddress(),
		StatusAddress: store.GetStatusAddress(),
		Labels:        store.GetLabels(),
		Version:       store.GetVersion(),
		GitHash:       store.GetGitHash(),
		DeployPath:    store.GetDeployPath(),
	}
}

// StoreStatus contains status about a store.
type StoreStatus struct {
	Capacity  typeutil.ByteSize `json:"capacity"`
	Available typeutil.ByteSize `json:"available"`
	UsedSize  typeutil.ByteSize `json:"used_size"`

	LeaderCount  int     `json:"leader_count"`
	LeaderWeight float64 `json:"leader_weight"`
	LeaderScore  float64 `json:"leader_score"`
	LeaderSize   int64   `json:"leader_size"`
	RegionCount  int     `json:"region_count"`
	RegionWeight float64 `json:"region_weight"`
	RegionScore  float64 `json:"region_score"`
	RegionSize   int64   `json:"region_size"`
	SlowScore    uint64  `json:"slow_score"`

	// If the store is physically destroyed, which means it can never up again.
	PhysicallyDestroyed bool   `json:"physically_destroyed,omitempty"`
	NodeState           string `json:"node_state"`
	HeartbeatStatus     string `json:"heartbeat_status"`

	StartTime         string             `json:"start_time,omitempty"`
	LastHeartbeatTime string             `json:"last_heartbeat_time,omitempty"`
	Uptime            *typeutil.Duration `json:"uptime,omitempty"`
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
		Store: NewMetaStore(store.GetMeta()),
		Status: &StoreStatus{
			Capacity:            typeutil.ByteSize(store.GetCapacity()),
			Available:           typeutil.ByteSize(store.GetAvailable()),
			UsedSize:            typeutil.ByteSize(store.GetUsedSize()),
			LeaderCount:         store.GetLeaderCount(),
			LeaderWeight:        store.GetLeaderWeight(),
			LeaderScore:         store.LeaderScore(core.StringToSchedulePolicy(cfg.LeaderSchedulePolicy), 0),
			LeaderSize:          store.GetLeaderSize(),
			RegionCount:         store.GetRegionCount(),
			RegionWeight:        store.GetRegionWeight(),
			RegionScore:         store.RegionScore(cfg.RegionScoreFormulaVersion, cfg.HighSpaceRatio, cfg.LowSpaceRatio, 0),
			RegionSize:          store.GetRegionSize(),
			SlowScore:           store.GetSlowScore(),
			NodeState:           store.GetNodeState().String(),
			HeartbeatStatus:     AliveStatusName,
			PhysicallyDestroyed: store.GetMeta().GetPhysicallyDestroyed(),
		},
	}

	if store.GetStoreStats() != nil {
		startTS := store.GetStartTime()
		s.Status.StartTime = startTS.Format(time.RFC3339)
	}
	if lastHeartbeat := store.GetLastHeartbeatTS(); !lastHeartbeat.IsZero() {
		s.Status.LastHeartbeatTime = lastHeartbeat.Format(time.RFC3339)
	}
	if upTime := store.GetUptime(); upTime > 0 {
		duration := typeutil.NewDuration(upTime)
		s.Status.Uptime = &duration
	}

	if store.DownTime() > cfg.MaxStoreDownTime.Duration {
		s.Status.HeartbeatStatus = DownStatusName
	} else if store.IsDisconnected() {
		s.Status.HeartbeatStatus = DisconnectedStatusName
	}

	return s
}
