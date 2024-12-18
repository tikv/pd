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
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/utils/typeutil"
)

// MetaStore contains meta information about a store.
type MetaStore struct {
	*metapb.Store
	StateName string `json:"state_name"`
}

// SlowTrend contains slow trend information about a store.
type SlowTrend struct {
	// CauseValue is the slow trend detecting raw input, it changes by the performance and pressure along time of the store.
	// The value itself is not important, what matter is:
	//   - The comparison result from store to store.
	//   - The change magnitude along time (represented by CauseRate).
	// Currently, it's one of store's internal latency (duration of waiting in the task queue of raftstore.store).
	CauseValue float64 `json:"cause_value"`
	// CauseRate is for measuring the change magnitude of CauseValue of the store,
	//   - CauseRate > 0 means the store is become slower currently
	//   - CauseRate < 0 means the store is become faster currently
	//   - CauseRate == 0 means the store's performance and pressure does not have significant changes
	CauseRate float64 `json:"cause_rate"`
	// ResultValue is the current gRPC QPS of the store.
	ResultValue float64 `json:"result_value"`
	// ResultRate is for measuring the change magnitude of ResultValue of the store.
	ResultRate float64 `json:"result_rate"`
}

// StoreStatus contains status about a store.
type StoreStatus struct {
	Capacity           typeutil.ByteSize  `json:"capacity"`
	Available          typeutil.ByteSize  `json:"available"`
	UsedSize           typeutil.ByteSize  `json:"used_size"`
	LeaderCount        int                `json:"leader_count"`
	LeaderWeight       float64            `json:"leader_weight"`
	LeaderScore        float64            `json:"leader_score"`
	LeaderSize         int64              `json:"leader_size"`
	RegionCount        int                `json:"region_count"`
	RegionWeight       float64            `json:"region_weight"`
	RegionScore        float64            `json:"region_score"`
	RegionSize         int64              `json:"region_size"`
	LearnerCount       int                `json:"learner_count,omitempty"`
	WitnessCount       int                `json:"witness_count,omitempty"`
	PendingPeerCount   int                `json:"pending_peer_count,omitempty"`
	SlowScore          uint64             `json:"slow_score,omitempty"`
	SlowTrend          *SlowTrend         `json:"slow_trend,omitempty"`
	SendingSnapCount   uint32             `json:"sending_snap_count,omitempty"`
	ReceivingSnapCount uint32             `json:"receiving_snap_count,omitempty"`
	IsBusy             bool               `json:"is_busy,omitempty"`
	StartTS            *time.Time         `json:"start_ts,omitempty"`
	LastHeartbeatTS    *time.Time         `json:"last_heartbeat_ts,omitempty"`
	Uptime             *typeutil.Duration `json:"uptime,omitempty"`
}

// StoreInfo contains information about a store.
type StoreInfo struct {
	Store  *MetaStore   `json:"store"`
	Status *StoreStatus `json:"status"`
}

const (
	// DisconnectedName is the name when store is disconnected.
	DisconnectedName = "Disconnected"
	// DownStateName is the name when store is down.
	DownStateName = "Down"
)

// BuildStoreInfo builds a storeInfo response.
func BuildStoreInfo(opt *sc.ScheduleConfig, store *core.StoreInfo) *StoreInfo {
	var slowTrend *SlowTrend
	coreSlowTrend := store.GetSlowTrend()
	if coreSlowTrend != nil {
		slowTrend = &SlowTrend{coreSlowTrend.CauseValue, coreSlowTrend.CauseRate, coreSlowTrend.ResultValue, coreSlowTrend.ResultRate}
	}
	s := &StoreInfo{
		Store: &MetaStore{
			Store:     store.GetMeta(),
			StateName: store.GetState().String(),
		},
		Status: &StoreStatus{
			Capacity:           typeutil.ByteSize(store.GetCapacity()),
			Available:          typeutil.ByteSize(store.GetAvailable()),
			UsedSize:           typeutil.ByteSize(store.GetUsedSize()),
			LeaderCount:        store.GetLeaderCount(),
			LeaderWeight:       store.GetLeaderWeight(),
			LeaderScore:        store.LeaderScore(constant.StringToSchedulePolicy(opt.LeaderSchedulePolicy), 0),
			LeaderSize:         store.GetLeaderSize(),
			RegionCount:        store.GetRegionCount(),
			RegionWeight:       store.GetRegionWeight(),
			RegionScore:        store.RegionScore(opt.RegionScoreFormulaVersion, opt.HighSpaceRatio, opt.LowSpaceRatio, 0),
			RegionSize:         store.GetRegionSize(),
			LearnerCount:       store.GetLearnerCount(),
			WitnessCount:       store.GetWitnessCount(),
			SlowScore:          store.GetSlowScore(),
			SlowTrend:          slowTrend,
			SendingSnapCount:   store.GetSendingSnapCount(),
			ReceivingSnapCount: store.GetReceivingSnapCount(),
			PendingPeerCount:   store.GetPendingPeerCount(),
			IsBusy:             store.IsBusy(),
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

	if store.GetState() == metapb.StoreState_Up {
		if store.DownTime() > opt.MaxStoreDownTime.Duration {
			s.Store.StateName = DownStateName
		} else if store.IsDisconnected() {
			s.Store.StateName = DisconnectedName
		}
	}
	return s
}

// StoresInfo records stores' info.
type StoresInfo struct {
	Count  int          `json:"count"`
	Stores []*StoreInfo `json:"stores"`
}
