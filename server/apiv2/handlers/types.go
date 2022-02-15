package handlers

import (
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
)

const (
	disconnectedName = "Disconnected"
	downStateName    = "Down"
)

// StoresInfo records stores' info.
type StoresInfo struct {
	Count  int          `json:"count"`
	Stores []*StoreInfo `json:"stores"`
}

// MetaStore contains meta information about a store.
type MetaStore struct {
	*metapb.Store
	StateName string `json:"state_name"`
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
	s := &StoreInfo{
		Store: &MetaStore{
			Store:     store.GetMeta(),
			StateName: store.GetState().String(),
		},
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

	if store.GetState() == metapb.StoreState_Up {
		if store.DownTime() > cfg.MaxStoreDownTime.Duration {
			s.Store.StateName = downStateName
		} else if store.IsDisconnected() {
			s.Store.StateName = disconnectedName
		}
	}
	return s
}
