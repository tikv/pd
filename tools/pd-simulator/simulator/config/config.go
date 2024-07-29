// Copyright 2021 TiKV Project Authors.
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

package config

import (
	"fmt"
	"os"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/docker/go-units"
	pdHttp "github.com/tikv/pd/client/http"
	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/utils/configutil"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/pkg/versioninfo"
	"github.com/tikv/pd/server/config"
)

const (
	// simulator
	defaultSimTickInterval             = 100 * time.Millisecond
	defaultTotalStore                  = 3
	defaultTotalRegion                 = 1000
	defaultEnableTransferRegionCounter = false
	defaultHibernatePercent            = 0
	// store
	defaultStoreIOMBPerSecond = 40
	defaultStoreHeartbeat     = 10 * time.Second
	defaultRegionHeartbeat    = 1 * time.Minute
	defaultRegionSplitKeys    = 960000
	defaultRegionSplitSize    = 96 * units.MiB
	defaultCapacity           = 1 * units.TiB
	defaultExtraUsedSpace     = 0
	// TSO Proxy related
	defaultMaxConcurrentTSOProxyStreamings = 5000
	// server
	defaultLeaderLease                 = 3
	defaultTSOSaveInterval             = 200 * time.Millisecond
	defaultTickInterval                = 100 * time.Millisecond
	defaultElectionInterval            = 3 * time.Second
	defaultLeaderPriorityCheckInterval = 100 * time.Millisecond
)

// SimConfig is the simulator configuration.
type SimConfig struct {
	// Simulator
	CaseName                    string            `toml:"case-name"`
	TotalStore                  int               `toml:"total-store"`
	TotalRegion                 int               `toml:"total-region"`
	EnableTransferRegionCounter bool              `toml:"enable-transfer-region-counter"`
	SimTickInterval             typeutil.Duration `toml:"sim-tick-interval"`
	HibernatePercent            int               `toml:"hibernate-percent"`
	// store
	StoreIOMBPerSecond int64       `toml:"store-io-per-second"`
	StoreVersion       string      `toml:"store-version"`
	RaftStore          RaftStore   `toml:"raftstore"`
	Coprocessor        Coprocessor `toml:"coprocessor"`
	// server
	ServerConfig *config.Config `toml:"server"`
}

// RaftStore the configuration for raft store.
type RaftStore struct {
	Capacity                typeutil.ByteSize `toml:"capacity" json:"capacity"`
	ExtraUsedSpace          typeutil.ByteSize `toml:"extra-used-space" json:"extra-used-space"`
	RegionHeartBeatInterval typeutil.Duration `toml:"pd-heartbeat-tick-interval" json:"pd-heartbeat-tick-interval"`
	StoreHeartBeatInterval  typeutil.Duration `toml:"pd-store-heartbeat-tick-interval" json:"pd-store-heartbeat-tick-interval"`
}

// Coprocessor the configuration for coprocessor.
type Coprocessor struct {
	RegionSplitSize typeutil.ByteSize `toml:"region-split-size" json:"region-split-size"`
	RegionSplitKey  uint64            `toml:"region-split-keys" json:"region-split-keys"`
}

// NewSimConfig create a new configuration of the simulator.
func NewSimConfig(serverLogLevel string) *SimConfig {
	sc.DefaultStoreLimit = sc.StoreLimit{AddPeer: 2000, RemovePeer: 2000}
	cfg := &config.Config{
		Name:       "pd",
		ClientUrls: tempurl.Alloc(),
		PeerUrls:   tempurl.Alloc(),
	}

	cfg.AdvertiseClientUrls = cfg.ClientUrls
	cfg.AdvertisePeerUrls = cfg.PeerUrls
	cfg.DataDir, _ = os.MkdirTemp("/tmp", "test_pd")
	cfg.InitialCluster = fmt.Sprintf("pd=%s", cfg.PeerUrls)
	cfg.Log.Level = serverLogLevel
	return &SimConfig{ServerConfig: cfg}
}

// Adjust is used to adjust configurations
func (sc *SimConfig) Adjust(meta *toml.MetaData) error {
	configutil.AdjustDuration(&sc.SimTickInterval, defaultSimTickInterval)
	configutil.AdjustInt(&sc.TotalStore, defaultTotalStore)
	configutil.AdjustInt(&sc.TotalRegion, defaultTotalRegion)
	configutil.AdjustInt(&sc.HibernatePercent, defaultHibernatePercent)
	configutil.AdjustBool(&sc.EnableTransferRegionCounter, defaultEnableTransferRegionCounter)
	configutil.AdjustInt64(&sc.StoreIOMBPerSecond, defaultStoreIOMBPerSecond)
	configutil.AdjustString(&sc.StoreVersion, versioninfo.PDReleaseVersion)
	configutil.AdjustDuration(&sc.RaftStore.RegionHeartBeatInterval, defaultRegionHeartbeat)
	configutil.AdjustDuration(&sc.RaftStore.StoreHeartBeatInterval, defaultStoreHeartbeat)
	configutil.AdjustByteSize(&sc.RaftStore.Capacity, defaultCapacity)
	configutil.AdjustByteSize(&sc.RaftStore.ExtraUsedSpace, defaultExtraUsedSpace)
	configutil.AdjustUint64(&sc.Coprocessor.RegionSplitKey, defaultRegionSplitKeys)
	configutil.AdjustByteSize(&sc.Coprocessor.RegionSplitSize, defaultRegionSplitSize)

	configutil.AdjustInt(&sc.ServerConfig.MaxConcurrentTSOProxyStreamings, defaultMaxConcurrentTSOProxyStreamings)

	configutil.AdjustInt64(&sc.ServerConfig.LeaderLease, defaultLeaderLease)
	configutil.AdjustDuration(&sc.ServerConfig.TSOSaveInterval, defaultTSOSaveInterval)
	configutil.AdjustDuration(&sc.ServerConfig.TickInterval, defaultTickInterval)
	configutil.AdjustDuration(&sc.ServerConfig.ElectionInterval, defaultElectionInterval)
	configutil.AdjustDuration(&sc.ServerConfig.LeaderPriorityCheckInterval, defaultLeaderPriorityCheckInterval)

	return sc.ServerConfig.Adjust(meta, false)
}
func (sc *SimConfig) Speed() uint64 {
	return uint64(time.Second / sc.SimTickInterval.Duration)
}

// PDConfig saves some config which may be changed in PD.
type PDConfig struct {
	PlacementRules []*pdHttp.Rule
	LocationLabels typeutil.StringSlice
}
