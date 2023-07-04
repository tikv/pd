// Copyright 2023 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,g
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/require"
)

func TestAdjust(t *testing.T) {
	re := require.New(t)
	cfgData := `
enable-one-way-merge = true
leader-schedule-limit = 0
max-snapshot-count = 10
max-pending-peer-count = 128
leader-schedule-policy = "size"
region-schedule-limit = 16
max-movable-hot-peer-size = 100
halt-scheduling = true
region-score-formula-version = "v1"
hot-region-schedule-limit = 10
hot-region-cache-hits-threshold = 10
tolerant-size-ratio = 0.1
scheduler-max-waiting-operator = 100
slow-store-evicting-affected-store-ratio-threshold = 0.1
low-space-ratio = 0.99
high-space-ratio = 0.98
`

	cfg := NewConfig()
	meta, err := toml.Decode(cfgData, &cfg)
	re.NoError(err)
	err = cfg.Adjust(&meta, false)
	re.NoError(err)

	re.True(cfg.EnableOneWayMerge)
	re.Equal(uint64(0), cfg.LeaderScheduleLimit)
	re.Equal(uint64(10), cfg.MaxSnapshotCount)
	re.Equal(uint64(128), cfg.MaxPendingPeerCount)
	re.Equal("size", cfg.LeaderSchedulePolicy)
	re.Equal(uint64(16), cfg.RegionScheduleLimit)
	re.Equal(uint64(100), cfg.MaxMovableHotPeerSize)
	re.True(cfg.HaltScheduling)
	re.Equal("v1", cfg.RegionScoreFormulaVersion)
	re.Equal(uint64(10), cfg.HotRegionScheduleLimit)
	re.Equal(uint64(10), cfg.HotRegionCacheHitsThreshold)
	re.Equal(float64(0.1), cfg.TolerantSizeRatio)
	re.Equal(uint64(100), cfg.SchedulerMaxWaitingOperator)
	re.Equal(float64(0.1), cfg.SlowStoreEvictingAffectedStoreRatioThreshold)
	re.Equal(float64(0.99), cfg.LowSpaceRatio)
	re.Equal(float64(0.98), cfg.HighSpaceRatio)
}
