// Copyright 2026 TiKV Project Authors.
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
	"path/filepath"
	"testing"

	"github.com/docker/go-units"
	"github.com/stretchr/testify/require"
)

func TestParseWithoutConfigUsesDefaults(t *testing.T) {
	cfg := NewConfig()
	require.NoError(t, cfg.Parse(nil))
	require.Equal(t, defaultStoreCount, cfg.StoreCount)
	require.Equal(t, defaultRegionCount, cfg.RegionCount)
	require.Equal(t, defaultReplica, cfg.Replica)
	require.Equal(t, uint64(defaultRegionSize), uint64(cfg.RegionSize))
	require.Equal(t, uint64(defaultStoreCapacity), uint64(cfg.StoreCapacity))
}

func TestParseConfigTemplate(t *testing.T) {
	cfg := NewConfig()
	require.NoError(t, cfg.Parse([]string{"--config", filepath.Join("..", "config-template.toml")}))
	require.Equal(t, 2_000_000, cfg.RegionCount)
	require.Equal(t, uint64(96*units.MiB), uint64(cfg.RegionSize))
	require.Equal(t, uint64(8*units.TiB), uint64(cfg.StoreCapacity))
	require.Equal(t, 1.0, cfg.ReportRatio)
	require.Equal(t, 0.35, cfg.FlowUpdateRatio)
	require.False(t, cfg.DeleteOperators)
}

func TestValidateUpdateRatiosUseTotalRegionCount(t *testing.T) {
	cfg := &Config{
		InitEpochVer:      1,
		StoreCount:        10,
		RegionCount:       1_000,
		Replica:           3,
		RegionSize:        1,
		RegionKeys:        1,
		StoreCapacity:     1,
		ReportRatio:       0.1,
		LeaderUpdateRatio: 0.03,
		EpochUpdateRatio:  0.02,
		SpaceUpdateRatio:  0.04,
		FlowUpdateRatio:   0.05,
	}
	require.NoError(t, cfg.Validate())

	cfg.FlowUpdateRatio = 0.11
	require.ErrorContains(t, cfg.Validate(), "larger than report-ratio")
}
