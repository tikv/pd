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

package server

import (
	"strings"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/mcs/utils/constant"
)

func TestConfigBasic(t *testing.T) {
	re := require.New(t)

	cfg := NewConfig()
	cfg, err := GenerateConfig(cfg)
	re.NoError(err)

	// Test default values.
	re.True(strings.HasPrefix(cfg.GetName(), defaultName))
	re.Equal(defaultBackendEndpoints, cfg.BackendEndpoints)
	re.Equal(defaultListenAddr, cfg.ListenAddr)
	re.Equal(constant.DefaultLeaderLease, cfg.LeaderLease)
	re.True(cfg.EnableGRPCGateway)
	re.Equal(defaultTSOSaveInterval, cfg.TSOSaveInterval.Duration)
	re.Equal(defaultTSOUpdatePhysicalInterval, cfg.TSOUpdatePhysicalInterval.Duration)
	re.Equal(defaultMaxResetTSGap, cfg.MaxResetTSGap.Duration)

	// Test setting values.
	cfg.Name = "test-name"
	cfg.BackendEndpoints = "test-endpoints"
	cfg.ListenAddr = "test-listen-addr"
	cfg.AdvertiseListenAddr = "test-advertise-listen-addr"
	cfg.LeaderLease = 123
	cfg.TSOSaveInterval.Duration = time.Duration(10) * time.Second
	cfg.TSOUpdatePhysicalInterval.Duration = time.Duration(100) * time.Millisecond
	cfg.MaxResetTSGap.Duration = time.Duration(1) * time.Hour

	re.Equal("test-name", cfg.GetName())
	re.Equal("test-endpoints", cfg.GeBackendEndpoints())
	re.Equal("test-listen-addr", cfg.GetListenAddr())
	re.Equal("test-advertise-listen-addr", cfg.GetAdvertiseListenAddr())
	re.Equal(int64(123), cfg.GetLeaderLease())
	re.Equal(time.Duration(10)*time.Second, cfg.TSOSaveInterval.Duration)
	re.Equal(time.Duration(100)*time.Millisecond, cfg.TSOUpdatePhysicalInterval.Duration)
	re.Equal(time.Duration(1)*time.Hour, cfg.MaxResetTSGap.Duration)
}

func TestLoadFromConfig(t *testing.T) {
	re := require.New(t)
	cfgData := `
backend-endpoints = "test-endpoints"
listen-addr = "test-listen-addr"
advertise-listen-addr = "test-advertise-listen-addr"
name = "tso-test-name"
data-dir = "/var/lib/tso"
enable-grpc-gateway = false
lease = 123
tso-save-interval = "10s"
tso-update-physical-interval = "100ms"
max-gap-reset-ts = "1h"
`

	cfg := NewConfig()
	meta, err := toml.Decode(cfgData, &cfg)
	re.NoError(err)
	err = cfg.Adjust(&meta)
	re.NoError(err)

	re.Equal("tso-test-name", cfg.GetName())
	re.Equal("test-endpoints", cfg.GeBackendEndpoints())
	re.Equal("test-listen-addr", cfg.GetListenAddr())
	re.Equal("test-advertise-listen-addr", cfg.GetAdvertiseListenAddr())
	re.Equal("/var/lib/tso", cfg.DataDir)
	re.Equal(int64(123), cfg.GetLeaderLease())
	re.Equal(time.Duration(10)*time.Second, cfg.TSOSaveInterval.Duration)
	re.Equal(time.Duration(100)*time.Millisecond, cfg.TSOUpdatePhysicalInterval.Duration)
	re.Equal(time.Duration(1)*time.Hour, cfg.MaxResetTSGap.Duration)
}
