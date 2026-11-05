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
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/require"
)

func TestGRPCGatewayDefault(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprintf("legacy=%t", legacy), func(t *testing.T) {
			re := require.New(t)
			dataDir := filepath.Join(t.TempDir(), "data")
			walDir := filepath.Join(dataDir, "member", "wal")
			if legacy {
				re.NoError(os.MkdirAll(walDir, 0700))
			}
			load := func(setting string) *Config {
				cfg := NewConfig()
				cfg.DataDir = dataDir
				meta, err := toml.Decode(setting, cfg)
				re.NoError(err)
				re.NoError(cfg.Adjust(&meta, false))
				return cfg
			}
			cfg := load("")
			re.Equal(legacy, cfg.EnableGRPCGateway)
			_, err := os.Stat(filepath.Join(dataDir, grpcGatewayDefaultFile))
			re.True(os.IsNotExist(err), "configuration parsing must not write files")
			re.NoError(cfg.PersistGRPCGatewayDefault())
			// Once etcd has started, both old and new members have a WAL.
			re.NoError(os.MkdirAll(walDir, 0700))
			re.Equal(legacy, load("").EnableGRPCGateway)
			for _, explicit := range []bool{false, true} {
				cfg = load(fmt.Sprintf("enable-grpc-gateway = %t", explicit))
				re.Equal(explicit, cfg.EnableGRPCGateway)
				re.NoError(cfg.PersistGRPCGatewayDefault())
				// Removing an override restores the member's original default.
				re.Equal(legacy, load("").EnableGRPCGateway)
			}
		})
	}
}

func TestGRPCGatewayDefaultErrors(t *testing.T) {
	for _, content := range []string{"", "null", "{}", "false trailing"} {
		t.Run(content, func(t *testing.T) {
			re := require.New(t)
			cfg := NewConfig()
			cfg.DataDir = t.TempDir()
			re.NoError(os.WriteFile(filepath.Join(cfg.DataDir, grpcGatewayDefaultFile), []byte(content), 0600))
			re.Error(cfg.Adjust(nil, false))
			re.False(cfg.EnableGRPCGateway)
		})
	}
	t.Run("invalid WAL path", func(t *testing.T) {
		re := require.New(t)
		cfg := NewConfig()
		cfg.DataDir = t.TempDir()
		re.NoError(os.MkdirAll(filepath.Join(cfg.DataDir, "member"), 0700))
		re.NoError(os.WriteFile(filepath.Join(cfg.DataDir, "member", "wal"), nil, 0600))
		re.Error(cfg.Adjust(nil, false))
	})
	t.Run("invalid data directory", func(t *testing.T) {
		re := require.New(t)
		cfg := NewConfig()
		cfg.DataDir = filepath.Join(t.TempDir(), "not-a-directory")
		re.NoError(os.WriteFile(cfg.DataDir, nil, 0600))
		re.Error(cfg.PersistGRPCGatewayDefault())
	})
}
