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
	"context"
	"os"

	"github.com/pingcap/log"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/testutil"
)

// NewTestServer creates a resource manager server for testing.
func NewTestServer(ctx context.Context, re *require.Assertions, cfg *Config) (*Server, testutil.CleanupFunc, error) {
	// New zap logger
	err := logutil.SetupLogger(cfg.Log, &cfg.Logger, &cfg.LogProps, cfg.Security.RedactInfoLog)
	re.NoError(err)
	log.ReplaceGlobals(cfg.Logger, cfg.LogProps)
	// Flushing any buffered log entries
	defer log.Sync()

	s := NewServer(ctx, cfg)
	if err = s.Run(); err != nil {
		return nil, nil, err
	}

	cleanup := func() {
		s.Close()
		os.RemoveAll(cfg.DataDir)
	}
	return s, cleanup, nil
}

// NewTestDefaultConfig creates a new default config for testing.
func NewTestDefaultConfig() (*Config, error) {
	cmd := &cobra.Command{
		Use:   "resource-manager",
		Short: "Run the resource manager service",
	}
	cfg := NewConfig()
	flagSet := cmd.Flags()
	return cfg, cfg.Parse(flagSet)
}
