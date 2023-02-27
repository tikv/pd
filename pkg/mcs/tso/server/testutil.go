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
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/utils/logutil"
)

// CleanupFunc closes test tso server(s) and deletes any files left behind.
type CleanupFunc func()

// NewTestServer creates a tso server for testing.
func newTestServer(ctx context.Context, cancel context.CancelFunc, re *require.Assertions, cfg *Config) (*Server, CleanupFunc, error) {
	// New zap logger
	err := logutil.SetupLogger(cfg.Log, &cfg.Logger, &cfg.LogProps, cfg.Security.RedactInfoLog)
	re.NoError(err)
	log.ReplaceGlobals(cfg.Logger, cfg.LogProps)
	// Flushing any buffered log entries
	defer log.Sync()

	s := CreateServer(ctx, cfg)
	if err = s.Run(); err != nil {
		cancel()
		return nil, nil, err
	}

	cleanup := func() {
		cancel()
		s.Close()
		os.RemoveAll(cfg.DataDir)
	}
	return s, cleanup, nil
}

// newTestDefaultConfig is only for test to create one pd.
// Because PD client also needs this, so export here.
func newTestDefaultConfig() (*Config, error) {
	cmd := NewTSOServiceCommand()
	cfg := NewConfig()
	flagSet := cmd.Flags()
	return cfg, cfg.Parse(flagSet)
}
