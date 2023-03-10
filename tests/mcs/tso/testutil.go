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

package tso

import (
	"context"
	"os"
	"time"

	"github.com/pingcap/log"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/testutil"
	tsosvr "github.com/tikv/pd/pkg/mcs/tso/server"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/tempurl"
)

// CleanupFunc closes test tso server(s) and deletes any files left behind.
type CleanupFunc func()

// newTSOTestServer creates a tso server for testing.
func newTSOTestServer(ctx context.Context, re *require.Assertions, cfg *tsosvr.Config) (*tsosvr.Server, CleanupFunc, error) {
	// New zap logger
	err := logutil.SetupLogger(cfg.Log, &cfg.Logger, &cfg.LogProps, cfg.Security.RedactInfoLog)
	re.NoError(err)
	log.ReplaceGlobals(cfg.Logger, cfg.LogProps)
	// Flushing any buffered log entries
	defer log.Sync()

	s := tsosvr.CreateServer(ctx, cfg)
	if err = s.Run(); err != nil {
		return nil, nil, err
	}

	cleanup := func() {
		s.Close()
		os.RemoveAll(cfg.DataDir)
	}
	return s, cleanup, nil
}

// newTSOTestDefaultConfig is only for test to create one pd.
// Because PD client also needs this, so export here.
func newTSOTestDefaultConfig() (*tsosvr.Config, error) {
	cmd := &cobra.Command{
		Use:   "tso",
		Short: "Run the tso service",
	}
	cfg := tsosvr.NewConfig()
	flagSet := cmd.Flags()
	return cfg, cfg.Parse(flagSet)
}

// startTSOTestServer creates and starts a tso server with default config for testing.
func startSingleTSOTestServer(ctx context.Context, re *require.Assertions, backendEndpoints string) (*tsosvr.Server, CleanupFunc, error) {
	cfg, err := newTSOTestDefaultConfig()
	re.NoError(err)
	cfg.BackendEndpoints = backendEndpoints
	cfg.ListenAddr = tempurl.Alloc()

	s, cleanup, err := newTSOTestServer(ctx, re, cfg)
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		return s.IsServing()
	}, testutil.WithWaitFor(5*time.Second), testutil.WithTickInterval(50*time.Millisecond))

	return s, cleanup, err
}

func setupCli(re *require.Assertions, ctx context.Context, endpoints []string, opts ...pd.ClientOption) pd.Client {
	// TODO: we use keyspace 0 as the default keyspace for now, which mightn't need change in the future
	cli, err := pd.NewTSOClientWithContext(ctx, 0, endpoints, pd.SecurityOption{}, opts...)
	re.NoError(err)
	return cli
}
