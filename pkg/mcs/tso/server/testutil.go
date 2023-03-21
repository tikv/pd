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
	"strings"

	"github.com/pingcap/kvproto/pkg/tsopb"
	"github.com/pingcap/log"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"google.golang.org/grpc"
)

// NewTSOTestServer creates a tso server for testing.
func NewTSOTestServer(ctx context.Context, re *require.Assertions, cfg *Config) (*Server, testutil.CleanupFunc, error) {
	// New zap logger
	err := logutil.SetupLogger(cfg.Log, &cfg.Logger, &cfg.LogProps, cfg.Security.RedactInfoLog)
	re.NoError(err)
	log.ReplaceGlobals(cfg.Logger, cfg.LogProps)
	// Flushing any buffered log entries
	defer log.Sync()

	s := CreateServer(ctx, cfg)
	if err = s.Run(); err != nil {
		return nil, nil, err
	}

	cleanup := func() {
		s.Close()
		os.RemoveAll(cfg.DataDir)
	}
	return s, cleanup, nil
}

// NewTSOTestDefaultConfig is only for test to create one pd.
// Because PD client also needs this, so export here.
func NewTSOTestDefaultConfig() (*Config, error) {
	cmd := &cobra.Command{
		Use:   "tso",
		Short: "Run the tso service",
	}
	cfg := NewConfig()
	flagSet := cmd.Flags()
	return cfg, cfg.Parse(flagSet)
}

// MustNewGrpcClient must create a new TSO grpc client.
func MustNewGrpcClient(re *require.Assertions, addr string) (*grpc.ClientConn, tsopb.TSOClient) {
	conn, err := grpc.Dial(strings.TrimPrefix(addr, "http://"), grpc.WithInsecure())
	re.NoError(err)
	return conn, tsopb.NewTSOClient(conn)
}
