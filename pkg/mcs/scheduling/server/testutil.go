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

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/mcs/scheduling/server/config"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/testutil"
)

// NewTestServer creates a resource manager server for testing.
func NewTestServer(ctx context.Context, re *require.Assertions, cfg *config.Config) (*Server, testutil.CleanupFunc, error) {
	// New zap logger
	err := logutil.SetupLogger(&cfg.Log, &cfg.Logger, &cfg.LogProps, cfg.Security.RedactInfoLog)
	re.NoError(err)
	log.ReplaceGlobals(cfg.Logger, cfg.LogProps)
	// Flushing any buffered log entries
	log.Sync()
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

// GenerateConfig generates a new config with the given options.
func GenerateConfig(c *config.Config) (*config.Config, error) {
	arguments := []string{
		"--name=" + c.Name,
		"--listen-addr=" + c.ListenAddr,
		"--advertise-listen-addr=" + c.AdvertiseListenAddr,
		"--backend-endpoints=" + c.BackendEndpoints,
	}

	flagSet := pflag.NewFlagSet("test", pflag.ContinueOnError)
	flagSet.StringP("name", "", "", "human-readable name for this scheduling member")
	flagSet.BoolP("version", "V", false, "print version information and exit")
	flagSet.StringP("config", "", "", "config file")
	flagSet.StringP("backend-endpoints", "", "", "url for etcd client")
	flagSet.StringP("listen-addr", "", "", "listen address for tso service")
	flagSet.StringP("advertise-listen-addr", "", "", "advertise urls for listen address (default '${listen-addr}')")
	flagSet.StringP("cacert", "", "", "path of file that contains list of trusted TLS CAs")
	flagSet.StringP("cert", "", "", "path of file that contains X509 certificate in PEM format")
	flagSet.StringP("key", "", "", "path of file that contains X509 key in PEM format")
	err := flagSet.Parse(arguments)
	if err != nil {
		return nil, err
	}
	cfg := config.NewConfig()
	err = cfg.Parse(flagSet)
	if err != nil {
		return nil, err
	}

	return cfg, nil
}
