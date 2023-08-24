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

package tests

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	bs "github.com/tikv/pd/pkg/basicserver"
	rm "github.com/tikv/pd/pkg/mcs/resourcemanager/server"
	scheduling "github.com/tikv/pd/pkg/mcs/scheduling/server"
	sc "github.com/tikv/pd/pkg/mcs/scheduling/server/config"
	tso "github.com/tikv/pd/pkg/mcs/tso/server"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/testutil"
)

var once sync.Once

// InitSchedulingLogger initializes the logger for test.
func InitSchedulingLogger(cfg *sc.Config) (err error) {
	once.Do(func() {
		// Setup the logger.
		err = logutil.SetupLogger(cfg.Log, &cfg.Logger, &cfg.LogProps, cfg.Security.RedactInfoLog)
		if err != nil {
			return
		}
		log.ReplaceGlobals(cfg.Logger, cfg.LogProps)
		// Flushing any buffered log entries.
		log.Sync()
	})
	return err
}

// InitTSOLogger initializes the logger for test.
func InitTSOLogger(cfg *tso.Config) (err error) {
	once.Do(func() {
		// Setup the logger.
		err = logutil.SetupLogger(cfg.Log, &cfg.Logger, &cfg.LogProps, cfg.Security.RedactInfoLog)
		if err != nil {
			return
		}
		log.ReplaceGlobals(cfg.Logger, cfg.LogProps)
		// Flushing any buffered log entries.
		log.Sync()
	})
	return err
}

// StartSingleResourceManagerTestServer creates and starts a resource manager server with default config for testing.
func StartSingleResourceManagerTestServer(ctx context.Context, re *require.Assertions, backendEndpoints, listenAddrs string) (*rm.Server, func()) {
	cfg := rm.NewConfig()
	cfg.BackendEndpoints = backendEndpoints
	cfg.ListenAddr = listenAddrs
	cfg, err := rm.GenerateConfig(cfg)
	re.NoError(err)

	s, cleanup, err := rm.NewTestServer(ctx, re, cfg)
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		return !s.IsClosed()
	}, testutil.WithWaitFor(5*time.Second), testutil.WithTickInterval(50*time.Millisecond))

	return s, cleanup
}

// StartSingleTSOTestServerWithoutCheck creates and starts a tso server with default config for testing.
func StartSingleTSOTestServerWithoutCheck(ctx context.Context, re *require.Assertions, backendEndpoints, listenAddrs string) (*tso.Server, func(), error) {
	cfg := tso.NewConfig()
	cfg.BackendEndpoints = backendEndpoints
	cfg.ListenAddr = listenAddrs
	cfg, err := tso.GenerateConfig(cfg)
	re.NoError(err)
	// Setup the logger.
	err = InitTSOLogger(cfg)
	re.NoError(err)
	return NewTSOTestServer(ctx, cfg)
}

// StartSingleTSOTestServer creates and starts a tso server with default config for testing.
func StartSingleTSOTestServer(ctx context.Context, re *require.Assertions, backendEndpoints, listenAddrs string) (*tso.Server, func()) {
	s, cleanup, err := StartSingleTSOTestServerWithoutCheck(ctx, re, backendEndpoints, listenAddrs)
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		return !s.IsClosed()
	}, testutil.WithWaitFor(5*time.Second), testutil.WithTickInterval(50*time.Millisecond))

	return s, cleanup
}

// NewTSOTestServer creates a tso server with given config for testing.
func NewTSOTestServer(ctx context.Context, cfg *tso.Config) (*tso.Server, testutil.CleanupFunc, error) {
	s := tso.CreateServer(ctx, cfg)
	if err := s.Run(); err != nil {
		return nil, nil, err
	}
	cleanup := func() {
		s.Close()
		os.RemoveAll(cfg.DataDir)
	}
	return s, cleanup, nil
}

// StartSingleSchedulingTestServer creates and starts a scheduling server with default config for testing.
func StartSingleSchedulingTestServer(ctx context.Context, re *require.Assertions, backendEndpoints, listenAddrs string) (*scheduling.Server, func()) {
	cfg := sc.NewConfig()
	cfg.BackendEndpoints = backendEndpoints
	cfg.ListenAddr = listenAddrs
	cfg, err := scheduling.GenerateConfig(cfg)
	re.NoError(err)

	s, cleanup, err := scheduling.NewTestServer(ctx, re, cfg)
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		return !s.IsClosed()
	}, testutil.WithWaitFor(5*time.Second), testutil.WithTickInterval(50*time.Millisecond))

	return s, cleanup
}

// NewSchedulingTestServer creates a scheduling server with given config for testing.
func NewSchedulingTestServer(ctx context.Context, cfg *sc.Config) (*scheduling.Server, testutil.CleanupFunc, error) {
	s := scheduling.CreateServer(ctx, cfg)
	if err := s.Run(); err != nil {
		return nil, nil, err
	}
	cleanup := func() {
		s.Close()
		os.RemoveAll(cfg.DataDir)
	}
	return s, cleanup, nil
}

// WaitForPrimaryServing waits for one of servers being elected to be the primary/leader
func WaitForPrimaryServing(re *require.Assertions, serverMap map[string]bs.Server) string {
	var primary string
	testutil.Eventually(re, func() bool {
		for name, s := range serverMap {
			if s.IsServing() {
				primary = name
				return true
			}
		}
		return false
	}, testutil.WithWaitFor(5*time.Second), testutil.WithTickInterval(50*time.Millisecond))

	return primary
}
