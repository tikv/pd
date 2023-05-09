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

package mcs

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	bs "github.com/tikv/pd/pkg/basicserver"
	rm "github.com/tikv/pd/pkg/mcs/resource_manager/server"
	tso "github.com/tikv/pd/pkg/mcs/tso/server"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/tsoutil"
)

var once sync.Once

// InitLogger initializes the logger for test.
func InitLogger(cfg *tso.Config) (err error) {
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

// SetupClientWithDefaultKeyspaceName creates a TSO client with default keyspace name for test.
func SetupClientWithDefaultKeyspaceName(
	ctx context.Context, re *require.Assertions, endpoints []string, opts ...pd.ClientOption,
) pd.Client {
	cli, err := pd.NewClientWithKeyspaceName(ctx, "", endpoints, pd.SecurityOption{}, opts...)
	re.NoError(err)
	return cli
}

// SetupClientWithKeyspaceID creates a TSO client with the given keyspace id for test.
func SetupClientWithKeyspaceID(
	ctx context.Context, re *require.Assertions,
	keyspaceID uint32, endpoints []string, opts ...pd.ClientOption,
) pd.Client {
	cli, err := pd.NewClientWithKeyspace(ctx, keyspaceID, endpoints, pd.SecurityOption{}, opts...)
	re.NoError(err)
	return cli
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
	err = InitLogger(cfg)
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

// WaitForTSOServiceAvailable waits for the pd client being served by the tso server side
func WaitForTSOServiceAvailable(ctx context.Context, pdClient pd.Client) error {
	var err error
	for i := 0; i < 30; i++ {
		if _, _, err := pdClient.GetTS(ctx); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(100 * time.Millisecond):
		}
	}
	return errors.WithStack(err)
}

// CheckMultiKeyspacesTSO checks the correctness of TSO for multiple keyspaces.
func CheckMultiKeyspacesTSO(
	ctx context.Context, re *require.Assertions,
	clients []pd.Client, parallelAct func(),
) {
	ctx, cancel := context.WithCancel(ctx)
	wg := sync.WaitGroup{}
	wg.Add(len(clients))

	for _, client := range clients {
		go func(cli pd.Client) {
			defer wg.Done()
			var ts, lastTS uint64
			for {
				select {
				case <-ctx.Done():
					// Make sure the lastTS is not empty
					re.NotEmpty(lastTS)
					return
				default:
				}
				physical, logical, err := cli.GetTS(ctx)
				// omit the error check since there are many kinds of errors
				if err != nil {
					continue
				}
				ts = tsoutil.ComposeTS(physical, logical)
				re.Less(lastTS, ts)
				lastTS = ts
			}
		}(client)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		parallelAct()
		cancel()
	}()

	wg.Wait()
}

// WaitForMultiKeyspacesTSOAvailable waits for the given keyspaces being served by the tso server side
func WaitForMultiKeyspacesTSOAvailable(
	ctx context.Context, re *require.Assertions,
	keyspaceIDs []uint32, backendEndpoints []string,
) []pd.Client {
	wg := sync.WaitGroup{}
	wg.Add(len(keyspaceIDs))

	clients := make([]pd.Client, 0, len(keyspaceIDs))
	for _, keyspaceID := range keyspaceIDs {
		cli := SetupClientWithKeyspaceID(ctx, re, keyspaceID, backendEndpoints)
		re.NotNil(cli)
		clients = append(clients, cli)

		go func() {
			defer wg.Done()
			testutil.Eventually(re, func() bool {
				_, _, err := cli.GetTS(ctx)
				return err == nil
			})
		}()
	}

	wg.Wait()
	return clients
}
