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
	"time"

	"github.com/stretchr/testify/require"
	scheduling "github.com/tikv/pd/pkg/mcs/scheduling/server"
	sc "github.com/tikv/pd/pkg/mcs/scheduling/server/config"
	"github.com/tikv/pd/pkg/schedule/schedulers"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"github.com/tikv/pd/pkg/utils/testutil"
)

// TestSchedulingCluster is a test cluster for scheduling.
type TestSchedulingCluster struct {
	ctx context.Context

	backendEndpoints string
	servers          map[string]*scheduling.Server
	cleanupFuncs     map[string]testutil.CleanupFunc
}

// NewTestSchedulingCluster creates a new scheduling test cluster.
func NewTestSchedulingCluster(ctx context.Context, initialServerCount int, backendEndpoints string) (tc *TestSchedulingCluster, err error) {
	schedulers.Register()
	tc = &TestSchedulingCluster{
		ctx:              ctx,
		backendEndpoints: backendEndpoints,
		servers:          make(map[string]*scheduling.Server, initialServerCount),
		cleanupFuncs:     make(map[string]testutil.CleanupFunc, initialServerCount),
	}
	for range initialServerCount {
		err = tc.AddServer(tempurl.Alloc())
		if err != nil {
			return nil, err
		}
	}
	return tc, nil
}

// AddServer adds a new scheduling server to the test cluster.
func (tc *TestSchedulingCluster) AddServer(addr string) error {
	cfg := sc.NewConfig()
	cfg.BackendEndpoints = tc.backendEndpoints
	cfg.ListenAddr = addr
	cfg.Name = cfg.ListenAddr
	generatedCfg, err := scheduling.GenerateConfig(cfg)
	if err != nil {
		return err
	}
	err = InitLogger(generatedCfg.Log, generatedCfg.Logger, generatedCfg.LogProps, generatedCfg.Security.RedactInfoLog)
	if err != nil {
		return err
	}
	server, cleanup, err := NewSchedulingTestServer(tc.ctx, generatedCfg)
	if err != nil {
		return err
	}
	tc.servers[generatedCfg.GetListenAddr()] = server
	tc.cleanupFuncs[generatedCfg.GetListenAddr()] = cleanup
	return nil
}

// Destroy stops and destroy the test cluster.
func (tc *TestSchedulingCluster) Destroy() {
	for _, cleanup := range tc.cleanupFuncs {
		cleanup()
	}
	tc.cleanupFuncs = nil
	tc.servers = nil
}

// DestroyServer stops and destroy the test server by the given address.
func (tc *TestSchedulingCluster) DestroyServer(addr string) {
	tc.cleanupFuncs[addr]()
	delete(tc.cleanupFuncs, addr)
	delete(tc.servers, addr)
}

// GetPrimaryServer returns the primary scheduling server.
func (tc *TestSchedulingCluster) GetPrimaryServer() *scheduling.Server {
	for _, server := range tc.servers {
		if server.IsServing() {
			return server
		}
	}
	return nil
}

// WaitForPrimaryServing waits for one of servers being elected to be the primary/leader of the given keyspace.
func (tc *TestSchedulingCluster) WaitForPrimaryServing(re *require.Assertions) *scheduling.Server {
	var primary *scheduling.Server
	testutil.Eventually(re, func() bool {
		for _, server := range tc.servers {
			if server.IsServing() {
				primary = server
				return true
			}
		}
		return false
	}, testutil.WithWaitFor(10*time.Second), testutil.WithTickInterval(50*time.Millisecond))

	return primary
}

// GetServer returns the scheduling server by the given address.
func (tc *TestSchedulingCluster) GetServer(addr string) *scheduling.Server {
	for srvAddr, server := range tc.servers {
		if srvAddr == addr {
			return server
		}
	}
	return nil
}

// GetServers returns all scheduling servers.
func (tc *TestSchedulingCluster) GetServers() map[string]*scheduling.Server {
	return tc.servers
}

// GetAddrs returns all scheduling server addresses.
func (tc *TestSchedulingCluster) GetAddrs() []string {
	addrs := make([]string, 0, len(tc.servers))
	for _, server := range tc.servers {
		addrs = append(addrs, server.GetAddr())
	}
	return addrs
}
