// Copyright 2025 TiKV Project Authors.
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

	rmserver "github.com/tikv/pd/pkg/mcs/resourcemanager/server"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"github.com/tikv/pd/pkg/utils/testutil"
)

// TestResourceManagerCluster is a test cluster for Resource Manager.
type TestResourceManagerCluster struct {
	ctx context.Context

	backendEndpoints string
	servers          map[string]*rmserver.Server
	cleanupFuncs     map[string]testutil.CleanupFunc
}

// NewTestResourceManagerCluster creates a new Resource Manager test cluster.
func NewTestResourceManagerCluster(ctx context.Context, initialServerCount int, backendEndpoints string) (tc *TestResourceManagerCluster, err error) {
	tc = &TestResourceManagerCluster{
		ctx:              ctx,
		backendEndpoints: backendEndpoints,
		servers:          make(map[string]*rmserver.Server, initialServerCount),
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

// AddServer adds a new Resource Manager server to the test cluster.
func (tc *TestResourceManagerCluster) AddServer(addr string) error {
	cfg := rmserver.NewConfig()
	cfg.BackendEndpoints = tc.backendEndpoints
	cfg.ListenAddr = addr
	cfg.Name = cfg.ListenAddr
	generatedCfg, err := rmserver.GenerateConfig(cfg)
	if err != nil {
		return err
	}
	err = InitLogger(generatedCfg.Log, generatedCfg.Logger, generatedCfg.LogProps, generatedCfg.Security.RedactInfoLog)
	if err != nil {
		return err
	}
	server, cleanup, err := NewResourceManagerTestServer(tc.ctx, generatedCfg)
	if err != nil {
		return err
	}
	tc.servers[generatedCfg.GetListenAddr()] = server
	tc.cleanupFuncs[generatedCfg.GetListenAddr()] = cleanup
	return nil
}

// Destroy stops and destroy the test cluster.
func (tc *TestResourceManagerCluster) Destroy() {
	for _, cleanup := range tc.cleanupFuncs {
		cleanup()
	}
	tc.cleanupFuncs = nil
	tc.servers = nil
}

// GetServers returns all Resource Manager servers.
func (tc *TestResourceManagerCluster) GetServers() map[string]*rmserver.Server {
	return tc.servers
}

// WaitForPrimaryServing waits for one of servers being elected to be the primary.
func (tc *TestResourceManagerCluster) WaitForPrimaryServing(re *require.Assertions) *rmserver.Server {
	var primary *rmserver.Server
	testutil.Eventually(re, func() bool {
		for _, server := range tc.servers {
			if server.IsServing() {
				primary = server
				return true
			}
		}
		return false
	}, testutil.WithWaitFor(30*time.Second), testutil.WithTickInterval(100*time.Millisecond))

	return primary
}
