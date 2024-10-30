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
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
	tso "github.com/tikv/pd/pkg/mcs/tso/server"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"github.com/tikv/pd/pkg/utils/testutil"
)

// TestTSOCluster is a test cluster for TSO.
type TestTSOCluster struct {
	ctx context.Context

	backendEndpoints string
	servers          map[string]*tso.Server
	cleanupFuncs     map[string]testutil.CleanupFunc
}

// NewTestTSOCluster creates a new TSO test cluster.
func NewTestTSOCluster(ctx context.Context, initialServerCount int, backendEndpoints string) (tc *TestTSOCluster, err error) {
	tc = &TestTSOCluster{
		ctx:              ctx,
		backendEndpoints: backendEndpoints,
		servers:          make(map[string]*tso.Server, initialServerCount),
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

// RestartTestTSOCluster restarts the TSO test cluster.
func RestartTestTSOCluster(
	ctx context.Context, cluster *TestTSOCluster,
) (newCluster *TestTSOCluster, err error) {
	newCluster = &TestTSOCluster{
		ctx:              ctx,
		backendEndpoints: cluster.backendEndpoints,
		servers:          make(map[string]*tso.Server, len(cluster.servers)),
		cleanupFuncs:     make(map[string]testutil.CleanupFunc, len(cluster.servers)),
	}
	var (
		serverMap  sync.Map
		cleanupMap sync.Map
		errorMap   sync.Map
	)
	wg := sync.WaitGroup{}
	for addr, cleanup := range cluster.cleanupFuncs {
		wg.Add(1)
		go func(addr string, clean testutil.CleanupFunc) {
			defer wg.Done()
			clean()
			serverCfg := cluster.servers[addr].GetConfig()
			newServer, newCleanup, err := NewTSOTestServer(ctx, serverCfg)
			serverMap.Store(addr, newServer)
			cleanupMap.Store(addr, newCleanup)
			errorMap.Store(addr, err)
		}(addr, cleanup)
	}
	wg.Wait()

	errorMap.Range(func(key, value any) bool {
		if value != nil {
			err = value.(error)
			return false
		}
		addr := key.(string)
		newServer, _ := serverMap.Load(addr)
		newCleanup, _ := cleanupMap.Load(addr)
		newCluster.servers[addr] = newServer.(*tso.Server)
		newCluster.cleanupFuncs[addr] = newCleanup.(testutil.CleanupFunc)
		return true
	})

	if err != nil {
		return nil, errors.New("failed to restart the cluster." + err.Error())
	}

	return newCluster, nil
}

// AddServer adds a new TSO server to the test cluster.
func (tc *TestTSOCluster) AddServer(addr string) error {
	cfg := tso.NewConfig()
	cfg.BackendEndpoints = tc.backendEndpoints
	cfg.ListenAddr = addr
	cfg.Name = cfg.ListenAddr
	generatedCfg, err := tso.GenerateConfig(cfg)
	if err != nil {
		return err
	}
	err = InitLogger(generatedCfg.Log, generatedCfg.Logger, generatedCfg.LogProps, generatedCfg.Security.RedactInfoLog)
	if err != nil {
		return err
	}
	server, cleanup, err := NewTSOTestServer(tc.ctx, generatedCfg)
	if err != nil {
		return err
	}
	tc.servers[generatedCfg.GetListenAddr()] = server
	tc.cleanupFuncs[generatedCfg.GetListenAddr()] = cleanup
	return nil
}

// Destroy stops and destroy the test cluster.
func (tc *TestTSOCluster) Destroy() {
	for _, cleanup := range tc.cleanupFuncs {
		cleanup()
	}
	tc.cleanupFuncs = nil
	tc.servers = nil
}

// DestroyServer stops and destroy the test server by the given address.
func (tc *TestTSOCluster) DestroyServer(addr string) {
	tc.cleanupFuncs[addr]()
	delete(tc.cleanupFuncs, addr)
	delete(tc.servers, addr)
}

// ResignPrimary resigns the primary TSO server.
func (tc *TestTSOCluster) ResignPrimary(keyspaceID, keyspaceGroupID uint32) error {
	primaryServer := tc.GetPrimaryServer(keyspaceID, keyspaceGroupID)
	if primaryServer == nil {
		return fmt.Errorf("no tso server serves this keyspace %d", keyspaceID)
	}
	return primaryServer.ResignPrimary(keyspaceID, keyspaceGroupID)
}

// GetPrimaryServer returns the primary TSO server of the given keyspace
func (tc *TestTSOCluster) GetPrimaryServer(keyspaceID, keyspaceGroupID uint32) *tso.Server {
	for _, server := range tc.servers {
		if server.IsKeyspaceServing(keyspaceID, keyspaceGroupID) {
			return server
		}
	}
	return nil
}

// WaitForPrimaryServing waits for one of servers being elected to be the primary/leader of the given keyspace.
func (tc *TestTSOCluster) WaitForPrimaryServing(re *require.Assertions, keyspaceID, keyspaceGroupID uint32) *tso.Server {
	var primary *tso.Server
	testutil.Eventually(re, func() bool {
		for _, server := range tc.servers {
			if server.IsKeyspaceServing(keyspaceID, keyspaceGroupID) {
				primary = server
				return true
			}
		}
		return false
	}, testutil.WithWaitFor(30*time.Second), testutil.WithTickInterval(100*time.Millisecond))

	return primary
}

// WaitForDefaultPrimaryServing waits for one of servers being elected to be the primary/leader of the default keyspace.
func (tc *TestTSOCluster) WaitForDefaultPrimaryServing(re *require.Assertions) *tso.Server {
	return tc.WaitForPrimaryServing(re, constant.DefaultKeyspaceID, constant.DefaultKeyspaceGroupID)
}

// GetServer returns the TSO server by the given address.
func (tc *TestTSOCluster) GetServer(addr string) *tso.Server {
	for srvAddr, server := range tc.servers {
		if srvAddr == addr {
			return server
		}
	}
	return nil
}

// GetServers returns all TSO servers.
func (tc *TestTSOCluster) GetServers() map[string]*tso.Server {
	return tc.servers
}

// GetKeyspaceGroupMember converts the TSO servers to KeyspaceGroupMember and returns.
func (tc *TestTSOCluster) GetKeyspaceGroupMember() (members []endpoint.KeyspaceGroupMember) {
	for _, server := range tc.servers {
		members = append(members, endpoint.KeyspaceGroupMember{
			Address:  server.GetAddr(),
			Priority: constant.DefaultKeyspaceGroupReplicaPriority,
		})
	}
	return
}

// GetAddrs returns all TSO server addresses.
func (tc *TestTSOCluster) GetAddrs() []string {
	addrs := make([]string, 0, len(tc.servers))
	for _, server := range tc.servers {
		addrs = append(addrs, server.GetAddr())
	}
	return addrs
}
