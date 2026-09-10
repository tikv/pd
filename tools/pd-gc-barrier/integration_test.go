// Copyright 2026 TiKV Project Authors.
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

//go:build integration

package main

import (
	"bytes"
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/clients/gc"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

// TestBarrierControlsGC verifies the command against the actual release's PD
// server and client, including persistence after each command's client closes.
func TestBarrierControlsGC(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1, func(conf *config.Config, _ string) {
		conf.Keyspace.WaitRegionSplit = false
	})
	require.NoError(t, err)
	defer cluster.Destroy()
	require.NoError(t, cluster.RunInitialServers())
	require.NotEmpty(t, cluster.WaitLeader())
	server := cluster.GetLeaderServer()
	require.NoError(t, server.BootstrapCluster())
	ks, err := server.GetKeyspaceManager().CreateKeyspace(&keyspace.CreateKeyspaceRequest{
		Name:   "barrier-recovery",
		Config: map[string]string{keyspace.GCManagementType: keyspace.KeyspaceLevelGC},
	})
	require.NoError(t, err)
	other, err := server.GetKeyspaceManager().CreateKeyspace(&keyspace.CreateKeyspaceRequest{
		Name:   "unaffected",
		Config: map[string]string{keyspace.GCManagementType: keyspace.KeyspaceLevelGC},
	})
	require.NoError(t, err)
	client, err := pd.NewClientWithContext(ctx, "barrier-tool-test", []string{server.GetAddr()}, pd.SecurityOption{})
	require.NoError(t, err)
	defer client.Close()
	for _, id := range []uint32{ks.GetId(), other.GetId()} {
		_, err = client.GetGCStatesClient(id).SetGCBarrier(ctx, "ticdc-old", 100, gc.TTLNeverExpire)
		require.NoError(t, err)
	}
	gcClient := client.GetGCStatesClient(ks.GetId())
	controller := client.GetGCInternalController(ks.GetId())
	assertLimit := func(want uint64) {
		result, err := controller.AdvanceTxnSafePoint(ctx, 1000)
		require.NoError(t, err)
		require.Equal(t, want, result.NewTxnSafePoint)
	}
	run := func(args ...string) string {
		cmd := newCommand(connectPD)
		var out bytes.Buffer
		cmd.SetOut(&out)
		cmd.SetArgs(append([]string{"--pd", server.GetAddr(), "--keyspace-id", strconv.FormatUint(uint64(ks.GetId()), 10)}, args...))
		require.NoError(t, cmd.ExecuteContext(ctx))
		return out.String()
	}
	assertLimit(100)
	run("set", "recovery", "200", "--ttl=never")
	assertLimit(100)
	run("delete", "ticdc-old")
	assertLimit(200)
	run("set", "recovery", "300", "--ttl=never")
	assertLimit(300)
	state, err := gcClient.GetGCState(ctx)
	require.NoError(t, err)
	require.Len(t, state.GCBarriers, 1)
	require.Equal(t, gc.TTLNeverExpire, state.GCBarriers[0].TTL)
	require.Contains(t, run("show"), `"tso": "300"`)
	run("delete", "recovery")
	assertLimit(1000)
	state, err = client.GetGCStatesClient(other.GetId()).GetGCState(ctx)
	require.NoError(t, err)
	require.Len(t, state.GCBarriers, 1)
	require.Equal(t, "ticdc-old", state.GCBarriers[0].BarrierID)
	require.Equal(t, uint64(100), state.GCBarriers[0].BarrierTS)
}
