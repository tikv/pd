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

package safepoint_test

import (
	"bytes"
	"encoding/json"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/versioninfo/kerneltype"
	"github.com/tikv/pd/server/config"
	pdTests "github.com/tikv/pd/tests"
	ctl "github.com/tikv/pd/tools/pd-ctl/pdctl"
	"github.com/tikv/pd/tools/pd-ctl/tests"
)

type gcStateCommandBarrier struct {
	BarrierID  string `json:"barrier_id"`
	BarrierTS  uint64 `json:"barrier_ts"`
	TTLSeconds int64  `json:"ttl_seconds"`
}

type gcStateCommandSingle struct {
	RequestedKeyspaceID uint32                  `json:"requested_keyspace_id"`
	EffectiveKeyspaceID uint32                  `json:"effective_keyspace_id"`
	IsKeyspaceLevelGC   bool                    `json:"is_keyspace_level_gc"`
	TxnSafePoint        uint64                  `json:"txn_safe_point"`
	GCSafePoint         uint64                  `json:"gc_safe_point"`
	GCBarriers          []gcStateCommandBarrier `json:"gc_barriers"`
}

type gcStateCommandState struct {
	KeyspaceID        uint32                  `json:"keyspace_id"`
	IsKeyspaceLevelGC bool                    `json:"is_keyspace_level_gc"`
	TxnSafePoint      uint64                  `json:"txn_safe_point"`
	GCSafePoint       uint64                  `json:"gc_safe_point"`
	GCBarriers        []gcStateCommandBarrier `json:"gc_barriers"`
}

type gcStateCommandAll struct {
	GCStates         []gcStateCommandState   `json:"gc_states"`
	GlobalGCBarriers []gcStateCommandBarrier `json:"global_gc_barriers"`
}

type gcStateCommandGlobal struct {
	GlobalGCBarriers []gcStateCommandBarrier `json:"global_gc_barriers"`
}

type expectedGCStateCommandBarrier struct {
	barrierID string
	barrierTS uint64
	expires   bool
	expired   bool
}

func requireGCStateCommandBarriers(
	re *require.Assertions,
	actual []gcStateCommandBarrier,
	expected []expectedGCStateCommandBarrier,
) {
	re.Len(actual, len(expected))
	for i, want := range expected {
		re.Equal(want.barrierID, actual[i].BarrierID)
		re.Equal(want.barrierTS, actual[i].BarrierTS)
		if want.expired {
			re.Zero(actual[i].TTLSeconds)
		} else if want.expires {
			re.Greater(actual[i].TTLSeconds, int64(3500))
			re.LessOrEqual(actual[i].TTLSeconds, int64(3600))
		} else {
			re.Equal(int64(math.MaxInt64), actual[i].TTLSeconds)
		}
	}
}

func TestGCState(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()
	cluster, err := pdTests.NewTestCluster(ctx, 1,
		func(conf *config.Config, _ string) {
			conf.Keyspace.WaitRegionSplit = false
		},
	)
	re.NoError(err)
	defer cluster.Destroy()
	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())
	leaderServer := cluster.GetLeaderServer()
	re.NoError(leaderServer.BootstrapCluster())

	keyspaceLevel, err := leaderServer.GetKeyspaceManager().CreateKeyspace(
		&keyspace.CreateKeyspaceRequest{
			Name: "gc_state_ks_level",
			Config: map[string]string{
				keyspace.GCManagementType: keyspace.KeyspaceLevelGC,
			},
			CreateTime: time.Now().Unix(),
		},
	)
	re.NoError(err)

	var unifiedKeyspaceID uint32
	if !kerneltype.IsNextGen() {
		unified, err := leaderServer.GetKeyspaceManager().CreateKeyspace(
			&keyspace.CreateKeyspaceRequest{
				Name: "gc_state_unified",
				Config: map[string]string{
					keyspace.GCManagementType: keyspace.UnifiedGC,
				},
				CreateTime: time.Now().Unix(),
			},
		)
		re.NoError(err)
		unifiedKeyspaceID = unified.Id
	}

	manager := leaderServer.GetServer().GetGCStateManager()
	now := time.Now()
	_, err = manager.AdvanceTxnSafePoint(constant.NullKeyspaceID, 100, now)
	re.NoError(err)
	_, _, err = manager.AdvanceGCSafePoint(constant.NullKeyspaceID, 90)
	re.NoError(err)
	_, err = manager.SetGCBarrier(
		constant.NullKeyspaceID,
		"z-null",
		120,
		time.Hour,
		now,
	)
	re.NoError(err)
	_, err = manager.SetGCBarrier(
		constant.NullKeyspaceID,
		"a-null",
		110,
		time.Duration(math.MaxInt64),
		now,
	)
	re.NoError(err)

	_, err = manager.AdvanceTxnSafePoint(keyspaceLevel.Id, 200, now)
	re.NoError(err)
	_, _, err = manager.AdvanceGCSafePoint(keyspaceLevel.Id, 190)
	re.NoError(err)
	_, err = manager.SetGCBarrier(
		keyspaceLevel.Id,
		"z-local",
		220,
		time.Hour,
		now,
	)
	re.NoError(err)
	_, err = manager.SetGCBarrier(
		keyspaceLevel.Id,
		"a-local",
		210,
		time.Duration(math.MaxInt64),
		now,
	)
	re.NoError(err)
	// Backdate the creation time so the barrier stays persisted until the next
	// safe-point advancement but is already inactive when the RPC reads it.
	_, err = manager.SetGCBarrier(
		keyspaceLevel.Id,
		"expired-local",
		230,
		time.Hour,
		now.Add(-2*time.Hour),
	)
	re.NoError(err)

	_, err = manager.SetGlobalGCBarrier(
		ctx,
		"z-global",
		320,
		time.Duration(math.MaxInt64),
		now,
	)
	re.NoError(err)
	_, err = manager.SetGlobalGCBarrier(
		ctx,
		"a-global",
		310,
		time.Duration(math.MaxInt64),
		now,
	)
	re.NoError(err)
	_, err = manager.SetGlobalGCBarrier(
		ctx,
		"expired-global",
		330,
		time.Hour,
		now.Add(-2*time.Hour),
	)
	re.NoError(err)

	pdAddr := cluster.GetConfig().GetClientURL()
	keyspaceLevelID := strconv.FormatUint(uint64(keyspaceLevel.Id), 10)
	output, err := tests.ExecuteCommand(
		ctl.GetRootCmd(), "-u", pdAddr, "gc-state", "keyspace", keyspaceLevelID,
	)
	re.NoError(err)
	var singleProperties map[string]json.RawMessage
	re.NoError(json.Unmarshal(output, &singleProperties), string(output))
	re.NotContains(singleProperties, "global_gc_barriers")
	var keyspaceLevelResponse gcStateCommandSingle
	re.NoError(json.Unmarshal(output, &keyspaceLevelResponse), string(output))
	re.Equal(keyspaceLevel.Id, keyspaceLevelResponse.RequestedKeyspaceID)
	re.Equal(keyspaceLevel.Id, keyspaceLevelResponse.EffectiveKeyspaceID)
	re.True(keyspaceLevelResponse.IsKeyspaceLevelGC)
	re.Equal(uint64(200), keyspaceLevelResponse.TxnSafePoint)
	re.Equal(uint64(190), keyspaceLevelResponse.GCSafePoint)
	requireGCStateCommandBarriers(re, keyspaceLevelResponse.GCBarriers, []expectedGCStateCommandBarrier{
		{barrierID: "a-local", barrierTS: 210},
		{barrierID: "z-local", barrierTS: 220, expires: true},
	})

	output, err = tests.ExecuteCommand(
		ctl.GetRootCmd(), "-u", pdAddr, "gc-state", "keyspace", keyspaceLevelID,
		"--include-expired",
	)
	re.NoError(err)
	var keyspaceLevelWithExpired gcStateCommandSingle
	re.NoError(json.Unmarshal(output, &keyspaceLevelWithExpired), string(output))
	requireGCStateCommandBarriers(re, keyspaceLevelWithExpired.GCBarriers, []expectedGCStateCommandBarrier{
		{barrierID: "a-local", barrierTS: 210},
		{barrierID: "z-local", barrierTS: 220, expires: true},
		{barrierID: "expired-local", barrierTS: 230, expired: true},
	})

	output, err = tests.ExecuteCommand(
		ctl.GetRootCmd(), "-u", pdAddr, "gc-state", "keyspace", "4294967295",
	)
	re.NoError(err)
	var nullKeyspaceResponse gcStateCommandSingle
	re.NoError(json.Unmarshal(output, &nullKeyspaceResponse), string(output))
	re.Equal(constant.NullKeyspaceID, nullKeyspaceResponse.RequestedKeyspaceID)
	re.Equal(constant.NullKeyspaceID, nullKeyspaceResponse.EffectiveKeyspaceID)
	re.False(nullKeyspaceResponse.IsKeyspaceLevelGC)
	re.Equal(uint64(100), nullKeyspaceResponse.TxnSafePoint)
	re.Equal(uint64(90), nullKeyspaceResponse.GCSafePoint)
	requireGCStateCommandBarriers(re, nullKeyspaceResponse.GCBarriers, []expectedGCStateCommandBarrier{
		{barrierID: "a-null", barrierTS: 110},
		{barrierID: "z-null", barrierTS: 120, expires: true},
	})

	_, err = tests.ExecuteCommand(
		ctl.GetRootCmd(), "-u", pdAddr, "gc-state", "keyspace", "16770000",
	)
	re.ErrorContains(err, "failed to get GC state for keyspace 16770000")

	output, err = tests.ExecuteCommand(ctl.GetRootCmd(), "-u", pdAddr, "gc-state", "all")
	re.NoError(err)
	re.Equal(1, bytes.Count(output, []byte(`"global_gc_barriers"`)), string(output))
	var all gcStateCommandAll
	re.NoError(json.Unmarshal(output, &all), string(output))
	for i := 1; i < len(all.GCStates); i++ {
		re.Less(all.GCStates[i-1].KeyspaceID, all.GCStates[i].KeyspaceID)
	}
	statesByID := make(map[uint32]gcStateCommandState, len(all.GCStates))
	for _, state := range all.GCStates {
		re.NotNil(state.GCBarriers)
		statesByID[state.KeyspaceID] = state
	}

	nullState, ok := statesByID[constant.NullKeyspaceID]
	re.True(ok)
	re.False(nullState.IsKeyspaceLevelGC)
	re.Equal(uint64(100), nullState.TxnSafePoint)
	re.Equal(uint64(90), nullState.GCSafePoint)
	requireGCStateCommandBarriers(re, nullState.GCBarriers, []expectedGCStateCommandBarrier{
		{barrierID: "a-null", barrierTS: 110},
		{barrierID: "z-null", barrierTS: 120, expires: true},
	})

	keyspaceLevelState, ok := statesByID[keyspaceLevel.Id]
	re.True(ok)
	re.True(keyspaceLevelState.IsKeyspaceLevelGC)
	re.Equal(uint64(200), keyspaceLevelState.TxnSafePoint)
	re.Equal(uint64(190), keyspaceLevelState.GCSafePoint)
	requireGCStateCommandBarriers(re, keyspaceLevelState.GCBarriers, []expectedGCStateCommandBarrier{
		{barrierID: "a-local", barrierTS: 210},
		{barrierID: "z-local", barrierTS: 220, expires: true},
	})
	requireGCStateCommandBarriers(re, all.GlobalGCBarriers, []expectedGCStateCommandBarrier{
		{barrierID: "a-global", barrierTS: 310},
		{barrierID: "z-global", barrierTS: 320},
	})

	output, err = tests.ExecuteCommand(
		ctl.GetRootCmd(), "-u", pdAddr, "gc-state", "all", "--include-expired",
	)
	re.NoError(err)
	var allWithExpired gcStateCommandAll
	re.NoError(json.Unmarshal(output, &allWithExpired), string(output))
	statesByIDWithExpired := make(map[uint32]gcStateCommandState, len(allWithExpired.GCStates))
	for _, state := range allWithExpired.GCStates {
		statesByIDWithExpired[state.KeyspaceID] = state
	}
	keyspaceLevelStateWithExpired, ok := statesByIDWithExpired[keyspaceLevel.Id]
	re.True(ok)
	requireGCStateCommandBarriers(re, keyspaceLevelStateWithExpired.GCBarriers, []expectedGCStateCommandBarrier{
		{barrierID: "a-local", barrierTS: 210},
		{barrierID: "z-local", barrierTS: 220, expires: true},
		{barrierID: "expired-local", barrierTS: 230, expired: true},
	})
	requireGCStateCommandBarriers(re, allWithExpired.GlobalGCBarriers, []expectedGCStateCommandBarrier{
		{barrierID: "a-global", barrierTS: 310},
		{barrierID: "z-global", barrierTS: 320},
		{barrierID: "expired-global", barrierTS: 330, expired: true},
	})

	output, err = tests.ExecuteCommand(
		ctl.GetRootCmd(),
		"-u",
		pdAddr,
		"gc-state",
		"global",
	)
	re.NoError(err)
	var globalProperties map[string]json.RawMessage
	re.NoError(json.Unmarshal(output, &globalProperties), string(output))
	re.Len(globalProperties, 1)
	re.Contains(globalProperties, "global_gc_barriers")
	re.NotContains(globalProperties, "gc_states")
	re.NotContains(globalProperties, "txn_safe_point")
	re.NotContains(globalProperties, "gc_safe_point")

	var global gcStateCommandGlobal
	re.NoError(json.Unmarshal(output, &global), string(output))
	re.NotNil(global.GlobalGCBarriers)
	re.Equal(all.GlobalGCBarriers, global.GlobalGCBarriers)

	output, err = tests.ExecuteCommand(
		ctl.GetRootCmd(), "-u", pdAddr, "gc-state", "global", "--include-expired",
	)
	re.NoError(err)
	var globalWithExpired gcStateCommandGlobal
	re.NoError(json.Unmarshal(output, &globalWithExpired), string(output))
	requireGCStateCommandBarriers(re, globalWithExpired.GlobalGCBarriers, []expectedGCStateCommandBarrier{
		{barrierID: "a-global", barrierTS: 310},
		{barrierID: "z-global", barrierTS: 320},
		{barrierID: "expired-global", barrierTS: 330, expired: true},
	})

	if kerneltype.IsNextGen() {
		systemState, ok := statesByID[constant.SystemKeyspaceID]
		re.True(ok)
		re.True(systemState.IsKeyspaceLevelGC)
	} else {
		unifiedKeyspaceIDString := strconv.FormatUint(uint64(unifiedKeyspaceID), 10)
		output, err = tests.ExecuteCommand(
			ctl.GetRootCmd(), "-u", pdAddr, "gc-state", "keyspace", unifiedKeyspaceIDString,
		)
		re.NoError(err)
		var unifiedKeyspaceResponse gcStateCommandSingle
		re.NoError(json.Unmarshal(output, &unifiedKeyspaceResponse), string(output))
		re.Equal(unifiedKeyspaceID, unifiedKeyspaceResponse.RequestedKeyspaceID)
		re.Equal(constant.NullKeyspaceID, unifiedKeyspaceResponse.EffectiveKeyspaceID)
		re.False(unifiedKeyspaceResponse.IsKeyspaceLevelGC)
		re.Equal(uint64(100), unifiedKeyspaceResponse.TxnSafePoint)
		re.Equal(uint64(90), unifiedKeyspaceResponse.GCSafePoint)
		requireGCStateCommandBarriers(re, unifiedKeyspaceResponse.GCBarriers, []expectedGCStateCommandBarrier{
			{barrierID: "a-null", barrierTS: 110},
			{barrierID: "z-null", barrierTS: 120, expires: true},
		})

		re.NotContains(statesByID, unifiedKeyspaceID)
	}

	output, err = tests.ExecuteCommand(ctl.GetRootCmd(), "-u", pdAddr, "service-gc-safepoint")
	re.NoError(err)
	re.True(json.Valid(output), string(output))
}
